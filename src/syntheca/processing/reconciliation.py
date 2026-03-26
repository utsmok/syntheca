"""Multi-source reconciliation for works, persons and organizations.

This module provides the explicit reconciliation step described in T010 of the
Syntheca metadata suite plan.  It takes canonical records from multiple sources,
matches them using a priority-ordered set of strategies (DOI → title fuzzy →
ORCID → name fuzzy), applies the field-level precedence rules from T002
(``config.source_precedence``), and returns merged canonical records alongside
a full audit trail of match decisions.

Design decisions
----------------
* Every match decision is recorded as a :class:`MatchResult` so that
  downstream consumers can inspect provenance and confidence.
* Matching is performed on in-memory Python lists of canonical models.  For
  UT-scale datasets (~10k works) this is efficient enough; Polars-level
  vectorisation is reserved for the aggregation/merge step inside
  ``merging.py``.
* The module deliberately does *not* mutate the original records — it returns
  new merged instances.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from Levenshtein import ratio
from loguru import logger
from pydantic import BaseModel, Field

from syntheca.config.source_precedence import (
    FieldPrecedence,
    Source,
)
from syntheca.models.canonical import (
    CanonicalOrganization,
    CanonicalPerson,
    CanonicalWork,
    SourceAssertion,
)

# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class ReconciliationError(Exception):
    """Typed error for reconciliation failures."""

    def __init__(  # noqa: D107
        self, message: str, *, entity_type: str = "", context: dict[str, Any] | None = None
    ) -> None:
        self.entity_type = entity_type
        self.context = context or {}
        super().__init__(message)


# ---------------------------------------------------------------------------
# Match result model
# ---------------------------------------------------------------------------


class MatchResult(BaseModel):
    """Audit record for a single match decision."""

    source_a: Source
    source_b: Source
    entity_type: str  # "work", "person", "organization"
    id_a: str
    id_b: str
    match_strategy: str  # "doi", "title_fuzzy", "orcid", "name_fuzzy"
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str
    accepted: bool


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


@dataclass
class ReconciliationMetrics:
    """Counters produced by a reconciliation run."""

    entity_type: str = ""
    total_input: int = 0
    matched: int = 0
    unmatched: int = 0
    conflicts: int = 0
    errors: int = 0
    match_results: list[MatchResult] = field(default_factory=list)

    @property
    def summary(self) -> str:
        """Human-readable summary of reconciliation metrics."""
        return (
            f"{self.entity_type}: {self.total_input} input, "
            f"{self.matched} matched, {self.unmatched} unmatched, "
            f"{self.conflicts} conflicts, {self.errors} errors"
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_DOI_CONFIDENCE = 0.99
_TITLE_FUZZY_THRESHOLD = 0.85
_ORCID_CONFIDENCE = 0.98
_NAME_FUZZY_THRESHOLD = 0.80


def _normalize_doi(doi: str | None) -> str | None:
    """Lowercase, strip whitespace and common prefixes."""
    if not doi:
        return None
    d = doi.strip().lower()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if d.startswith(prefix):
            d = d[len(prefix) :]
    d = d.strip()
    return d if d else None


def _source_for_record(
    record: CanonicalWork | CanonicalPerson | CanonicalOrganization,
) -> Source:
    """Best-effort extraction of the primary source from a canonical record."""
    if record.provenance:
        return record.provenance[0].source
    return Source.MANUAL


def _get_provenance_value(
    record: CanonicalWork | CanonicalPerson | CanonicalOrganization,
    field_name: str,
) -> SourceAssertion | None:
    """Return the first provenance assertion for *field_name*, if any."""
    for a in record.provenance:
        if a.field_name == field_name:
            return a
    return None


# ---------------------------------------------------------------------------
# Field precedence helpers
# ---------------------------------------------------------------------------

# Maps FieldPrecedence members to tuples of canonical field names they govern.
_PRECEDENCE_FIELD_MAP: dict[FieldPrecedence, tuple[str, ...]] = {
    FieldPrecedence.OA_STATUS: ("is_oa", "oa_color"),
    FieldPrecedence.ACCESS_RIGHT: ("access_right",),
    FieldPrecedence.LICENSE: ("license",),
    FieldPrecedence.PUBLISHER_NAME: ("publisher", "primary_host_name"),
    FieldPrecedence.CITATION_METRICS: ("cited_by_count", "fwci"),
    FieldPrecedence.PUBLICATION_TYPE: ("type",),
    FieldPrecedence.ABSTRACT: ("abstract",),
    FieldPrecedence.KEYWORDS: ("keywords",),
}


def _pick_field_value(
    field_name: str,
    records_by_source: dict[Source, CanonicalWork],
    precedence: FieldPrecedence,
) -> Any:
    """Select the best value for *field_name* according to *precedence*.

    Returns the value from the preferred source if non-None, otherwise
    falls back.
    """
    preferred = precedence.preferred_source
    fallback = precedence.fallback_source

    # Check preferred source first
    if preferred in records_by_source:
        val = getattr(records_by_source[preferred], field_name, None)
        if val is not None:
            return val

    # Try fallback
    if fallback and fallback in records_by_source:
        val = getattr(records_by_source[fallback], field_name, None)
        if val is not None:
            return val

    # Fall through to any source that has a value
    for rec in records_by_source.values():
        val = getattr(rec, field_name, None)
        if val is not None:
            return val
    return None


# ---------------------------------------------------------------------------
# Work reconciliation
# ---------------------------------------------------------------------------


def reconcile_works(
    sources: dict[Source, list[CanonicalWork]],
    precedence: type[FieldPrecedence] = FieldPrecedence,
) -> tuple[list[CanonicalWork], list[MatchResult], ReconciliationMetrics]:
    """Reconcile work records from multiple sources.

    Strategy:
    1. Build a DOI index and match by normalised DOI (highest confidence).
    2. For remaining unmatched records, attempt title fuzzy matching.
    3. For each matched group, apply field-level precedence rules.

    Returns:
        A tuple of (merged works, match audit trail, metrics).

    Raises:
        ReconciliationError: on structural issues that prevent reconciliation.
    """
    metrics = ReconciliationMetrics(entity_type="work")
    match_results: list[MatchResult] = []

    # Flatten all records with source tags
    tagged: list[tuple[Source, CanonicalWork]] = []
    for src, works in sources.items():
        metrics.total_input += len(works)
        for w in works:
            tagged.append((src, w))

    if not tagged:
        return [], match_results, metrics

    # ---- Phase 1: DOI matching ----
    doi_index: dict[str, list[tuple[Source, CanonicalWork]]] = {}
    no_doi: list[tuple[Source, CanonicalWork]] = []

    for src, work in tagged:
        ndoi = _normalize_doi(work.doi)
        if ndoi:
            doi_index.setdefault(ndoi, []).append((src, work))
        else:
            no_doi.append((src, work))

    matched_groups: list[dict[Source, CanonicalWork]] = []
    consumed: set[int] = set()  # ids of records already grouped

    for ndoi, group in doi_index.items():
        if len(group) >= 2:
            group_dict: dict[Source, CanonicalWork] = {}
            first_src, first_work = group[0]
            group_dict[first_src] = first_work
            consumed.add(id(first_work))

            for src, work in group[1:]:
                consumed.add(id(work))
                mr = MatchResult(
                    source_a=first_src,
                    source_b=src,
                    entity_type="work",
                    id_a=first_work.internal_id,
                    id_b=work.internal_id,
                    match_strategy="doi",
                    confidence=_DOI_CONFIDENCE,
                    reason=f"DOI match: {ndoi}",
                    accepted=True,
                )
                match_results.append(mr)
                metrics.matched += 1
                # If same source appears twice, keep first (dedup within source)
                if src not in group_dict:
                    group_dict[src] = work
            matched_groups.append(group_dict)
        else:
            # Single source for this DOI — mark consumed so it goes to output
            consumed.add(id(group[0][1]))
            matched_groups.append({group[0][0]: group[0][1]})

    # ---- Phase 2: Title fuzzy matching for no-DOI records ----
    unmatched_no_doi: list[tuple[Source, CanonicalWork]] = list(no_doi)
    # Also include DOI-bearing records from single-source groups that weren't
    # cross-matched (they might match a no-DOI record by title)
    title_candidates: list[tuple[Source, CanonicalWork, int]] = [
        (src, w, idx) for idx, (src, w) in enumerate(unmatched_no_doi)
    ]

    title_consumed: set[int] = set()
    for i, (src_i, w_i, _idx_i) in enumerate(title_candidates):
        if i in title_consumed:
            continue
        title_i = (w_i.title or "").strip().lower()
        if not title_i:
            continue
        for j in range(i + 1, len(title_candidates)):
            if j in title_consumed:
                continue
            src_j, w_j, _idx_j = title_candidates[j]
            if src_j == src_i:
                continue
            title_j = (w_j.title or "").strip().lower()
            if not title_j:
                continue
            score = ratio(title_i, title_j)
            accepted = score >= _TITLE_FUZZY_THRESHOLD
            mr = MatchResult(
                source_a=src_i,
                source_b=src_j,
                entity_type="work",
                id_a=w_i.internal_id,
                id_b=w_j.internal_id,
                match_strategy="title_fuzzy",
                confidence=round(score, 4),
                reason=f"Title fuzzy score={score:.4f} (threshold={_TITLE_FUZZY_THRESHOLD})",
                accepted=accepted,
            )
            match_results.append(mr)
            if accepted:
                # Merge into a group
                merged = False
                for g in matched_groups:
                    if id(w_i) in {id(v) for v in g.values()}:
                        if src_j not in g:
                            g[src_j] = w_j
                        merged = True
                        break
                if not merged:
                    matched_groups.append({src_i: w_i, src_j: w_j})
                title_consumed.add(j)
                metrics.matched += 1

    # Collect singletons (records not part of any multi-source group)
    all_grouped_ids = {id(w) for g in matched_groups for w in g.values()}
    for src, work in tagged:
        if id(work) not in all_grouped_ids:
            matched_groups.append({src: work})
            metrics.unmatched += 1

    # ---- Phase 3: Apply field precedence ----
    merged_works = apply_field_precedence(matched_groups, precedence)
    metrics.match_results = match_results
    logger.info("Work reconciliation: {}", metrics.summary)
    return merged_works, match_results, metrics


# ---------------------------------------------------------------------------
# Field precedence application
# ---------------------------------------------------------------------------


def apply_field_precedence(
    matched_groups: list[dict[Source, CanonicalWork]],
    precedence: type[FieldPrecedence] = FieldPrecedence,
) -> list[CanonicalWork]:
    """Merge each group of matched records using field-level precedence rules.

    For each group:
    - Identity fields (internal_id, doi, title) come from Pure if available.
    - Governed fields are selected per the :class:`FieldPrecedence` rulebook.
    - Provenance from all sources is preserved.

    Returns:
        List of merged :class:`CanonicalWork` instances.
    """
    results: list[CanonicalWork] = []
    for group in matched_groups:
        if not group:
            continue
        results.append(_merge_work_group(group, precedence))
    return results


def _merge_work_group(
    group: dict[Source, CanonicalWork],
    precedence: type[FieldPrecedence],
) -> CanonicalWork:
    """Merge a single group of canonical work records."""
    # Pick the base record — prefer Pure, then first available
    base_src = Source.PURE if Source.PURE in group else next(iter(group))
    base = group[base_src]

    # Collect all provenance
    all_provenance: list[SourceAssertion] = []
    for rec in group.values():
        all_provenance.extend(rec.provenance)

    # Merge source_ids
    merged_source_ids: dict[str, str] = {}
    for rec in group.values():
        merged_source_ids.update(rec.source_ids)

    # Identity: prefer Pure
    internal_id = base.internal_id
    doi = base.doi
    title = base.title
    pub_year = base.publication_year
    pub_date = base.publication_date
    language = base.language

    # For identity fields, fill from other sources if the base is empty
    for rec in group.values():
        if not doi and rec.doi:
            doi = rec.doi
        if not title and rec.title:
            title = rec.title
        if pub_year is None and rec.publication_year is not None:
            pub_year = rec.publication_year
        if pub_date is None and rec.publication_date is not None:
            pub_date = rec.publication_date
        if language is None and rec.language is not None:
            language = rec.language

    # Governed fields via precedence
    governed_values: dict[str, Any] = {}
    for prec_rule in precedence:
        field_names = _PRECEDENCE_FIELD_MAP.get(prec_rule, ())
        for fn in field_names:
            governed_values[fn] = _pick_field_value(fn, group, prec_rule)

    # Authors: prefer the longest list (most complete)
    best_authors: list[str] = []
    for rec in group.values():
        if len(rec.authors) > len(best_authors):
            best_authors = list(rec.authors)

    # UT corresponding: any True wins
    ut_corresponding = any(rec.ut_is_corresponding for rec in group.values())

    return CanonicalWork(
        internal_id=internal_id,
        doi=doi,
        title=title,
        publication_year=pub_year,
        publication_date=pub_date,
        type=governed_values.get("type") or base.type,
        language=language,
        authors=best_authors,
        source_ids=merged_source_ids,
        is_oa=governed_values.get("is_oa"),
        oa_color=governed_values.get("oa_color"),
        cited_by_count=governed_values.get("cited_by_count"),
        fwci=governed_values.get("fwci"),
        publisher=governed_values.get("publisher") or base.publisher,
        primary_host_name=governed_values.get("primary_host_name") or base.primary_host_name,
        ut_is_corresponding=ut_corresponding or None,
        access_right=governed_values.get("access_right") or base.access_right,
        license=governed_values.get("license") or base.license,
        keywords=governed_values.get("keywords") or base.keywords,
        abstract=governed_values.get("abstract") or base.abstract,
        provenance=all_provenance,
    )


# ---------------------------------------------------------------------------
# Person reconciliation
# ---------------------------------------------------------------------------


def reconcile_persons(
    sources: dict[Source, list[CanonicalPerson]],
) -> tuple[list[CanonicalPerson], list[MatchResult], ReconciliationMetrics]:
    """Reconcile person records from multiple sources.

    Strategy:
    1. Match by ORCID (high confidence).
    2. Match by name fuzzy (lower confidence).
    Pure ``internal_repository_id`` is authoritative per T002.

    Returns:
        (merged persons, match audit trail, metrics)
    """
    metrics = ReconciliationMetrics(entity_type="person")
    match_results: list[MatchResult] = []

    tagged: list[tuple[Source, CanonicalPerson]] = []
    for src, persons in sources.items():
        metrics.total_input += len(persons)
        for p in persons:
            tagged.append((src, p))

    if not tagged:
        return [], match_results, metrics

    # Phase 1: ORCID matching
    orcid_index: dict[str, list[tuple[Source, CanonicalPerson]]] = {}
    no_orcid: list[tuple[Source, CanonicalPerson]] = []
    for src, person in tagged:
        orcid = (person.orcid or "").strip()
        if orcid:
            orcid_index.setdefault(orcid, []).append((src, person))
        else:
            no_orcid.append((src, person))

    matched_groups: list[dict[Source, CanonicalPerson]] = []
    all_grouped_ids: set[int] = set()

    for orcid, group in orcid_index.items():
        if len(group) >= 2:
            group_dict: dict[Source, CanonicalPerson] = {}
            first_src, first_person = group[0]
            group_dict[first_src] = first_person
            all_grouped_ids.add(id(first_person))

            for src, person in group[1:]:
                all_grouped_ids.add(id(person))
                mr = MatchResult(
                    source_a=first_src,
                    source_b=src,
                    entity_type="person",
                    id_a=first_person.internal_id,
                    id_b=person.internal_id,
                    match_strategy="orcid",
                    confidence=_ORCID_CONFIDENCE,
                    reason=f"ORCID match: {orcid}",
                    accepted=True,
                )
                match_results.append(mr)
                metrics.matched += 1
                if src not in group_dict:
                    group_dict[src] = person
            matched_groups.append(group_dict)
        else:
            all_grouped_ids.add(id(group[0][1]))
            matched_groups.append({group[0][0]: group[0][1]})

    # Phase 2: Name fuzzy for no-ORCID records
    name_candidates = [(src, p) for src, p in no_orcid if id(p) not in all_grouped_ids]
    name_consumed: set[int] = set()

    for i, (src_i, p_i) in enumerate(name_candidates):
        if i in name_consumed:
            continue
        name_i = (p_i.name or "").strip().lower()
        if not name_i:
            continue
        for j in range(i + 1, len(name_candidates)):
            if j in name_consumed:
                continue
            src_j, p_j = name_candidates[j]
            if src_j == src_i:
                continue
            name_j = (p_j.name or "").strip().lower()
            if not name_j:
                continue
            score = ratio(name_i, name_j)
            accepted = score >= _NAME_FUZZY_THRESHOLD
            mr = MatchResult(
                source_a=src_i,
                source_b=src_j,
                entity_type="person",
                id_a=p_i.internal_id,
                id_b=p_j.internal_id,
                match_strategy="name_fuzzy",
                confidence=round(score, 4),
                reason=f"Name fuzzy score={score:.4f} (threshold={_NAME_FUZZY_THRESHOLD})",
                accepted=accepted,
            )
            match_results.append(mr)
            if accepted:
                merged = False
                for g in matched_groups:
                    if id(p_i) in {id(v) for v in g.values()}:
                        if src_j not in g:
                            g[src_j] = p_j
                        merged = True
                        break
                if not merged:
                    matched_groups.append({src_i: p_i, src_j: p_j})
                    all_grouped_ids.add(id(p_i))
                name_consumed.add(j)
                all_grouped_ids.add(id(p_j))
                metrics.matched += 1

    # Singletons
    for src, person in tagged:
        if id(person) not in all_grouped_ids:
            matched_groups.append({src: person})
            metrics.unmatched += 1
            all_grouped_ids.add(id(person))

    merged = [_merge_person_group(g) for g in matched_groups]
    metrics.match_results = match_results
    logger.info("Person reconciliation: {}", metrics.summary)
    return merged, match_results, metrics


def _merge_person_group(group: dict[Source, CanonicalPerson]) -> CanonicalPerson:
    """Merge a group of matched person records.  Pure is authoritative."""
    base_src = Source.PURE if Source.PURE in group else next(iter(group))
    base = group[base_src]

    all_provenance: list[SourceAssertion] = []
    for rec in group.values():
        all_provenance.extend(rec.provenance)

    # Merge identifiers: prefer Pure internal_id
    internal_id = base.internal_id
    name = base.name
    orcid = base.orcid
    scopus_id = base.scopus_author_id

    # Fill from other sources
    for rec in group.values():
        if not orcid and rec.orcid:
            orcid = rec.orcid
        if not scopus_id and rec.scopus_author_id:
            scopus_id = rec.scopus_author_id
        if not name and rec.name:
            name = rec.name

    # Merge affiliations (union)
    all_affiliations: list[dict[str, Any]] = []
    seen: set[str] = set()
    for rec in group.values():
        for aff in rec.affiliations:
            key = str(aff)
            if key not in seen:
                seen.add(key)
                all_affiliations.append(aff)

    return CanonicalPerson(
        internal_id=internal_id,
        name=name,
        orcid=orcid,
        scopus_author_id=scopus_id,
        affiliations=all_affiliations,
        provenance=all_provenance,
    )


# ---------------------------------------------------------------------------
# Organization reconciliation
# ---------------------------------------------------------------------------


def reconcile_organizations(
    sources: dict[Source, list[CanonicalOrganization]],
) -> tuple[list[CanonicalOrganization], list[MatchResult], ReconciliationMetrics]:
    """Reconcile organization records from multiple sources.

    Pure hierarchy is authoritative per T002.  Match by name.

    Returns:
        (merged organizations, match audit trail, metrics)
    """
    metrics = ReconciliationMetrics(entity_type="organization")
    match_results: list[MatchResult] = []

    tagged: list[tuple[Source, CanonicalOrganization]] = []
    for src, orgs in sources.items():
        metrics.total_input += len(orgs)
        for org in orgs:
            tagged.append((src, org))

    if not tagged:
        return [], match_results, metrics

    # Index by normalised name
    name_index: dict[str, list[tuple[Source, CanonicalOrganization]]] = {}
    for src, org in tagged:
        norm_name = (org.name or "").strip().lower()
        if norm_name:
            name_index.setdefault(norm_name, []).append((src, org))

    matched_groups: list[dict[Source, CanonicalOrganization]] = []
    all_grouped_ids: set[int] = set()

    for norm_name, group in name_index.items():
        if len(group) >= 2:
            group_dict: dict[Source, CanonicalOrganization] = {}
            first_src, first_org = group[0]
            group_dict[first_src] = first_org
            all_grouped_ids.add(id(first_org))

            for src, org in group[1:]:
                all_grouped_ids.add(id(org))
                mr = MatchResult(
                    source_a=first_src,
                    source_b=src,
                    entity_type="organization",
                    id_a=first_org.internal_id,
                    id_b=org.internal_id,
                    match_strategy="name_exact",
                    confidence=0.95,
                    reason=f"Exact name match: {norm_name}",
                    accepted=True,
                )
                match_results.append(mr)
                metrics.matched += 1
                if src not in group_dict:
                    group_dict[src] = org
            matched_groups.append(group_dict)
        else:
            all_grouped_ids.add(id(group[0][1]))
            matched_groups.append({group[0][0]: group[0][1]})

    # Singletons
    for src, org in tagged:
        if id(org) not in all_grouped_ids:
            matched_groups.append({src: org})
            metrics.unmatched += 1
            all_grouped_ids.add(id(org))

    merged = [_merge_org_group(g) for g in matched_groups]
    metrics.match_results = match_results
    logger.info("Organization reconciliation: {}", metrics.summary)
    return merged, match_results, metrics


def _merge_org_group(group: dict[Source, CanonicalOrganization]) -> CanonicalOrganization:
    """Merge a group of matched organization records.  Pure is authoritative."""
    base_src = Source.PURE if Source.PURE in group else next(iter(group))
    base = group[base_src]

    all_provenance: list[SourceAssertion] = []
    for rec in group.values():
        all_provenance.extend(rec.provenance)

    return CanonicalOrganization(
        internal_id=base.internal_id,
        name=base.name,
        type=base.type or next((r.type for r in group.values() if r.type), None),
        parent_id=base.parent_id
        or next((r.parent_id for r in group.values() if r.parent_id), None),
        provenance=all_provenance,
    )
