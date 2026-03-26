"""Adapter functions converting source-specific records to canonical form.

Each adapter:
1. Maps source-specific field names to canonical field names.
2. Attaches :class:`~syntheca.models.canonical.SourceAssertion` provenance
   for every field populated from the source.
3. Returns a typed canonical model instance.

The adapters accept either raw ``dict`` records (Pure OAI / UT People) or
typed dataclass instances (OpenAlex ``Work``) or typed Pydantic models
(OpenAIRE ``OpenAIREResearchProduct`` / ``OpenAIREOrganization``).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from syntheca.config.source_precedence import Source
from syntheca.models.canonical import (
    CanonicalOrganization,
    CanonicalPerson,
    CanonicalWork,
    SourceAssertion,
)
from syntheca.models.openaire import OpenAIREOrganization, OpenAIREResearchProduct


def _assertion(source: Source, field_name: str, value: Any) -> SourceAssertion:
    """Build a :class:`SourceAssertion` with a UTC timestamp."""
    return SourceAssertion(
        source=source,
        field_name=field_name,
        value=value,
        timestamp=datetime.now(UTC),
    )


def _assertions_for(source: Source, mapping: dict[str, Any]) -> list[SourceAssertion]:
    """Create assertions for every non-None value in *mapping*."""
    return [_assertion(source, k, v) for k, v in mapping.items() if v is not None]


# ---------------------------------------------------------------------------
# Pure publication → CanonicalWork
# ---------------------------------------------------------------------------


def pure_publication_to_canonical(record: dict[str, Any]) -> CanonicalWork:
    """Convert a parsed Pure OAI publication dict to a :class:`CanonicalWork`.

    The *record* dict is expected to have the shape produced by
    :meth:`PureOAIClient._parse_publication`.
    """
    pub_date = record.get("publication_date")
    pub_year: int | None = None
    if pub_date:
        try:
            pub_year = int(str(pub_date)[:4])
        except ValueError, TypeError:
            pub_year = None

    # Extract flat author names
    raw_authors = record.get("authors") or []
    author_names: list[str] = []
    for a in raw_authors:
        if isinstance(a, dict):
            parts = [a.get("first_names") or "", a.get("family_names") or ""]
            name = " ".join(p for p in parts if p).strip()
            if name:
                author_names.append(name)
        elif isinstance(a, str):
            author_names.append(a)

    internal_id = record.get("id") or ""
    doi = record.get("doi")
    title = record.get("title") or ""

    field_map: dict[str, Any] = {
        "internal_id": internal_id,
        "doi": doi,
        "title": title,
        "publication_year": pub_year,
        "publication_date": pub_date,
        "type": record.get("type"),
        "language": record.get("language"),
        "authors": author_names or None,
        "access_right": record.get("access_right"),
        "license": record.get("license"),
        "publisher": record.get("publisher_name"),
        "abstract": record.get("abstract"),
        "keywords": record.get("keywords") or None,
    }

    source_ids = {"pure": internal_id} if internal_id else {}

    return CanonicalWork(
        internal_id=internal_id,
        doi=doi,
        title=title,
        publication_year=pub_year,
        publication_date=pub_date,
        type=field_map["type"],
        language=field_map["language"],
        authors=author_names,
        source_ids=source_ids,
        access_right=field_map["access_right"],
        license=field_map["license"],
        publisher=field_map["publisher"],
        abstract=field_map["abstract"],
        keywords=field_map.get("keywords") or [],
        provenance=_assertions_for(Source.PURE, field_map),
    )


# ---------------------------------------------------------------------------
# OpenAlex Work → CanonicalWork
# ---------------------------------------------------------------------------


def openalex_work_to_canonical(work: Any) -> CanonicalWork:
    """Convert an OpenAlex :class:`~syntheca.models.openalex.Work` to a :class:`CanonicalWork`.

    Accepts either a typed ``Work`` dataclass or a plain ``dict`` (e.g. from
    ``dataclasses.asdict``).
    """
    if isinstance(work, dict):
        return _openalex_dict_to_canonical(work)

    # Typed Work dataclass
    openalex_id = getattr(work, "id", None) or ""
    doi = getattr(work, "doi", None)
    title = getattr(work, "title", None) or getattr(work, "display_name", None) or ""
    pub_year = getattr(work, "publication_year", None)
    pub_date = getattr(work, "publication_date", None)
    work_type = getattr(work, "type", None)
    language = getattr(work, "language", None)

    # Author names from authorships
    author_names: list[str] = []
    for authorship in getattr(work, "authorships", None) or []:
        if authorship is None:
            continue
        raw_name = getattr(authorship, "raw_author_name", None)
        if raw_name:
            author_names.append(raw_name)
        elif hasattr(authorship, "author") and authorship.author:
            dn = getattr(authorship.author, "display_name", None)
            if dn:
                author_names.append(dn)

    # OA fields
    oa = getattr(work, "open_access", None)
    is_oa = getattr(oa, "is_oa", None) if oa else None
    oa_color = getattr(oa, "oa_status", None) if oa else None

    cited_by_count = getattr(work, "cited_by_count", None)
    fwci = getattr(work, "fwci", None)

    # Publisher / venue from primary_location
    publisher = None
    primary_host_name = None
    primary_loc = getattr(work, "primary_location", None)
    if primary_loc:
        src = getattr(primary_loc, "source", None)
        if src:
            primary_host_name = getattr(src, "display_name", None)
            publisher = getattr(src, "host_organization_name", None)

    # Keywords
    keywords: list[str] = []
    for kw in getattr(work, "keywords", None) or []:
        if kw is None:
            continue
        dn = getattr(kw, "display_name", None)
        if dn:
            keywords.append(dn)

    # UT corresponding
    ut_corresponding = _check_ut_corresponding(work)

    source_ids: dict[str, str] = {}
    if openalex_id:
        source_ids["openalex"] = openalex_id
    if doi:
        source_ids["doi"] = doi

    field_map: dict[str, Any] = {
        "internal_id": openalex_id,
        "doi": doi,
        "title": title,
        "publication_year": pub_year,
        "publication_date": pub_date,
        "type": work_type,
        "language": language,
        "authors": author_names or None,
        "is_oa": is_oa,
        "oa_color": oa_color,
        "cited_by_count": cited_by_count,
        "fwci": fwci,
        "publisher": publisher,
        "primary_host_name": primary_host_name,
        "keywords": keywords or None,
        "ut_is_corresponding": ut_corresponding,
    }

    return CanonicalWork(
        internal_id=openalex_id,
        doi=doi,
        title=title,
        publication_year=pub_year,
        publication_date=pub_date,
        type=str(work_type) if work_type else None,
        language=language,
        authors=author_names,
        source_ids=source_ids,
        is_oa=is_oa,
        oa_color=oa_color,
        cited_by_count=cited_by_count,
        fwci=fwci,
        publisher=publisher,
        primary_host_name=primary_host_name,
        keywords=keywords,
        ut_is_corresponding=ut_corresponding,
        provenance=_assertions_for(Source.OPENALEX, field_map),
    )


def _openalex_dict_to_canonical(d: dict[str, Any]) -> CanonicalWork:
    """Adapter for dict-form OpenAlex work (e.g. ``dataclasses.asdict(work)``)."""
    openalex_id = d.get("id") or ""
    doi = d.get("doi")
    title = d.get("title") or d.get("display_name") or ""
    pub_year = d.get("publication_year")
    pub_date = d.get("publication_date")
    work_type = d.get("type")
    language = d.get("language")

    author_names: list[str] = []
    for authorship in d.get("authorships") or []:
        if not authorship:
            continue
        raw_name = authorship.get("raw_author_name")
        if raw_name:
            author_names.append(raw_name)
        else:
            author = authorship.get("author") or {}
            dn = author.get("display_name")
            if dn:
                author_names.append(dn)

    oa = d.get("open_access") or {}
    is_oa = oa.get("is_oa")
    oa_color = oa.get("oa_status")

    cited_by_count = d.get("cited_by_count")
    fwci = d.get("fwci")

    publisher = None
    primary_host_name = None
    primary_loc = d.get("primary_location") or {}
    src = primary_loc.get("source") or {}
    if src:
        primary_host_name = src.get("display_name")
        publisher = src.get("host_organization_name")

    keywords: list[str] = []
    for kw in d.get("keywords") or []:
        if not kw:
            continue
        dn = kw.get("display_name") if isinstance(kw, dict) else None
        if dn:
            keywords.append(dn)

    source_ids: dict[str, str] = {}
    if openalex_id:
        source_ids["openalex"] = openalex_id
    if doi:
        source_ids["doi"] = doi

    field_map: dict[str, Any] = {
        "internal_id": openalex_id,
        "doi": doi,
        "title": title,
        "publication_year": pub_year,
        "publication_date": pub_date,
        "type": work_type,
        "language": language,
        "authors": author_names or None,
        "is_oa": is_oa,
        "oa_color": oa_color,
        "cited_by_count": cited_by_count,
        "fwci": fwci,
        "publisher": publisher,
        "primary_host_name": primary_host_name,
        "keywords": keywords or None,
    }

    return CanonicalWork(
        internal_id=openalex_id,
        doi=doi,
        title=title,
        publication_year=pub_year,
        publication_date=pub_date,
        type=str(work_type) if work_type else None,
        language=language,
        authors=author_names,
        source_ids=source_ids,
        is_oa=is_oa,
        oa_color=oa_color,
        cited_by_count=cited_by_count,
        fwci=fwci,
        publisher=publisher,
        primary_host_name=primary_host_name,
        keywords=keywords,
        provenance=_assertions_for(Source.OPENALEX, field_map),
    )


def _check_ut_corresponding(work: Any) -> bool | None:
    """Check if University of Twente is a corresponding institution.

    Returns ``True`` when a UT-related institution ID appears in the work's
    ``corresponding_institution_ids``, ``False`` if the field exists but UT
    is not listed, or ``None`` if the data is not available.
    """
    ids = getattr(work, "corresponding_institution_ids", None) or []
    if not ids:
        return None
    # OpenAlex ID for University of Twente
    ut_oa_id = "https://openalex.org/I121955964"
    return ut_oa_id in ids


# ---------------------------------------------------------------------------
# Pure person → CanonicalPerson
# ---------------------------------------------------------------------------


def pure_person_to_canonical(record: dict[str, Any]) -> CanonicalPerson:
    """Convert a parsed Pure OAI person dict to a :class:`CanonicalPerson`.

    The *record* dict is expected to have the shape produced by
    :meth:`PureOAIClient._parse_person`.
    """
    internal_id = record.get("id") or ""
    first = record.get("first_names") or ""
    family = record.get("family_names") or ""
    name = f"{first} {family}".strip()

    field_map: dict[str, Any] = {
        "internal_id": internal_id,
        "name": name,
        "orcid": record.get("orcid"),
        "scopus_author_id": record.get("scopus_author_id"),
        "affiliations": record.get("affiliations") or None,
    }

    return CanonicalPerson(
        internal_id=internal_id,
        name=name,
        orcid=record.get("orcid"),
        scopus_author_id=record.get("scopus_author_id"),
        affiliations=record.get("affiliations") or [],
        provenance=_assertions_for(Source.PURE, field_map),
    )


# ---------------------------------------------------------------------------
# Pure org-unit → CanonicalOrganization
# ---------------------------------------------------------------------------


def pure_orgunit_to_canonical(record: dict[str, Any]) -> CanonicalOrganization:
    """Convert a parsed Pure OAI org-unit dict to a :class:`CanonicalOrganization`.

    The *record* dict is expected to have the shape produced by
    :meth:`PureOAIClient._parse_orgunit`.
    """
    internal_id = record.get("id") or ""
    name = record.get("name") or ""
    org_type = record.get("type")
    parent_id = record.get("part_of_org_id")

    field_map: dict[str, Any] = {
        "internal_id": internal_id,
        "name": name,
        "type": org_type,
        "parent_id": parent_id,
    }

    return CanonicalOrganization(
        internal_id=internal_id,
        name=name,
        type=org_type,
        parent_id=parent_id,
        provenance=_assertions_for(Source.PURE, field_map),
    )


# ---------------------------------------------------------------------------
# OpenAIRE research product → CanonicalWork
# ---------------------------------------------------------------------------


def openaire_product_to_canonical(
    product: dict[str, Any] | OpenAIREResearchProduct,
) -> CanonicalWork:
    """Convert an OpenAIRE Graph research product to :class:`CanonicalWork`.

    Accepts either a raw ``dict`` straight from the API or a parsed
    :class:`~syntheca.models.openaire.OpenAIREResearchProduct`.
    """
    if isinstance(product, dict):
        product = OpenAIREResearchProduct.model_validate(product)

    openaire_id = product.id or ""
    title = product.main_title or ""

    # Extract DOI from pids list
    doi: str | None = None
    for pid in product.pids:
        if pid.scheme and pid.scheme.lower() == "doi" and pid.value:
            doi = pid.value
            break

    pub_date = product.publication_date
    pub_year: int | None = None
    if pub_date:
        try:
            pub_year = int(str(pub_date)[:4])
        except ValueError, TypeError:
            pub_year = None

    language = product.language.label if product.language else None

    # Author names
    author_names: list[str] = []
    for author in product.authors:
        if author.full_name:
            author_names.append(author.full_name)

    # OA
    is_oa: bool | None = None
    oa_color = product.open_access_color
    if product.best_access_right:
        label = (product.best_access_right.label or "").upper()
        is_oa = label == "OPEN"

    # Publisher / venue
    publisher = product.publisher
    primary_host_name: str | None = None
    if product.container:
        primary_host_name = product.container.name

    # Keywords from subjects
    keywords: list[str] = []
    for subj in product.subjects:
        if subj.subject and subj.subject.value:
            keywords.append(subj.subject.value)

    # Citation count from BIP indicators
    cited_by_count: int | None = None
    if product.indicators and product.indicators.bip_indicators:
        raw_cc = product.indicators.bip_indicators.citation_count
        if raw_cc is not None:
            cited_by_count = int(raw_cc)

    # Abstract (first description)
    abstract = product.descriptions[0] if product.descriptions else None

    source_ids: dict[str, str] = {}
    if openaire_id:
        source_ids["openaire"] = openaire_id
    if doi:
        source_ids["doi"] = doi

    field_map: dict[str, Any] = {
        "internal_id": openaire_id,
        "doi": doi,
        "title": title,
        "publication_year": pub_year,
        "publication_date": pub_date,
        "type": product.type,
        "language": language,
        "authors": author_names or None,
        "is_oa": is_oa,
        "oa_color": oa_color,
        "cited_by_count": cited_by_count,
        "publisher": publisher,
        "primary_host_name": primary_host_name,
        "keywords": keywords or None,
        "abstract": abstract,
    }

    return CanonicalWork(
        internal_id=openaire_id,
        doi=doi,
        title=title,
        publication_year=pub_year,
        publication_date=pub_date,
        type=product.type,
        language=language,
        authors=author_names,
        source_ids=source_ids,
        is_oa=is_oa,
        oa_color=oa_color,
        cited_by_count=cited_by_count,
        publisher=publisher,
        primary_host_name=primary_host_name,
        keywords=keywords,
        abstract=abstract,
        provenance=_assertions_for(Source.OPENAIRE, field_map),
    )


# ---------------------------------------------------------------------------
# OpenAIRE organization → CanonicalOrganization
# ---------------------------------------------------------------------------


def openaire_org_to_canonical(
    org: dict[str, Any] | OpenAIREOrganization,
) -> CanonicalOrganization:
    """Convert an OpenAIRE Graph organization to :class:`CanonicalOrganization`.

    Accepts either a raw ``dict`` or a parsed
    :class:`~syntheca.models.openaire.OpenAIREOrganization`.
    """
    if isinstance(org, dict):
        org = OpenAIREOrganization.model_validate(org)

    internal_id = org.id or ""
    name = org.legal_name or org.legal_short_name or ""
    org_type: str | None = None  # Graph API orgs don't carry an explicit type field
    # OpenAIRE orgs are flat — no parent hierarchy exposed
    parent_id: str | None = None

    field_map: dict[str, Any] = {
        "internal_id": internal_id,
        "name": name,
        "type": org_type,
        "parent_id": parent_id,
    }

    return CanonicalOrganization(
        internal_id=internal_id,
        name=name,
        type=org_type,
        parent_id=parent_id,
        provenance=_assertions_for(Source.OPENAIRE, field_map),
    )
