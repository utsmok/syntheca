"""Co-authorship analysis for canonical work records.

This module builds publication-to-author link tables, co-author edge tables,
and collaboration rollups from reconciled canonical data.  All outputs are
Polars DataFrames with stable, documented schemas suitable for downstream
reporting and export.

Typical usage::

    from syntheca.analysis.coauthorship import generate_coauthorship_report

    report = generate_coauthorship_report(works, persons=persons, organizations=orgs)
    report.coauthor_edges  # Polars DataFrame of co-author pairs
"""

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import combinations

import polars as pl

from syntheca.models.canonical import (
    CanonicalOrganization,
    CanonicalPerson,
    CanonicalWork,
)

# ---------------------------------------------------------------------------
# Schema constants
# ---------------------------------------------------------------------------

LINK_SCHEMA = {
    "work_id": pl.Utf8,
    "work_doi": pl.Utf8,
    "work_title": pl.Utf8,
    "author_name": pl.Utf8,
    "author_orcid": pl.Utf8,
    "author_internal_id": pl.Utf8,
    "author_position": pl.Utf8,
}

EDGE_SCHEMA = {
    "author_a_name": pl.Utf8,
    "author_a_id": pl.Utf8,
    "author_b_name": pl.Utf8,
    "author_b_id": pl.Utf8,
    "shared_works_count": pl.UInt32,
    "shared_work_ids": pl.List(pl.Utf8),
}


def _empty_link_df() -> pl.DataFrame:
    return pl.DataFrame(schema=LINK_SCHEMA)


def _empty_edge_df() -> pl.DataFrame:
    return pl.DataFrame(schema=EDGE_SCHEMA)


# ---------------------------------------------------------------------------
# Author-publication links
# ---------------------------------------------------------------------------


def _position_label(index: int, total: int) -> str:
    """Return 'first', 'last', or 'middle' based on position."""
    if total <= 0:
        return "unknown"
    if index == 0:
        return "first"
    if index == total - 1 and total > 1:
        return "last"
    return "middle"


def build_author_publication_links(
    works: list[CanonicalWork],
    persons: list[CanonicalPerson] | None = None,
) -> pl.DataFrame:
    """Create a publication-to-author link table.

    Each row represents one (work, author) pair.  When *persons* data is
    provided the link table is enriched with ``author_orcid`` and
    ``author_internal_id`` by fuzzy-matching display names against the
    person registry.

    Args:
        works: Reconciled canonical work records.
        persons: Optional list of canonical person records for enrichment.

    Returns:
        Polars DataFrame with columns defined in :data:`LINK_SCHEMA`.
    """
    if not works:
        return _empty_link_df()

    # Build a name→person lookup for enrichment
    person_by_name: dict[str, CanonicalPerson] = {}
    if persons:
        for p in persons:
            person_by_name[p.name.strip().lower()] = p

    rows: list[dict[str, str | None]] = []
    for work in works:
        n_authors = len(work.authors)
        for idx, author_name in enumerate(work.authors):
            name_key = author_name.strip().lower()
            person = person_by_name.get(name_key)
            rows.append(
                {
                    "work_id": work.internal_id,
                    "work_doi": work.doi,
                    "work_title": work.title,
                    "author_name": author_name,
                    "author_orcid": person.orcid if person else None,
                    "author_internal_id": person.internal_id if person else None,
                    "author_position": _position_label(idx, n_authors),
                }
            )

    return pl.DataFrame(rows, schema=LINK_SCHEMA)


# ---------------------------------------------------------------------------
# Co-author edges
# ---------------------------------------------------------------------------


def build_coauthor_edges(links: pl.DataFrame) -> pl.DataFrame:
    """Build a co-author edge table from publication-author links.

    An edge between two authors exists when they share at least one
    publication.  The edge weight is the number of shared publications.

    Args:
        links: Output of :func:`build_author_publication_links`.

    Returns:
        Polars DataFrame with columns defined in :data:`EDGE_SCHEMA`.
    """
    if links.is_empty():
        return _empty_edge_df()

    # Group authors per work
    work_groups = (
        links.select("work_id", "author_name", "author_internal_id")
        .group_by("work_id")
        .agg(
            pl.col("author_name").alias("names"),
            pl.col("author_internal_id").alias("ids"),
        )
    )

    # Build edge accumulator: (author_a_key, author_b_key) → list[work_id]
    edge_map: dict[tuple[str, str], list[str]] = {}

    for row in work_groups.iter_rows(named=True):
        work_id: str = row["work_id"]
        names: list[str | None] = row["names"]
        ids: list[str | None] = row["ids"]
        n = len(names)
        if n < 2:
            continue

        # Create canonical (name, id) tuples
        authors = [(names[i] or "", ids[i]) for i in range(n)]

        for (name_a, id_a), (name_b, id_b) in combinations(authors, 2):
            # Use a consistent key ordering to deduplicate edges
            key_a = id_a or name_a
            key_b = id_b or name_b
            if key_a > key_b:
                name_a, name_b = name_b, name_a
                id_a, id_b = id_b, id_a
                key_a, key_b = key_b, key_a
            edge_key = (key_a, key_b)
            edge_map.setdefault(edge_key, []).append(work_id)

    if not edge_map:
        return _empty_edge_df()

    # Build a name/id lookup from the full links table
    name_for_key: dict[str, str] = {}
    id_for_key: dict[str, str | None] = {}
    for row in links.iter_rows(named=True):
        key = row["author_internal_id"] or row["author_name"]
        name_for_key.setdefault(key, row["author_name"])
        id_for_key.setdefault(key, row["author_internal_id"])

    edge_rows: list[dict] = []
    for (key_a, key_b), work_ids in edge_map.items():
        unique_works = sorted(set(work_ids))
        edge_rows.append(
            {
                "author_a_name": name_for_key.get(key_a, key_a),
                "author_a_id": id_for_key.get(key_a),
                "author_b_name": name_for_key.get(key_b, key_b),
                "author_b_id": id_for_key.get(key_b),
                "shared_works_count": len(unique_works),
                "shared_work_ids": unique_works,
            }
        )

    return pl.DataFrame(edge_rows, schema=EDGE_SCHEMA)


# ---------------------------------------------------------------------------
# Collaboration rollups
# ---------------------------------------------------------------------------

_UT_ORG_MARKERS = {"university of twente", "universiteit twente", "utwente", "ut"}


def _is_ut_person(person: CanonicalPerson) -> bool:
    """Heuristic: person has at least one UT affiliation."""
    for aff in person.affiliations:
        org_name = str(aff.get("name", "")).strip().lower()
        if any(marker in org_name for marker in _UT_ORG_MARKERS):
            return True
    return False


def _org_type_for_person(
    person: CanonicalPerson,
    org_lookup: dict[str, CanonicalOrganization],
) -> str | None:
    """Return the organization type for the person's primary affiliation."""
    for aff in person.affiliations:
        org_id = aff.get("internal_id") or aff.get("id")
        if org_id and org_id in org_lookup:
            return org_lookup[org_id].type
    return None


def _country_for_person(person: CanonicalPerson) -> str | None:
    """Extract country from person affiliations if available."""
    for aff in person.affiliations:
        country = aff.get("country") or aff.get("country_code")
        if country:
            return str(country)
    return None


def build_collaboration_rollups(
    edges: pl.DataFrame,
    persons: list[CanonicalPerson] | None = None,
    organizations: list[CanonicalOrganization] | None = None,
) -> dict[str, pl.DataFrame]:
    """Build collaboration rollup DataFrames from co-author edges.

    Rollups classify edges by affiliation characteristics of the authors:

    - **ut_vs_external**: UT-UT, UT-external, external-external edges.
    - **university_rollup**: Edges grouped by university affiliation.
    - **company_rollup**: Edges involving company-affiliated authors.
    - **country_rollup**: Edges grouped by country pairs.

    Args:
        edges: Co-author edge table from :func:`build_coauthor_edges`.
        persons: Optional canonical person records for classification.
        organizations: Optional canonical organization records for type lookups.

    Returns:
        Dict mapping rollup name to a Polars DataFrame.
    """
    result: dict[str, pl.DataFrame] = {}

    if edges.is_empty() or not persons:
        # Return empty rollup stubs
        result["ut_vs_external"] = pl.DataFrame(
            schema={
                "collab_type": pl.Utf8,
                "edge_count": pl.UInt32,
                "total_shared_works": pl.UInt32,
            }
        )
        result["university_rollup"] = pl.DataFrame(
            schema={"org_type": pl.Utf8, "edge_count": pl.UInt32}
        )
        result["company_rollup"] = pl.DataFrame(
            schema={"org_type": pl.Utf8, "edge_count": pl.UInt32}
        )
        result["country_rollup"] = pl.DataFrame(
            schema={"country_a": pl.Utf8, "country_b": pl.Utf8, "edge_count": pl.UInt32}
        )
        return result

    # Build lookups
    person_by_id: dict[str, CanonicalPerson] = {p.internal_id: p for p in persons}
    person_by_name: dict[str, CanonicalPerson] = {p.name.strip().lower(): p for p in persons}
    org_lookup: dict[str, CanonicalOrganization] = {}
    if organizations:
        org_lookup = {o.internal_id: o for o in organizations}

    def _resolve_person(name: str, pid: str | None) -> CanonicalPerson | None:
        if pid and pid in person_by_id:
            return person_by_id[pid]
        return person_by_name.get(name.strip().lower())

    # --- UT vs external ---
    ut_counts: dict[str, int] = {"ut_ut": 0, "ut_external": 0, "external_external": 0}
    ut_works: dict[str, int] = {"ut_ut": 0, "ut_external": 0, "external_external": 0}

    # --- University / company / country accumulators ---
    org_type_counts: dict[str, int] = {}
    company_counts: dict[str, int] = {}
    country_pair_counts: dict[tuple[str, str], int] = {}

    for row in edges.iter_rows(named=True):
        person_a = _resolve_person(row["author_a_name"], row["author_a_id"])
        person_b = _resolve_person(row["author_b_name"], row["author_b_id"])
        shared = row["shared_works_count"]

        a_ut = _is_ut_person(person_a) if person_a else False
        b_ut = _is_ut_person(person_b) if person_b else False

        if a_ut and b_ut:
            ut_counts["ut_ut"] += 1
            ut_works["ut_ut"] += shared
        elif a_ut or b_ut:
            ut_counts["ut_external"] += 1
            ut_works["ut_external"] += shared
        else:
            ut_counts["external_external"] += 1
            ut_works["external_external"] += shared

        # Org type rollups
        for person in (person_a, person_b):
            if person:
                otype = _org_type_for_person(person, org_lookup)
                if otype:
                    if otype.lower() in ("company", "corporate", "industry"):
                        company_counts[otype] = company_counts.get(otype, 0) + 1
                    else:
                        org_type_counts[otype] = org_type_counts.get(otype, 0) + 1

        # Country rollups
        country_a = _country_for_person(person_a) if person_a else None
        country_b = _country_for_person(person_b) if person_b else None
        if country_a and country_b:
            pair = (country_a, country_b) if country_a <= country_b else (country_b, country_a)
            country_pair_counts[pair] = country_pair_counts.get(pair, 0) + 1

    result["ut_vs_external"] = pl.DataFrame(
        {
            "collab_type": list(ut_counts.keys()),
            "edge_count": list(ut_counts.values()),
            "total_shared_works": list(ut_works.values()),
        },
        schema={"collab_type": pl.Utf8, "edge_count": pl.UInt32, "total_shared_works": pl.UInt32},
    )

    result["university_rollup"] = pl.DataFrame(
        {
            "org_type": list(org_type_counts.keys()) if org_type_counts else [],
            "edge_count": list(org_type_counts.values()) if org_type_counts else [],
        },
        schema={"org_type": pl.Utf8, "edge_count": pl.UInt32},
    )

    result["company_rollup"] = pl.DataFrame(
        {
            "org_type": list(company_counts.keys()) if company_counts else [],
            "edge_count": list(company_counts.values()) if company_counts else [],
        },
        schema={"org_type": pl.Utf8, "edge_count": pl.UInt32},
    )

    country_rows_a = [p[0] for p in country_pair_counts]
    country_rows_b = [p[1] for p in country_pair_counts]
    result["country_rollup"] = pl.DataFrame(
        {
            "country_a": country_rows_a if country_pair_counts else [],
            "country_b": country_rows_b if country_pair_counts else [],
            "edge_count": list(country_pair_counts.values()) if country_pair_counts else [],
        },
        schema={"country_a": pl.Utf8, "country_b": pl.Utf8, "edge_count": pl.UInt32},
    )

    return result


# ---------------------------------------------------------------------------
# Report dataclass
# ---------------------------------------------------------------------------


@dataclass
class CoauthorshipReport:
    """Complete co-authorship analysis report.

    Attributes:
        author_publication_links: Publication-to-author link table.
        coauthor_edges: Co-author pair edge table with shared work counts.
        ut_vs_external: UT-internal vs external collaboration summary.
        university_rollup: Edges grouped by university-type affiliations.
        company_rollup: Edges involving company-affiliated authors.
        country_rollup: Edges grouped by country pairs.
        summary: High-level statistics dict.
    """

    author_publication_links: pl.DataFrame
    coauthor_edges: pl.DataFrame
    ut_vs_external: pl.DataFrame
    university_rollup: pl.DataFrame
    company_rollup: pl.DataFrame
    country_rollup: pl.DataFrame
    summary: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Convenience pipeline
# ---------------------------------------------------------------------------


def generate_coauthorship_report(
    works: list[CanonicalWork],
    persons: list[CanonicalPerson] | None = None,
    organizations: list[CanonicalOrganization] | None = None,
) -> CoauthorshipReport:
    """Run the full co-authorship analysis pipeline.

    Args:
        works: Reconciled canonical work records.
        persons: Optional person records for enrichment and classification.
        organizations: Optional organization records for type-based rollups.

    Returns:
        A :class:`CoauthorshipReport` containing all analysis artefacts.
    """
    links = build_author_publication_links(works, persons=persons)
    edges = build_coauthor_edges(links)
    rollups = build_collaboration_rollups(edges, persons=persons, organizations=organizations)

    # Compute summary statistics
    unique_authors = links["author_name"].n_unique() if not links.is_empty() else 0
    ut_authors = 0
    external_authors = 0
    if persons:
        person_names_in_links = (
            set(links["author_name"].to_list()) if not links.is_empty() else set()
        )
        for p in persons:
            if p.name in person_names_in_links:
                if _is_ut_person(p):
                    ut_authors += 1
                else:
                    external_authors += 1

    summary = {
        "total_works": len(works),
        "total_authors": unique_authors,
        "total_edges": len(edges),
        "ut_authors": ut_authors,
        "external_authors": external_authors,
        "total_links": len(links),
    }

    return CoauthorshipReport(
        author_publication_links=links,
        coauthor_edges=edges,
        ut_vs_external=rollups["ut_vs_external"],
        university_rollup=rollups["university_rollup"],
        company_rollup=rollups["company_rollup"],
        country_rollup=rollups["country_rollup"],
        summary=summary,
    )
