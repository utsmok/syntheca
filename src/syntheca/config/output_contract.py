"""Stable output contract for Syntheca publication exports.

The pipeline currently produces 274-column ``final_results`` and 93-column
``enriched`` outputs.  This module defines the *stable core* — the subset
of columns that downstream consumers (dashboards, reports, institutional
APIs) may rely on.  Columns outside the stable core are considered
**optional** and may be added, renamed, or dropped between releases.

Usage::

    from syntheca.config.output_contract import STABLE_COLUMNS, OutputStability

    for col in df.columns:
        stability = COLUMN_REGISTRY.get(col, OutputStability.OPTIONAL)
        ...
"""

from __future__ import annotations

from enum import StrEnum

import polars as pl


class OutputStability(StrEnum):
    """Column stability tier."""

    STABLE = "stable"
    """Column is part of the public contract and will not be removed or
    renamed without a major version bump."""

    OPTIONAL = "optional"
    """Column may be present depending on the data sources used.  Its
    name and semantics may change between minor releases."""


# ------------------------------------------------------------------
# Stable core columns
# ------------------------------------------------------------------
# These are the columns that MUST be present in every merged output
# DataFrame produced by the pipeline.  They form the public contract.

STABLE_COLUMNS: list[str] = [
    # === Identity ===
    "doi",
    "title",
    "internal_repository_id",  # Pure canonical ID
    "id",  # OpenAlex canonical ID
    # === Bibliographic core ===
    "publication_year",
    "publication_date",
    "type",  # work type (article, book-chapter, …)
    "language",
    # === Authorship ===
    "authors",  # list-of-structs with at least name + internal_repository_id
    # === Open Access ===
    "is_oa",
    "oa_color",  # gold / green / hybrid / bronze / closed
    # === Citation ===
    "cited_by_count",
    # === Publisher / venue ===
    "publisher",
    "primary_host_name",  # journal or venue display name
    # === UT-specific ===
    "ut_is_corresponding",  # UT is listed as corresponding institution
]

# ------------------------------------------------------------------
# Optional columns (non-exhaustive — listed for documentation)
# ------------------------------------------------------------------
# These columns may appear depending on enrichment steps.  They are
# NOT part of the stable contract and may change at any time.

OPTIONAL_COLUMNS: list[str] = [
    # Open Access details
    "oa_url",
    "in_repository",
    "oa_host_org",
    "oa_host_name",
    "oa_host_type",
    "primary_url",
    "primary_host_org",
    "primary_host_type",
    "all_host_orgs",
    # Topic / classification
    "topic",
    "subfield",
    "field",
    "domain",
    # APC
    "listed_apc_usd",
    "paid_apc_usd",
    # Citation enrichment
    "fwci",
    "citation_normalized_percentile",
    # Faculty / org enrichment
    "faculty",
    "faculty_abbr",
    "institute",
    "department",
    "group",
    "affiliation_names_pure",
    "affiliation_ids_pure",
    # Boolean faculty flags
    "eemcs",
    "et",
    "bms",
    "tnw",
    "itc",
    # Identifiers
    "orcid",
    "people_page_url",
    # Funding
    "funders",
    "grants",
    # Misc
    "abstract_inverted_index",
    "keywords",
    "license",
    "is_retracted",
    "is_paratext",
    "sustainable_development_goals",
]


# ------------------------------------------------------------------
# Combined registry: column name → stability tier
# ------------------------------------------------------------------

COLUMN_REGISTRY: dict[str, OutputStability] = {
    col: OutputStability.STABLE for col in STABLE_COLUMNS
}
COLUMN_REGISTRY.update({col: OutputStability.OPTIONAL for col in OPTIONAL_COLUMNS})


def ensure_publication_contract(df: pl.DataFrame) -> pl.DataFrame:
    """Return *df* with every stable publication column present.

    The merged pipeline output is still assembled from heterogeneous source
    frames. During the audit-remediation transition we enforce the stable
    publication contract by materializing any missing stable column as a null
    column rather than silently omitting it from exported artifacts.

    Args:
        df: Publication output DataFrame.

    Returns:
        A DataFrame that contains all :data:`STABLE_COLUMNS`.
    """
    missing = [column for column in STABLE_COLUMNS if column not in df.columns]
    if not missing:
        return df

    return df.with_columns([pl.lit(None).alias(column) for column in missing])
