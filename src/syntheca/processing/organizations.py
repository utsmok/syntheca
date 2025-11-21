"""Organization parsing and mapping helpers.

This module implements utilities to resolve a hierarchy of organizational
units (org units returned by Pure OAI) and map authors' affiliations to
faculty/institute flags and convenience columns used downstream for
aggregation.
"""

from __future__ import annotations

import polars as pl

from syntheca.config import settings
from syntheca.utils.validation import normalize_orgs_df


def resolve_org_hierarchy(orgs_df: pl.DataFrame) -> pl.DataFrame:
    """Resolve parent/child relationships for organization units.

    This function reads the `part_of` relationship in the `orgs_df` which is
    expected to be the flattened output from `PureOAIClient` parsing. It will
    return a DataFrame that contains the parent org name (if present) and maps
    any full faculty names to short codes from `faculties.json`.

    Args:
        orgs_df: Polars DataFrame with org units (id, name, acronym, part_of...)

    Returns:
        pl.DataFrame: DataFrame with `internal_repository_id`, `name`, `parent_org`,
            and boolean faculty columns (tnw/et/eemcs/...).

    """
    if orgs_df is None or orgs_df.height == 0:
        return orgs_df or pl.DataFrame()

    # load mapping
    mapping = {}
    if settings.faculties_mapping_path.exists():
        import json

        mapping = json.loads(settings.faculties_mapping_path.read_text(encoding="utf8")).get(
            "mapping", {}
        )

    short_faculties = list(mapping.values())

    # Normalize org DF to guarantee `name` and `parent_org` are scalar strings
    orgs_df = normalize_orgs_df(orgs_df)

    # If the column 'part_of' is present and is a struct with 'name', map to parent_org
    df = orgs_df
    if "part_of" in df.columns:
        try:
            df = df.with_columns(pl.col("part_of").struct.field("name").alias("parent_org")).drop(
                "part_of"
            )
        except Exception:
            # ignore if structure not available
            df = df

    # Replace full faculty names using mapping - create alias columns
    if mapping:
        # Function to map a value via our mapping safely handling lists or None
        def _replace(v):
            if v is None:
                return None
            if isinstance(v, list) and v:
                v = v[0]
            if isinstance(v, str):
                return mapping.get(v, v)
            return v

        name_vals = [_replace(val) for val in df["name"].to_list()] if "name" in df.columns else []
        parent_vals = (
            [_replace(val) for val in df["parent_org"].to_list()]
            if "parent_org" in df.columns
            else []
        )
        if name_vals:
            df = df.with_columns(pl.Series(name_vals).alias("name"))
        if parent_vals:
            df = df.with_columns(pl.Series(parent_vals).alias("parent_org"))

        # Adjust parent_org to be faculty if present
        df = df.with_columns(
            pl.when(pl.col("name").is_in(short_faculties))
            .then(pl.col("name"))
            .otherwise(pl.col("parent_org"))
            .alias("parent_org")
        )

    # Create boolean flags for known faculties
    for short in short_faculties:
        df = df.with_columns((pl.col("parent_org") == short).alias(short))

    return df


def map_author_affiliations(
    authors_df: pl.DataFrame, processed_orgs_df: pl.DataFrame
) -> pl.DataFrame:
    """Map author rows to enriched organization flags and convenience columns.

    The function expects the authors DataFrame to have either `pure_id`/`internal_repository_id` and a
    list column `affiliation_ids` or `affiliations` (structs) with nested `internal_repository_id`.

    It joins the exploded authors -> affiliations and maps parent_org and boolean faculty flags
    back to the author rows, aggregating with any() semantics per author.

    Args:
        authors_df: Polars DataFrame of authors.
        processed_orgs_df: Output from `resolve_org_hierarchy`.

    Returns:
        pl.DataFrame: Enriched authors DataFrame with boolean faculty flags and columns like
            `affiliation_names_pure`, `affiliation_ids_pure`.

    """
    if authors_df is None or authors_df.height == 0:
        return pl.DataFrame()

    # Ensure we have pure_id as canonical key
    if "pure_id" not in authors_df.columns and "internal_repository_id" in authors_df.columns:
        authors_df = authors_df.rename({"internal_repository_id": "pure_id"})

    # Get affiliation ids list
    if "affiliations" in authors_df.columns:
        authors_with_affil = authors_df.with_columns(
            pl.col("affiliations")
            .list.eval(pl.element().struct.field("internal_repository_id"))
            .list.drop_nulls()
            .alias("affiliation_ids")
        )
    elif "affiliation_ids" in authors_df.columns:
        authors_with_affil = authors_df
    else:
        # no-affil data, return authors as-is
        return authors_df

    # Explode and join
    exploded = authors_with_affil.explode("affiliation_ids")
    # normalize processed_orgs_df inputs
    if processed_orgs_df is not None and processed_orgs_df.height:
        processed_orgs_df = normalize_orgs_df(processed_orgs_df)

    if processed_orgs_df is None or processed_orgs_df.height == 0:
        # return original authors but with convenience cols
        return authors_with_affil.with_columns(
            pl.col("affiliation_ids").list.join(", ").alias("affiliation_ids_pure")
        )

    # Join by affiliation id
    if "internal_repository_id" in processed_orgs_df.columns:
        join_left = exploded.join(
            processed_orgs_df,
            left_on="affiliation_ids",
            right_on="internal_repository_id",
            how="left",
        )
    else:
        join_left = exploded

    # Set up aggregation: boolean faculty columns and list fields
    faculty_cols = [
        c for c in processed_orgs_df.columns if c in getattr(settings, "faculties_short_names", [])
    ]
    # fallback to keys from mapping if missing settings
    if not faculty_cols:
        faculty_cols = [
            c
            for c in processed_orgs_df.columns
            if c not in ("internal_repository_id", "name", "parent_org")
        ]

    agg_exprs = [pl.col(col).any().alias(col) for col in faculty_cols if col in join_left.columns]
    # names and ids
    agg_exprs.extend(
        [
            pl.col("name").drop_nulls().unique().alias("affiliation_names_pure")
            if "name" in join_left.columns
            else pl.lit(None).alias("affiliation_names_pure")
        ]
    )
    agg_exprs.extend(
        [pl.col("affiliation_ids").drop_nulls().unique().alias("affiliation_ids_pure")]
    )

    grouped = join_left.group_by("pure_id").agg(agg_exprs)

    out = authors_with_affil.join(grouped, on="pure_id", how="left")
    return out
