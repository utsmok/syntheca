"""Organization hierarchy resolution and author-organization mapping.

This module provides functions to process organizational hierarchies from Pure OAI
data, resolve parent-child relationships, and map authors to their organizational
affiliations with faculty membership flags.
"""

from __future__ import annotations

import json
import pathlib

import polars as pl

from syntheca.config import settings


def load_faculty_mapping() -> dict[str, dict[str, list[str] | str]]:
    """Load the complete faculty mapping from faculties.json.

    Returns:
        dict: A dictionary containing 'mapping', 'short_names', 'ut_uuid', and 'openalex_ut_id'.

    """
    path = settings.faculties_mapping_path
    if not path.exists():
        return {"mapping": {}, "short_names": [], "ut_uuid": "", "openalex_ut_id": ""}

    with pathlib.Path(path).open(encoding="utf8") as fh:
        return json.load(fh)


def resolve_org_hierarchy(orgs_df: pl.DataFrame) -> pl.DataFrame:
    """Resolve organizational hierarchy by mapping parent relationships to faculty codes.

    This function processes the 'part_of' relationship in the organizations DataFrame
    to identify parent organizations and maps them to faculty short codes using the
    faculties mapping. If an org is itself a faculty, it uses its own name as parent.

    Args:
        orgs_df (pl.DataFrame): DataFrame containing organization data with columns:
            - internal_repository_id: unique org identifier
            - name: organization name
            - part_of: struct containing parent organization info

    Returns:
        pl.DataFrame: Processed DataFrame with additional columns:
            - parent_org: resolved parent organization name (mapped to short codes)
            - Boolean columns for each faculty (e.g., tnw, eemcs, bms, etc.)

    """
    # Load faculty mappings
    faculty_data = load_faculty_mapping()
    full_names = list(faculty_data.get("mapping", {}).keys())
    short_names = faculty_data.get("short_names", [])

    if not full_names or not short_names:
        # No mapping available; return original
        return orgs_df

    # Process organizations that have a parent (part_of)
    if "part_of" not in orgs_df.columns:
        return orgs_df

    # Extract parent org name from nested struct and map to short codes
    clean_org_data = (
        orgs_df.filter(pl.col("part_of").is_not_null())
        .with_columns(pl.col("part_of").struct.field("name").alias("parent_org"))
        .with_columns(
            pl.col("parent_org").str.replace_many(full_names, short_names),
            pl.col("name").str.replace_many(full_names, short_names),
        )
        # If the org itself is a faculty, use its own name as parent_org
        .with_columns(
            pl.when(pl.col("name").is_in(short_names))
            .then(pl.col("name"))
            .otherwise(pl.col("parent_org"))
            .alias("parent_org")
        )
    )

    # Add boolean columns for each faculty
    boolean_exprs = [
        pl.when(pl.col("parent_org") == short_name)
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
        .alias(short_name)
        for short_name in short_names
    ]

    if boolean_exprs:
        clean_org_data = clean_org_data.with_columns(boolean_exprs)

    return clean_org_data


def map_author_affiliations(
    authors_df: pl.DataFrame, processed_orgs_df: pl.DataFrame
) -> pl.DataFrame:
    """Map authors to organizations and add faculty membership flags.

    This function joins authors with their organizational affiliations and adds
    boolean flags indicating whether the author belongs to specific faculties.
    It also adds an 'is_ut' flag based on the UT UUID.

    Args:
        authors_df (pl.DataFrame): DataFrame of authors/persons with columns:
            - affiliations: list of affiliation structs containing internal_repository_id
        processed_orgs_df (pl.DataFrame): DataFrame from resolve_org_hierarchy with
            faculty boolean columns

    Returns:
        pl.DataFrame: Authors DataFrame enriched with:
            - affiliation_names_pure: list of affiliation names
            - affiliation_ids_pure: list of affiliation IDs
            - is_ut: boolean indicating UT affiliation
            - Boolean columns for each faculty (aggregated via 'any' across affiliations)

    """
    faculty_data = load_faculty_mapping()
    short_names = faculty_data.get("short_names", [])
    ut_uuid = faculty_data.get("ut_uuid", "")

    if "affiliations" not in authors_df.columns:
        return authors_df

    # Extract affiliation IDs and names from nested structs
    authors_with_affils = authors_df.with_columns(
        pl.col("affiliations")
        .list.eval(pl.element().struct.field("internal_repository_id"))
        .list.drop_nulls()
        .alias("affiliation_ids_pure"),
        pl.col("affiliations")
        .list.eval(pl.element().struct.field("name"))
        .list.drop_nulls()
        .alias("affiliation_names_pure"),
    )

    # Add is_ut flag based on UT UUID
    if ut_uuid:
        authors_with_affils = authors_with_affils.with_columns(
            pl.col("affiliation_ids_pure").list.contains(ut_uuid).alias("is_ut")
        )

    # Join with organization data to get faculty flags
    # Explode affiliations, join with orgs, then aggregate back
    if "internal_repository_id" in processed_orgs_df.columns and short_names:
        exploded = (
            authors_with_affils.with_columns(
                pl.col("affiliations")
                .list.eval(pl.element().struct.field("internal_repository_id"))
                .list.drop_nulls()
                .alias("_affil_ids")
            )
            .explode("_affil_ids")
            .join(
                processed_orgs_df,
                left_on="_affil_ids",
                right_on="internal_repository_id",
                how="left",
            )
        )

        # Aggregate faculty booleans using 'any' (if any affiliation is in faculty, flag is True)
        # Group by a unique identifier; use 'internal_repository_id' from authors if available
        if "internal_repository_id" in authors_with_affils.columns:
            group_key = "internal_repository_id"
        elif "pure_id" in authors_with_affils.columns:
            group_key = "pure_id"
        else:
            # No suitable grouping key; return original
            return authors_with_affils.drop("affiliations")

        agg_exprs = [
            pl.col(short_name).any().alias(short_name)
            for short_name in short_names
            if short_name in exploded.columns
        ]

        if agg_exprs:
            aggregated = exploded.group_by(group_key).agg(agg_exprs)
            authors_with_affils = authors_with_affils.join(aggregated, on=group_key, how="left")

    # Drop original nested affiliations column
    return (
        authors_with_affils.drop("affiliations")
        if "affiliations" in authors_with_affils.columns
        else authors_with_affils
    )
