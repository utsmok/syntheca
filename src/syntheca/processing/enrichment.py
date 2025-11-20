"""Enrichment helpers for authors and organizations.

This module contains helpers to load static mappings and enrich author
DataFrames with faculty membership and related organization flags.
Includes full person data cleaning logic from the monolith.
"""

from __future__ import annotations

import pathlib

import polars as pl

from syntheca.config import settings


def load_faculty_mapping() -> dict[str, str]:
    """Load the faculty mapping from file in `settings.faculties_mapping_path`.

    Returns:
        dict[str, str]: A mapping from full faculty/organization name to the
            preferred short code used in the project (e.g., "Faculty of Science" -> "tnw").

    """
    path = settings.faculties_mapping_path
    data = {}
    if path.exists():
        import json

        with pathlib.Path(path).open(encoding="utf8") as fh:
            data = json.load(fh)
    mapping = data.get("mapping", {}) if isinstance(data, dict) else {}
    return mapping


def clean_and_enrich_persons_data(
    person_data: pl.DataFrame,
    org_data: pl.DataFrame,
) -> pl.DataFrame:
    """Clean and enrich person data with faculty affiliations.

    Replicates the monolith's clean_and_enrich_persons_data logic:
    - Rename columns (internal_repository_id -> pure_id, etc.)
    - Add people page URLs
    - Parse organization hierarchy and match to faculties
    - Add boolean faculty columns and affiliation lists
    - Filter to UT-affiliated persons

    Args:
        person_data (pl.DataFrame): Raw person data from Pure OAI.
        org_data (pl.DataFrame): Raw organization data from Pure OAI.

    Returns:
        pl.DataFrame: Cleaned and enriched person data with faculty flags.

    """
    full_faculty_names = [
        "Faculty of Science and Technology",
        "Faculty of Engineering Technology",
        "Faculty of Electrical Engineering, Mathematics and Computer Science",
        "Faculty of Behavioural, Management and Social Sciences",
        "Faculty of Geo-Information Science and Earth Observation",
        "TechMed Centre",
        "Digital Society Institute",
        "MESA+ Institute",
    ]
    short_faculty_names = ["tnw", "et", "eemcs", "bms", "itc", "techmed", "dsi", "mesa"]

    # Rename columns
    rename_map = {}
    if "internal_repository_id" in person_data.columns:
        rename_map["internal_repository_id"] = "pure_id"
    if "family_names" in person_data.columns:
        rename_map["family_names"] = "last_name"
    if "first_names" in person_data.columns:
        rename_map["first_names"] = "first_name"
    if rename_map:
        person_data = person_data.rename(rename_map)

    # Drop unnecessary columns
    drop_cols = [
        "scopus_affil_id", "researcher_id", "isni", "cris-id",
        "uuid", "uri", "url"
    ]
    person_data = person_data.drop([col for col in drop_cols if col in person_data.columns])

    # Add people page URLs
    if "last_name" in person_data.columns and "first_name" in person_data.columns and "pure_id" in person_data.columns:
        people_page_url = "https://people.utwente.nl/overview?query="
        names_for_people_page = (
            person_data.select(["last_name", "first_name", "pure_id"])
            .drop_nulls()
            .to_dicts()
        )
        people_page_urls = [
            {
                "pure_id": pers["pure_id"],
                "url": "".join([
                    people_page_url,
                    str(pers["first_name"]),
                    "%20",
                    str(pers["last_name"]),
                ]).replace(" ", "%20"),
            }
            for pers in names_for_people_page
        ]
        if people_page_urls:
            person_data = person_data.join(
                pl.from_dicts(people_page_urls), on="pure_id", how="left"
            )

    # Check for affiliations column
    if "affiliations" not in person_data.columns:
        return person_data

    # Extract unique affiliations
    found_unique_affils = (
        person_data.filter(pl.col("affiliations").is_not_null())
        .select("affiliations")
        .explode("affiliations")
        .unnest("affiliations")
        .unique(["name", "internal_repository_id"])
    )

    # Clean organization data
    if "part_of" in org_data.columns:
        clean_org_data = (
            org_data.filter(pl.col("part_of").is_not_null())
            .with_columns(pl.col("part_of").struct.field("name").alias("parent_org"))
            .drop([col for col in ["identifiers", "part_of", "acronym", "url"] if col in org_data.columns])
            .with_columns(
                pl.col("parent_org").str.replace_many(full_faculty_names, short_faculty_names),
                pl.col("name").str.replace_many(full_faculty_names, short_faculty_names),
            )
            .with_columns(
                pl.when(pl.col("name").is_in(short_faculty_names))
                .then(pl.col("name"))
                .otherwise(pl.col("parent_org"))
                .alias("parent_org")
            )
        )

        # Enhance org data with boolean faculty flags
        enhanced_org_data = (
            found_unique_affils.join(
                clean_org_data,
                left_on="internal_repository_id",
                right_on="internal_repository_id",
                how="left",
            )
            .drop_nulls()
            .with_columns([
                pl.when(pl.col("parent_org").eq(col))
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias(col)
                for col in short_faculty_names
            ])
        )

        # Join back to person data with faculty flags
        pure_persons_with_affil_ids = (
            person_data.with_columns(
                pl.col("affiliations")
                .list.eval(pl.element().struct.field("internal_repository_id"))
                .list.drop_nulls()
                .alias("affiliation_ids")
            )
            .explode("affiliation_ids")
            .join(
                enhanced_org_data,
                left_on="affiliation_ids",
                right_on="internal_repository_id",
                how="left",
            )
            .group_by("pure_id")
            .agg([pl.col(col).any().alias(col) for col in short_faculty_names])
        )

        person_data = person_data.join(
            pure_persons_with_affil_ids, on="pure_id", how="left"
        )

    # Extract affiliation names and IDs
    person_data = person_data.with_columns([
        pl.col("affiliations")
        .list.eval(pl.element().struct.field("name"))
        .list.drop_nulls()
        .alias("affiliation_names_pure"),
        pl.col("affiliations")
        .list.eval(pl.element().struct.field("internal_repository_id"))
        .list.drop_nulls()
        .alias("affiliation_ids_pure"),
    ])

    # Add is_ut flag and filter to UT persons
    ut_uuid = "491145c6-1c9b-4338-aedd-98315c166d7e"
    person_data = person_data.with_columns(
        pl.col("affiliation_ids_pure")
        .list.contains(ut_uuid)
        .alias("is_ut")
    )

    # Drop affiliations column and filter to UT
    person_data = person_data.drop("affiliations")
    if "is_ut" in person_data.columns:
        person_data = person_data.filter(pl.col("is_ut")).drop("is_ut")

    return person_data


def enrich_authors_with_faculties(authors_df: pl.DataFrame) -> pl.DataFrame:
    """Enrich a DataFrame of authors with boolean faculty membership columns.

    The function expects a column named `affiliation_names_pure` that contains a
    list of strings per row; for each faculty mapping (loaded via
    `load_faculty_mapping`) it adds a boolean column named after the faculty
    short-code that indicates whether the author has the mapped organization.

    Args:
        authors_df (pl.DataFrame): DataFrame containing `affiliation_names_pure`.

    Returns:
        pl.DataFrame: A new DataFrame with the additional boolean faculty columns
            or the original DataFrame if no mapping or column exists.

    """
    mapping = load_faculty_mapping()
    if not mapping:
        return authors_df

    if "affiliation_names_pure" not in authors_df.columns:
        # nothing to do
        return authors_df

    # Add a boolean column per short name
    exprs = []
    for full_name, short in mapping.items():
        # Use list.contains for lists of affiliation names; ensures set membership semantics.
        exprs.append(pl.col("affiliation_names_pure").list.contains(full_name).alias(short))

    return authors_df.with_columns(exprs)
