"""Enrichment helpers for authors and organizations.

This module contains helpers to load static mappings and enrich author
DataFrames with faculty membership and related organization flags.
"""

from __future__ import annotations

import json
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
        with pathlib.Path(path).open(encoding="utf8") as fh:
            data = json.load(fh)
    mapping = data.get("mapping", {}) if isinstance(data, dict) else {}
    return mapping


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


def parse_scraped_org_details(authors_df: pl.DataFrame) -> pl.DataFrame:
    """Parse nested organizational details from scraped profile data.

    This function processes the 'org_details_pp' column (list of dicts with nested
    structure) to extract faculty, institute, department, and group information,
    and creates boolean flags for each faculty/institute.

    Args:
        authors_df (pl.DataFrame): DataFrame containing 'org_details_pp' column
            with nested organizational data from scraped profiles.

    Returns:
        pl.DataFrame: DataFrame with added columns:
            - Boolean columns for each faculty/institute (tnw, eemcs, bms, et, itc, dsi, mesa, techmed)
            - institute, faculty, department, group (comma-separated strings)
            - faculty_abbr, department_abbr, group_abbr (comma-separated strings)

    """
    # Load faculty mapping to get parsing rules
    path = settings.faculties_mapping_path
    if not path.exists():
        return authors_df

    with pathlib.Path(path).open(encoding="utf8") as fh:
        faculty_data = json.load(fh)

    # Build parsing mapping from full names to short codes
    parsing_mapping = faculty_data.get("mapping", {})
    if not parsing_mapping:
        return authors_df

    # Initialize boolean columns (default False)
    short_names = list(parsing_mapping.values())
    df = authors_df.with_columns([pl.lit(False).alias(col_name) for col_name in short_names])

    if "org_details_pp" not in df.columns:
        return df

    # Parse faculty names from org_details and map to short codes
    df = (
        df.with_columns(
            pl.col("org_details_pp")
            .list.eval(
                pl.element()
                .struct.field("faculty")
                .struct.field("name")
                .replace_strict(parsing_mapping, default=None)
            )
            .alias("parsed_name")
        )
        # Create boolean columns by checking if parsed_name contains each short code
        .with_columns(
            [
                pl.col("parsed_name").list.contains(col_name).alias(col_name + "_new")
                for col_name in short_names
            ]
        )
        # Fill nulls with False
        .with_columns([pl.col(col_name + "_new").fill_null(False) for col_name in short_names])
        # Logical OR with existing values (in case some were set from Pure data)
        .with_columns(
            [
                (pl.col(col_name) | pl.col(col_name + "_new")).alias(col_name)
                for col_name in short_names
            ]
        )
        # Clean up temporary columns
        .drop([col_name + "_new" for col_name in short_names] + ["parsed_name"])
    )

    # Extract organization names and abbreviations into separate columns
    # Note: department and group fields may be all-null which results in list[null] type
    # In such cases, we can't access nested struct fields, so we return empty strings

    # First, extract faculty-related fields (these always exist as struct)
    df = df.with_columns(
        # Institute names (non-faculty items)
        pl.col("org_details_pp")
        .list.eval(pl.element().struct.field("faculty").struct.field("name"))
        .list.filter(~pl.element().str.contains("Faculty"))
        .list.join(", ")
        .alias("institute"),
        # Faculty names
        pl.col("org_details_pp")
        .list.eval(pl.element().struct.field("faculty").struct.field("name"))
        .list.filter(pl.element().str.contains("Faculty"))
        .list.join(", ")
        .alias("faculty"),
        # Faculty abbreviations
        pl.col("org_details_pp")
        .list.eval(pl.element().struct.field("faculty").struct.field("abbr"))
        .list.filter(pl.element().is_not_null())
        .list.join(", ")
        .alias("faculty_abbr"),
    )

    # For department and group, we need to handle the case where all values are null
    # which results in list[null] type and prevents struct field access
    # We'll use a try-except pattern via cast or handle it differently

    # Try to extract department fields - if all null, return empty strings
    try:
        df = df.with_columns(
            pl.col("org_details_pp")
            .list.eval(
                pl.when(pl.element().struct.field("department").is_null())
                .then(None)
                .otherwise(pl.element().struct.field("department").struct.field("name"))
            )
            .list.drop_nulls()
            .list.join(", ")
            .alias("department"),
            pl.col("org_details_pp")
            .list.eval(
                pl.when(pl.element().struct.field("department").is_null())
                .then(None)
                .otherwise(pl.element().struct.field("department").struct.field("abbr"))
            )
            .list.drop_nulls()
            .list.join(", ")
            .alias("department_abbr"),
        )
    except Exception:
        # If extraction fails (all null case), add empty string columns
        df = df.with_columns(
            pl.lit("").alias("department"),
            pl.lit("").alias("department_abbr"),
        )

    # Try to extract group fields - if all null, return empty strings
    try:
        df = df.with_columns(
            pl.col("org_details_pp")
            .list.eval(
                pl.when(pl.element().struct.field("group").is_null())
                .then(None)
                .otherwise(pl.element().struct.field("group").struct.field("name"))
            )
            .list.drop_nulls()
            .list.join(", ")
            .alias("group"),
            pl.col("org_details_pp")
            .list.eval(
                pl.when(pl.element().struct.field("group").is_null())
                .then(None)
                .otherwise(pl.element().struct.field("group").struct.field("abbr"))
            )
            .list.drop_nulls()
            .list.join(", ")
            .alias("group_abbr"),
        )
    except Exception:
        # If extraction fails (all null case), add empty string columns
        df = df.with_columns(
            pl.lit("").alias("group"),
            pl.lit("").alias("group_abbr"),
        )

    return df


def apply_manual_corrections(df: pl.DataFrame) -> pl.DataFrame:
    """Apply manual affiliation corrections from corrections.json.

    This function loads manual corrections for specific authors and updates their
    affiliation information, including faculty/institute flags and abbreviations.

    Args:
        df (pl.DataFrame): Publications DataFrame with author information.
            Expected to have 'pure_authors_names' column (list of author names per publication).

    Returns:
        pl.DataFrame: DataFrame with corrected affiliation data for matched authors.

    """
    # Load corrections
    corrections_path = settings.corrections_mapping_path
    if not corrections_path.exists():
        return df

    with pathlib.Path(corrections_path).open(encoding="utf8") as fh:
        corrections_data = json.load(fh)

    if not corrections_data:
        return df

    # Build updates DataFrame from corrections
    bool_cols = ["tnw", "eemcs", "et", "bms", "itc", "dsi", "techmed", "mesa"]
    list_cols = ["faculty_abbr", "department_abbr", "group_abbr", "institute"]

    updates = []
    for correction in corrections_data:
        name = correction.get("name")
        affils = correction.get("affiliations", [])

        update_dict = dict.fromkeys(bool_cols, False)
        update_dict.update(dict.fromkeys(list_cols))
        update_dict["name"] = name

        # Parse affiliations - collect all parts into lists
        faculties = []
        departments = []
        groups = []
        institutes = []

        for abbr in affils:
            if "-" in abbr:
                # Faculty-Department-Group format
                parts = str(abbr).split("-")
                if len(parts) >= 3:
                    faculties.append(parts[0])
                    departments.append(parts[1])
                    groups.append(parts[2])
                    # Set faculty boolean
                    faculty_key = parts[0].lower()
                    if faculty_key in bool_cols:
                        update_dict[faculty_key] = True
            else:
                # Institute only
                institutes.append(abbr)
                if abbr.lower() in bool_cols:
                    update_dict[abbr.lower()] = True

        # Set the list fields as comma-separated strings to match expected format
        if faculties:
            update_dict["faculty_abbr"] = ", ".join(faculties)
        if departments:
            update_dict["department_abbr"] = ", ".join(departments)
        if groups:
            update_dict["group_abbr"] = ", ".join(groups)
        if institutes:
            update_dict["institute"] = ", ".join(institutes)

        updates.append(update_dict)

    if not updates:
        return df

    updates_df = pl.from_dicts(updates)

    # Check if we have the expected column to match on
    if "pure_authors_names" not in df.columns:
        return df

    # Explode by author names, join corrections, then aggregate back
    df_exploded = df.explode("pure_authors_names").join(
        updates_df,
        left_on="pure_authors_names",
        right_on="name",
        how="left",
        suffix="_upd",
    )

    # Update list columns by merging comma-separated strings
    for col in list_cols:
        if col in df_exploded.columns and col + "_upd" in df_exploded.columns:
            df_exploded = df_exploded.with_columns(
                pl.when(pl.col(col + "_upd").is_not_null())
                .then(
                    pl.concat_str(
                        [pl.col(col).fill_null(""), pl.col(col + "_upd").fill_null("")],
                        separator=", ",
                    )
                    .str.replace("^, ", "")
                    .str.replace(", $", "")
                )
                .otherwise(pl.col(col))
                .alias(col)
            )

    # Update boolean columns with logical OR
    for col in bool_cols:
        if col in df_exploded.columns and col + "_upd" in df_exploded.columns:
            df_exploded = df_exploded.with_columns(
                pl.when(pl.col(col + "_upd").is_not_null())
                .then(pl.col(col).fill_null(False) | pl.col(col + "_upd").fill_null(False))
                .otherwise(pl.col(col))
                .alias(col)
            )

    # Drop update columns and name column from join
    drop_cols = [
        col + "_upd"
        for col in list_cols + bool_cols + ["name"]
        if col + "_upd" in df_exploded.columns
    ]
    if drop_cols:
        df_exploded = df_exploded.drop(drop_cols)

    # Group back by publication ID to un-explode
    if "pure_id" in df_exploded.columns:
        # Re-aggregate by pure_id - use first for scalar values and rebuild list for names
        group_exprs = []
        for col in df_exploded.columns:
            if col == "pure_authors_names":
                # Rebuild the list of author names
                group_exprs.append(
                    pl.col("pure_authors_names").drop_nulls().alias("pure_authors_names")
                )
            elif col != "pure_id":
                # For all other columns, use first (should be consistent per publication)
                group_exprs.append(pl.col(col).first().alias(col))

        df_result = df_exploded.group_by("pure_id").agg(group_exprs)
        return df_result

    return df_exploded
