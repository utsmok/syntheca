"""Enrichment helpers for authors and organizations.

This module contains helpers to load static mappings and enrich author
DataFrames with faculty membership and related organization flags.
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
    """Parse the `org_details_pp` column from scraped UT People profiles.

    This function mirrors the parsing logic from the legacy notebook, using the
    faculty mapping to set boolean flags and extracting convenience columns
    such as `institute`, `faculty`, `department`, `group`, and abbreviation
    columns.

    Args:
        authors_df: DataFrame with `org_details_pp` column.

    Returns:
        pl.DataFrame: DataFrame with flags and parsed name columns appended.

    """
    mapping = load_faculty_mapping()
    if "org_details_pp" not in authors_df.columns or authors_df.height == 0:
        return authors_df

    # Build mapping from full name to short code like 'tnw'
    reverse_map = mapping  # full -> short
    # prepare list of short codes
    short_codes = list(mapping.values())

    # create boolean flag columns and parse names/abbrs
    df = authors_df
    # add default bool columns (False)
    df = df.with_columns([pl.lit(False).alias(col) for col in short_codes])

    # parse the nested org details for faculty names
    df = (
        df.with_columns(
            pl.col("org_details_pp")
            .list.eval(pl.element().struct.field("faculty").struct.field("name"))
            .alias("_parsed_faculty_names")
        )
        .with_columns(
            [
                pl.col("_parsed_faculty_names").list.contains(name).alias(code)
                for name, code in reverse_map.items()
            ]
        )
        .with_columns(
            pl.col("org_details_pp")
            .list.eval(pl.element().struct.field("faculty").struct.field("name"))
            .list.join(", ")
            .alias("faculty")
        )
        .with_columns(
            pl.col("org_details_pp")
            .list.eval(pl.element().struct.field("faculty").struct.field("abbr"))
            .list.join(", ")
            .alias("faculty_abbr")
        )
        .with_columns(
            pl.col("org_details_pp")
            .list.eval(pl.element().struct.field("department").struct.field("name"))
            .list.join(", ")
            .alias("department")
        )
        .with_columns(
            pl.col("org_details_pp")
            .list.eval(pl.element().struct.field("department").struct.field("abbr"))
            .list.join(", ")
            .alias("department_abbr")
        )
        .with_columns(
            pl.col("org_details_pp")
            .list.eval(pl.element().struct.field("group").struct.field("name"))
            .list.join(", ")
            .alias("group")
        )
        .with_columns(
            pl.col("org_details_pp")
            .list.eval(pl.element().struct.field("group").struct.field("abbr"))
            .list.join(", ")
            .alias("group_abbr")
        )
        .drop("_parsed_faculty_names")
    )

    return df


def apply_manual_corrections(authors_df: pl.DataFrame) -> pl.DataFrame:
    """Applies manual affiliation corrections from `corrections.json`.

    The corrections file contains a list of dicts with `name` and
    `affiliations` lists (string-affiliation short codes). Corrections are
    applied based on exact match against a `found_name` column, or falling
    back to `first_names` + `family_names`.
    """
    path = settings.corrections_mapping_path
    if not path.exists() or authors_df is None or authors_df.height == 0:
        return authors_df

    import json

    corrections = json.loads(path.read_text(encoding="utf8"))
    if not isinstance(corrections, list):
        return authors_df

    df = authors_df
    # Build a lookup map by lower-case name
    lookup = {
        c["name"].strip().lower(): c.get("affiliations", []) for c in corrections if c.get("name")
    }

    def apply_row(row):
        name_candidates = []
        if row.get("found_name"):
            name_candidates.append(row.get("found_name"))
        if row.get("found_name_pp"):
            name_candidates.append(row.get("found_name_pp"))
        if row.get("first_names") and row.get("family_names"):
            name_candidates.append(f"{row.get('first_names')} {row.get('family_names')}")
        for n in name_candidates:
            if not n:
                continue
            affils = lookup.get(n.strip().lower())
            if affils:
                # overlay/extend columns
                out = {**row}
                out["affiliation_ids_pure"] = affils
                return out
        return row

    # apply across rows
    rows = [apply_row(r) for r in df.to_dicts()]
    out = pl.from_dicts(rows)
    # Ensure affiliation_ids_pure is a list type; if strings are present, wrap them
    if "affiliation_ids_pure" in out.columns:
        # Ensure column values are lists of strings for uniformity
        vals = out["affiliation_ids_pure"].to_list()
        new_vals = []
        for v in vals:
            if v is None:
                new_vals.append(None)
            elif isinstance(v, list):
                new_vals.append(v)
            else:
                new_vals.append([v])
        out = out.with_columns(pl.Series(new_vals).alias("affiliation_ids_pure"))
    return out
