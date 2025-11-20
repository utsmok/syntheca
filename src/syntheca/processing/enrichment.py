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
