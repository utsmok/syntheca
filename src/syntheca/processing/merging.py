"""Merging helpers to join and deduplicate publication datasets.

This module contains functions which normalize DOIs across DataFrames and
merge/deduplicate datasets using DOI as primary key with title fallback.
"""

from __future__ import annotations

import polars as pl

from syntheca.processing.cleaning import normalize_doi


def merge_datasets(
    oils_df: pl.DataFrame,
    full_df: pl.DataFrame,
    doi_col_oils: str = "doi",
    doi_col_full: str = "doi",
) -> pl.DataFrame:
    """Join two DataFrames on normalized DOIs.

    Both DataFrames will have their DOI columns normalized (via `normalize_doi`) and
    then a left join of `full_df` onto `oils_df` is performed.

    Args:
        oils_df (pl.DataFrame): The primary publications DataFrame (e.g., Pure OAI).
        full_df (pl.DataFrame): Additional works DataFrame to merge in (e.g., OpenAlex).
        doi_col_oils (str): Column name for DOI in `oils_df`.
        doi_col_full (str): Column name for DOI in `full_df`.

    Returns:
        pl.DataFrame: The joined DataFrame containing fields from both inputs.

    """
    oils = normalize_doi(oils_df, doi_col_oils, new_col="_norm_doi")
    full = normalize_doi(full_df, doi_col_full, new_col="_norm_doi")

    merged = full.join(oils, left_on="_norm_doi", right_on="_norm_doi", how="left", suffix="_oils")
    return merged


def deduplicate(df: pl.DataFrame, doi_col: str = "doi", title_col: str = "title") -> pl.DataFrame:
    """Produce a deduplicated DataFrame by DOI and normalized title fallback.

    Strategy:
        1. Normalize DOIs and remove duplicate DOIs, keeping the first occurrence.
        2. For rows without DOIs, normalize titles and remove duplicates.

    Args:
        df (pl.DataFrame): The DataFrame to deduplicate.
        doi_col (str): Name of the DOI column.
        title_col (str): Name of the title column used as fallback dedupe key.

    Returns:
        pl.DataFrame: A deduplicated DataFrame.

    """
    # Normalize DOIs, use helper
    df_norm = normalize_doi(df, doi_col, new_col="_norm_doi")
    # remove duplicates by DOI first
    df_with_doi = df_norm.filter(pl.col("_norm_doi").is_not_null())
    df_no_dups = df_with_doi.unique(subset=["_norm_doi"]) if df_with_doi.height else df_with_doi

    # now add rows without DOI, dedup by cleaned title
    no_doi = df_norm.filter(pl.col("_norm_doi").is_null())
    if title_col in no_doi.columns and no_doi.height:
        no_doi = no_doi.with_columns(
            pl.col(title_col).str.to_lowercase().str.strip_chars().alias("_norm_title")
        )
        no_doi = no_doi.unique(subset=["_norm_title"]).drop("_norm_title")

    combined = pl.concat([df_no_dups, no_doi], how="vertical")
    # final unique rows (safe) - preserve first occurrence
    return combined.unique()
