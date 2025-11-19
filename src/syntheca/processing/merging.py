from __future__ import annotations

import polars as pl

from syntheca.processing.cleaning import normalize_doi


def merge_datasets(
    oils_df: pl.DataFrame,
    full_df: pl.DataFrame,
    doi_col_oils: str = "doi",
    doi_col_full: str = "doi",
) -> pl.DataFrame:
    """Normalize DOIs and join two dataframes on normalized DOIs.

    The function will ensure DOIs in both frames are normalized using `normalize_doi`
    and then perform a left join of `full_df` onto `oils_df`.
    """

    oils = normalize_doi(oils_df, doi_col_oils, new_col="_norm_doi")
    full = normalize_doi(full_df, doi_col_full, new_col="_norm_doi")

    merged = full.join(oils, left_on="_norm_doi", right_on="_norm_doi", how="left", suffix="_oils")
    return merged


def deduplicate(df: pl.DataFrame, doi_col: str = "doi", title_col: str = "title") -> pl.DataFrame:
    """Return a deduplicated DataFrame.

    Strategy:
    - Normalize DOIs and drop duplicate DOIs keeping the first occurrence.
    - For rows without DOIs, drop duplicates by normalized title (lowercase, stripped).
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
