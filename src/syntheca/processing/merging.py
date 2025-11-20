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


def join_authors_and_publications(
    publications_df: pl.DataFrame, authors_df: pl.DataFrame
) -> pl.DataFrame:
    """Join authors and publications to aggregate author affiliation data into publications.

    This function explodes publications by author ID, joins with enriched author data,
    and aggregates faculty/institute/department/group information back to the publication level.
    For boolean columns (faculty flags), it uses 'any' aggregation (if any author is in a faculty,
    the publication is flagged for that faculty). For list columns, it collects unique values.

    Args:
        publications_df (pl.DataFrame): DataFrame of publications with columns:
            - pure_id: unique publication identifier
            - authors: list of author structs containing internal_repository_id
        authors_df (pl.DataFrame): DataFrame of authors from clean_and_enrich_persons_data
            with columns like pure_id or internal_repository_id, and faculty/org flags.

    Returns:
        pl.DataFrame: Publications DataFrame enriched with aggregated author affiliation data.

    """
    # Ensure authors_df has a 'pure_id' column
    if "internal_repository_id" in authors_df.columns and "pure_id" not in authors_df.columns:
        authors_df = authors_df.rename({"internal_repository_id": "pure_id"})

    if "pure_id" not in authors_df.columns:
        raise ValueError(
            "authors_df must contain either a 'internal_repository_id' or 'pure_id' column."
        )

    # Extract author IDs from publications
    pubs_with_author_ids = publications_df.with_columns(
        pl.col("authors")
        .list.eval(pl.element().struct.field("internal_repository_id"))
        .list.drop_nulls()
        .alias("author_pure_ids")
    )

    # Explode publications by author
    exploded_pubs = pubs_with_author_ids.select(["pure_id", "author_pure_ids"]).explode(
        "author_pure_ids"
    )

    # Join with author details
    author_details = exploded_pubs.join(
        authors_df, left_on="author_pure_ids", right_on="pure_id", how="left", suffix="_author"
    )

    # Define aggregation strategy
    merge_cols_bool = ["dsi", "mesa", "techmed", "eemcs", "et", "bms", "tnw", "itc"]
    merge_cols_lists = [
        "faculty",
        "institute",
        "department",
        "group",
        "faculty_abbr",
        "department_abbr",
        "group_abbr",
    ]
    merge_cols_str = ["orcid"]

    # Build aggregation expressions
    agg_exprs = []

    # Boolean columns: use 'any' (if any author has True, publication gets True)
    agg_exprs.extend(
        [pl.col(col).any().alias(col) for col in merge_cols_bool if col in author_details.columns]
    )

    # List columns: split by delimiters, flatten, get unique values
    agg_exprs.extend(
        [
            pl.col(col)
            .str.split(by=", ")
            .flatten()
            .unique()
            .replace("", None)
            .drop_nulls()
            .alias(col)
            for col in merge_cols_lists
            if col in author_details.columns
        ]
    )

    # String columns: collect all non-null unique strings into a list
    agg_exprs.extend(
        [
            pl.col(col).drop_nulls().unique().alias(col + "s")
            for col in merge_cols_str
            if col in author_details.columns
        ]
    )

    # Group by publication ID and aggregate
    merged_author_data = author_details.group_by("pure_id").agg(agg_exprs)

    # Join aggregated data back to original publications
    final_df = publications_df.join(merged_author_data, on="pure_id", how="left")

    return final_df
