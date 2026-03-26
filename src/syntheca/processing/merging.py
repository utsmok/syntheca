"""Merging helpers to join and deduplicate publication datasets.

This module contains functions which normalize DOIs across DataFrames and
merge/deduplicate datasets using DOI as primary key with title fallback.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl
from loguru import logger

from syntheca.processing.cleaning import normalize_doi

# ---------------------------------------------------------------------------
# Merge statistics
# ---------------------------------------------------------------------------


@dataclass
class MergeStats:
    """Structured report for a merge operation."""

    operation: str = ""
    input_left: int = 0
    input_right: int = 0
    output_rows: int = 0
    matched: int = 0
    unmatched_left: int = 0
    unmatched_right: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def summary(self) -> str:
        """Human-readable summary of the merge operation."""
        return (
            f"{self.operation}: left={self.input_left} right={self.input_right} "
            f"output={self.output_rows} matched={self.matched} "
            f"unmatched_left={self.unmatched_left} unmatched_right={self.unmatched_right} "
            f"errors={len(self.errors)}"
        )


def merge_datasets(
    pure_publications_df: pl.DataFrame,
    openalex_works_df: pl.DataFrame,
    doi_col_pure: str = "doi",
    doi_col_openalex: str = "doi",
) -> pl.DataFrame:
    """Join two DataFrames on normalized DOIs.

    Both DataFrames will have their DOI columns normalized (via `normalize_doi`) and
    then a left join of `openalex_works_df` onto `pure_publications_df` is performed.

    Args:
        pure_publications_df (pl.DataFrame): The primary publications DataFrame (e.g., Pure OAI).
        openalex_works_df (pl.DataFrame): Additional works DataFrame to merge in (e.g., OpenAlex).
        doi_col_pure (str): Column name for DOI in `pure_publications_df`.
        doi_col_openalex (str): Column name for DOI in `openalex_works_df`.

    Returns:
        pl.DataFrame: The joined DataFrame containing fields from both inputs.

    """
    stats = MergeStats(
        operation="merge_datasets",
        input_left=openalex_works_df.height,
        input_right=pure_publications_df.height,
    )
    pure = normalize_doi(pure_publications_df, doi_col_pure, new_col="_norm_doi")
    oa = normalize_doi(openalex_works_df, doi_col_openalex, new_col="_norm_doi")

    merged = oa.join(pure, left_on="_norm_doi", right_on="_norm_doi", how="left", suffix="_pure")
    stats.output_rows = merged.height

    # Count matched/unmatched
    if "_norm_doi" in merged.columns:
        has_match = merged.filter(
            pl.col("_norm_doi").is_not_null() & (pl.col("_norm_doi") != "")
        ).height
        stats.matched = has_match
        stats.unmatched_left = stats.input_left - has_match

    logger.debug("merge_datasets: {}", stats.summary)
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
    authors_df: pl.DataFrame, publications_df: pl.DataFrame
) -> pl.DataFrame:
    """Join author information into publications frames.

    The function expects `authors_df` to include `pure_id`/`internal_repository_id` and boolean
    faculty columns (e.g., 'tnw', 'eemcs'), as well as convenience columns like `faculty`,
    `institute`, `department`, `group`, etc. The publications frame should include an
    `authors` column that is a list of structs where each struct contains
    'internal_repository_id' mapping to the author's pure id.

    Returns:
        pl.DataFrame: publications_df joined with aggregated author-level data.

    """
    if "internal_repository_id" not in authors_df.columns and "pure_id" not in authors_df.columns:
        raise ValueError("authors_df must contain either 'internal_repository_id' or 'pure_id'")
    if "pure_id" not in authors_df.columns:
        authors_df = authors_df.rename({"internal_repository_id": "pure_id"})

    # explode author ids from publications
    pubs_with_author_ids = publications_df.with_columns(
        pl.col("authors")
        .list.eval(pl.element().struct.field("internal_repository_id"))
        .list.drop_nulls()
        .alias("author_pure_ids")
    )
    exploded = pubs_with_author_ids.select(["pure_id", "author_pure_ids"]).explode(
        "author_pure_ids"
    )

    author_details = exploded.join(
        authors_df, left_on="author_pure_ids", right_on="pure_id", how="left"
    )

    # cols to aggregate: we need this defined before normalizing list-like columns
    merge_cols_bool = [
        c
        for c in [
            "dsi",
            "mesa",
            "techmed",
            "eemcs",
            "et",
            "bms",
            "tnw",
            "itc",
        ]
        if c in author_details.columns
    ]

    merge_cols_lists = [
        c
        for c in [
            "faculty",
            "institute",
            "department",
            "group",
            "faculty_abbr",
            "department_abbr",
            "group_abbr",
        ]
        if c in author_details.columns
    ]

    # normalize list-like 'list' columns to real Python lists so they can be flattened and aggregated
    for col in merge_cols_lists:
        if col in author_details.columns:
            vals = author_details[col].to_list()

            def to_list(v):
                if v is None:
                    return []
                if isinstance(v, list):
                    return v
                if isinstance(v, str):
                    return [x.strip() for x in v.split(",") if x.strip()]
                return [v]

            converted = [to_list(v) for v in vals]
            author_details = author_details.with_columns(pl.Series(converted).alias(col))

            # convert to comma separated string per row so aggregation can reuse `str.split` semantics
            def to_str(v):
                if v is None:
                    return ""
                if isinstance(v, list):
                    return ", ".join([str(x) for x in v if x is not None])
                if isinstance(v, str):
                    return v
                return str(v)

            new_vals_str = [to_str(v) for v in converted]
            author_details = author_details.with_columns(pl.Series(new_vals_str).alias(col))

    merge_cols_bool = [
        c
        for c in [
            "dsi",
            "mesa",
            "techmed",
            "eemcs",
            "et",
            "bms",
            "tnw",
            "itc",
        ]
        if c in author_details.columns
    ]

    merge_cols_lists = [
        c
        for c in [
            "faculty",
            "institute",
            "department",
            "group",
            "faculty_abbr",
            "department_abbr",
            "group_abbr",
        ]
        if c in author_details.columns
    ]

    merge_cols_str = [c for c in ["orcid"] if c in author_details.columns]

    # ---- Polars-native aggregation (replaces Python dict loop) ----
    agg_exprs: list[pl.Expr] = []
    for col in merge_cols_bool:
        agg_exprs.append(pl.col(col).any().alias(col))
    for col in merge_cols_lists:
        # Concatenate all string values in the group, then post-process
        agg_exprs.append(pl.col(col).cast(pl.Utf8).fill_null("").str.join(delimiter=",").alias(col))
    for col in merge_cols_str:
        agg_exprs.append(pl.col(col).drop_nulls().unique().alias(col + "s"))

    if not agg_exprs:
        logger.debug("join_authors_and_publications: no columns to aggregate")
        return publications_df

    merged_author_data = author_details.group_by("pure_id").agg(agg_exprs)

    # Post-process list columns: split concatenated strings, deduplicate, rejoin
    for col in merge_cols_lists:
        if col in merged_author_data.columns:
            merged_author_data = merged_author_data.with_columns(
                pl.col(col)
                .str.split(by=",")
                .list.eval(pl.element().str.strip_chars().filter(pl.element() != ""))
                .list.unique()
                .list.sort()
                .list.join(", ")
                .alias(col)
            )
            # Convert empty strings to null
            merged_author_data = merged_author_data.with_columns(
                pl.when(pl.col(col) == "").then(None).otherwise(pl.col(col)).alias(col)
            )

    stats = MergeStats(
        operation="join_authors_and_publications",
        input_left=publications_df.height,
        input_right=authors_df.height,
    )

    final_df = publications_df.join(merged_author_data, on="pure_id", how="left")
    stats.output_rows = final_df.height
    logger.debug("join_authors_and_publications: {}", stats.summary)
    return final_df
