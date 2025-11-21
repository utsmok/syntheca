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

    agg_exprs = [pl.col(col).any().alias(col) for col in merge_cols_bool]
    agg_exprs.extend(
        [
            pl.col(col)
            .str.split(by=", ")
            .flatten()
            .unique()
            .replace("", None)
            .drop_nulls()
            .list.join(", ")
            .alias(col)
            for col in merge_cols_lists
        ]
    )
    agg_exprs.extend([pl.col(col).drop_nulls().unique().alias(col + "s") for col in merge_cols_str])

    # Build aggregation using Python to avoid complex polars list/str coercions in the groupby
    grouped = {}
    for row in author_details.to_dicts():
        key = row.get("pure_id")
        if key not in grouped:
            grouped[key] = {"pure_id": key}
            # initialize booleans
            for col in merge_cols_bool:
                grouped[key][col] = False
            # initialize lists
            for col in merge_cols_lists:
                grouped[key][col] = []
            for col in merge_cols_str:
                grouped[key][col + "s"] = []
        # aggregate booleans
        for col in merge_cols_bool:
            if row.get(col):
                grouped[key][col] = True
        # aggregate lists
        for col in merge_cols_lists:
            val = row.get(col)
            if val:
                if isinstance(val, list):
                    grouped[key][col].extend(val)
                else:
                    # split strings on comma
                    grouped[key][col].extend([x.strip() for x in str(val).split(",") if x.strip()])
        for col in merge_cols_str:
            v = row.get(col)
            if v:
                grouped[key][col + "s"].append(v)

    # prepare list of dicts
    merged_records = []
    for k, v in grouped.items():
        rec = {"pure_id": k}
        for col in merge_cols_bool:
            rec[col] = v.get(col, False)
        for col in merge_cols_lists:
            items = list({x for x in v.get(col, []) if x})
            rec[col] = ", ".join(sorted(items)) if items else None
        for col in merge_cols_str:
            items = list({x for x in v.get(col + "s", []) if x})
            rec[col + "s"] = items if items else None
        merged_records.append(rec)

    merged_author_data = pl.from_dicts(merged_records) if merged_records else pl.DataFrame()

    final_df = publications_df.join(merged_author_data, on="pure_id", how="left")
    return final_df
