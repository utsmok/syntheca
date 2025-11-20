"""Merging helpers to join and deduplicate publication datasets.

This module contains functions which normalize DOIs across DataFrames and
merge/deduplicate datasets using DOI as primary key with title fallback.
Includes logic for OILS merging, author name extraction, and manual corrections.
"""

from __future__ import annotations

import json
import pathlib

import polars as pl

from syntheca.config import settings
from syntheca.processing.cleaning import normalize_doi


def merge_datasets(
    oils_df: pl.DataFrame,
    full_df: pl.DataFrame,
    doi_col_oils: str = "doi",
    doi_col_full: str = "doi",
) -> pl.DataFrame:
    """Join two DataFrames on normalized DOIs (legacy behavior).

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


def merge_oils_with_all(oils_df: pl.DataFrame, full_df: pl.DataFrame) -> pl.DataFrame:
    """Merge OILS dataset with full publication dataset using full outer join.

    This function replicates the monolith's `merge_oils_with_all` logic:
    - Renames columns in OILS dataset
    - Adds "_oils" suffix to all OILS columns
    - Performs full outer join on DOI
    - Adds match tracking columns (openalex_match, oils_match, pure_match)

    Args:
        oils_df (pl.DataFrame): The OILS dataset with usage data.
        full_df (pl.DataFrame): The full publications dataset (Pure/OpenAlex merged).

    Returns:
        pl.DataFrame: Merged DataFrame with match tracking columns.

    """
    # Rename specific OILS columns
    rename_dict = {
        "Title_1": "Journal",
        "Keywords (free keywords)": "Keywords",
        "Pure ID": "PureID",
        "DOI": "doi",
    }
    rename_dict = {k: v for k, v in rename_dict.items() if k in oils_df.columns}
    oils = oils_df.rename(rename_dict)

    # Add "_oils" suffix to all OILS columns
    oils = oils.rename(
        {col: (col + "_oils").lower().replace(" ", "_") for col in oils.columns}
    )

    # Full outer join on DOI
    merged = full_df.join(oils, left_on="doi", right_on="doi_oils", how="full")

    # Add match tracking columns
    match_expressions = []
    if "id" in merged.columns:
        match_expressions.append(
            pl.when(pl.col("id").is_not_null())
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("openalex_match")
        )
    if "pureid_oils" in merged.columns:
        match_expressions.append(
            pl.when(pl.col("pureid_oils").is_not_null())
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("oils_match")
        )
    if "pure_id" in merged.columns:
        match_expressions.append(
            pl.when(pl.col("pure_id").is_not_null())
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("pure_match")
        )

    if match_expressions:
        merged = merged.with_columns(match_expressions)

    return merged


def extract_author_and_funder_names(df: pl.DataFrame) -> pl.DataFrame:
    """Extract author and funder display names from nested struct columns.

    Replicates the monolith's `extract_author_and_funder_names` logic:
    - Extracts display names and ORCIDs from OpenAlex authorships
    - Extracts funder display names from funders struct
    - Extracts Pure author names from authors struct

    Args:
        df (pl.DataFrame): DataFrame with nested authorship/funder structs.

    Returns:
        pl.DataFrame: DataFrame with extracted name columns added.

    """
    extractions = []

    # Extract OpenAlex author names and ORCIDs
    if "authorships" in df.columns:
        try:
            if df["authorships"].dtype == pl.Struct or str(df["authorships"].dtype).startswith("List(Struct"):
                extractions.extend([
                    pl.col("authorships")
                    .list.eval(
                        pl.element().struct.field("author").struct.field("display_name").drop_nulls()
                    )
                    .alias("oa_authors_names"),
                    pl.col("authorships")
                    .list.eval(
                        pl.element().struct.field("author").struct.field("orcid").drop_nulls()
                    )
                    .alias("oa_authors_orcids"),
                ])
        except Exception:
            pass

    # Extract funder names
    if "funders" in df.columns:
        try:
            if df["funders"].dtype == pl.Struct or str(df["funders"].dtype).startswith("List(Struct"):
                extractions.append(
                    pl.col("funders")
                    .list.eval(pl.element().struct.field("display_name").drop_nulls())
                    .alias("funders")
                )
        except Exception:
            pass

    # Extract Pure author names
    if "authors" in df.columns:
        try:
            if df["authors"].dtype == pl.Struct or str(df["authors"].dtype).startswith("List(Struct"):
                extractions.append(
                    pl.col("authors")
                    .list.eval(
                        pl.concat_str([
                            pl.element().struct.field("first_names"),
                            pl.element().struct.field("family_names"),
                        ], separator=" ").drop_nulls()
                    )
                    .alias("pure_authors_names")
                )
        except Exception:
            pass

    if extractions:
        return df.with_columns(extractions)
    return df


def add_missing_affils(df: pl.DataFrame, more_data: list[dict] | None = None) -> pl.DataFrame:
    """Apply manual affiliation corrections for specific authors.

    Replicates the monolith's `add_missing_affils` logic:
    - Loads affiliation corrections from config or uses provided data
    - Parses faculty/department/group abbreviations
    - Updates boolean faculty columns and abbreviation lists for matched authors

    Args:
        df (pl.DataFrame): DataFrame with pure_authors_names column.
        more_data (list[dict] | None): Optional list of correction dicts,
            defaults to loading from corrections.json.

    Returns:
        pl.DataFrame: DataFrame with updated affiliation data.

    """
    if more_data is None:
        # Load from corrections.json
        path = settings.corrections_mapping_path
        if path.exists():
            with pathlib.Path(path).open(encoding="utf8") as fh:
                more_data = json.load(fh)
        else:
            return df

    if not more_data or "pure_authors_names" not in df.columns:
        return df

    bool_cols = ["tnw", "eemcs", "et", "bms", "itc", "dsi", "techmed", "mesa"]
    list_cols = ["faculty_abbr", "department_abbr", "group_abbr", "institute"]

    updates = []
    for entry in more_data:
        name = entry.get("name")
        affils = entry.get("affiliations", [])
        if not name or not affils:
            continue

        update_dict = dict.fromkeys(bool_cols, False)
        update_dict.update(dict.fromkeys(list_cols))
        update_dict["name"] = name

        for abbr in affils:
            if "-" in abbr:
                parts = str(abbr).split("-")
                if len(parts) >= 3:
                    update_dict["faculty_abbr"] = parts[0]
                    update_dict["department_abbr"] = parts[1]
                    update_dict["group_abbr"] = parts[2]
                    fac_lower = parts[0].lower()
                    if fac_lower in bool_cols:
                        update_dict[fac_lower] = True
            else:
                update_dict["institute"] = abbr
                abbr_lower = abbr.lower()
                if abbr_lower in bool_cols:
                    update_dict[abbr_lower] = True
        updates.append(update_dict)

    if not updates:
        return df

    updates_df = pl.from_dicts(updates)

    # Explode authors, join updates, then aggregate back
    df = df.explode("pure_authors_names").join(
        updates_df,
        left_on="pure_authors_names",
        right_on="name",
        how="left",
        suffix="_upd",
    )

    # Append new unique values to list[str] cols
    list_exprs = []
    for col in list_cols:
        if col + "_upd" in df.columns and col in df.columns:
            list_exprs.append(
                pl.when(pl.col(col + "_upd").is_not_null())
                .then(pl.concat_list([pl.col(col), pl.col(col + "_upd")]).list.unique())
                .otherwise(pl.col(col))
                .alias(col)
            )
    if list_exprs:
        df = df.with_columns(list_exprs)
        df = df.drop([name + "_upd" for name in list_cols if name + "_upd" in df.columns])

    # Logical OR for bool cols
    bool_exprs = []
    for col in bool_cols:
        if col + "_upd" in df.columns and col in df.columns:
            bool_exprs.append(
                pl.when(pl.col(col + "_upd").is_not_null())
                .then(pl.col(col).or_(pl.col(col + "_upd")))
                .otherwise(pl.col(col))
                .alias(col)
            )
    if bool_exprs:
        df = df.with_columns(bool_exprs)
        df = df.drop([name + "_upd" for name in bool_cols if name + "_upd" in df.columns])

    # Drop 'name' if it was added by the join
    if "name" in df.columns:
        df = df.drop("name")

    # Undo explode by grouping
    group_cols = [col for col in df.columns if col != "pure_authors_names"]
    if group_cols:
        df = df.group_by(group_cols).agg(pl.col("pure_authors_names"))

    return df


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
