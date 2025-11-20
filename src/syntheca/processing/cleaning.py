"""Publication cleaning utilities for the syntheca pipeline.

This module contains small, testable functions that normalize DOI values and
perform basic date parsing required by the pipeline's cleaning stage.
"""

from __future__ import annotations

import json
import pathlib

import polars as pl

from syntheca.config import settings


def normalize_doi(df: pl.DataFrame, col_name: str, new_col: str | None = None) -> pl.DataFrame:
    """Normalize DOIs in a DataFrame column.

    This helper lowercases DOIs, removes `https://doi.org/` prefixes and trims
    whitespace, returning a new DataFrame.

    Args:
        df (pl.DataFrame): Input Polars DataFrame.
        col_name (str): Name of the column containing DOI strings.
        new_col (str | None): Optional column name to write normalized DOIs into.
            If `None`, the original column is overwritten.

    Returns:
        pl.DataFrame: A new DataFrame with normalized DOI values.

    """
    new_col = new_col or col_name
    if col_name not in df.columns:
        # Create placeholder column with nulls to allow later joins/ops to proceed
        return df.with_columns(pl.lit(None).cast(pl.Utf8).alias(new_col))

    return df.with_columns(
        pl.col(col_name)
        .cast(pl.Utf8)
        .fill_null("")
        .str.replace("https://doi.org/", "")
        .str.to_lowercase()
        .str.strip_chars()
        .alias(new_col)
    )


def load_publisher_mapping() -> dict[str, str]:
    """Load the publisher mapping from file in `settings.publishers_mapping_path`.

    Returns:
        dict[str, str]: A flipped mapping from variant publisher name to the
            canonical name (e.g., "Elsevier B.V." -> "Elsevier").

    """
    path = settings.publishers_mapping_path
    data = {}
    if path.exists():
        with pathlib.Path(path).open(encoding="utf8") as fh:
            data = json.load(fh)
    # Flip the mapping: from {canonical: [variants]} to {variant: canonical}
    flipped = {}
    for canonical, variants in data.items():
        for variant in variants:
            flipped[variant] = canonical
    return flipped


def clean_publications(df: pl.DataFrame) -> pl.DataFrame:
    """Perform basic cleaning on publication DataFrames.

    Aligns with the legacy monolith logic including:
    - Filter out rows without DOIs
    - Rename internal_repository_id to pure_id
    - Normalize DOI and publication dates
    - Apply publisher name mapping
    - Extract ISSN/ISBN from nested structures
    - Parse journal/source title from part_of
    - Drop fully null columns and unnecessary fields
    - Convert list columns to joined strings where appropriate

    Args:
        df (pl.DataFrame): Input Polars DataFrame representing publications.

    Returns:
        pl.DataFrame: A cleaned DataFrame matching monolith transformations.

    """
    # Handle empty or None input defensively
    if df is None or df.is_empty():
        # Return empty DF with minimal expected schema to prevent join errors
        return pl.DataFrame(
            schema={
                "doi": pl.Utf8,
                "title": pl.Utf8,
                "pure_id": pl.Utf8,
                "publication_year": pl.Int64,
            }
        )

    out = df.clone()

    # Filter out rows without DOIs (matching monolith)
    if "doi" in out.columns:
        out = out.filter(pl.col("doi").is_not_null())

    # Rename internal_repository_id to pure_id
    if "internal_repository_id" in out.columns:
        out = out.rename({"internal_repository_id": "pure_id"})

    # Handle known bad date values (from monolith)
    if "publication_date" in out.columns:
        out = out.with_columns(
            pl.col("publication_date")
            .cast(pl.Utf8)
            .str.replace("2-01-23", "2023-01-02")
            .str.replace("3-03-15", "2015-03-03")
        )

    # Normalize DOI
    if "doi" in out.columns:
        out = out.with_columns(
            pl.col("doi")
            .cast(pl.Utf8)
            .str.to_lowercase()
            .str.replace("https://doi.org/", "")
            .str.strip_chars()
        )

    # Parse publication dates
    if "publication_date" in out.columns:
        out = out.with_columns(
            pl.coalesce(
                [
                    pl.col("publication_date").cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d", strict=False),
                    pl.col("publication_date").cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m", strict=False),
                    pl.col("publication_date").cast(pl.Utf8).str.strptime(pl.Date, "%Y", strict=False),
                ]
            ).alias("publication_date_cleaned")
        )
        out = out.with_columns(pl.col("publication_date_cleaned").dt.year().alias("publication_year"))

    # Drop fully null columns (matching monolith)
    non_null_cols = [
        col for col in out.columns
        if out.select(pl.col(col).is_not_null().any()).to_series()[0]
    ]
    out = out.select(non_null_cols)

    # Drop unnecessary columns (from monolith)
    drop_cols = [
        "language", "number", "start_page", "edition", "issue",
        "isi", "endpage", "references"
    ]
    out = out.drop([col for col in drop_cols if col in out.columns])

    # Convert list columns to strings (from monolith)
    list_cols_to_reduce = ["title", "subtitle", "publishers", "license"]
    for col in list_cols_to_reduce:
        if col in out.columns and (out.schema[col] == pl.List(pl.Utf8) or str(out.schema[col]).startswith("List")):
            # Check if it's a list type
            out = out.with_columns(pl.col(col).list.join("; "))

    # Apply publisher mapping (from monolith)
    if "publishers" in out.columns:
        mapping = load_publisher_mapping()
        if mapping:
            out = out.with_columns(pl.col("publishers").replace_strict(mapping, default=pl.col("publishers")))

    # ISSN extraction (from monolith)
    if "issn" in out.columns and "pure_id" in out.columns:
        try:
            issn_list = out.select(["pure_id", "issn"]).to_dicts()
            issn_list = [x for x in issn_list if x.get("issn")]
            flat_list = []
            for item in issn_list:
                issns = set()
                issn_val = item["issn"]
                if isinstance(issn_val, list):
                    for entry in issn_val:
                        if isinstance(entry, dict):
                            if entry.get("Print"):
                                issns.add(entry["Print"])
                            if entry.get("Online"):
                                issns.add(entry["Online"])
                if issns:
                    flat_list.append({"pure_id": item["pure_id"], "issns": list(issns)})

            if flat_list:
                issn_df = pl.from_dicts(flat_list)
                out = out.join(issn_df, on="pure_id", how="left")
            out = out.drop("issn")
        except Exception:
            # If ISSN parsing fails, just drop the column
            out = out.drop("issn")

    # ISBN extraction (from monolith)
    if "isbn" in out.columns:
        try:
            # Extract 'value' field from list of structs
            out = out.with_columns(
                pl.col("isbn")
                .list.eval(pl.element().struct.field("value"))
                .list.drop_nulls()
                .list.unique()
                .alias("isbns")
            )
            out = out.drop("isbn")
        except Exception:
            # If ISBN parsing fails, just drop the column
            out = out.drop("isbn")

    # Parse part_of for journal/source information (from monolith)
    if "part_of" in out.columns:
        try:
            out = out.with_columns(
                pl.col("part_of").struct.field("cerif:Publication").alias("part_of")
            )
            out = out.with_columns(pl.col("part_of").struct.unnest())

            # Rename extracted fields
            rename_map = {}
            if "cerif:Title" in out.columns:
                rename_map["cerif:Title"] = "journal_name"
            if "cerif:Subtitle" in out.columns:
                rename_map["cerif:Subtitle"] = "journal_extra"
            if rename_map:
                out = out.rename(rename_map)

            # Extract text from nested dicts
            if "journal_name" in out.columns:
                out = out.with_columns(
                    pl.col("journal_name").struct.field("#text").alias("source_title")
                )
            if "journal_extra" in out.columns:
                out = out.with_columns(
                    pl.col("journal_extra").struct.field("#text").alias("source_subtitle")
                )

            # Drop intermediate columns
            drop_intermediate = ["part_of", "journal_extra", "journal_name", "pubt:Type"]
            out = out.drop([col for col in drop_intermediate if col in out.columns])
        except Exception:
            # If part_of parsing fails, just drop the column
            if "part_of" in out.columns:
                out = out.drop("part_of")

    return out
