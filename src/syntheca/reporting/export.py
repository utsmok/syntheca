"""Reporting export helpers for writing DataFrame outputs.

This module contains small convenience functions to write Polars DataFrames to
Parquet and formatted Excel files as used by the pipeline and CLI utilities.
Includes logic for column ordering, dropping, and URL cleanup.
"""

from __future__ import annotations

import pathlib
from typing import Any

import polars as pl
import polars.selectors as cs


def write_parquet(df: pl.DataFrame, path: str | pathlib.Path) -> pathlib.Path:
    """Write a Polars DataFrame to Parquet.

    Args:
        df (pl.DataFrame): The DataFrame to write.
        path (str | pathlib.Path): Path to the output parquet file.

    Returns:
        pathlib.Path: Path object pointing to the file written.

    """
    p = pathlib.Path(path)
    if p.is_dir():
        raise ValueError(f"Cannot write parquet into a directory: {p}")
    df.write_parquet(str(p))
    return p


def write_formatted_excel(
    df: pl.DataFrame,
    path: str | pathlib.Path,
    *,
    reorder_columns: bool = True,
    drop_nested_columns: bool = True,
    cleanup_urls: bool = True,
) -> pathlib.Path:
    """Write a Polars DataFrame to an Excel workbook with formatting.

    Replicates the monolith's export_data logic:
    - Reorder columns to put important ones first
    - Drop unnecessary nested/complex columns
    - Strip "https://" from URL columns for Excel compatibility
    - Apply proper column formats

    Args:
        df (pl.DataFrame): DataFrame to export to Excel.
        path (str | pathlib.Path): Path to write the Excel file to.
        reorder_columns (bool): Whether to reorder columns (default True).
        drop_nested_columns (bool): Whether to drop complex nested columns (default True).
        cleanup_urls (bool): Whether to strip "https://" from URLs (default True).

    Returns:
        pathlib.Path: Path object pointing to the file written.

    """
    p = pathlib.Path(path)
    if p.suffix.lower() not in (".xlsx", ".xlsm", ".xls"):
        p = p.with_suffix(".xlsx")

    # Make a copy to avoid modifying the original
    df_export = df.clone()

    # Drop unnecessary nested/complex columns (from monolith)
    if drop_nested_columns:
        drop_cols = [
            "authorships", "authors", "locations", "sustainable_development_goals",
            "referenced_works", "counts_by_year", "locations_count", "topics",
            "primary_topic", "cited_by_percentile_year", "citation_normalized_percentile",
            "apc_paid", "apc_list", "corresponding_institution_ids",
            "corresponding_author_ids", "indexed_in", "language",
            "referenced_works_count", "grants",
        ]
        df_export = df_export.drop([col for col in drop_cols if col in df_export.columns])

    # Reorder columns to put important ones first (from monolith)
    if reorder_columns:
        first_cols = [
            "doi_url", "id", "deal_oils", "listed_apc_usd", "paid_apc_usd",
            "ut_is_corresponding", "oa_color", "license", "publishers",
            "primary_host_org", "all_host_orgs", "publisher_oils",
            "oils_match", "openalex_match", "pure_match",
            "publication_year_oa", "year_oils", "publication_year", "faculty_abbr",
        ]
        # Get all columns, filter out first_cols, then prepend first_cols
        all_cols = list(df_export.columns)
        remaining_cols = [col for col in all_cols if col not in first_cols]
        ordered_cols = [col for col in first_cols if col in all_cols] + remaining_cols
        df_export = df_export.select(ordered_cols)

    # Strip "https://" from URL columns for Excel compatibility (from monolith)
    if cleanup_urls:
        url_cols = ["id", "doi", "doi_url", "url"]
        url_exprs = []
        for col in url_cols:
            if col in df_export.columns:
                url_exprs.append(
                    pl.col(col).cast(pl.Utf8).str.replace("https://", "").alias(col)
                )
        if url_exprs:
            df_export = df_export.with_columns(url_exprs)

    # Use polars native write_excel with formatting
    # The monolith uses column_formats={~cs.temporal(): "General"}
    # which means non-temporal columns get General format
    dtype_formats: dict[Any, str] = {pl.Date: "YYYY-MM-DD", pl.Datetime: "YYYY-MM-DD HH:MM:SS"}
    column_formats = {~cs.temporal(): "General"}

    df_export.write_excel(
        str(p),
        worksheet="data",
        autofit=True,
        dtype_formats=dtype_formats,
        column_formats=column_formats,
    )
    return p
