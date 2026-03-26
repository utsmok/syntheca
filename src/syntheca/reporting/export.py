"""Reporting export helpers for writing DataFrame outputs.

This module contains small convenience functions to write Polars DataFrames to
Parquet and formatted Excel files as used by the pipeline and CLI utilities.
"""

from __future__ import annotations

import pathlib
from typing import Any

import polars as pl


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


def write_formatted_excel(df: pl.DataFrame, path: str | pathlib.Path) -> pathlib.Path:
    """Write a Polars DataFrame to an Excel workbook with basic formatting.

    Uses `polars` `write_excel` which internally delegates to pandas/xlsxwriter
    for the writer. The function sets a reasonable default for date formatting
    and attempts to autofit columns when supported.

    Args:
        df (pl.DataFrame): DataFrame to export to Excel.
        path (str | pathlib.Path): Path to write the Excel file to.

    Returns:
        pathlib.Path: Path object pointing to the file written.

    """
    p = pathlib.Path(path)
    if p.suffix.lower() not in (".xlsx", ".xlsm", ".xls"):
        p = p.with_suffix(".xlsx")

    # Use polars native write_excel with some default formatting.
    # Build column widths using an autofit approach; polars supports `autofit=True`
    # so we rely on that behaviour, and provide a dtype_formats for dates.
    dtype_formats: dict[Any, str] = {pl.Date: "YYYY-MM-DD"}
    df.write_excel(str(p), worksheet="data", autofit=True, dtype_formats=dtype_formats)
    return p


def save_coauthorship_report(
    report: CoauthorshipReport,  # noqa: F821 — forward ref to avoid circular import
    output_dir: str | pathlib.Path,
) -> list[pathlib.Path]:
    """Write all DataFrames from a CoauthorshipReport to Parquet files.

    Creates a ``coauthorship/`` subdirectory under *output_dir* and writes
    each report component as a separate Parquet file.

    Args:
        report: A :class:`~syntheca.analysis.coauthorship.CoauthorshipReport`.
        output_dir: Base output directory.

    Returns:
        List of paths to the written Parquet files.
    """
    from syntheca.analysis.coauthorship import CoauthorshipReport  # deferred import

    if not isinstance(report, CoauthorshipReport):
        raise TypeError(f"Expected CoauthorshipReport, got {type(report).__name__}")

    base = pathlib.Path(output_dir) / "coauthorship"
    base.mkdir(parents=True, exist_ok=True)

    written: list[pathlib.Path] = []
    frames = {
        "author_publication_links": report.author_publication_links,
        "coauthor_edges": report.coauthor_edges,
        "ut_vs_external": report.ut_vs_external,
        "university_rollup": report.university_rollup,
        "company_rollup": report.company_rollup,
        "country_rollup": report.country_rollup,
    }
    for name, df in frames.items():
        path = write_parquet(df, base / f"{name}.parquet")
        written.append(path)

    return written
