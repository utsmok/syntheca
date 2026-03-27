"""Reporting export helpers for writing DataFrame outputs.

This module contains small convenience functions to write Polars DataFrames to
Parquet and formatted Excel files as used by the pipeline and CLI utilities.
"""

from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING, Any

import polars as pl

from syntheca.utils.polars_frames import robust_from_dicts

if TYPE_CHECKING:
    from syntheca.analysis.coauthorship import CoauthorshipReport
    from syntheca.analysis.policy_citations import PolicyCitationReport
    from syntheca.comparison.scopus import ComparisonResult


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
    report: CoauthorshipReport,
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


def save_comparison_result(
    result: ComparisonResult,
    output_dir: str | pathlib.Path,
) -> list[pathlib.Path]:
    """Write all DataFrames from a ComparisonResult to Parquet files.

    Args:
        result: A :class:`~syntheca.comparison.scopus.ComparisonResult`.
        output_dir: Base output directory.

    Returns:
        List of paths written to the ``comparison/`` output group.
    """
    from syntheca.comparison.scopus import ComparisonResult  # deferred import

    if not isinstance(result, ComparisonResult):
        raise TypeError(f"Expected ComparisonResult, got {type(result).__name__}")

    base = pathlib.Path(output_dir) / "comparison"
    base.mkdir(parents=True, exist_ok=True)

    written: list[pathlib.Path] = []
    frames = {
        "scopus_matched": _ensure_comparison_doi_column(result.matched),
        "scopus_only": result.scopus_only,
        "internal_only": result.internal_only,
        "scopus_mismatches": result.mismatch_details,
    }
    for name, df in frames.items():
        path = write_parquet(df, base / f"{name}.parquet")
        written.append(path)

    return written


def save_policy_citation_report(
    report: PolicyCitationReport,
    output_dir: str | pathlib.Path,
) -> list[pathlib.Path]:
    """Write policy-citation outputs to the documented output group.

    Args:
        report: A :class:`~syntheca.analysis.policy_citations.PolicyCitationReport`.
        output_dir: Base output directory.

    Returns:
        Paths to the candidate CSV and review-queue workbook.
    """
    from syntheca.analysis.policy_citations import (  # deferred import
        PolicyCitationReport,
        export_review_queue,
    )

    if not isinstance(report, PolicyCitationReport):
        raise TypeError(f"Expected PolicyCitationReport, got {type(report).__name__}")

    base = pathlib.Path(output_dir) / "policy_citations"
    base.mkdir(parents=True, exist_ok=True)

    candidates_path = base / "policy_candidates.csv"
    review_path = base / "policy_review_queue.xlsx"

    rows = [candidate.model_dump() for candidate in report.candidates]
    for row in rows:
        row["evidence"] = "; ".join(row.get("evidence", []))

    if rows:
        robust_from_dicts(rows).write_csv(candidates_path)
    else:
        candidates_path.write_text("", encoding="utf-8")

    export_review_queue(report, review_path)
    return [candidates_path, review_path]


def _ensure_comparison_doi_column(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure comparison exports expose a contract-level ``doi`` column."""
    if "doi" in df.columns:
        return df

    candidate_columns = [
        column for column in ("scopus_doi", "internal_doi") if column in df.columns
    ]
    if not candidate_columns:
        return df.with_columns(pl.lit(None).alias("doi"))

    return df.with_columns(pl.coalesce(candidate_columns).alias("doi"))
