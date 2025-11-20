from __future__ import annotations

import pathlib
from typing import Any

import polars as pl


def write_parquet(df: pl.DataFrame, path: str | pathlib.Path) -> pathlib.Path:
    """Write DataFrame to a parquet file.

    Returns the path written.
    """
    p = pathlib.Path(path)
    if p.is_dir():
        raise ValueError(f"Cannot write parquet into a directory: {p}")
    df.write_parquet(str(p))
    return p


def write_formatted_excel(df: pl.DataFrame, path: str | pathlib.Path) -> pathlib.Path:
    """Write DataFrame to an Excel workbook with some basic formatting.

    Implementation notes:
    - Uses polars' to_pandas() and pandas/xlsxwriter for column sizing and format.
    - This keeps the heavy-lifting in polars while using the robust Excel writer in pandas.
    """
    p = pathlib.Path(path)
    if p.suffix.lower() not in (".xlsx", ".xlsm", ".xls"):
        p = p.with_suffix(".xlsx")

    # Use polars native write_excel with some default formatting.
    # Build column widths using an autofit approach; polars supports `autofit=True`
    # so we rely on that behaviour, and provide a dtype_formats for dates.
    dtype_formats: dict[Any, str] = {pl.Date: "yyyy-mm-dd"}
    df.write_excel(str(p), worksheet="data", autofit=True, dtype_formats=dtype_formats)
    return p
