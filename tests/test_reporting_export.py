import datetime
import pathlib

import polars as pl

from syntheca.reporting.export import write_formatted_excel, write_parquet


def test_write_parquet_creates_file(tmp_path: pathlib.Path):
    df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    p = tmp_path / "out.parquet"
    res = write_parquet(df, p)
    assert res.exists()


def test_write_formatted_excel_creates_file_with_date(tmp_path: pathlib.Path):
    df = pl.DataFrame({"a": [1, 2], "d": [datetime.date(2020, 1, 1), datetime.date(2021, 2, 2)]})
    p = tmp_path / "out.xlsx"
    res = write_formatted_excel(df, p)
    assert res.exists()
