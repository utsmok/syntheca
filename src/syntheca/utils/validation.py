"""Small validation and normalization helpers for Polars DataFrames.

The goal is to centralize common defensive patterns (normalizing list vs scalar
columns, ensuring column presence, and simple type coercion) to reduce
repetition in processing modules.
"""

from __future__ import annotations

import typing

import polars as pl


def _coerce_to_str_scalar(value: typing.Any) -> str | None:
    """Coerce a possibly-list/string/None value into a single string or None.

    - If input is None: returns None.
    - If input is a list: returns the first element coerced to string or None when empty.
    - If input is a scalar (str): returns as-is.
    """
    if value is None:
        return None
    if isinstance(value, list):
        if not value:
            return None
        return str(value[0]) if value[0] is not None else None
    return str(value)


def normalize_str_column(df: pl.DataFrame, column: str) -> pl.DataFrame:
    """Normalize a column so its values are scalar strings (or None).

    Works if the column is missing or contains lists, strings, or None.
    If the column does not exist, it is added as a None column.
    """
    if column not in df.columns:
        return df.with_columns(pl.lit(None).alias(column))
    vals = df[column].to_list()
    coerced = [_coerce_to_str_scalar(v) for v in vals]
    return df.with_columns(pl.Series(coerced).alias(column))


def ensure_columns(df: pl.DataFrame, cols: dict[str, type]) -> pl.DataFrame:
    """Ensure DataFrame has the given columns; add them as None of the right type when missing.

    Args:
        df: Polars DataFrame
        cols: Mapping of column name -> Python type or polars dtype
    Returns:
        DataFrame with guaranteed columns.

    """
    out = df
    for name, _typ in cols.items():
        if name not in out.columns:
            out = out.with_columns(pl.lit(None).cast(pl.Utf8).alias(name))
    return out


def normalize_orgs_df(orgs_df: pl.DataFrame) -> pl.DataFrame:
    """Normalize the orgs dataframe schema and columns for consistent downstream use.

    - Ensures `internal_repository_id`, `name`, `parent_org` are present.
    - Normalizes `name` and `parent_org` to scalar utf8 strings.
    - Leaves boolean columns as-is.
    """
    if orgs_df is None or orgs_df.height == 0:
        return pl.DataFrame()

    # Ensure required columns present
    required = {"internal_repository_id": str, "name": str, "parent_org": str}
    out = ensure_columns(orgs_df, required)

    # Normalize list-like columns that must be string scalars.
    out = normalize_str_column(out, "name")
    out = normalize_str_column(out, "parent_org")

    return out
