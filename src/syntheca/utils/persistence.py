"""Helpers to persist Polars DataFrames to a cache directory.

This module provides `save_dataframe_parquet` and `load_dataframe_parquet` to
write and read DataFrames to/from the configured project cache directory.
"""

from __future__ import annotations

import pathlib

import polars as pl

from syntheca.config import settings


def save_dataframe_parquet(df: pl.DataFrame, name: str) -> pathlib.Path:
    """Save a dataframe to the project cache directory as parquet.

    Args:
        df (pl.DataFrame): The DataFrame to persist.
        name (str): Logical name to use for the file; the function will append `.parquet`.

    Returns:
        pathlib.Path: Path to the written parquet file.

    """
    cache_dir = pathlib.Path(settings.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    p = cache_dir / f"{name}.parquet"
    df.write_parquet(str(p))
    return p


def load_dataframe_parquet(name: str) -> pl.DataFrame | None:
    """Load a previously saved parquet file from the cache directory.

    Args:
        name (str): The logical name used to save the parquet file (without suffix).

    Returns:
        pl.DataFrame | None: The loaded DataFrame or `None` when the file isn't present.

    """
    p = pathlib.Path(settings.cache_dir) / f"{name}.parquet"
    if not p.exists():
        return None
    return pl.read_parquet(str(p))


def init_incremental_parquet(name: str, df: pl.DataFrame) -> pathlib.Path:
    """Write the first chunk of data, establishing the parquet file and schema.

    Args:
        name: Logical name (without .parquet suffix).
        df: DataFrame with at least one row to establish column schema.

    Returns:
        Path to the written parquet file.
    """
    return save_dataframe_parquet(df, name)


def append_to_parquet(name: str, df: pl.DataFrame) -> pathlib.Path:
    """Append a DataFrame to an existing parquet file, or create it if missing.

    Uses ``pl.concat`` to merge with existing data.  Callers should batch
    rows (e.g. one append per API batch, not per individual row) to avoid
    O(n^2) read-rewrite overhead.

    Args:
        name: Logical name (without .parquet suffix).
        df: DataFrame to append.

    Returns:
        Path to the written parquet file.
    """
    cache_dir = pathlib.Path(settings.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    p = cache_dir / f"{name}.parquet"
    if p.exists():
        existing = pl.read_parquet(str(p))
        df = pl.concat([existing, df])
    df.write_parquet(str(p))
    return p
