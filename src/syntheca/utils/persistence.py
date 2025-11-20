from __future__ import annotations

import pathlib

import polars as pl

from syntheca.config import settings


def save_dataframe_parquet(df: pl.DataFrame, name: str) -> pathlib.Path:
    """Save the given DataFrame to the cache_dir with given name and return the path.

    Uses parquet for performance and portability.
    """
    cache_dir = pathlib.Path(settings.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    p = cache_dir / f"{name}.parquet"
    df.write_parquet(str(p))
    return p


def load_dataframe_parquet(name: str) -> pl.DataFrame | None:
    """Load a DataFrame from the cache_dir with the provided name or return None if not present.
    """
    p = pathlib.Path(settings.cache_dir) / f"{name}.parquet"
    if not p.exists():
        return None
    return pl.read_parquet(str(p))
