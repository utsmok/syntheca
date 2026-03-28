"""Helpers to persist Polars DataFrames to a cache directory.

This module provides `save_dataframe_parquet` and `load_dataframe_parquet` to
write and read DataFrames to/from the configured project cache directory.
"""

from __future__ import annotations

import pathlib
import time

import polars as pl
from polars.exceptions import SchemaError, ShapeError
from loguru import logger

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
    _t0 = time.monotonic()
    df.write_parquet(str(p))
    logger.info(
        "persistence: wrote {} rows to {} ({:.1f}s)", df.height, p.name, time.monotonic() - _t0
    )
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
        logger.info("persistence: file not found for {}", name)
        return None
    _t0 = time.monotonic()
    result = pl.read_parquet(str(p))
    logger.info(
        "persistence: loaded {} ({} rows, {:.1f}s)", p.name, result.height, time.monotonic() - _t0
    )
    return result


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
    _t0 = time.monotonic()
    if p.exists():
        existing = pl.read_parquet(str(p))
        df = pl.concat([existing, df])
    df.write_parquet(str(p))
    logger.info(
        "persistence: appended {} rows to {} ({:.1f}s)", df.height, p.name, time.monotonic() - _t0
    )
    return p


def save_parquet_chunk(name: str, chunk_index: int, df: pl.DataFrame) -> pathlib.Path:
    """Write a numbered chunk file under the ``_chunks/<name>/`` subdirectory.

    Each call produces a standalone parquet file; no ``pl.concat`` is
    performed during writes.  This avoids struct-schema mismatches between
    chunks that would occur with :func:`append_to_parquet`.

    Args:
        name: Logical dataset name (without .parquet suffix).
        chunk_index: Zero-based chunk number (used for file naming and ordering).
        df: DataFrame to write as a chunk.

    Returns:
        Path to the written chunk file.
    """
    chunk_dir = pathlib.Path(settings.cache_dir) / "_chunks" / name
    chunk_dir.mkdir(parents=True, exist_ok=True)
    p = chunk_dir / f"{chunk_index:04d}.parquet"
    _t0 = time.monotonic()
    df.write_parquet(str(p))
    logger.info(
        "persistence: wrote chunk {} for {} ({} rows, {:.1f}s)",
        chunk_index,
        name,
        df.height,
        time.monotonic() - _t0,
    )
    return p


def load_parquet_all(name: str) -> pl.DataFrame | None:
    """Load data from either chunk files or a single consolidated parquet.

    Checks for chunk files first (from an in-progress or interrupted run),
    then falls back to the single consolidated file.  Corrupted or
    unreadable chunk files are skipped so a single bad chunk does not
    prevent loading the rest.

    Args:
        name: Logical dataset name (without .parquet suffix).

    Returns:
        The loaded DataFrame, or ``None`` when no data is found.
    """
    # Prefer chunks (in-progress or crash-recovery data)
    _t0 = time.monotonic()
    chunk_dir = pathlib.Path(settings.cache_dir) / "_chunks" / name
    if chunk_dir.exists():
        files = sorted(chunk_dir.glob("*.parquet"))
        if files:
            loaded: list[pl.DataFrame] = []
            for f in files:
                try:
                    loaded.append(pl.read_parquet(str(f)))
                except Exception:
                    pass  # skip corrupted / partially-written chunks
            if loaded:
                try:
                    result = pl.concat(loaded)
                except (ShapeError, SchemaError):
                    # Fallback: struct schemas or column types may differ
                    # across chunks from interrupted / pre-fix runs or when
                    # sparse and dense items land in different chunks.
                    result = pl.concat(loaded, how="diagonal_relaxed")
                logger.info(
                    "persistence: loaded {} chunks for {} ({} total rows, {:.1f}s)",
                    len(files),
                    name,
                    result.height,
                    time.monotonic() - _t0,
                )
                return result

    # Fall back to consolidated single file
    single = load_dataframe_parquet(name)
    if single is None:
        logger.info("persistence: no data found for {}", name)
    return single


def cleanup_chunks(name: str) -> None:
    """Remove the ``_chunks/<name>/`` directory after consolidation.

    Safe to call even when no chunks exist.

    Args:
        name: Logical dataset name (without .parquet suffix).
    """
    import shutil

    chunk_dir = pathlib.Path(settings.cache_dir) / "_chunks" / name
    if chunk_dir.exists():
        files = list(chunk_dir.glob("*.parquet"))
        shutil.rmtree(chunk_dir)
        logger.info("persistence: cleaned up {} chunk files for {}", len(files), name)
