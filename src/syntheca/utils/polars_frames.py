"""Helpers for robust Polars DataFrame materialization from row dictionaries."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import polars as pl
from polars.datatypes.classes import DataTypeClass


def robust_from_dicts(
    rows: Sequence[Mapping[str, Any]] | None,
    *,
    schema: Any = None,
    schema_overrides: Mapping[str, DataTypeClass | pl.DataType] | None = None,
) -> pl.DataFrame:
    """Materialize row dictionaries with full-schema inference.

    Polars samples only a bounded prefix of rows by default when inferring
    schema from dictionaries. External payloads in this project regularly
    contain sparse fields whose first non-null value arrives well after the
    default sample window, or columns whose semantic contract requires explicit
    overrides. This helper centralizes the robust settings used across runtime
    ingestion and export paths.

    Args:
        rows: Row dictionaries to materialize.
        schema: Optional explicit schema passed through to Polars.
        schema_overrides: Optional per-column dtype overrides.

    Returns:
        A Polars DataFrame constructed with full-row schema inference.
    """
    if not rows:
        if schema is not None:
            return pl.DataFrame(schema=schema)
        return pl.DataFrame()

    return pl.from_dicts(
        [dict(row) for row in rows],
        schema=schema,
        schema_overrides=dict(schema_overrides) if schema_overrides is not None else None,
        infer_schema_length=None,
    )
