from __future__ import annotations

import pathlib

import polars as pl

from syntheca.config import settings


def load_faculty_mapping() -> dict[str, str]:
    """Load the faculty mapping from configuration.

    Returns a mapping from full faculty name -> short name (eg "Faculty of Science and Technology" -> "tnw").
    """

    path = settings.faculties_mapping_path
    data = {}
    if path.exists():
        import json

        with pathlib.Path(path).open(encoding="utf8") as fh:
            data = json.load(fh)
    mapping = data.get("mapping", {}) if isinstance(data, dict) else {}
    return mapping


def enrich_authors_with_faculties(authors_df: pl.DataFrame) -> pl.DataFrame:
    """Add boolean faculty columns to `authors_df`.

    The function will look for a column named `affiliation_names_pure` (a list[str] per row)
    with organization names. For each mapping in `faculties.json` we add a new boolean column
    with the mapping's short code.

    We intentionally keep this function pure and defensive — if the source column is missing,
    returns the DataFrame unchanged.
    """

    mapping = load_faculty_mapping()
    if not mapping:
        return authors_df

    if "affiliation_names_pure" not in authors_df.columns:
        # nothing to do
        return authors_df

    # Add a boolean column per short name
    exprs = []
    for full_name, short in mapping.items():
        # Use list.contains for lists of affiliation names; ensures set membership semantics.
        exprs.append(pl.col("affiliation_names_pure").list.contains(full_name).alias(short))

    return authors_df.with_columns(exprs)
