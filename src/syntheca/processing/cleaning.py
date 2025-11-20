from __future__ import annotations

import polars as pl


def normalize_doi(df: pl.DataFrame, col_name: str, new_col: str | None = None) -> pl.DataFrame:
    """Return a new DataFrame where DOIs in `col_name` are normalized (lowercased,
    remove https://doi.org prefix and whitespace trimmed).

    Parameters
    ----------
    df: pl.DataFrame
        Input Polars DataFrame.
    col_name: str
        Name of the column containing DOI strings.
    new_col: str | None
        Optional. If provided, write normalized DOIs to this column name. If None,
        the original column is overwritten.

    Returns
    -------
    pl.DataFrame
        A new DataFrame with the normalized DOI column.
    """

    new_col = new_col or col_name
    if col_name not in df.columns:
        # Create placeholder column with nulls to allow later joins/ops to proceed
        return df.with_columns(pl.lit(None).cast(pl.Utf8).alias(new_col))

    return df.with_columns(
        pl.col(col_name)
        .cast(pl.Utf8)
        .fill_null("")
        .str.replace("https://doi.org/", "")
        .str.to_lowercase()
        .str.strip_chars()
        .alias(new_col)
    )


def clean_publications(df: pl.DataFrame) -> pl.DataFrame:
    """Lightweight, testable cleaning for publication DataFrames used by pipeline.

    This function purposely focuses on DOI normalisation and minor date parsing.
    It is not the full port of the monolith; that happens later.
    """

    out = df.clone()
    if "doi" in out.columns:
        out = normalize_doi(out, "doi")

    if "publication_date" in out.columns:
        # Keep this simple and robust for the unit tests. The monolith had a few
        # manual replacements that are not necessary here.
        out = out.with_columns(
            pl.col("publication_date").cast(pl.Utf8).alias("publication_date_raw")
        )

    # Year extraction from date values (best effort)
    if "publication_date_raw" in out.columns:
        out = out.with_columns(
            pl.coalesce(
                [
                    pl.col("publication_date_raw")
                    .cast(pl.Utf8)
                    .str.strptime(pl.Date, "%Y-%m-%d", strict=False),
                    pl.col("publication_date_raw")
                    .cast(pl.Utf8)
                    .str.strptime(pl.Date, "%Y-%m", strict=False),
                    pl.col("publication_date_raw")
                    .cast(pl.Utf8)
                    .str.strptime(pl.Date, "%Y", strict=False),
                ]
            ).alias("publication_date_cleaned")
        ).with_columns(pl.col("publication_date_cleaned").dt.year().alias("publication_year"))

    return out
