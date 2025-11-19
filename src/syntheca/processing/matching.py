from __future__ import annotations

from typing import Any

import polars as pl
from Levenshtein import ratio

UT_OPENALEX_ID = "https://openalex.org/I94624287"


def calculate_fuzzy_match(
    df: pl.DataFrame, left_col: str, right_col: str, result_col: str = "fuzzy_score"
) -> pl.DataFrame:
    """Calculate a fuzzy match score between two string columns using
    Levenshtein.ratio.

    Implementation note:
    - The function uses pl.struct([...]).map_elements to invoke a Python function
      row-wise while keeping most of the operation inside an expression.
    - This performs much better than a full pandas UDF on large DataFrames.

    Returns a new DataFrame with the added `result_col` (float in 0..1).
    """

    def _ratio(x: dict[str, str]) -> float:
        # Polars will pass Python types as plain Python values, no None -> ''
        a = x.get(left_col) or ""
        b = x.get(right_col) or ""
        try:
            return float(ratio(a, b))
        except Exception:
            return 0.0

    return df.with_columns(
        pl.struct([pl.col(left_col), pl.col(right_col)])
        .map_elements(_ratio)
        .cast(pl.Float64)
        .alias(result_col)
    )


async def resolve_missing_ids(
    df: pl.DataFrame,
    client: Any,
    title_col: str = "title",
    doi_col: str = "doi",
    id_col: str = "id",
    threshold: float = 0.9,
) -> pl.DataFrame:
    """Resolve missing OpenAlex IDs by searching OpenAlex works by title.

    This function is async because it calls the OpenAlex client. It will search
    for titles where `id_col` is null and try to find candidate works. The best
    match with Levenshtein.ratio >= threshold will be used to fill the id/doi
    columns. If the work includes the UT OpenAlex id in its corresponding
    institutions, it will be preferred when above a lower threshold.
    """

    candidates = []
    # gather unique titles that need resolution
    to_search = (
        df.filter(pl.col(id_col).is_null())
        .filter(pl.col(title_col).is_not_null())
        .select(title_col)
        .unique()
        .to_series()
        .to_list()
    )

    # Query OpenAlex for each title and pick best match
    for t in to_search:
        works = []
        try:
            works = await client.get_works_by_title(t)
        except Exception:
            works = []
        best = None
        best_score = 0.0
        for w in works or []:
            name = getattr(w, "display_name", None) or ""
            score = ratio(name.lower().strip(), str(t).lower().strip())
            # small boost when UT is listed as corresponding
            has_ut = UT_OPENALEX_ID in (getattr(w, "corresponding_institution_ids", []) or [])
            if has_ut:
                score += 0.05
            if score > best_score:
                best = w
                best_score = score

        if best and best_score >= threshold:
            candidates.append(
                {
                    "search_title": t,
                    "oa_id": getattr(best, "id", None),
                    "oa_doi": getattr(best, "doi", None),
                }
            )

    if not candidates:
        return df

    cand_df = pl.from_dicts(candidates)
    # Join back into original df on title and fill missing id/doi
    out = df.join(cand_df, left_on=title_col, right_on="search_title", how="left")
    # when id is null but oa_id found, set it; same for doi
    out = out.with_columns(
        pl.when(pl.col(id_col).is_null())
        .then(pl.col("oa_id"))
        .otherwise(pl.col(id_col))
        .alias(id_col),
        pl.when(pl.col(doi_col).is_null())
        .then(pl.col("oa_doi"))
        .otherwise(pl.col(doi_col))
        .alias(doi_col),
    )
    # safe drop in case any of the columns are missing
    drop_cols = [c for c in ["oa_id", "oa_doi", "search_title"] if c in out.columns]
    if drop_cols:
        out = out.drop(drop_cols)

    return out
