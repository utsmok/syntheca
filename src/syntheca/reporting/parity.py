"""Parity validation for Syntheca pipeline outputs.

Provides schema validation, regression metric computation, and baseline
comparison so that pipeline changes can be checked for unexpected drift.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


def validate_output_schema(df: pl.DataFrame, expected_columns: list[str]) -> list[str]:
    """Return column names from *expected_columns* missing in *df*.

    Args:
        df: The output DataFrame to validate.
        expected_columns: Column names that should be present.

    Returns:
        List of missing column names (empty when the schema is satisfied).
    """
    present = set(df.columns)
    return [c for c in expected_columns if c not in present]


# ---------------------------------------------------------------------------
# Regression metrics
# ---------------------------------------------------------------------------

#: Metric keys that this module knows how to compute.
KNOWN_METRIC_KEYS: list[str] = [
    "row_count.merged_final",
    "doi_fill_rate",
    "openalex_hit_rate",
    "org_mapping_coverage",
    "unresolved_person_count",
    "title_fallback_count",
]


def compute_regression_metrics(output_dir: Path) -> dict[str, float | int | None]:
    """Compute regression metrics from files in *output_dir*.

    Metrics produced match those defined in ``regression_baseline.json``.
    When a required input file is missing, the affected metric value is
    ``None`` (not an error).

    Args:
        output_dir: Directory containing pipeline output artefacts.

    Returns:
        Dict mapping dotted metric names to computed values.
    """
    metrics: dict[str, float | int | None] = {}

    # --- merged output row count & DOI fill rate ---
    merged_path = output_dir / "merged.parquet"
    if merged_path.exists():
        merged = pl.read_parquet(merged_path)
        metrics["row_count.merged_final"] = merged.height

        if "doi" in merged.columns and merged.height > 0:
            non_null = merged.filter(pl.col("doi").is_not_null() & (pl.col("doi") != "")).height
            metrics["doi_fill_rate"] = round(non_null / merged.height * 100, 2)
        else:
            metrics["doi_fill_rate"] = None
    else:
        metrics["row_count.merged_final"] = None
        metrics["doi_fill_rate"] = None

    # --- OpenAlex hit rate ---
    # Requires both merged (to know total with DOI) and openalex intermediate
    oa_path = output_dir / "openalex_works_clean.parquet"
    if merged_path.exists() and oa_path.exists():
        merged = pl.read_parquet(merged_path)
        oa = pl.read_parquet(oa_path)
        total_with_doi = merged.filter(pl.col("doi").is_not_null() & (pl.col("doi") != "")).height
        if total_with_doi > 0:
            metrics["openalex_hit_rate"] = round(oa.height / total_with_doi * 100, 2)
        else:
            metrics["openalex_hit_rate"] = None
    else:
        metrics["openalex_hit_rate"] = None

    # --- org mapping coverage ---
    authors_path = output_dir / "authors_enriched.parquet"
    if authors_path.exists():
        authors = pl.read_parquet(authors_path)
        if authors.height > 0 and "faculty" in authors.columns:
            with_org = authors.filter(
                pl.col("faculty").is_not_null() & (pl.col("faculty") != "")
            ).height
            metrics["org_mapping_coverage"] = round(with_org / authors.height * 100, 2)
        else:
            metrics["org_mapping_coverage"] = None
    else:
        metrics["org_mapping_coverage"] = None

    # --- unresolved person count ---
    if authors_path.exists():
        authors = pl.read_parquet(authors_path)
        if authors.height > 0 and "faculty" in authors.columns:
            unresolved = authors.filter(
                pl.col("faculty").is_null() | (pl.col("faculty") == "")
            ).height
            metrics["unresolved_person_count"] = unresolved
        else:
            metrics["unresolved_person_count"] = None
    else:
        metrics["unresolved_person_count"] = None

    # --- title fallback count ---
    # This metric requires a column or log that tracks title-based matches.
    # We approximate by looking for a flag column in merged output.
    if merged_path.exists():
        merged = pl.read_parquet(merged_path)
        if "match_method" in merged.columns:
            metrics["title_fallback_count"] = merged.filter(
                pl.col("match_method") == "title"
            ).height
        else:
            metrics["title_fallback_count"] = None
    else:
        metrics["title_fallback_count"] = None

    return metrics


# ---------------------------------------------------------------------------
# Parity check
# ---------------------------------------------------------------------------


def check_parity(
    current_metrics: dict[str, float | int | None],
    baseline_metrics: dict[str, float | int | None],
    tolerance: float = 0.05,
) -> dict[str, bool]:
    """Check each metric against *baseline_metrics* within *tolerance*.

    A metric passes when:
    - Both current and baseline are ``None`` (no data to compare).
    - The baseline is ``None`` (no established baseline yet).
    - The relative difference ``|current - baseline| / max(|baseline|, 1)``
      is within *tolerance*.

    Args:
        current_metrics: Metrics from the current pipeline run.
        baseline_metrics: Expected baseline values.
        tolerance: Maximum allowed relative deviation (default 5 %).

    Returns:
        Dict mapping metric names to ``True`` (pass) or ``False`` (fail).
    """
    results: dict[str, bool] = {}
    all_keys = set(current_metrics) | set(baseline_metrics)

    for key in sorted(all_keys):
        current = current_metrics.get(key)
        baseline = baseline_metrics.get(key)

        if baseline is None:
            # No established baseline — automatically pass.
            results[key] = True
            continue

        if current is None:
            # Baseline exists but current is missing — fail.
            results[key] = False
            continue

        denominator = max(abs(baseline), 1)
        deviation = abs(current - baseline) / denominator
        results[key] = deviation <= tolerance

    return results


# ---------------------------------------------------------------------------
# Baseline I/O helpers
# ---------------------------------------------------------------------------


def load_baseline(path: Path) -> dict[str, float | int | None]:
    """Load baseline metric values from *regression_baseline.json*.

    Flattens the nested structure into dotted keys matching those produced
    by :func:`compute_regression_metrics`.
    """
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)

    flat: dict[str, float | int | None] = {}
    metrics_section = raw.get("metrics", {})

    for metric_key, metric_def in metrics_section.items():
        if isinstance(metric_def, dict):
            if "baseline" in metric_def:
                flat[metric_key] = metric_def["baseline"]
            elif "outputs" in metric_def:
                for output_name, output_def in metric_def["outputs"].items():
                    flat[f"{metric_key}.{output_name}"] = output_def.get("baseline")

    return flat
