"""Parity validation for Syntheca pipeline outputs.

Provides schema validation, regression metric computation, and baseline
comparison so that pipeline changes can be checked for unexpected drift.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from syntheca.reporting.output_groups import GROUP_REGISTRY, OutputGroupName

BASELINE_STATUS_TEMPLATE = "template"
BASELINE_STATUS_REAL = "real"
DEFAULT_PARITY_CLAIM_RULE = (
    "Release parity cannot be claimed until tests/regression_baseline.json is marked "
    "as a real baseline and every tracked metric has a non-null baseline value."
)


@dataclass(frozen=True)
class BaselineReadiness:
    """Release-readiness state for the configured regression baseline."""

    status: str
    claim_rule: str
    missing_metrics: list[str]
    release_ready: bool


@dataclass(frozen=True)
class OutputFileValidation:
    """Validation result for a single exported file contract."""

    filename: str
    exists: bool
    missing_columns: list[str]


@dataclass(frozen=True)
class OutputGroupValidation:
    """Validation result for one named output group."""

    group: OutputGroupName
    files: list[OutputFileValidation]

    @property
    def is_valid(self) -> bool:
        """Return ``True`` when every file exists and has all required columns."""
        return all(file.exists and not file.missing_columns for file in self.files)


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


def validate_output_group_contract(
    output_dir: Path,
    group: OutputGroupName | str,
) -> OutputGroupValidation:
    """Validate one output group against the committed file contracts."""
    group_name = OutputGroupName(group)
    contract = GROUP_REGISTRY[group_name]
    base = output_dir / contract.subdirectory if contract.subdirectory else output_dir

    results: list[OutputFileValidation] = []
    for file_contract in contract.files:
        path = base / file_contract.filename
        if not path.exists():
            results.append(
                OutputFileValidation(
                    filename=file_contract.filename,
                    exists=False,
                    missing_columns=list(file_contract.required_columns),
                )
            )
            continue

        if path.suffix.lower() == ".parquet":
            df = pl.read_parquet(path)
        elif path.suffix.lower() in {".xlsx", ".xls"}:
            df = pl.read_excel(path, engine="openpyxl")
        elif path.suffix.lower() == ".csv":
            df = pl.read_csv(path)
        else:
            df = pl.DataFrame()

        results.append(
            OutputFileValidation(
                filename=file_contract.filename,
                exists=True,
                missing_columns=validate_output_schema(df, file_contract.required_columns),
            )
        )

    return OutputGroupValidation(group=group_name, files=results)


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

    pure_publications_path = output_dir / "pure_publications_clean.parquet"
    pure_persons_path = output_dir / "pure_persons.parquet"
    pure_orgunits_path = output_dir / "pure_orgunits.parquet"

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

    metrics["row_count.pure_publications"] = (
        pl.read_parquet(pure_publications_path).height if pure_publications_path.exists() else None
    )
    metrics["row_count.pure_persons"] = (
        pl.read_parquet(pure_persons_path).height if pure_persons_path.exists() else None
    )
    metrics["row_count.pure_orgunits"] = (
        pl.read_parquet(pure_orgunits_path).height if pure_orgunits_path.exists() else None
    )

    # --- OpenAlex hit rate ---
    # Requires both merged (to know total with DOI) and openalex intermediate
    oa_path = output_dir / "openalex_works_clean.parquet"
    if oa_path.exists():
        metrics["row_count.openalex_works"] = pl.read_parquet(oa_path).height
    else:
        metrics["row_count.openalex_works"] = None

    if merged_path.exists() and oa_path.exists():
        merged = pl.read_parquet(merged_path)
        oa = pl.read_parquet(oa_path)
        total_with_doi = merged.filter(pl.col("doi").is_not_null() & (pl.col("doi") != "")).height
        if total_with_doi > 0:
            metrics["openalex_hit_rate"] = round(oa.height / total_with_doi * 100, 2)
        else:
            metrics["openalex_hit_rate"] = None
    elif merged_path.exists():
        merged = pl.read_parquet(merged_path)
        total_with_doi = merged.filter(pl.col("doi").is_not_null() & (pl.col("doi") != "")).height
        openalex_col = next(
            (column for column in ("openalex_id", "id") if column in merged.columns), None
        )
        if total_with_doi > 0 and openalex_col is not None:
            hits = merged.filter(
                pl.col("doi").is_not_null()
                & (pl.col("doi") != "")
                & pl.col(openalex_col).is_not_null()
                & (pl.col(openalex_col).cast(pl.Utf8) != "")
            ).height
            metrics["openalex_hit_rate"] = round(hits / total_with_doi * 100, 2)
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
    elif merged_path.exists():
        merged = pl.read_parquet(merged_path)
        if merged.height > 0 and "faculty" in merged.columns:
            with_org = merged.filter(
                pl.col("faculty").is_not_null() & (pl.col("faculty") != "")
            ).height
            metrics["org_mapping_coverage"] = round(with_org / merged.height * 100, 2)
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
    elif merged_path.exists():
        merged = pl.read_parquet(merged_path)
        if "person_resolved" in merged.columns:
            metrics["unresolved_person_count"] = merged.filter(
                pl.col("person_resolved") == False  # noqa: E712
            ).height
        elif "faculty" in merged.columns:
            metrics["unresolved_person_count"] = merged.filter(
                pl.col("faculty").is_null() | (pl.col("faculty") == "")
            ).height
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
                pl.col("match_method").cast(pl.Utf8).str.starts_with("title")
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
    - The baseline is ``None`` (no established baseline yet; informational only).
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
    by :func:`compute_regression_metrics`. Top-level metadata fields are
    ignored by this helper.
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


def load_baseline_metadata(path: Path) -> dict[str, str]:
    """Load baseline metadata that describes whether parity is claimable."""
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)

    return {
        "status": raw.get("_baseline_status", BASELINE_STATUS_TEMPLATE),
        "claim_rule": raw.get("_parity_claim_rule", DEFAULT_PARITY_CLAIM_RULE),
    }


def assess_baseline_readiness(path: Path) -> BaselineReadiness:
    """Return whether the configured regression baseline is release-ready."""
    baseline = load_baseline(path)
    metadata = load_baseline_metadata(path)
    missing_metrics = sorted(key for key, value in baseline.items() if value is None)
    status = metadata["status"]
    claim_rule = metadata["claim_rule"]
    release_ready = status == BASELINE_STATUS_REAL and not missing_metrics

    return BaselineReadiness(
        status=status,
        claim_rule=claim_rule,
        missing_metrics=missing_metrics,
        release_ready=release_ready,
    )
