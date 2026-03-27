"""Tests for reporting.parity — schema validation, metric computation, parity checks."""

from __future__ import annotations

import json
import pathlib

import polars as pl
import pytest

from syntheca.reporting.output_groups import OutputGroupName
from syntheca.reporting.parity import (
    BASELINE_STATUS_REAL,
    OutputGroupValidation,
    assess_baseline_readiness,
    check_parity,
    compute_regression_metrics,
    load_baseline,
    load_baseline_metadata,
    validate_output_group_contract,
    validate_output_schema,
)

# ---------------------------------------------------------------------------
# validate_output_schema
# ---------------------------------------------------------------------------


class TestValidateOutputSchema:
    def test_all_columns_present(self) -> None:
        df = pl.DataFrame({"a": [1], "b": [2], "c": [3]})
        assert validate_output_schema(df, ["a", "b", "c"]) == []

    def test_some_columns_missing(self) -> None:
        df = pl.DataFrame({"a": [1], "b": [2]})
        missing = validate_output_schema(df, ["a", "b", "c", "d"])
        assert missing == ["c", "d"]

    def test_empty_expected(self) -> None:
        df = pl.DataFrame({"a": [1]})
        assert validate_output_schema(df, []) == []

    def test_empty_dataframe(self) -> None:
        df = pl.DataFrame({"x": []})
        assert validate_output_schema(df, ["x"]) == []
        assert validate_output_schema(df, ["y"]) == ["y"]


# ---------------------------------------------------------------------------
# compute_regression_metrics
# ---------------------------------------------------------------------------


class TestComputeRegressionMetrics:
    def test_missing_directory_returns_nones(self, tmp_path: pathlib.Path) -> None:
        metrics = compute_regression_metrics(tmp_path)
        assert metrics["row_count.merged_final"] is None
        assert metrics["doi_fill_rate"] is None

    def test_merged_parquet_present(self, tmp_path: pathlib.Path) -> None:
        df = pl.DataFrame(
            {
                "doi": ["10.1234/a", "10.1234/b", None],
                "title": ["A", "B", "C"],
            }
        )
        df.write_parquet(tmp_path / "merged.parquet")

        metrics = compute_regression_metrics(tmp_path)
        assert metrics["row_count.merged_final"] == 3
        # 2 out of 3 have a DOI → ~66.67%
        assert metrics["doi_fill_rate"] == pytest.approx(66.67, abs=0.01)

    def test_all_dois_filled(self, tmp_path: pathlib.Path) -> None:
        df = pl.DataFrame({"doi": ["10.1/a", "10.1/b"], "title": ["X", "Y"]})
        df.write_parquet(tmp_path / "merged.parquet")

        metrics = compute_regression_metrics(tmp_path)
        assert metrics["doi_fill_rate"] == 100.0

    def test_reads_full_metric_pack_from_exported_artifacts(self, tmp_path: pathlib.Path) -> None:
        merged = pl.DataFrame(
            {
                "doi": ["10.1/a", "10.1/b", None],
                "title": ["A", "B", "C"],
                "openalex_id": ["W1", "W2", None],
                "faculty": ["TNW", "BMS", "ET"],
                "person_resolved": [True, True, False],
                "match_method": ["doi", "doi", "title_fuzzy"],
            }
        )
        merged.write_parquet(tmp_path / "merged.parquet")
        pl.DataFrame({"doi": ["10.1/a", "10.1/b"]}).write_parquet(
            tmp_path / "openalex_works_clean.parquet"
        )
        pl.DataFrame({"doi": ["10.1/a", "10.1/b", None]}).write_parquet(
            tmp_path / "pure_publications_clean.parquet"
        )
        pl.DataFrame({"id": ["p1", "p2", "p3"]}).write_parquet(tmp_path / "pure_persons.parquet")
        pl.DataFrame({"id": ["o1", "o2", "o3"]}).write_parquet(tmp_path / "pure_orgunits.parquet")
        pl.DataFrame({"faculty": ["TNW", "BMS", "ET"]}).write_parquet(
            tmp_path / "authors_enriched.parquet"
        )

        metrics = compute_regression_metrics(tmp_path)
        assert metrics["row_count.merged_final"] == 3
        assert metrics["row_count.pure_publications"] == 3
        assert metrics["row_count.pure_persons"] == 3
        assert metrics["row_count.pure_orgunits"] == 3
        assert metrics["row_count.openalex_works"] == 2
        assert metrics["doi_fill_rate"] == pytest.approx(66.67, abs=0.01)
        assert metrics["openalex_hit_rate"] == 100.0
        assert metrics["org_mapping_coverage"] == 100.0
        assert metrics["unresolved_person_count"] == 0
        assert metrics["title_fallback_count"] == 1


# ---------------------------------------------------------------------------
# check_parity
# ---------------------------------------------------------------------------


class TestCheckParity:
    def test_within_tolerance(self) -> None:
        baseline = {"doi_fill_rate": 80.0, "row_count.merged_final": 1000}
        current = {"doi_fill_rate": 81.0, "row_count.merged_final": 1020}
        results = check_parity(current, baseline, tolerance=0.05)
        assert results["doi_fill_rate"] is True
        assert results["row_count.merged_final"] is True

    def test_outside_tolerance(self) -> None:
        baseline = {"doi_fill_rate": 80.0}
        current = {"doi_fill_rate": 60.0}
        results = check_parity(current, baseline, tolerance=0.05)
        assert results["doi_fill_rate"] is False

    def test_baseline_none_passes(self) -> None:
        baseline = {"doi_fill_rate": None}
        current = {"doi_fill_rate": 99.0}
        results = check_parity(current, baseline, tolerance=0.05)
        assert results["doi_fill_rate"] is True

    def test_current_none_with_baseline_fails(self) -> None:
        baseline = {"doi_fill_rate": 80.0}
        current = {"doi_fill_rate": None}
        results = check_parity(current, baseline, tolerance=0.05)
        assert results["doi_fill_rate"] is False

    def test_both_none_passes(self) -> None:
        baseline = {"metric_x": None}
        current = {"metric_x": None}
        assert check_parity(current, baseline)["metric_x"] is True

    def test_exact_match(self) -> None:
        baseline = {"m": 100}
        current = {"m": 100}
        assert check_parity(current, baseline)["m"] is True


# ---------------------------------------------------------------------------
# load_baseline
# ---------------------------------------------------------------------------


class TestLoadBaseline:
    def test_load_from_regression_file(self) -> None:
        """Verify we can parse the actual regression_baseline.json."""
        baseline_path = pathlib.Path(__file__).parent / "regression_baseline.json"
        if not baseline_path.exists():
            pytest.skip("regression_baseline.json not present")

        flat = load_baseline(baseline_path)
        assert "row_count.merged_final" in flat
        assert "doi_fill_rate" in flat
        assert flat["row_count.merged_final"] == 3
        assert flat["doi_fill_rate"] == 66.67

    def test_load_custom_baseline(self, tmp_path: pathlib.Path) -> None:
        data = {
            "metrics": {
                "doi_fill_rate": {"baseline": 85.0, "tolerance": 2.0},
                "row_count": {
                    "outputs": {
                        "merged_final": {"baseline": 500},
                    }
                },
            }
        }
        p = tmp_path / "baseline.json"
        p.write_text(json.dumps(data))

        flat = load_baseline(p)
        assert flat["doi_fill_rate"] == 85.0
        assert flat["row_count.merged_final"] == 500


# ---------------------------------------------------------------------------
# Baseline metadata / release readiness
# ---------------------------------------------------------------------------


class TestBaselineMetadata:
    def test_loads_template_metadata_from_regression_file(self) -> None:
        baseline_path = pathlib.Path(__file__).parent / "regression_baseline.json"
        if not baseline_path.exists():
            pytest.skip("regression_baseline.json not present")

        metadata = load_baseline_metadata(baseline_path)
        assert metadata["status"] == BASELINE_STATUS_REAL
        assert "frozen offline regression pack" in metadata["claim_rule"].lower()

    def test_regression_baseline_is_release_ready(self) -> None:
        baseline_path = pathlib.Path(__file__).parent / "regression_baseline.json"
        if not baseline_path.exists():
            pytest.skip("regression_baseline.json not present")

        readiness = assess_baseline_readiness(baseline_path)
        assert readiness.status == BASELINE_STATUS_REAL
        assert readiness.release_ready is True
        assert readiness.missing_metrics == []

    def test_real_baseline_with_all_values_is_release_ready(self, tmp_path: pathlib.Path) -> None:
        data = {
            "_baseline_status": "real",
            "_parity_claim_rule": "release parity is allowed",
            "metrics": {
                "doi_fill_rate": {"baseline": 85.0, "tolerance": 2.0},
                "row_count": {
                    "outputs": {
                        "merged_final": {"baseline": 500},
                    }
                },
            },
        }
        p = tmp_path / "baseline.json"
        p.write_text(json.dumps(data))

        readiness = assess_baseline_readiness(p)
        assert readiness.status == BASELINE_STATUS_REAL
        assert readiness.release_ready is True
        assert readiness.missing_metrics == []


class TestOutputGroupContracts:
    def test_core_output_group_contract_validation(self, tmp_path: pathlib.Path) -> None:
        df = pl.DataFrame(
            {
                "doi": ["10.1/a"],
                "title": ["A"],
                "publication_year": [2024],
                "type": ["article"],
            }
        )
        df.write_parquet(tmp_path / "merged.parquet")
        df.write_excel(tmp_path / "merged.xlsx")

        result = validate_output_group_contract(tmp_path, OutputGroupName.CORE_DATA)
        assert isinstance(result, OutputGroupValidation)
        assert result.is_valid is True
