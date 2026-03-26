"""Tests for syntheca.comparison.scopus — Scopus export reading and comparison."""

from __future__ import annotations

import pathlib

import polars as pl
import pytest

from syntheca.comparison.scopus import (
    ComparisonResult,
    ScopusComparison,
    ScopusExportReader,
)

FIXTURE_DIR = pathlib.Path(__file__).parent / "fixtures" / "comparison"
SAMPLE_XLSX = FIXTURE_DIR / "scopus_export_sample.xlsx"

# ---------------------------------------------------------------------------
# Internal DataFrame used for comparison tests
# ---------------------------------------------------------------------------


@pytest.fixture
def internal_df() -> pl.DataFrame:
    """Internal records with pre-normalized DOIs.

    Includes:
    - 5 records that match Scopus fixture DOIs
    - 1 record that matches but has field mismatches (title + type differ)
    - 1 record that is internal-only (DOI not in Scopus)
    - 1 record with no DOI (always internal-only)
    """
    return pl.DataFrame(
        {
            "doi": [
                "10.1234/synth.2025.001",
                "10.1234/synth.2025.002",
                "10.1234/synth.2025.003",
                "10.1234/synth.2025.004",  # matches but fields differ
                "10.1234/synth.2025.005",
                "10.1234/synth.2025.006",
                "10.1234/synth.2025.007",
                "10.5555/internal.only.001",  # internal-only
                None,  # no DOI → internal-only
            ],
            "title": [
                "Advances in Metadata Retrieval",
                "A Survey of Open Access Policies",
                "Deep Learning for Scholarly Graphs",
                "Internal Title for Mismatch Check",  # differs from Scopus
                "Scalable Repository Harvesting",
                "FAIR Data Practices in Institutions",
                "Persistent Identifiers for Research",
                "Internal Exclusive Paper",
                "Record Without DOI Internal",
            ],
            "type": [
                "article",
                "review",
                "article",
                "article",  # Scopus says "Conference Paper"
                "article",
                "review",
                "article",
                "article",
                "editorial",
            ],
            "publication_year": [
                2025,
                2025,
                2024,
                2025,
                2025,
                2024,
                2025,
                2025,
                2023,
            ],
        }
    )


# ===================================================================
# ScopusExportReader
# ===================================================================


class TestScopusExportReader:
    def test_read_xlsx(self):
        df = ScopusExportReader.read_export(SAMPLE_XLSX)
        assert len(df) == 10
        # Column names should be canonical lower-case
        assert "doi" in df.columns
        assert "title" in df.columns
        assert "eid" in df.columns

    def test_doi_normalization(self):
        df = ScopusExportReader.read_export(SAMPLE_XLSX)
        dois = df["doi"].to_list()
        # Row 3 originally had "https://doi.org/10.1234/SYNTH.2025.003"
        assert "10.1234/synth.2025.003" in dois
        # All DOIs should be lowercase and prefix-free
        for d in dois:
            if d and d != "":
                assert "https://doi.org/" not in d
                assert d == d.lower()

    def test_column_name_variants(self):
        df = ScopusExportReader.read_export(SAMPLE_XLSX)
        # "Source title" → "source_title", "Document Type" → "document_type",
        # "Cited by" → "cited_by", "Language of Original Document" → "language"
        assert "source_title" in df.columns
        assert "document_type" in df.columns
        assert "cited_by" in df.columns
        assert "language" in df.columns

    def test_read_csv(self, tmp_path: pathlib.Path):
        # Create a minimal CSV and ensure it can be read
        csv_path = tmp_path / "test_export.csv"
        csv_path.write_text(
            "DOI,Title,Year\n"
            "10.1234/test.001,Test Paper,2025\n"
            "10.1234/test.002,Another Paper,2024\n"
        )
        df = ScopusExportReader.read_export(csv_path)
        assert len(df) == 2
        assert "doi" in df.columns
        assert "title" in df.columns

    def test_unsupported_format(self, tmp_path: pathlib.Path):
        bad = tmp_path / "data.json"
        bad.write_text("{}")
        with pytest.raises(ValueError, match="Unsupported file format"):
            ScopusExportReader.read_export(bad)

    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            ScopusExportReader.read_export("/nonexistent/path.xlsx")

    def test_missing_doi_column(self, tmp_path: pathlib.Path):
        csv_path = tmp_path / "no_doi.csv"
        csv_path.write_text("Title,Year\nSome Paper,2025\n")
        df = ScopusExportReader.read_export(csv_path)
        # Should still return a DataFrame; no crash
        assert len(df) == 1
        assert "title" in df.columns


# ===================================================================
# ScopusComparison
# ===================================================================


class TestScopusComparison:
    def test_basic_comparison(self, internal_df: pl.DataFrame):
        scopus_df = ScopusExportReader.read_export(SAMPLE_XLSX)
        result = ScopusComparison.compare(scopus_df, internal_df)

        assert isinstance(result, ComparisonResult)
        assert isinstance(result.matched, pl.DataFrame)
        assert isinstance(result.scopus_only, pl.DataFrame)
        assert isinstance(result.internal_only, pl.DataFrame)
        assert isinstance(result.summary, dict)

    def test_matched_count(self, internal_df: pl.DataFrame):
        scopus_df = ScopusExportReader.read_export(SAMPLE_XLSX)
        result = ScopusComparison.compare(scopus_df, internal_df)

        # DOIs that should match: .001, .002, .003, .004, .005, .006, .007 → 7
        assert result.summary["matched"] == 7

    def test_scopus_only(self, internal_df: pl.DataFrame):
        scopus_df = ScopusExportReader.read_export(SAMPLE_XLSX)
        result = ScopusComparison.compare(scopus_df, internal_df)

        # Scopus-only DOIs: scopus.only.001, scopus.only.002, + 1 row with no DOI → 3
        assert result.summary["scopus_only"] == 3
        assert len(result.scopus_only) == 3

    def test_internal_only(self, internal_df: pl.DataFrame):
        scopus_df = ScopusExportReader.read_export(SAMPLE_XLSX)
        result = ScopusComparison.compare(scopus_df, internal_df)

        # Internal-only: internal.only.001 + 1 row with no DOI → 2
        assert result.summary["internal_only"] == 2
        assert len(result.internal_only) == 2

    def test_mismatch_detection(self, internal_df: pl.DataFrame):
        scopus_df = ScopusExportReader.read_export(SAMPLE_XLSX)
        result = ScopusComparison.compare(scopus_df, internal_df)

        # Record .004 has title + type mismatches → at least 2 mismatch rows
        assert len(result.mismatch_details) >= 2
        mismatch_fields = result.mismatch_details["field"].to_list()
        assert "title_mismatch" in mismatch_fields
        assert "document_type_mismatch" in mismatch_fields

    def test_empty_scopus(self, internal_df: pl.DataFrame):
        empty = pl.DataFrame(schema={"doi": pl.Utf8, "title": pl.Utf8})
        result = ScopusComparison.compare(empty, internal_df)
        assert result.summary["matched"] == 0
        assert result.summary["scopus_only"] == 0

    def test_empty_internal(self):
        scopus_df = ScopusExportReader.read_export(SAMPLE_XLSX)
        empty = pl.DataFrame(schema={"doi": pl.Utf8, "title": pl.Utf8})
        result = ScopusComparison.compare(scopus_df, empty)
        assert result.summary["matched"] == 0
        assert result.summary["internal_only"] == 0

    def test_both_empty(self):
        empty_s = pl.DataFrame(schema={"doi": pl.Utf8})
        empty_i = pl.DataFrame(schema={"doi": pl.Utf8})
        result = ScopusComparison.compare(empty_s, empty_i)
        assert result.summary["matched"] == 0
        assert result.summary["scopus_only"] == 0
        assert result.summary["internal_only"] == 0

    def test_missing_doi_columns(self):
        scopus = pl.DataFrame({"title": ["A"]})
        internal = pl.DataFrame({"title": ["B"]})
        # Should handle gracefully — no DOI column → all records are *_only
        result = ScopusComparison.compare(scopus, internal)
        assert result.summary["matched"] == 0

    def test_summary_keys(self, internal_df: pl.DataFrame):
        scopus_df = ScopusExportReader.read_export(SAMPLE_XLSX)
        result = ScopusComparison.compare(scopus_df, internal_df)
        expected_keys = {
            "total_scopus",
            "total_internal",
            "matched",
            "scopus_only",
            "internal_only",
            "mismatched_records",
        }
        assert expected_keys.issubset(result.summary.keys())
