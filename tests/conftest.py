"""Shared pytest fixtures and marker registration for syntheca tests."""

from __future__ import annotations

import json
import pathlib

import polars as pl
import pytest

from syntheca.config import settings

# ---------------------------------------------------------------------------
# Directories
# ---------------------------------------------------------------------------

FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures"
FIXTURE_AREA_NAMES = (
    "openalex",
    "pure",
    "openaire",
    "ut_people",
    "comparison",
    "merged",
)


@pytest.fixture
def fixtures_dir() -> pathlib.Path:
    """Return the path to the shared test fixtures directory."""
    return FIXTURES_DIR


@pytest.fixture
def fixture_area_dirs() -> dict[str, pathlib.Path]:
    """Return the known per-area fixture directories keyed by area name."""
    return {name: FIXTURES_DIR / name for name in FIXTURE_AREA_NAMES}


# ---------------------------------------------------------------------------
# Common sample DataFrames
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_publications_df() -> pl.DataFrame:
    """Provide a minimal Pure-style publications DataFrame."""
    return pl.DataFrame(
        {
            "id": ["pub-001", "pub-002", "pub-003"],
            "title": [
                "Advances in Metadata Retrieval",
                "A Survey of Open Access Policies",
                "Machine Learning for Scholarly Data",
            ],
            "doi": [
                "10.1234/synth.2025.001",
                "10.1234/synth.2025.002",
                None,
            ],
            "publication_date": ["2025-01-15", "2025-03-20", "2024-11-01"],
            "language": ["en", "en", "en"],
            "status": ["published", "published", "published"],
        }
    )


@pytest.fixture
def sample_authors_df() -> pl.DataFrame:
    """Provide a minimal authors / persons DataFrame."""
    return pl.DataFrame(
        {
            "pure_id": ["p-100", "p-200", "p-300"],
            "first_names": ["Alice", "Bob", "Charlie"],
            "family_names": ["Researcher", "Scientist", "Engineer"],
            "orcid": ["0000-0001-0000-0001", "0000-0002-0000-0002", None],
            "affiliation_ids_pure": [["org-10"], ["org-20"], ["org-10", "org-20"]],
            "affiliation_names_pure": [
                ["Faculty of Science"],
                ["Faculty of Engineering"],
                ["Faculty of Science", "Faculty of Engineering"],
            ],
        }
    )


@pytest.fixture
def sample_orgunits_df() -> pl.DataFrame:
    """Provide a minimal organizational units DataFrame."""
    return pl.DataFrame(
        {
            "internal_repository_id": ["org-10", "org-20", "org-30"],
            "name": [
                "Faculty of Science and Technology",
                "Faculty of Engineering Technology",
                "Department of Computer Science",
            ],
            "acronym": ["TNW", "ET", "CS"],
            "parent_org": [None, None, "org-10"],
        }
    )


@pytest.fixture
def sample_openalex_works_response() -> dict:
    """Load a realistic OpenAlex /works response dict (single page)."""
    fixture_path = FIXTURES_DIR / "openalex" / "works_response.json"
    return json.loads(fixture_path.read_text())


# ---------------------------------------------------------------------------
# Mock client config helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_client_config() -> dict:
    """Provide common configuration values for mock HTTP clients."""
    return {
        "headers": {"User-Agent": "syntheca-test/0.1"},
        "timeout": 5,
    }


@pytest.fixture(autouse=True)
def isolate_cache_dir(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> pathlib.Path:
    """Isolate on-disk cache artifacts per test to avoid cross-test leakage."""
    cache_dir = tmp_path / ".cache"
    monkeypatch.setattr(settings, "cache_dir", cache_dir)
    return cache_dir
