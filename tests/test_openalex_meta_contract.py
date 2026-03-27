"""Contract tests for the OpenAlex Meta model.

Verifies that the Meta model can handle live API response shapes, including
the ``cost_usd`` field that the API now returns but the model doesn't declare.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from syntheca.models.openalex import Meta, Response, Work

FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures"


def _load_openalex_fixture(name: str) -> dict:
    path = FIXTURES_DIR / "openalex" / name
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture
def openalex_response_data() -> dict:
    """Load the OpenAlex works_response fixture."""
    return _load_openalex_fixture("works_response.json")


@pytest.fixture
def openalex_live_meta_data() -> dict:
    """Load the trimmed saved-live OpenAlex response fixture."""
    return _load_openalex_fixture("works_response_live.json")


def test_meta_parses_known_fields(openalex_response_data: dict):
    """Meta.from_dict parses all declared fields correctly."""
    meta = Meta.from_dict(openalex_response_data["meta"])
    assert meta.count == 2
    assert meta.db_response_time_ms == 42
    assert meta.page == 1
    assert meta.per_page == 25
    assert meta.groups_count is None
    assert meta.next_cursor is None


def test_meta_survives_cost_usd_field(openalex_response_data: dict):
    """Meta.from_dict does not fail when the response includes ``cost_usd``.

    The live OpenAlex API now returns a ``cost_usd`` field that the current
    ``Meta`` dataclass does not declare. Because ``strict=False`` in the
    dacite config, this should be silently ignored rather than raising.
    """
    raw_meta = openalex_response_data["meta"]
    assert "cost_usd" in raw_meta, "Fixture must include cost_usd to test schema drift"
    meta = Meta.from_dict(raw_meta)
    # The model should parse without error; cost_usd is not a declared field
    assert meta.count == 2


def test_meta_survives_saved_live_payload_without_next_cursor(openalex_live_meta_data: dict):
    """Saved live-like payloads may omit ``next_cursor`` while adding ``cost_usd``."""
    raw_meta = openalex_live_meta_data["meta"]

    assert "cost_usd" in raw_meta
    assert "next_cursor" not in raw_meta

    meta = Meta.from_dict(raw_meta)

    assert meta.per_page == 1
    assert meta.next_cursor is None


def test_meta_required_fields_present(openalex_response_data: dict):
    """Every field declared on ``Meta`` is present in the fixture response."""
    raw_meta = openalex_response_data["meta"]
    declared_fields = {f.name for f in Meta.__dataclass_fields__.values()}
    # All declared fields must be present in the API response
    missing = declared_fields - set(raw_meta.keys())
    assert not missing, f"Meta model fields missing from fixture: {missing}"


def test_full_response_parses_with_work_type(openalex_response_data: dict):
    """Response.from_dict returns a typed Response[Work] from the fixture."""
    resp = Response.from_dict(openalex_response_data, result_type=Work)
    assert resp.meta.count == 2
    assert len(resp.results) == 2
    work = resp.results[0]
    assert work is not None
    assert work.doi == "https://doi.org/10.1234/synth.2025.001"
