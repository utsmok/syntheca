"""Offline contract checks for audited live/export-derived fixtures."""

from __future__ import annotations

import json
import pathlib

import xmltodict

REQUIRED_FIXTURE_AREAS = (
    "openalex",
    "pure",
    "openaire",
    "ut_people",
    "comparison",
    "merged",
)


def _load_json(path: pathlib.Path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_fixture_areas_have_provenance_readmes(fixture_area_dirs) -> None:
    for area in REQUIRED_FIXTURE_AREAS:
        readme = fixture_area_dirs[area] / "README.md"
        assert readme.exists(), f"Missing provenance README for {area}"
        assert "provenance" in readme.read_text(encoding="utf-8").lower()


def test_openalex_live_fixture_preserves_structured_awards_without_grants(
    fixture_area_dirs,
) -> None:
    payload = _load_json(fixture_area_dirs["openalex"] / "works_response_live.json")
    work = payload["results"][0]

    assert isinstance(work["awards"], list)
    assert isinstance(work["awards"][0], dict)
    assert "funder_award_id" in work["awards"][0]
    assert "grants" not in work


def test_pure_live_publication_fixture_has_repeated_affiliations(fixture_area_dirs) -> None:
    payload = (fixture_area_dirs["pure"] / "publication_getrecord_live.xml").read_text(
        encoding="utf-8"
    )
    parsed = xmltodict.parse(payload)
    publication = parsed["OAI-PMH"]["GetRecord"]["record"]["metadata"]["cerif:Publication"]
    first_author = publication["cerif:Authors"]["cerif:Author"][0]

    affiliations = first_author["cerif:Affiliation"]
    assert isinstance(affiliations, list)
    assert len(affiliations) == 2


def test_pure_live_orgunit_fixture_has_repeated_identifiers(fixture_area_dirs) -> None:
    payload = (fixture_area_dirs["pure"] / "orgunit_getrecord_live.xml").read_text(encoding="utf-8")
    parsed = xmltodict.parse(payload)
    orgunit = parsed["OAI-PMH"]["GetRecord"]["record"]["metadata"]["cerif:OrgUnit"]

    identifiers = orgunit["cerif:Identifier"]
    assert isinstance(identifiers, list)
    assert len(identifiers) >= 2
    assert identifiers[0]["@type"] == "Scopus affiliation ID"


def test_openaire_live_fixture_uses_citation_impact_shape(fixture_area_dirs) -> None:
    payload = _load_json(fixture_area_dirs["openaire"] / "research_product_live_response.json")
    indicators = payload["results"][0]["indicators"]

    assert "citationImpact" in indicators
    assert "bipIndicators" not in indicators
    assert indicators["citationImpact"]["citationCount"] == 108.0


def test_ut_people_live_rpc_fixture_captures_paging_and_absolute_urls(fixture_area_dirs) -> None:
    payload = _load_json(fixture_area_dirs["ut_people"] / "rpc_live_response.json")
    result = payload["result"]

    assert result["totalcount"] > result["options"]["resultsperpage"]
    assert "https://people.utwente.nl/" in result["resultshtml"]


def test_ut_people_live_profile_fixture_preserves_nonfaculty_level_one(fixture_area_dirs) -> None:
    html = (fixture_area_dirs["ut_people"] / "profile_live_page.html").read_text(encoding="utf-8")

    assert "widget-linklist__item--level1" in html
    assert "Library, ICT-Services" in html
    assert "Embedded Information Services" in html


def test_comparison_artifact_shapes_capture_scival_alias_pressure(fixture_area_dirs) -> None:
    artifacts = _load_json(fixture_area_dirs["comparison"] / "artifact_shapes.json")
    flattened = "\n".join(
        f"{item.get('header_row', [])}\n{item.get('first_data_row', [])}" for item in artifacts
    ).lower()

    assert "publication_type" in flattened or "publication type" in flattened
    assert "scopus_source_title" in flattened or "scopus source title" in flattened


def test_merged_fixture_readme_marks_samples_as_non_baseline(fixture_area_dirs) -> None:
    readme = (fixture_area_dirs["merged"] / "README.md").read_text(encoding="utf-8").lower()
    assert "not" in readme
    assert "release baselines" in readme
    assert "_baseline_status" in readme
