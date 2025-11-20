"""Tests for organization hierarchy resolution and author-org mapping."""

from __future__ import annotations

import json

import polars as pl

from syntheca.processing.organizations import (
    load_faculty_mapping,
    map_author_affiliations,
    resolve_org_hierarchy,
)


def test_load_faculty_mapping(tmp_path, monkeypatch):
    """Test loading faculty mapping from JSON file."""
    # Create a temporary faculties.json
    faculties_data = {
        "mapping": {"Faculty of Science": "tnw", "Faculty of Engineering": "et"},
        "short_names": ["tnw", "et"],
        "ut_uuid": "test-uuid",
        "openalex_ut_id": "test-id",
    }
    faculties_path = tmp_path / "faculties.json"
    with faculties_path.open("w", encoding="utf8") as f:
        json.dump(faculties_data, f)

    # Mock settings
    from syntheca.config import settings

    monkeypatch.setattr(settings, "faculties_mapping_path", faculties_path)

    result = load_faculty_mapping()
    assert result["mapping"] == {"Faculty of Science": "tnw", "Faculty of Engineering": "et"}
    assert result["short_names"] == ["tnw", "et"]
    assert result["ut_uuid"] == "test-uuid"


def test_resolve_org_hierarchy_basic(tmp_path, monkeypatch):
    """Test basic org hierarchy resolution."""
    # Setup faculty mapping
    faculties_data = {
        "mapping": {"Faculty of Science": "tnw", "Faculty of Engineering": "et"},
        "short_names": ["tnw", "et"],
    }
    faculties_path = tmp_path / "faculties.json"
    with faculties_path.open("w", encoding="utf8") as f:
        json.dump(faculties_data, f)

    from syntheca.config import settings

    monkeypatch.setattr(settings, "faculties_mapping_path", faculties_path)

    # Create sample org data
    orgs_df = pl.from_dicts(
        [
            {
                "internal_repository_id": "org1",
                "name": "Department of Physics",
                "part_of": {"name": "Faculty of Science", "id": "fac1"},
            },
            {
                "internal_repository_id": "org2",
                "name": "Department of Mech Eng",
                "part_of": {"name": "Faculty of Engineering", "id": "fac2"},
            },
            {
                "internal_repository_id": "fac1",
                "name": "Faculty of Science",
                "part_of": {"name": "University", "id": "uni1"},
            },
        ]
    )

    result = resolve_org_hierarchy(orgs_df)

    # Check that parent_org is resolved and mapped
    assert "parent_org" in result.columns
    assert "tnw" in result.columns
    assert "et" in result.columns

    # Check specific mappings
    physics_row = result.filter(pl.col("internal_repository_id") == "org1").to_dicts()[0]
    assert physics_row["parent_org"] == "tnw"
    assert physics_row["tnw"] is True
    assert physics_row["et"] is False


def test_resolve_org_hierarchy_faculty_self_reference(tmp_path, monkeypatch):
    """Test that faculties use their own name as parent_org."""
    faculties_data = {
        "mapping": {"Faculty of Science": "tnw"},
        "short_names": ["tnw"],
    }
    faculties_path = tmp_path / "faculties.json"
    with faculties_path.open("w", encoding="utf8") as f:
        json.dump(faculties_data, f)

    from syntheca.config import settings

    monkeypatch.setattr(settings, "faculties_mapping_path", faculties_path)

    orgs_df = pl.from_dicts(
        [
            {
                "internal_repository_id": "fac1",
                "name": "Faculty of Science",
                "part_of": {"name": "University", "id": "uni1"},
            }
        ]
    )

    result = resolve_org_hierarchy(orgs_df)

    # Faculty should use its own name (mapped to short code) as parent_org
    fac_row = result.to_dicts()[0]
    assert fac_row["parent_org"] == "tnw"
    assert fac_row["tnw"] is True


def test_map_author_affiliations_basic(tmp_path, monkeypatch):
    """Test mapping authors to organizations with faculty flags."""
    # Setup
    faculties_data = {
        "mapping": {"Faculty of Science": "tnw"},
        "short_names": ["tnw"],
        "ut_uuid": "ut-test-uuid",
    }
    faculties_path = tmp_path / "faculties.json"
    with faculties_path.open("w", encoding="utf8") as f:
        json.dump(faculties_data, f)

    from syntheca.config import settings

    monkeypatch.setattr(settings, "faculties_mapping_path", faculties_path)

    # Authors with affiliations
    authors_df = pl.from_dicts(
        [
            {
                "internal_repository_id": "author1",
                "name": "John Doe",
                "affiliations": [
                    {"internal_repository_id": "org1", "name": "Department of Physics"},
                    {"internal_repository_id": "ut-test-uuid", "name": "University of Twente"},
                ],
            }
        ]
    )

    # Processed orgs with faculty flags
    orgs_df = pl.from_dicts(
        [
            {"internal_repository_id": "org1", "name": "Department of Physics", "tnw": True},
            {
                "internal_repository_id": "ut-test-uuid",
                "name": "University of Twente",
                "tnw": False,
            },
        ]
    )

    result = map_author_affiliations(authors_df, orgs_df)

    # Check result
    assert "affiliation_names_pure" in result.columns
    assert "affiliation_ids_pure" in result.columns
    assert "is_ut" in result.columns
    assert "tnw" in result.columns

    author_row = result.to_dicts()[0]
    assert author_row["is_ut"] is True
    assert author_row["tnw"] is True


def test_map_author_affiliations_no_affiliations_column():
    """Test handling when affiliations column is missing."""
    authors_df = pl.from_dicts([{"internal_repository_id": "author1", "name": "John Doe"}])
    orgs_df = pl.from_dicts([{"internal_repository_id": "org1", "name": "Department"}])

    result = map_author_affiliations(authors_df, orgs_df)

    # Should return original DataFrame unchanged
    assert result.equals(authors_df)


def test_resolve_org_hierarchy_no_part_of_column():
    """Test handling when part_of column is missing."""
    orgs_df = pl.from_dicts([{"internal_repository_id": "org1", "name": "Department of Physics"}])

    result = resolve_org_hierarchy(orgs_df)

    # Should return original DataFrame unchanged
    assert result.equals(orgs_df)
