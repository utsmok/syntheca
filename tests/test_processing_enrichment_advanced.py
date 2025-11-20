"""Tests for parse_scraped_org_details and apply_manual_corrections."""

from __future__ import annotations

import json

import polars as pl

from syntheca.processing.enrichment import apply_manual_corrections, parse_scraped_org_details


def test_parse_scraped_org_details_basic(tmp_path, monkeypatch):
    """Test parsing of nested organizational details from scraped data."""
    # Setup faculty mapping
    faculties_data = {
        "mapping": {
            "Faculty of Science and Technology": "tnw",
            "Digital Society Institute": "dsi",
        },
        "short_names": ["tnw", "dsi"],
    }
    faculties_path = tmp_path / "faculties.json"
    with faculties_path.open("w", encoding="utf8") as f:
        json.dump(faculties_data, f)

    from syntheca.config import settings

    monkeypatch.setattr(settings, "faculties_mapping_path", faculties_path)

    # Create sample DataFrame with org_details_pp
    authors_df = pl.from_dicts(
        [
            {
                "name": "John Doe",
                "org_details_pp": [
                    {
                        "faculty": {"name": "Faculty of Science and Technology", "abbr": "TNW"},
                        "department": {"name": "Department of Physics", "abbr": "PHY"},
                        "group": {"name": "Quantum Research", "abbr": "QR"},
                    },
                    {
                        "faculty": {"name": "Digital Society Institute", "abbr": "DSI"},
                        "department": None,
                        "group": None,
                    },
                ],
            }
        ]
    )

    result = parse_scraped_org_details(authors_df)

    # Check boolean flags are set
    assert "tnw" in result.columns
    assert "dsi" in result.columns
    row = result.to_dicts()[0]
    assert row["tnw"] is True
    assert row["dsi"] is True

    # Check extracted strings
    assert "faculty" in result.columns
    assert "institute" in result.columns
    assert "department" in result.columns
    assert "group" in result.columns
    assert row["faculty"] == "Faculty of Science and Technology"
    assert row["institute"] == "Digital Society Institute"
    assert row["department"] == "Department of Physics"
    assert row["group"] == "Quantum Research"


def test_parse_scraped_org_details_no_column(tmp_path, monkeypatch):
    """Test handling when org_details_pp column is missing."""
    faculties_data = {"mapping": {"Faculty of Science": "tnw"}, "short_names": ["tnw"]}
    faculties_path = tmp_path / "faculties.json"
    with faculties_path.open("w", encoding="utf8") as f:
        json.dump(faculties_data, f)

    from syntheca.config import settings

    monkeypatch.setattr(settings, "faculties_mapping_path", faculties_path)

    authors_df = pl.from_dicts([{"name": "John Doe"}])

    result = parse_scraped_org_details(authors_df)

    # Should add boolean columns but not fail
    assert "tnw" in result.columns
    assert result.to_dicts()[0]["tnw"] is False


def test_apply_manual_corrections_basic(tmp_path, monkeypatch):
    """Test applying manual corrections from corrections.json."""
    # Setup corrections
    corrections_data = [
        {"name": "John Smith", "affiliations": ["TNW-PHY-QR"]},
        {"name": "Jane Doe", "affiliations": ["dsi"]},
    ]
    corrections_path = tmp_path / "corrections.json"
    with corrections_path.open("w", encoding="utf8") as f:
        json.dump(corrections_data, f)

    from syntheca.config import settings

    monkeypatch.setattr(settings, "corrections_mapping_path", corrections_path)

    # Create publications DataFrame with author names
    pubs_df = pl.from_dicts(
        [
            {
                "pure_id": "pub1",
                "title": "Paper 1",
                "pure_authors_names": ["John Smith", "Other Author"],
                "tnw": False,
                "dsi": False,
                "faculty_abbr": None,
                "department_abbr": None,
                "group_abbr": None,
                "institute": None,
            },
            {
                "pure_id": "pub2",
                "title": "Paper 2",
                "pure_authors_names": ["Jane Doe"],
                "tnw": False,
                "dsi": False,
                "faculty_abbr": None,
                "department_abbr": None,
                "group_abbr": None,
                "institute": None,
            },
        ]
    )

    result = apply_manual_corrections(pubs_df)

    # Check that corrections were applied
    pub1 = result.filter(pl.col("pure_id") == "pub1").to_dicts()[0]
    assert pub1["tnw"] is True
    assert pub1["faculty_abbr"] == "TNW"
    assert pub1["department_abbr"] == "PHY"
    assert pub1["group_abbr"] == "QR"

    pub2 = result.filter(pl.col("pure_id") == "pub2").to_dicts()[0]
    assert pub2["dsi"] is True
    assert pub2["institute"] == "dsi"


def test_apply_manual_corrections_no_file(tmp_path, monkeypatch):
    """Test handling when corrections file doesn't exist."""
    corrections_path = tmp_path / "nonexistent.json"

    from syntheca.config import settings

    monkeypatch.setattr(settings, "corrections_mapping_path", corrections_path)

    pubs_df = pl.from_dicts([{"pure_id": "pub1", "title": "Paper 1"}])

    result = apply_manual_corrections(pubs_df)

    # Should return original DataFrame unchanged
    assert result.equals(pubs_df)


def test_apply_manual_corrections_no_pure_authors_names(tmp_path, monkeypatch):
    """Test handling when pure_authors_names column is missing."""
    corrections_data = [{"name": "John Smith", "affiliations": ["TNW-PHY-QR"]}]
    corrections_path = tmp_path / "corrections.json"
    with corrections_path.open("w", encoding="utf8") as f:
        json.dump(corrections_data, f)

    from syntheca.config import settings

    monkeypatch.setattr(settings, "corrections_mapping_path", corrections_path)

    pubs_df = pl.from_dicts([{"pure_id": "pub1", "title": "Paper 1"}])

    result = apply_manual_corrections(pubs_df)

    # Should return original DataFrame unchanged
    assert result.equals(pubs_df)


def test_parse_scraped_org_details_filters_faculty_vs_institute(tmp_path, monkeypatch):
    """Test that faculty and institute names are correctly filtered."""
    faculties_data = {
        "mapping": {"Faculty of Science": "tnw", "DSI Institute": "dsi"},
        "short_names": ["tnw", "dsi"],
    }
    faculties_path = tmp_path / "faculties.json"
    with faculties_path.open("w", encoding="utf8") as f:
        json.dump(faculties_data, f)

    from syntheca.config import settings

    monkeypatch.setattr(settings, "faculties_mapping_path", faculties_path)

    authors_df = pl.from_dicts(
        [
            {
                "name": "John Doe",
                "org_details_pp": [
                    {
                        "faculty": {"name": "Faculty of Science", "abbr": "TNW"},
                        "department": None,
                        "group": None,
                    },
                    {
                        "faculty": {"name": "DSI Institute", "abbr": "DSI"},
                        "department": None,
                        "group": None,
                    },
                ],
            }
        ]
    )

    result = parse_scraped_org_details(authors_df)

    row = result.to_dicts()[0]
    # Faculty should contain items with "Faculty" in the name
    assert "Faculty of Science" in row["faculty"]
    # Institute should contain items without "Faculty"
    assert "DSI Institute" in row["institute"]
