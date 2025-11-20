"""Tests for join_authors_and_publications function."""

from __future__ import annotations

import polars as pl
import pytest

from syntheca.processing.merging import join_authors_and_publications


def test_join_authors_and_publications_basic():
    """Test basic author-publication joining with faculty aggregation."""
    # Create sample publications with authors
    publications_df = pl.from_dicts(
        [
            {
                "pure_id": "pub1",
                "title": "Paper 1",
                "authors": [
                    {"internal_repository_id": "author1", "name": "John Doe"},
                    {"internal_repository_id": "author2", "name": "Jane Smith"},
                ],
            },
            {
                "pure_id": "pub2",
                "title": "Paper 2",
                "authors": [{"internal_repository_id": "author1", "name": "John Doe"}],
            },
        ]
    )

    # Create authors DataFrame with faculty flags
    authors_df = pl.from_dicts(
        [
            {
                "pure_id": "author1",
                "name": "John Doe",
                "tnw": True,
                "eemcs": False,
                "faculty": "Faculty of Science",
                "department": "Physics",
                "orcid": "0000-0001-2345-6789",
            },
            {
                "pure_id": "author2",
                "name": "Jane Smith",
                "tnw": False,
                "eemcs": True,
                "faculty": "Faculty of EEMCS",
                "department": "Computer Science",
                "orcid": "0000-0001-9876-5432",
            },
        ]
    )

    result = join_authors_and_publications(publications_df, authors_df)

    # Check result structure
    assert "tnw" in result.columns
    assert "eemcs" in result.columns
    assert "orcids" in result.columns

    # Check pub1 (has both authors, should have both faculties)
    pub1 = result.filter(pl.col("pure_id") == "pub1").to_dicts()[0]
    assert pub1["tnw"] is True
    assert pub1["eemcs"] is True
    assert len(pub1["orcids"]) == 2

    # Check pub2 (has only author1, should have only tnw)
    pub2 = result.filter(pl.col("pure_id") == "pub2").to_dicts()[0]
    assert pub2["tnw"] is True
    assert pub2["eemcs"] is False
    assert len(pub2["orcids"]) == 1


def test_join_authors_and_publications_list_aggregation():
    """Test that list columns are properly aggregated and deduplicated."""
    publications_df = pl.from_dicts(
        [
            {
                "pure_id": "pub1",
                "title": "Paper 1",
                "authors": [
                    {"internal_repository_id": "author1", "name": "John Doe"},
                    {"internal_repository_id": "author2", "name": "Jane Smith"},
                ],
            }
        ]
    )

    authors_df = pl.from_dicts(
        [
            {
                "pure_id": "author1",
                "faculty": "Faculty of Science, Institute A",
                "department": "Physics",
                "group": "Quantum Group",
            },
            {
                "pure_id": "author2",
                "faculty": "Faculty of Science, Institute B",
                "department": "Chemistry, Physics",  # Duplicate Physics
                "group": "Materials Group, Quantum Group",  # Duplicate Quantum Group
            },
        ]
    )

    result = join_authors_and_publications(publications_df, authors_df)

    pub1 = result.to_dicts()[0]

    # Check that list columns contain unique values from both authors
    assert "faculty" in result.columns
    faculty_items = pub1["faculty"]
    assert len(faculty_items) == 3  # Science, Institute A, Institute B (unique)

    department_items = pub1["department"]
    # Should have Physics, Chemistry (unique)
    assert len(department_items) == 2

    group_items = pub1["group"]
    # Should have Quantum Group, Materials Group (unique)
    assert len(group_items) == 2


def test_join_authors_and_publications_missing_authors():
    """Test handling publications with no matching authors."""
    publications_df = pl.from_dicts(
        [
            {
                "pure_id": "pub1",
                "title": "Paper 1",
                "authors": [{"internal_repository_id": "author999", "name": "Unknown"}],
            }
        ]
    )

    authors_df = pl.from_dicts(
        [{"pure_id": "author1", "name": "John Doe", "tnw": True, "eemcs": False}]
    )

    result = join_authors_and_publications(publications_df, authors_df)

    # Publication should still exist but with null/false faculty flags
    pub1 = result.to_dicts()[0]
    assert pub1["pure_id"] == "pub1"
    # Flags should be null or false since no matching author
    if "tnw" in pub1:
        assert pub1["tnw"] is None or pub1["tnw"] is False


def test_join_authors_and_publications_rename_internal_repository_id():
    """Test that internal_repository_id is renamed to pure_id if needed."""
    publications_df = pl.from_dicts(
        [
            {
                "pure_id": "pub1",
                "title": "Paper 1",
                "authors": [{"internal_repository_id": "author1", "name": "John Doe"}],
            }
        ]
    )

    # Use internal_repository_id instead of pure_id
    authors_df = pl.from_dicts(
        [{"internal_repository_id": "author1", "name": "John Doe", "tnw": True}]
    )

    result = join_authors_and_publications(publications_df, authors_df)

    # Should work without error
    assert "tnw" in result.columns
    pub1 = result.to_dicts()[0]
    assert pub1["tnw"] is True


def test_join_authors_and_publications_no_pure_id_raises_error():
    """Test that missing pure_id or internal_repository_id raises an error."""
    publications_df = pl.from_dicts(
        [
            {
                "pure_id": "pub1",
                "title": "Paper 1",
                "authors": [{"internal_repository_id": "author1", "name": "John Doe"}],
            }
        ]
    )

    # Authors without proper ID column
    authors_df = pl.from_dicts([{"name": "John Doe", "tnw": True}])

    with pytest.raises(ValueError, match="must contain either"):
        join_authors_and_publications(publications_df, authors_df)


def test_join_authors_and_publications_boolean_any_logic():
    """Test that boolean columns use 'any' logic correctly."""
    publications_df = pl.from_dicts(
        [
            {
                "pure_id": "pub1",
                "title": "Paper 1",
                "authors": [
                    {"internal_repository_id": "author1", "name": "Author 1"},
                    {"internal_repository_id": "author2", "name": "Author 2"},
                    {"internal_repository_id": "author3", "name": "Author 3"},
                ],
            }
        ]
    )

    authors_df = pl.from_dicts(
        [
            {"pure_id": "author1", "tnw": False, "dsi": False},
            {"pure_id": "author2", "tnw": True, "dsi": False},  # One author has tnw=True
            {"pure_id": "author3", "tnw": False, "dsi": False},
        ]
    )

    result = join_authors_and_publications(publications_df, authors_df)

    pub1 = result.to_dicts()[0]
    # Since one author has tnw=True, publication should have tnw=True
    assert pub1["tnw"] is True
    # No authors have dsi=True, so publication should have dsi=False
    assert pub1["dsi"] is False
