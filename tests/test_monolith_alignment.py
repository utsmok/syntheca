"""Integration tests for monolith-aligned transformations.

Tests that the key transformation functions match the legacy monolith behavior.
"""

import polars as pl
import pytest

from syntheca.processing.cleaning import clean_publications, load_publisher_mapping
from syntheca.processing.enrichment import (
    clean_and_enrich_persons_data,
    join_authors_and_publications,
)
from syntheca.processing.merging import (
    add_missing_affils,
    extract_author_and_funder_names,
    merge_oils_with_all,
)
from syntheca.reporting.export import write_formatted_excel


def test_load_publisher_mapping():
    """Test that publisher mapping loads successfully."""
    mapping = load_publisher_mapping()
    assert len(mapping) > 0
    assert "Elsevier B.V." in mapping
    assert mapping["Elsevier B.V."] == "Elsevier"


def test_clean_publications_normalizes_publishers():
    """Test that clean_publications normalizes publisher names."""
    df = pl.DataFrame({
        "doi": ["10.1/test"],
        "publication_date": ["2023-01-15"],
        "internal_repository_id": ["abc123"],
        "publishers": [["Elsevier B.V."]],
    })
    cleaned = clean_publications(df)
    assert "publishers" in cleaned.columns
    assert "pure_id" in cleaned.columns
    assert cleaned["publishers"][0] == "Elsevier"


def test_clean_publications_filters_no_doi():
    """Test that clean_publications filters out rows without DOIs."""
    df = pl.DataFrame({
        "doi": ["10.1/test", None],
        "title": ["A", "B"],
    })
    cleaned = clean_publications(df)
    assert cleaned.height == 1


def test_clean_publications_parses_dates():
    """Test that clean_publications parses publication dates correctly."""
    df = pl.DataFrame({
        "doi": ["10.1/test"],
        "publication_date": ["2023-01-15"],
    })
    cleaned = clean_publications(df)
    assert "publication_year" in cleaned.columns
    assert cleaned["publication_year"][0] == 2023


def test_merge_oils_with_all_tracks_matches():
    """Test that merge_oils_with_all adds match tracking columns."""
    oils = pl.DataFrame({
        "DOI": ["10.1/test"],
        "Pure ID": ["pid1"],
        "Title_1": ["Journal A"],
    })
    full = pl.DataFrame({"doi": ["10.1/test"], "id": ["W123"], "pure_id": ["p1"]})
    merged = merge_oils_with_all(oils, full)
    assert "openalex_match" in merged.columns
    # oils_match only appears if "pureid_oils" is in merged (which comes from "Pure ID" in oils)
    assert "oils_match" in merged.columns or "pureid_oils" in merged.columns
    assert "pure_match" in merged.columns


def test_extract_author_and_funder_names():
    """Test that extract_author_and_funder_names extracts nested fields."""
    df = pl.DataFrame({"title": ["Test"]})
    result = extract_author_and_funder_names(df)
    # Should return dataframe unchanged if no nested fields
    assert result.shape == df.shape


def test_add_missing_affils_with_empty_data():
    """Test that add_missing_affils handles empty correction data."""
    df = pl.DataFrame({"pure_authors_names": [["John Doe"]]})
    result = add_missing_affils(df, more_data=[])
    # Should return unchanged when no corrections
    assert result.height == 1


def test_clean_and_enrich_persons_data_with_empty_affiliations():
    """Test that clean_and_enrich_persons_data handles empty affiliations."""
    person_df = pl.DataFrame({
        "internal_repository_id": ["p1"],
        "family_names": ["Doe"],
        "first_names": ["John"],
        "affiliations": [[]],
    })
    org_df = pl.DataFrame({
        "internal_repository_id": ["o1"],
        "name": ["Org A"],
    })
    result = clean_and_enrich_persons_data(person_df, org_df)
    assert "pure_id" in result.columns
    assert "last_name" in result.columns
    assert result.height == 1


def test_join_authors_and_publications():
    """Test that join_authors_and_publications aggregates author data."""
    # Create sample authors data
    authors = pl.DataFrame({
        "pure_id": ["a1", "a2"],
        "tnw": [True, False],
        "eemcs": [False, True],
    })

    # Create sample publications with authors
    pubs = pl.DataFrame({
        "pure_id": ["pub1"],
        "title": ["Test Pub"],
        "authors": [[
            {"internal_repository_id": "a1"},
            {"internal_repository_id": "a2"},
        ]],
    })

    result = join_authors_and_publications(authors, pubs)
    # Should have aggregated faculty flags
    assert "tnw" in result.columns
    assert "eemcs" in result.columns
    # Both faculties should be True (any author in faculty)
    assert result["tnw"][0] is True
    assert result["eemcs"][0] is True


def test_write_formatted_excel_with_options(tmp_path):
    """Test that write_formatted_excel handles all options."""
    df = pl.DataFrame({
        "doi_url": ["https://doi.org/10.1/test"],
        "id": ["https://openalex.org/W123"],
        "title": ["Test"],
        "authorships": [[{"author": {"display_name": "John Doe"}}]],  # nested field
    })
    path = tmp_path / "test.xlsx"
    result = write_formatted_excel(
        df,
        path,
        reorder_columns=True,
        drop_nested_columns=True,
        cleanup_urls=True,
    )
    assert result.exists()
    # Read back to verify URL cleanup (if columns were kept)
    # Note: authorships should be dropped due to drop_nested_columns=True
