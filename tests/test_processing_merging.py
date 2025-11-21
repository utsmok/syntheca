import polars as pl

from syntheca.processing.merging import merge_datasets
from syntheca.processing.merging import join_authors_and_publications


def test_merge_datasets_normalizes_and_joins():
    oils = pl.DataFrame(
        {
            "doi": ["https://doi.org/10.11/AA ", "10.22/BB"],
            "oils_extra": [1, 2],
        }
    )
    full = pl.DataFrame({"doi": ["10.11/aa", "10.33/cc"], "title": ["A", "B"]})

    merged = merge_datasets(oils, full)
    # check that a normalized DOI used to join results in data from both sides
    assert "title" in merged.columns
    # joined row for first doi should have title 'A'
    assert merged.filter(pl.col("doi") == "10.11/aa").select("title").to_series()[0] == "A"


def test_join_authors_and_publications_aggregates_author_flags():
    authors = pl.DataFrame(
        {
            "pure_id": [1],
            "tnw": [True],
            "faculty": ["Faculty of Science and Technology"],
            "orcid": ["0000-0000"],
        }
    )
    pubs = pl.DataFrame(
        {"pure_id": ["p1"], "authors": [[{"internal_repository_id": 1}]], "title": ["T"]}
    )
    out = join_authors_and_publications(authors, pubs)
    # Expect the bool column to be present and aggregated
    assert "tnw" in out.columns
    assert out["tnw"][0] is True
    # Expect faculty column present with expected string
    assert "faculty" in out.columns
    faculty_val = out["faculty"].to_list()[0]
    assert isinstance(faculty_val, str)
    assert "Faculty of Science" in faculty_val
