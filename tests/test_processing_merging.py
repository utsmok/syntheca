import polars as pl

from syntheca.processing.merging import merge_datasets


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
