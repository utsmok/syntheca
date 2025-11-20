import polars as pl

from syntheca.processing.enrichment import enrich_authors_with_faculties


def test_enrich_authors_with_faculties_adds_columns(tmp_path):
    # Create a dataframe with affiliation_names_pure including a known Faculty name
    df = pl.DataFrame(
        {
            "pure_id": [1],
            "affiliation_names_pure": [["Faculty of Science and Technology"]],
        }
    )
    out = enrich_authors_with_faculties(df)
    # Based on our faculties.json, this should add 'tnw' column
    assert "tnw" in out.columns
    assert out["tnw"][0] is True
