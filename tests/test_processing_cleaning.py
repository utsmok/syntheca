import polars as pl

from syntheca.processing.cleaning import clean_publications, normalize_doi


def test_normalize_doi_lowercases_and_strips():
    df = pl.DataFrame({"doi": ["https://doi.org/10.1000/ABC ", " 10.123/XYZ"]})
    out = normalize_doi(df, "doi")
    assert out["doi"][0] == "10.1000/abc"
    assert out["doi"][1] == "10.123/xyz"


def test_clean_publications_parses_year():
    df = pl.DataFrame({"doi": ["10.1/2"], "publication_date": ["2020-05-01"]})
    out = clean_publications(df)
    assert "publication_year" in out.columns
    assert out["publication_year"][0] == 2020
