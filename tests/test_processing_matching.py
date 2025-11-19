import polars as pl

from syntheca.processing.matching import calculate_fuzzy_match


def test_calculate_fuzzy_match_exact():
    df = pl.DataFrame({"a": ["Hello"], "b": ["Hello"]})
    out = calculate_fuzzy_match(df, "a", "b")
    # ratio of identical strings should be 1.0
    assert out["fuzzy_score"][0] == 1.0


def test_calculate_fuzzy_match_partial():
    df = pl.DataFrame({"a": ["Foo"], "b": ["Foe"]})
    out = calculate_fuzzy_match(df, "a", "b")
    assert 0.4 < out["fuzzy_score"][0] < 1.0
