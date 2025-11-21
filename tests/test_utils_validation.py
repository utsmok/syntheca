import polars as pl
from syntheca.utils.validation import normalize_str_column, ensure_columns, normalize_orgs_df


def test_normalize_str_column_list_and_none():
    df = pl.DataFrame({"name": [["Faculty of Science and Technology"], [], ["Some Org"]]})
    out = normalize_str_column(df, "name")
    assert out["name"].to_list() == ["Faculty of Science and Technology", None, "Some Org"]


def test_ensure_columns_adds_missing():
    df = pl.DataFrame({"a": [1, 2]})
    out = ensure_columns(df, {"a": int, "b": str, "c": str})
    assert "b" in out.columns and "c" in out.columns
    assert out["b"].is_null().all()


def test_normalize_orgs_df_turns_list_columns_to_scalar():
    df = pl.DataFrame(
        {
            "internal_repository_id": ["org1"],
            "name": [["Faculty of Science and Technology"]],
            "parent_org": [[None]],
            "tnw": [True],
        }
    )
    out = normalize_orgs_df(df)
    assert out["name"].to_list()[0] == "Faculty of Science and Technology"
    assert out["parent_org"].to_list()[0] is None
