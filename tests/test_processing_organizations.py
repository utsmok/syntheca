import polars as pl

from syntheca.processing.organizations import resolve_org_hierarchy, map_author_affiliations


def test_resolve_org_hierarchy_maps_parent_and_flags():
    # Create a simple orgs dataframe with part_of information
    df = pl.DataFrame(
        {
            "internal_repository_id": ["id1", "id2"],
            "name": ["Faculty of Science and Technology", "Some Department"],
            "part_of": [None, {"name": "Faculty of Science and Technology"}],
        }
    )
    out = resolve_org_hierarchy(df)
    assert "parent_org" in out.columns
    assert (
        out.filter(pl.col("internal_repository_id") == "id2").select("parent_org").to_series()[0]
        == "tnw"
    )
    # id1 is the faculty itself; parent should be itself when name is faculty
    assert (
        out.filter(pl.col("internal_repository_id") == "id1").select("parent_org").to_series()[0]
        == "tnw"
    )
    assert "tnw" in out.columns
    assert (
        out.filter(pl.col("internal_repository_id") == "id1").select("tnw").to_series()[0] is True
    )


def test_map_author_affiliations_aggregates_flags_and_names():
    authors = pl.DataFrame(
        {"pure_id": [1, 2], "affiliations": [[{"internal_repository_id": "id2"}], None]}
    )
    orgs = pl.DataFrame(
        {
            "internal_repository_id": ["id2"],
            "name": ["Some Department"],
            "parent_org": ["tnw"],
            "tnw": [True],
        }
    )
    out = map_author_affiliations(authors, orgs)
    assert "affiliation_ids_pure" in out.columns
    assert out.filter(pl.col("pure_id") == 1).select("tnw").to_series()[0] is True
    assert out.filter(pl.col("pure_id") == 2).select("tnw").to_series()[0] is False


def test_resolve_org_hierarchy_handles_list_input_names():
    # ensure our normalization handles lists and empty lists as names
    df = pl.DataFrame(
        {
            "internal_repository_id": ["id1", "id2"],
            "name": [["Faculty of Science and Technology"], []],
            "part_of": [None, {"name": "Faculty of Science and Technology"}],
        }
    )
    out = resolve_org_hierarchy(df)
    assert out["name"].to_list()[0] == "tnw"
    # name list empty should get parent_org mapping
    assert (out["parent_org"].to_list()[1] == "tnw") or (out["name"].to_list()[1] == "tnw")
