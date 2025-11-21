import polars as pl

from syntheca.processing.enrichment import parse_scraped_org_details, apply_manual_corrections


def test_parse_scraped_org_details_creates_columns():
    # Org details example: list of dicts of structures
    df = pl.DataFrame(
        {
            "pure_id": [1],
            "org_details_pp": [
                [
                    {
                        "faculty": {"name": "Faculty of Science and Technology", "abbr": "TNW"},
                        "department": {"name": "Computer Science", "abbr": "CS"},
                        "group": {"name": "Human-Computer Interaction", "abbr": "HCI"},
                    },
                ]
            ],
        }
    )
    out = parse_scraped_org_details(df)
    assert "faculty" in out.columns
    assert out["faculty"][0] == "Faculty of Science and Technology"
    assert "faculty_abbr" in out.columns
    assert out["faculty_abbr"][0] == "TNW"
    assert "tnw" in out.columns
    assert out["tnw"][0] is True


def test_apply_manual_corrections_overlays_affiliations(tmp_path):
    # corrections.json is present in repo; we will use an example from the mapping
    df = pl.DataFrame(
        {
            "pure_id": [1],
            "first_names": ["Ioannis"],
            "family_names": ["Sechopoulos"],
            "found_name": [None],
        }
    )
    out = apply_manual_corrections(df)
    # Expect the manual corrections to add affiliation_ids_pure or update rows
    assert "affiliation_ids_pure" in out.columns
    # Expect the corrected row to include the known mapping from corrections.json
    vals = out["affiliation_ids_pure"].to_list()[0]
    assert vals is not None and isinstance(vals, list)
