import polars as pl

from syntheca.clients.ut_people import UTPeopleClient
from syntheca.config import settings
from syntheca.processing.enrichment import apply_manual_corrections, parse_scraped_org_details


def test_parse_scraped_org_details_creates_columns():
    # Org details example: list of dicts of structures
    df = pl.DataFrame(
        {
            "pure_id": [1],
            "org_details_pp": [
                [
                    {
                        "unit": {"name": "Faculty of Science and Technology", "abbr": "TNW"},
                        "faculty": {"name": "Faculty of Science and Technology", "abbr": "TNW"},
                        "department": {"name": "Computer Science", "abbr": "CS"},
                        "group": {"name": "Human-Computer Interaction", "abbr": "HCI"},
                        "hierarchy": [
                            {
                                "name": "Faculty of Science and Technology",
                                "abbr": "TNW",
                                "level": 1,
                                "raw_text": "Faculty of Science and Technology (TNW)",
                            },
                            {
                                "name": "Computer Science",
                                "abbr": "CS",
                                "level": 2,
                                "raw_text": "Computer Science (CS)",
                            },
                            {
                                "name": "Human-Computer Interaction",
                                "abbr": "HCI",
                                "level": 3,
                                "raw_text": "Human-Computer Interaction (HCI)",
                            },
                        ],
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


def test_parse_scraped_org_details_keeps_nonfaculty_branch_out_of_faculty_columns():
    client = UTPeopleClient()
    parsed = client._parse_organization_details(
        '<h2 class="heading2">Organisations</h2>'
        '<div class="widget widget-linklist"><ul>'
        '<li class="widget-linklist__item widget-linklist__item--level1">'
        '<span class="widget-linklist__text">Library, ICT-Services &amp; Archive (LISA)</span>'
        "</li>"
        '<li class="widget-linklist__item widget-linklist__item--level2">'
        '<span class="widget-linklist__text">Embedded Information Services (LISA-EIS)</span>'
        "</li>"
        '<li class="widget-linklist__item widget-linklist__item--level1">'
        '<span class="widget-linklist__text">Faculty of Electrical Engineering, Mathematics and Computer Science (EEMCS)</span>'
        "</li>"
        "</ul></div>"
    )

    df = pl.DataFrame({"pure_id": [1], "org_details_pp": [parsed]})
    out = parse_scraped_org_details(df)

    assert parsed is not None
    assert parsed[0]["faculty"]["name"] is None
    assert parsed[0]["hierarchy"][0]["abbr"] == "LISA"
    assert (
        out["faculty"][0] == "Faculty of Electrical Engineering, Mathematics and Computer Science"
    )
    assert out["department"][0] is None
    assert out["eemcs"][0] is True


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


def test_apply_manual_corrections_normalizes_mixed_affiliation_id_values_before_dataframe_creation(
    tmp_path,
):
    corrections_path = tmp_path / "corrections.json"
    corrections_path.write_text(
        '[{"name": "Bob Two", "affiliations": ["org-corrected", "org-extra"]}]',
        encoding="utf-8",
    )

    original_path = settings.corrections_mapping_path
    settings.corrections_mapping_path = corrections_path

    try:
        df = pl.DataFrame(
            {
                "pure_id": [1, 2],
                "first_names": ["Alice", "Bob"],
                "family_names": ["One", "Two"],
                "found_name": ["Alice One", "Bob Two"],
                "affiliation_ids_pure": ["org-existing", None],
            }
        )

        out = apply_manual_corrections(df)

        assert out.schema["affiliation_ids_pure"] == pl.List(pl.Utf8)
        assert out["affiliation_ids_pure"].to_list() == [
            ["org-existing"],
            ["org-corrected", "org-extra"],
        ]
    finally:
        settings.corrections_mapping_path = original_path
