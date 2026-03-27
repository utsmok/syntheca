from syntheca.utils.polars_frames import robust_from_dicts


def test_robust_from_dicts_infers_sparse_late_columns_beyond_default_sample_window() -> None:
    rows: list[dict[str, object]] = [
        {"id": f"row-{index}", "title": f"Title {index}"} for index in range(150)
    ]
    rows[-1]["authors"] = [{"person_id": "p-1", "family_names": "Example"}]

    df = robust_from_dicts(rows)

    assert "authors" in df.columns
    assert df["authors"].to_list()[0] is None
    assert df["authors"].to_list()[-1] == [{"person_id": "p-1", "family_names": "Example"}]
