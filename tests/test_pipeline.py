import pathlib

import polars as pl
import pytest

from syntheca.pipeline import Pipeline


@pytest.mark.asyncio
async def test_pipeline_merge_and_write(tmp_path: pathlib.Path):
    oils = pl.DataFrame(
        {
            "doi": ["10.1000/ABC", "10.2000/DEF"],
            "title": ["A study on X", "Another study"],
        }
    )
    full = pl.DataFrame(
        {
            "doi": ["https://doi.org/10.1000/abc"],
            "title": ["A study on X"],
            "extra": ["info"],
        }
    )

    p = Pipeline()
    merged = await p.run(pure_publications_df=oils, openalex_works_df=full, output_dir=tmp_path)
    # Expect merged contains the 'extra' column and has merged rows
    assert "extra" in merged.columns
    assert (tmp_path / "merged.parquet").exists()
    assert (tmp_path / "merged.xlsx").exists()
    assert (tmp_path / "merged.reconciled.parquet").exists()
    assert (tmp_path / "merged.reconciled.xlsx").exists()
    assert (tmp_path / "pure_publications_clean.parquet").exists()
    assert (tmp_path / "openalex_works_clean.parquet").exists()


@pytest.mark.asyncio
async def test_pipeline_does_not_use_cached_orgunits_by_default(monkeypatch):
    calls: list[str] = []

    def _fake_load_dataframe_parquet(name: str):
        calls.append(name)
        return pl.DataFrame(
            {"internal_repository_id": ["org-10"], "name": ["TNW"], "parent_org": [None]}
        )

    monkeypatch.setattr(
        "syntheca.utils.persistence.load_dataframe_parquet",
        _fake_load_dataframe_parquet,
    )

    publications = pl.DataFrame({"doi": ["10.1000/abc"], "title": ["A study on X"]})
    authors = pl.DataFrame(
        {
            "pure_id": ["p-1"],
            "first_names": ["Alice"],
            "family_names": ["Researcher"],
            "affiliation_ids": [["org-10"]],
        }
    )

    merged = await Pipeline().run(pure_publications_df=publications, authors_df=authors)

    assert merged.height == 1
    assert calls == []


@pytest.mark.asyncio
async def test_pipeline_allows_explicit_cached_orgunits_fallback(monkeypatch):
    calls: list[str] = []

    def _fake_load_dataframe_parquet(name: str):
        calls.append(name)
        return pl.DataFrame(
            {
                "internal_repository_id": ["org-10"],
                "name": ["Faculty of Science and Technology"],
                "parent_org": [None],
            }
        )

    monkeypatch.setattr(
        "syntheca.utils.persistence.load_dataframe_parquet",
        _fake_load_dataframe_parquet,
    )

    publications = pl.DataFrame({"doi": ["10.1000/abc"], "title": ["A study on X"]})
    authors = pl.DataFrame(
        {
            "pure_id": ["p-1"],
            "first_names": ["Alice"],
            "family_names": ["Researcher"],
            "affiliation_ids": [["org-10"]],
        }
    )

    merged = await Pipeline().run(
        pure_publications_df=publications,
        authors_df=authors,
        allow_cached_orgunits_fallback=True,
    )

    assert merged.height == 1
    assert calls == ["openaire_cris_orgunits"]


@pytest.mark.asyncio
async def test_pipeline_rejects_dual_orgunit_parameters() -> None:
    publications = pl.DataFrame({"doi": ["10.1000/abc"], "title": ["A study on X"]})
    org_units = pl.DataFrame(
        {"internal_repository_id": ["org-10"], "name": ["TNW"], "parent_org": [None]}
    )

    with pytest.raises(ValueError, match="org-unit input parameter"):
        await Pipeline().run(
            pure_publications_df=publications,
            org_units_df=org_units,
            orgunits_df=org_units,
        )
