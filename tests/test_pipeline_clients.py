import pathlib
from typing import cast

import polars as pl
import pytest

from syntheca.clients.openalex import OpenAlexClient
from syntheca.clients.pure_oai import PureOAIClient
from syntheca.clients.ut_people import UTPeopleClient
from syntheca.pipeline import Pipeline


class FakePureClient:
    async def get_all_records(self, collections):
        return {
            "publications": [
                {
                    "id": "oils:1",
                    "title": "A sample oils publication",
                    "doi": "10.1/test",
                }
            ]
        }


class FakeOpenAlexWork:
    def __init__(self, id, doi, display_name, publication_year):
        self.id = id
        self.doi = doi
        self.display_name = display_name
        self.publication_year = publication_year


class FakeOpenAlexClient:
    def __init__(self, works=None):
        self.works = works or [FakeOpenAlexWork("I1", "10.1/test", "Openalex Paper", 2020)]

    async def get_works_by_ids(self, ids, position: int | None = None):
        return self.works


class FakeUTPeopleClient:
    async def search_person(self, name: str):
        return [
            {
                "id": f"ut-{name}",
                "family_names": "Doe",
                "first_names": "John",
                "affiliation_names_pure": ["Faculty of Science and Technology"],
            }
        ]


@pytest.mark.asyncio
async def test_pipeline_ingest_pure(tmp_path: pathlib.Path):
    pure = FakePureClient()
    p = Pipeline()
    merged = await p.run(
        oils_df=None,
        full_df=pl.DataFrame(),
        output_dir=tmp_path,
        pure_client=cast(PureOAIClient, pure),
    )
    assert "title" in merged.columns or "doi" in merged.columns
    assert (tmp_path / "merged.parquet").exists()


@pytest.mark.asyncio
async def test_pipeline_ingest_openalex(tmp_path: pathlib.Path):
    openalex_client = FakeOpenAlexClient()
    p = Pipeline()
    merged = await p.run(
        oils_df=pl.DataFrame(),
        full_df=None,
        output_dir=tmp_path,
        openalex_client=cast(OpenAlexClient, openalex_client),
        openalex_ids=["10.1/test"],
    )
    assert "display_name" in merged.columns or "doi" in merged.columns
    assert (tmp_path / "merged.parquet").exists()


@pytest.mark.asyncio
async def test_pipeline_ingest_ut_people(tmp_path: pathlib.Path):
    ut = FakeUTPeopleClient()
    p = Pipeline()
    merged = await p.run(
        oils_df=pl.DataFrame(),
        full_df=pl.DataFrame(),
        output_dir=tmp_path,
        ut_people_client=cast(UTPeopleClient, ut),
        people_search_names=["john.doe"],
    )
    assert isinstance(merged, pl.DataFrame)
    # pipeline should not raise; authors ingestion should have produced a DataFrame internally
    assert (tmp_path / "merged.parquet").exists()
