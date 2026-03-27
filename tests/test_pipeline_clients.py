import pathlib
from typing import cast

import polars as pl
import pytest
from httpx import HTTPStatusError, Request, Response

from syntheca.clients.openalex import OpenAlexClient
from syntheca.clients.pure_oai import PureOAIClient
from syntheca.clients.ut_people import UTPeopleClient
from syntheca.pipeline import Pipeline


class FakePureClient:
    async def get_all_records(self, collections):
        return {
            "openaire_cris_publications": [
                {
                    "id": "oils:1",
                    "title": "A sample oils publication",
                    "doi": "10.1/test",
                }
            ]
        }


class MixedSchemaPureClient:
    async def get_all_records(self, collections):
        return {
            "openaire_cris_publications": [
                {
                    "id": "pub-1",
                    "title": "First mixed publication",
                    "doi": "10.1/mixed-1",
                    "volume": 12,
                    "issue": 4,
                    "start_page": 100,
                    "end_page": 115,
                },
                {
                    "id": "pub-2",
                    "title": "Second mixed publication",
                    "doi": "10.1/mixed-2",
                    "volume": "13",
                    "issue": "32",
                    "start_page": "210",
                    "end_page": "230",
                },
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


class FailingOpenAlexClient:
    async def get_works_by_ids(self, ids, position: int | None = None):
        request = Request("GET", "https://api.openalex.org/works")
        response = Response(400, request=request)
        raise HTTPStatusError("OpenAlex batch failed", request=request, response=response)


@pytest.mark.asyncio
async def test_pipeline_ingest_pure(tmp_path: pathlib.Path):
    pure = FakePureClient()
    p = Pipeline()
    merged = await p.run(
        pure_publications_df=None,
        openalex_works_df=pl.DataFrame(),
        output_dir=tmp_path,
        pure_client=cast(PureOAIClient, pure),
    )
    assert "title" in merged.columns or "doi" in merged.columns
    assert (tmp_path / "merged.parquet").exists()


@pytest.mark.asyncio
async def test_pipeline_ingest_pure_with_mixed_bibliographic_scalar_types(
    tmp_path: pathlib.Path,
):
    pure = MixedSchemaPureClient()
    p = Pipeline()

    merged = await p.run(
        pure_publications_df=None,
        openalex_works_df=pl.DataFrame(),
        output_dir=tmp_path,
        pure_client=cast(PureOAIClient, pure),
    )

    assert merged.height == 2
    assert (tmp_path / "merged.parquet").exists()


@pytest.mark.asyncio
async def test_pipeline_ingest_openalex(tmp_path: pathlib.Path):
    openalex_client = FakeOpenAlexClient()
    p = Pipeline()
    merged = await p.run(
        pure_publications_df=pl.DataFrame(),
        openalex_works_df=None,
        output_dir=tmp_path,
        openalex_client=cast(OpenAlexClient, openalex_client),
        openalex_ids=["10.1/test"],
    )
    assert "display_name" in merged.columns or "doi" in merged.columns
    assert (tmp_path / "merged.parquet").exists()


@pytest.mark.asyncio
async def test_pipeline_continues_when_openalex_retrieval_fails(tmp_path: pathlib.Path):
    p = Pipeline()
    merged = await p.run(
        pure_publications_df=pl.DataFrame(
            {"title": ["Pure fallback publication"], "doi": ["10.1/pure-only"]}
        ),
        openalex_works_df=None,
        output_dir=tmp_path,
        openalex_client=cast(OpenAlexClient, FailingOpenAlexClient()),
        openalex_ids=["10.1/will-fail"],
    )

    assert isinstance(merged, pl.DataFrame)
    assert merged.height == 1
    assert "doi" in merged.columns
    assert merged["doi"].to_list() == ["10.1/pure-only"]
    assert (tmp_path / "merged.parquet").exists()


@pytest.mark.asyncio
async def test_pipeline_ingest_ut_people(tmp_path: pathlib.Path):
    ut = FakeUTPeopleClient()
    p = Pipeline()
    merged = await p.run(
        pure_publications_df=pl.DataFrame(),
        openalex_works_df=pl.DataFrame(),
        output_dir=tmp_path,
        ut_people_client=cast(UTPeopleClient, ut),
        people_search_names=["john.doe"],
    )
    assert isinstance(merged, pl.DataFrame)
    # pipeline should not raise; authors ingestion should have produced a DataFrame internally
    assert (tmp_path / "merged.parquet").exists()
