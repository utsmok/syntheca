import os
import pathlib
from typing import cast

import polars as pl
import pytest

from syntheca.pipeline import Pipeline
from syntheca.config import settings
from syntheca.utils.persistence import save_dataframe_parquet
from syntheca.clients.openalex import OpenAlexClient
from syntheca.clients.pure_oai import PureOAIClient
from syntheca.clients.ut_people import UTPeopleClient


class FakePureClient:
    async def get_all_records(self, collections):
        return {
            "publications": [
                {
                    "id": "o1",
                    "title": "Integration test publication",
                    "doi": "10.1234/test-integration",
                    "authors": [
                        {
                            "internal_repository_id": "p-1",
                            "first_names": "John",
                            "family_names": "Doe",
                            "affiliation_id": "org1",
                            "affiliation_name": "Faculty of Science and Technology",
                        }
                    ],
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
        self.works = works or [
            FakeOpenAlexWork("I1", "10.1234/test-integration", "Openalex Paper", 2020)
        ]

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
async def test_pipeline_integration_mock_end_to_end(tmp_path: pathlib.Path):
    # Use a temporary cache dir so persistence from pipeline uses this space
    old_cache = settings.cache_dir
    settings.cache_dir = tmp_path
    settings.persist_intermediate = True

    # Persist a minimal orgs DataFrame so the pipeline can resolve hierarchy
    orgs = pl.DataFrame(
        {
            "internal_repository_id": ["org1"],
            "name": ["Faculty of Science and Technology"],
            "parent_org": [None],
            "tnw": [True],
        }
    )
    save_dataframe_parquet(orgs, "openaire_cris_orgunits")

    p = Pipeline()
    # Provide authors_df so pipeline can map and aggregate author flags
    authors_df = pl.DataFrame(
        {
            "pure_id": ["p-1"],
            "first_names": ["John"],
            "family_names": ["Doe"],
            "affiliation_ids_pure": [["org1"]],
            "affiliation_names_pure": [["Faculty of Science and Technology"]],
        }
    )
    merged = await p.run(
        oils_df=None,
        full_df=None,
        authors_df=authors_df,
        output_dir=tmp_path,
        pure_client=cast(PureOAIClient, FakePureClient()),
        openalex_client=cast(OpenAlexClient, FakeOpenAlexClient()),
        ut_people_client=cast(UTPeopleClient, FakeUTPeopleClient()),
    )

    # Basic expectations: merged should be a DataFrame with publication info
    assert isinstance(merged, pl.DataFrame)
    # Expect the pipeline to return at least one row and some publication identifier
    assert merged.height > 0
    assert any(col in merged.columns for col in ("doi", "display_name", "title"))

    # cleanup
    settings.cache_dir = old_cache
    settings.persist_intermediate = False


@pytest.mark.asyncio
async def test_pipeline_integration_live_minimal(tmp_path: pathlib.Path):
    # Use small inputs to avoid large network activity
    old_cache = settings.cache_dir
    settings.cache_dir = tmp_path
    settings.persist_intermediate = False

    oils = pl.DataFrame(
        {
            "doi": ["10.1234/test-integration"],
            "title": ["A minimal test"],
        }
    )

    # Pass a minimal authors_df to avoid hitting UT People scraping.
    authors = pl.DataFrame(
        {
            "pure_id": ["p-1"],
            "first_names": ["John"],
            "family_names": ["Doe"],
            "affiliation_names_pure": [["Faculty of Science and Technology"]],
        }
    )

    p = Pipeline()
    # Use the real OpenAlex client to fetch one ID; other clients are not used to avoid large downloads.
    openalex_client = OpenAlexClient()
    merged = await p.run(
        oils_df=oils,
        full_df=None,
        authors_df=authors,
        output_dir=tmp_path,
        openalex_client=openalex_client,
        openalex_ids=["10.1038/nature12373"],  # single DOI to keep retrieval minimal
    )

    assert isinstance(merged, pl.DataFrame)
    # pipeline returns a DataFrame containing at least one publication identifier
    assert merged.height > 0
    assert any(col in merged.columns for col in ("doi", "display_name", "title"))
    # ensure the doi col has a value
    assert merged.filter(pl.col("doi").is_not_null()).height > 0
    settings.cache_dir = old_cache
