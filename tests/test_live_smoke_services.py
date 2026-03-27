"""Opt-in live smoke checks for the officially supported external services."""

from __future__ import annotations

from typing import cast

import pytest

from syntheca.clients.openaire import OpenAIREClient
from syntheca.clients.openalex import OpenAlexClient
from syntheca.clients.pure_oai import PureOAIClient
from syntheca.clients.ut_people import UTPeopleClient
from syntheca.config.ut_profile import ut_profile


@pytest.mark.live
@pytest.mark.network
@pytest.mark.asyncio
async def test_openalex_live_smoke() -> None:
    async with OpenAlexClient() as base_client:
        client = cast(OpenAlexClient, base_client)
        works = await client.get_works_by_ids(["10.1038/nature12373"])
    assert isinstance(works, list)
    assert works


@pytest.mark.live
@pytest.mark.network
@pytest.mark.asyncio
async def test_pure_oai_live_smoke() -> None:
    async with PureOAIClient() as base_client:
        client = cast(PureOAIClient, base_client)
        response = await client.request("GET", f"{ut_profile.pure_oai_endpoint}?verb=Identify")
    assert response.status_code == 200
    assert "OAI-PMH" in response.text


@pytest.mark.live
@pytest.mark.network
@pytest.mark.asyncio
async def test_ut_people_live_smoke() -> None:
    async with UTPeopleClient() as base_client:
        client = cast(UTPeopleClient, base_client)
        candidates = await client.search_person("samuel")
    assert isinstance(candidates, list)


@pytest.mark.live
@pytest.mark.network
@pytest.mark.asyncio
async def test_openaire_live_smoke() -> None:
    async with OpenAIREClient() as base_client:
        client = cast(OpenAIREClient, base_client)
        organizations = await client.get_organizations(
            name="University of Twente",
            precise=True,
            page_size=1,
        )
    assert isinstance(organizations, list)
