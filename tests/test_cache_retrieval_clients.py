import json
import pathlib

import pytest
from httpx import MockTransport, Response

from syntheca.clients.openalex import OpenAlexClient
from syntheca.clients.ut_people import UTPeopleClient
from syntheca.config import settings

FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures"


def _load_openalex_fixture() -> dict:
    return json.loads(
        (FIXTURES_DIR / "openalex" / "works_response_live_contract.json").read_text(
            encoding="utf-8"
        )
    )


def _load_ut_people_fixture() -> dict:
    return json.loads((FIXTURES_DIR / "ut_people" / "rpc_response.json").read_text())


@pytest.mark.asyncio
async def test_pure_oai_cache_load(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "cache_dir", tmp_path)
    monkeypatch.setattr(settings, "use_cache_for_retrieval", True)
    sample = _load_openalex_fixture()
    request_count = {"n": 0}

    async def handler(request):
        request_count["n"] += 1
        return Response(200, json=sample)

    async with OpenAlexClient() as first_client:
        first_client.client = first_client.client.__class__(transport=MockTransport(handler))
        first_result = await first_client.get_works_by_ids(["10.123/test"])

    async def fail_handler(request):
        raise AssertionError("OpenAlex network call should have been served from cache")

    async with OpenAlexClient() as second_client:
        second_client.client = second_client.client.__class__(transport=MockTransport(fail_handler))
        second_result = await second_client.get_works_by_ids(["10.123/test"])

    assert request_count["n"] == 1
    assert len(first_result) == len(second_result) == 1
    assert second_result[0].id == first_result[0].id


@pytest.mark.asyncio
async def test_openalex_cache_load(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "cache_dir", tmp_path)
    monkeypatch.setattr(settings, "use_cache_for_retrieval", True)
    rpc_json = _load_ut_people_fixture()
    request_count = {"n": 0}

    async def handler(request):
        request_count["n"] += 1
        return Response(200, json=rpc_json)

    async with UTPeopleClient() as first_client:
        first_client.client = first_client.client.__class__(transport=MockTransport(handler))
        first_result = await first_client.search_person("Alice Researcher", rank=False)

    async def fail_handler(request):
        raise AssertionError("UT People RPC POST should have been served from cache")

    async with UTPeopleClient() as second_client:
        second_client.client = second_client.client.__class__(transport=MockTransport(fail_handler))
        second_result = await second_client.search_person("Alice Researcher", rank=False)

    assert request_count["n"] == 1
    assert second_result == first_result
