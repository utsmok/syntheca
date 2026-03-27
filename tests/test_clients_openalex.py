import json
import pathlib
from copy import deepcopy

import pytest
from httpx import MockTransport, Response

from syntheca.clients.openalex import OpenAlexClient
from syntheca.config import settings
from syntheca.utils.persistence import load_dataframe_parquet

FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures" / "openalex"


def _load_openalex_fixture(name: str) -> dict:
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


def _make_work_for_identifier(identifier: str) -> dict:
    work = deepcopy(_load_openalex_fixture("works_response_live_contract.json")["results"][0])
    suffix = identifier.replace("/", "_")
    work["id"] = f"https://openalex.org/W{abs(hash(identifier)) % 10_000_000}"
    work["doi"] = identifier
    work["display_name"] = f"Work for {suffix}"
    return work


@pytest.mark.asyncio
async def test_get_works_by_ids_parses_live_like_awards_without_silent_drop():
    sample = _load_openalex_fixture("works_response_live_contract.json")

    async def handler(request):
        # Return the same JSON for any call
        return Response(200, json=sample)

    transport = MockTransport(handler)
    client = OpenAlexClient()
    client.client = client.client.__class__(transport=transport)

    works = await client.get_works_by_ids(["10.123/test"])
    assert len(works) == 1
    assert works[0].id == sample["results"][0]["id"]
    assert works[0].grants == []
    assert works[0].awards is not None
    assert works[0].awards[0].funder_award_id == "EP/S019472/1"


@pytest.mark.asyncio
async def test_get_works_by_ids_persistent_cache(tmp_path: pathlib.Path):
    sample = _load_openalex_fixture("works_response_live_contract.json")
    old_cache = settings.cache_dir
    old_persist = settings.persist_intermediate
    settings.cache_dir = tmp_path
    settings.persist_intermediate = True

    async def handler(request):
        return Response(200, json=sample)

    transport = MockTransport(handler)
    client = OpenAlexClient()
    client.client = client.client.__class__(transport=transport)

    try:
        await client.get_works_by_ids(["10.123/test"])
        df = load_dataframe_parquet("openalex_works")
        assert df is not None
        assert df.height == 1
    finally:
        settings.persist_intermediate = old_persist
        settings.cache_dir = old_cache


@pytest.mark.asyncio
async def test_get_works_by_ids_retries_then_splits_failed_batches(monkeypatch):
    request_filters: list[str] = []

    async def fake_sleep(_: float):
        return None

    async def handler(request):
        filter_value = request.url.params["filter"]
        request_filters.append(filter_value)
        identifiers = filter_value.split(":", 1)[1].split("|")
        if len(identifiers) > 2:
            return Response(400, request=request)
        return Response(200, json={"results": [_make_work_for_identifier(i) for i in identifiers]})

    monkeypatch.setattr("syntheca.clients.openalex.asyncio.sleep", fake_sleep)

    client = OpenAlexClient()
    client.PER_PAGE = 4
    client.client = client.client.__class__(transport=MockTransport(handler))

    works = await client.get_works_by_ids(
        ["10.1/a", "10.1/b", "10.1/c", "10.1/d"],
    )

    assert len(works) == 4
    assert any(filter_value.count("|") == 3 for filter_value in request_filters)
    assert request_filters.count("doi:10.1/a|10.1/b|10.1/c|10.1/d") >= 2
    assert "doi:10.1/a|10.1/b" in request_filters
    assert "doi:10.1/c|10.1/d" in request_filters


@pytest.mark.asyncio
async def test_get_works_by_ids_skips_single_identifier_after_retries(monkeypatch):
    request_filters: list[str] = []

    async def fake_sleep(_: float):
        return None

    async def handler(request):
        filter_value = request.url.params["filter"]
        request_filters.append(filter_value)
        identifiers = filter_value.split(":", 1)[1].split("|")
        if "10.bad/item" in identifiers:
            return Response(400, request=request)
        return Response(200, json={"results": [_make_work_for_identifier(i) for i in identifiers]})

    monkeypatch.setattr("syntheca.clients.openalex.asyncio.sleep", fake_sleep)

    client = OpenAlexClient()
    client.PER_PAGE = 2
    client.client = client.client.__class__(transport=MockTransport(handler))

    works = await client.get_works_by_ids(["10.good/item", "10.bad/item"])

    assert len(works) == 1
    assert works[0].doi == "10.good/item"
    assert request_filters.count("doi:10.good/item|10.bad/item") >= 2
    assert request_filters.count("doi:10.bad/item") >= 3


@pytest.mark.asyncio
async def test_get_works_by_title_keeps_successful_details():
    work_json = _load_openalex_fixture("works_response_live_contract.json")["results"][0]
    autocomplete = {
        "results": [
            {"id": work_json["id"], "display_name": work_json["display_name"]},
            {"id": "https://openalex.org/W404", "display_name": "Broken Work"},
        ]
    }

    async def handler(request):
        url = str(request.url)
        if "/autocomplete/works" in url:
            return Response(200, json=autocomplete)
        if "W404" in url:
            return Response(500)
        return Response(200, json=work_json)

    transport = MockTransport(handler)
    client = OpenAlexClient()
    client.client = client.client.__class__(transport=transport)

    results = await client.get_works_by_title("Some title")
    assert len(results) == 1
    assert results[0].id == work_json["id"]
    assert results[0].display_name == work_json["display_name"]


def test_clean_openalex_raw_data():
    client = OpenAlexClient()
    sample = {
        "id": "https://openalex.org/W1",
        "display_name": "Test Work",
        "doi": "10.123/test",
        "publication_year": 2020,
        "open_access": {
            "is_oa": True,
            "oa_status": "gold",
            "any_repository_has_fulltext": True,
            "oa_url": "https://example.org",
        },
        "best_oa_location": {
            "landing_page_url": "https://example.org/fulltext",
            "source": {
                "host_organization_name": "ExampleHost",
                "display_name": "ExampleHost",
                "type": "repository",
            },
        },
        "primary_location": {
            "landing_page_url": "https://example.org/primary",
            "source": {
                "host_organization_name": "PrimaryHost",
                "display_name": "PrimaryHost",
                "type": "journal",
            },
        },
        "locations": [
            {"source": {"host_organization_name": "ExampleHost"}},
            {"source": {"host_organization_name": "PrimaryHost"}},
        ],
        "primary_topic": {
            "display_name": "TopicName",
            "subfield": {"display_name": "Subfield"},
            "field": {"display_name": "Field"},
            "domain": {"display_name": "Domain"},
        },
        "apc_list": {"value_usd": 2000},
        "apc_paid": {"value_usd": 1500},
        "corresponding_institution_ids": [
            "https://openalex.org/I94624287",
            "https://openalex.org/I2",
        ],
    }
    cleaned = client.clean_openalex_raw_data([sample])[0]
    assert cleaned["is_oa"] is True
    assert cleaned["oa_color"] == "gold"
    assert cleaned["main_url"] == "https://example.org/fulltext"
    assert "ExampleHost" in cleaned["all_host_orgs"]
    assert cleaned["topic"] == "TopicName"
    assert cleaned["listed_apc_usd"] == 2000
    assert cleaned["ut_is_corresponding"] is True
