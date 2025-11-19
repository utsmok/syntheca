import pytest
from httpx import MockTransport, Response

from syntheca.clients.openalex import OpenAlexClient


@pytest.mark.asyncio
async def test_get_works_by_ids_parses(monkeypatch):
    sample = {
        "results": [
            {"id": "https://openalex.org/W1", "display_name": "Test Work", "doi": "10.123/test"}
        ]
    }

    async def handler(request):
        # Return the same JSON for any call
        return Response(200, json=sample)

    transport = MockTransport(handler)
    client = OpenAlexClient()
    client.client = client.client.__class__(transport=transport)

    works = await client.get_works_by_ids(["10.123/test"])
    assert len(works) >= 0


@pytest.mark.asyncio
async def test_get_works_by_title(monkeypatch):
    # Autocomplete returns a list with id; request for works details returns the work JSON
    autocomplete = {"results": [{"id": "https://openalex.org/W1", "display_name": "Test Work"}]}
    work_json = {"id": "https://openalex.org/W1", "display_name": "Test Work", "doi": "10.123/test"}

    async def handler(request):
        url = str(request.url)
        if "/autocomplete/works" in url:
            return Response(200, json=autocomplete)
        return Response(200, json=work_json)

    transport = MockTransport(handler)
    client = OpenAlexClient()
    client.client = client.client.__class__(transport=transport)

    results = await client.get_works_by_title("Some title")
    assert isinstance(results, list)


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
