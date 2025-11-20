import asyncio

import pytest
from httpx import MockTransport, Response

from syntheca.clients.openalex import OpenAlexClient
from syntheca.clients.pure_oai import PureOAIClient


def sample_oai_xml():
    return (
        "<OAI-PMH><ListRecords><record><metadata>"
        "<cerif:Publication xmlns:cerif=\"uri\">"
        "<cerif:Title>Title</cerif:Title><cerif:DOI>10.1/test</cerif:DOI>"
        "</cerif:Publication></metadata></record></ListRecords></OAI-PMH>"
    )


def fake_openalex_page():
    return {"results": [{"id": "https://openalex.org/W1", "doi": "10.1/test", "display_name":"Test"}]}


@pytest.mark.asyncio
async def test_concurrent_progress_bars(monkeypatch):
    from syntheca.utils.progress import reset_positions
    reset_positions()
    # Pure OAI mock
    async def oai_handler(request):
        return Response(200, content=sample_oai_xml())

    pure_transport = MockTransport(oai_handler)
    pure = PureOAIClient()
    pure.client = pure.client.__class__(transport=pure_transport)

    # OpenAlex mock: chunk request and detail requests
    async def openalex_handler(request):
        if "autocomplete" in str(request.url.path):
            return Response(200, json={"results": [{"id": "W1"}]})
        if "/works/" in str(request.url.path) or "/works" in str(request.url.path):
            return Response(200, json=fake_openalex_page())
        return Response(200, json={})

    openalex_transport = MockTransport(openalex_handler)
    openalex = OpenAlexClient()
    openalex.client = openalex.client.__class__(transport=openalex_transport)

    # Run both concurrently to test progress bars don't crash in asyncio
    res = await asyncio.gather(
        pure.get_all_records(["openaire_cris_publications"]),
        openalex.get_works_by_ids(["10.1/test"]),
        return_exceptions=False,
    )

    assert isinstance(res[0], dict)
    assert isinstance(res[1], list)


@pytest.mark.asyncio
async def test_multiple_pure_oai_concurrent(monkeypatch):
    from syntheca.utils.progress import reset_positions
    reset_positions()
    async def oai_handler(request):
        return Response(200, content=sample_oai_xml())

    pure_transport = MockTransport(oai_handler)
    pure = PureOAIClient()
    pure.client = pure.client.__class__(transport=pure_transport)

    # Run two Pure OAI collection fetches concurrently — progress bars should be allocated unique positions
    res = await asyncio.gather(
        pure.get_all_records(["openaire_cris_publications"]),
        pure.get_all_records(["openaire_cris_persons"]),
        return_exceptions=False,
    )
    assert isinstance(res[0], dict)
    assert isinstance(res[1], dict)


@pytest.mark.asyncio
async def test_progress_disabled(monkeypatch):
    from syntheca.config import settings
    old_flag = settings.enable_progress
    settings.enable_progress = False

    async def oai_handler(request):
        return Response(200, content=sample_oai_xml())

    pure_transport = MockTransport(oai_handler)
    pure = PureOAIClient()
    pure.client = pure.client.__class__(transport=pure_transport)

    async def openalex_handler(request):
        return Response(200, json=fake_openalex_page())

    openalex_transport = MockTransport(openalex_handler)
    openalex = OpenAlexClient()
    openalex.client = openalex.client.__class__(transport=openalex_transport)

    res = await asyncio.gather(
        pure.get_all_records(["openaire_cris_publications"]),
        openalex.get_works_by_ids(["10.1/test"]),
        return_exceptions=False,
    )
    assert isinstance(res[0], dict)
    assert isinstance(res[1], list)

    settings.enable_progress = old_flag
