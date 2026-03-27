import gzip

import httpx
import pytest

from syntheca.clients.base import BaseClient
from syntheca.config import settings
from syntheca.utils.caching import build_request_cache_key, get_raw_response_cache_path


class MockBaseClient(BaseClient):
    """Test helper wrapping ``BaseClient`` with an optional mock transport."""

    def __init__(self, *, transport=None):
        # build AsyncClient with provided transport for testing
        super().__init__(headers={"User-Agent": "test"}, timeout=5)
        # replace underlying client with a test AsyncClient if provided
        if transport is not None:
            self.client = httpx.AsyncClient(transport=transport)


@pytest.mark.asyncio
async def test_base_client_context_manager():
    client = MockBaseClient()
    async with client:
        assert client.client is not None
    # after exit AsyncClient should be closed
    assert client.client.is_closed


@pytest.mark.asyncio
async def test_retry_on_429_then_success():
    state = {"count": 0}

    async def handler(request):
        # first call returns 429, second call returns 200
        state["count"] += 1
        if state["count"] == 1:
            return httpx.Response(429, content=b"Too many requests")
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    client = MockBaseClient(transport=transport)

    # ensure retries happen and we get 200 eventually
    async with client:
        resp = await client.request("GET", "https://example.org")
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_request_cache_hit_on_repeated_get(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "cache_dir", tmp_path)
    monkeypatch.setattr(settings, "use_cache_for_retrieval", True)
    state = {"count": 0}

    async def handler(request):
        state["count"] += 1
        return httpx.Response(200, json={"ok": True, "count": state["count"]})

    client = MockBaseClient(transport=httpx.MockTransport(handler))

    async with client:
        resp1 = await client.request(
            "GET",
            "https://example.org/cache?a=1",
            params={"b": "2"},
        )
        resp2 = await client.request(
            "GET",
            "https://example.org/cache",
            params={"b": "2", "a": "1"},
        )

    cache_key = build_request_cache_key(
        "GET",
        "https://example.org/cache",
        params={"a": "1", "b": "2"},
    )
    cache_path = get_raw_response_cache_path(tmp_path, cache_key)

    assert state["count"] == 1
    assert resp1.json() == resp2.json()
    assert resp2.extensions["syntheca.from_cache"] is True
    assert cache_path.exists()
    assert cache_path.suffixes[-2:] == [".httpx", ".gz"]
    with gzip.open(cache_path, "rb") as handle:
        assert handle.read()


@pytest.mark.asyncio
async def test_request_cache_toggle_disables_raw_response_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "cache_dir", tmp_path)
    monkeypatch.setattr(settings, "use_cache_for_retrieval", False)
    state = {"count": 0}

    async def handler(request):
        state["count"] += 1
        return httpx.Response(200, json={"ok": True, "count": state["count"]})

    client = MockBaseClient(transport=httpx.MockTransport(handler))

    async with client:
        resp1 = await client.request("GET", "https://example.org/no-cache")
        resp2 = await client.request("GET", "https://example.org/no-cache")

    assert state["count"] == 2
    assert resp1.json() != resp2.json()
    assert not list(tmp_path.rglob("*.httpx.gz"))
