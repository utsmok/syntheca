import httpx
import pytest

from syntheca.clients.base import BaseClient


class TestClient(BaseClient):
    def __init__(self, *, transport=None):
        # build AsyncClient with provided transport for testing
        super().__init__(headers={"User-Agent": "test"}, timeout=5)
        # replace underlying client with a test AsyncClient if provided
        if transport is not None:
            self.client = httpx.AsyncClient(transport=transport)


@pytest.mark.asyncio
async def test_base_client_context_manager():
    client = TestClient()
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
    client = TestClient(transport=transport)

    # ensure retries happen and we get 200 eventually
    async with client:
        resp = await client.request("GET", "https://example.org")
        assert resp.status_code == 200
