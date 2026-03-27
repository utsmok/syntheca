import asyncio
import gzip
import pickle
from pathlib import Path

import pytest

from syntheca.config import settings
from syntheca.utils.caching import (
    build_request_cache_key,
    file_cache,
    get_raw_response_cache_path,
    load_raw_response,
)


@pytest.mark.asyncio
async def test_file_cache_async(tmp_path, monkeypatch):
    # Ensure we use a temporary cache directory
    monkeypatch.setattr(settings, "cache_dir", tmp_path)

    call_count = {"n": 0}

    @file_cache(prefix="add")
    async def add(x, y):
        # expensive async op simulation
        await asyncio.sleep(0.01)
        call_count["n"] += 1
        return x + y

    # first call should run the function
    result1 = await add(1, 2)
    assert result1 == 3
    assert call_count["n"] == 1

    # second call with same args should be cached
    result2 = await add(1, 2)
    assert result2 == 3
    assert call_count["n"] == 1

    # cache files exist
    files = list(Path(tmp_path).iterdir())
    assert any("add_" in f.name for f in files)


def test_file_cache_sync(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "cache_dir", tmp_path)

    called = {"n": 0}

    @file_cache(prefix="mul")
    def mul(a, b):
        called["n"] += 1
        return a * b

    assert mul(2, 3) == 6
    assert called["n"] == 1
    assert mul(2, 3) == 6
    assert called["n"] == 1


def test_build_request_cache_key_is_stable_for_semantically_equivalent_requests():
    key_a = build_request_cache_key(
        "POST",
        "https://people.utwente.nl/wh_services/utwente_ppp/rpc/",
        json={
            "id": 1,
            "method": "SearchPersons",
            "params": [{"query": "Alice Researcher", "page": 0, "resultsperpage": 20}],
        },
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    key_b = build_request_cache_key(
        "post",
        "https://people.utwente.nl/wh_services/utwente_ppp/rpc/",
        json={
            "method": "SearchPersons",
            "params": [{"resultsperpage": 20, "page": 0, "query": "Alice Researcher"}],
            "id": 1,
        },
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    key_c = build_request_cache_key(
        "POST",
        "https://people.utwente.nl/wh_services/utwente_ppp/rpc/",
        json={
            "id": 1,
            "method": "SearchPersons",
            "params": [{"query": "Alice Researcher", "page": 1, "resultsperpage": 20}],
        },
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )

    assert key_a == key_b
    assert key_a != key_c


def test_load_raw_response_handles_legacy_compressed_headers(tmp_path):
    cache_key = "a" * 64
    cache_path = get_raw_response_cache_path(tmp_path, cache_key)
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    decoded_content = b"<OAI-PMH><record>ok</record></OAI-PMH>"
    legacy_payload = {
        "version": 1,
        "request": {
            "method": "GET",
            "url": "https://ris.utwente.nl/ws/oai?verb=ListRecords",
        },
        "response": {
            "status_code": 200,
            "headers": [
                ("Content-Encoding", "gzip"),
                ("Content-Length", str(len(gzip.compress(decoded_content)))),
                ("Content-Type", "application/xml; charset=utf-8"),
            ],
            "content": decoded_content,
        },
    }

    with gzip.open(cache_path, "wb", compresslevel=6) as handle:
        pickle.dump(legacy_payload, handle, protocol=pickle.HIGHEST_PROTOCOL)

    response = load_raw_response(tmp_path, cache_key)

    assert response is not None
    assert response.content == decoded_content
    assert response.headers.get("content-encoding") is None
    assert response.headers["content-length"] == str(len(decoded_content))
    assert response.headers["content-type"] == "application/xml; charset=utf-8"
    assert response.extensions["syntheca.from_cache"] is True
    assert response.extensions["syntheca.cache_key"] == cache_key
    assert response.request.method == "GET"
    assert str(response.request.url) == "https://ris.utwente.nl/ws/oai?verb=ListRecords"
