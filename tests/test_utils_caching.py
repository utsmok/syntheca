import asyncio
from pathlib import Path

import pytest

from syntheca.config import settings
from syntheca.utils.caching import file_cache


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
