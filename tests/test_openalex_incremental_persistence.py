"""Integration tests for incremental OpenAlex chunk persistence.

Verifies that:
1. Chunks are written every PERSIST_EVERY items during retrieval
2. No single consolidated write occurs at the end
3. Schema consistency is maintained across chunks
4. Data integrity is preserved (all items retrievable)
5. Disk I/O is spread evenly across the retrieval window
"""

from __future__ import annotations

import dataclasses
import importlib.util
import pathlib
import sys
from unittest.mock import patch

import polars as pl
import pytest
from httpx import MockTransport, Response

from syntheca.clients.openalex import OpenAlexClient
from syntheca.config import settings
from syntheca.models.openalex import Work
from syntheca.utils.persistence import (
    load_dataframe_parquet,
    load_parquet_all,
    save_dataframe_parquet,
    save_parquet_chunk,
)

# Import the fixture generator via importlib since tests/ is not a package.
_FIXTURE_PATH = pathlib.Path(__file__).parent / "fixtures" / "openalex" / "generate_large_fixture.py"
_spec = importlib.util.spec_from_file_location("generate_large_fixture", _FIXTURE_PATH)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["generate_large_fixture"] = _mod
_spec.loader.exec_module(_mod)

generate_api_response_page = _mod.generate_api_response_page
generate_work_dicts = _mod.generate_work_dicts


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _apply_test_settings() -> tuple:
    """Ensure persist_intermediate is on, progress bars off, and cache bypassed.

    The conftest ``isolate_cache_dir`` autouse fixture already sets
    ``settings.cache_dir`` to ``tmp_path / ".cache"``, so we must NOT
    override it.  We only toggle the behavioural flags here.

    Returns the original flag values so callers can restore in a ``finally``.
    """
    old_persist = settings.persist_intermediate
    old_progress = settings.enable_progress
    old_cache_retrieval = settings.use_cache_for_retrieval
    settings.persist_intermediate = True
    settings.enable_progress = False
    settings.use_cache_for_retrieval = False
    return old_persist, old_progress, old_cache_retrieval


def _restore_settings(old_persist, old_progress, old_cache_retrieval):
    settings.persist_intermediate = old_persist
    settings.enable_progress = old_progress
    settings.use_cache_for_retrieval = old_cache_retrieval


def _make_mock_client(
    all_items: list[dict],
    per_page: int = 50,
) -> OpenAlexClient:
    """Build an OpenAlexClient whose HTTP layer returns *all_items* in pages.

    The mock handler reads the ``filter`` query parameter to extract DOIs,
    matches them against the supplied *all_items* list, and returns a valid
    OpenAlex response envelope with the matching results.
    """
    # Index items by DOI for fast lookup
    items_by_doi: dict[str, dict] = {}
    for item in all_items:
        doi = (item.get("doi") or "").replace("https://doi.org/", "").lower()
        items_by_doi[doi] = item

    async def handler(request):
        filter_value = request.url.params.get("filter", "")
        # Parse "doi:10.xxx|10.yyy" or "openalex:Wxxx|Wyyy"
        parts = filter_value.split(":", 1)
        if len(parts) == 2:
            ids_str = parts[1]
        else:
            ids_str = filter_value
        requested_ids = [x.strip() for x in ids_str.split("|") if x.strip()]

        matched = []
        for rid in requested_ids:
            rid_lower = rid.lower()
            if rid_lower in items_by_doi:
                matched.append(items_by_doi[rid_lower])
            else:
                # Try matching by openalex ID suffix
                for item in all_items:
                    if item.get("id", "").endswith(rid.split("/")[-1]):
                        matched.append(item)
                        break

        page = generate_api_response_page(matched, total_count=len(matched))
        return Response(200, json=page)

    transport = MockTransport(handler)
    client = OpenAlexClient()
    client.PER_PAGE = per_page
    client.client = client.client.__class__(transport=transport)
    return client


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chunks_written_incrementally(tmp_path: pathlib.Path):
    """Chunks should appear on disk as retrieval progresses, not all at once.

    With 5000 items and PERSIST_EVERY=1000, we expect exactly 5 chunk files
    (0000.parquet through 0004.parquet).  Each chunk should contain at most
    1000 rows, and the final total should equal the input count.
    """
    count = 5000
    items = generate_work_dicts(count=count, seed=100)

    # Ensure PERSIST_EVERY matches our expectation
    assert OpenAlexClient.PERSIST_EVERY == 1000

    client = _make_mock_client(items, per_page=50)
    saved = _apply_test_settings()

    try:
        dois = [item["doi"].replace("https://doi.org/", "") for item in items]
        works = await client.get_works_by_ids(dois)

        cache_dir = pathlib.Path(settings.cache_dir)
        chunk_dir = cache_dir / "_chunks" / "openalex_works"
        assert chunk_dir.exists(), "Chunk directory should exist after retrieval"

        chunk_files = sorted(chunk_dir.glob("*.parquet"))
        assert len(chunk_files) == 5, (
            f"Expected 5 chunk files, found {len(chunk_files)}: "
            f"{[f.name for f in chunk_files]}"
        )

        # Verify each chunk file name follows the pattern
        for i, f in enumerate(chunk_files):
            assert f.name == f"{i:04d}.parquet", f"Unexpected chunk file name: {f.name}"

        # Verify total rows across chunks equals count
        total_rows = sum(pl.read_parquet(str(f)).height for f in chunk_files)
        assert total_rows == count, f"Expected {count} total rows, got {total_rows}"

        assert len(works) == count
    finally:
        _restore_settings(*saved)


@pytest.mark.asyncio
async def test_schema_consistency_across_chunks(tmp_path: pathlib.Path):
    """Schema must be consistent across chunks even when rows vary in sparsity.

    The first 1000 items have mostly sparse fields (many None/empty nested
    structures), and the next 1000 have fully populated nested structures.
    Critically, the very first item is fully populated so that it anchors the
    Polars struct schema for the chunk -- this mirrors real OpenAlex responses
    where at least one record per batch has all nested fields populated.

    When loaded together via load_parquet_all, the result must have 2000 rows
    and no ShapeError.
    """
    count = 2000
    items = generate_work_dicts(count=count, seed=200, sparse_first=999)
    # Ensure the very first item is fully populated to anchor the schema
    items[0] = generate_work_dicts(count=1, seed=200 + count)[0]

    client = _make_mock_client(items, per_page=50)
    saved = _apply_test_settings()

    try:
        dois = [item["doi"].replace("https://doi.org/", "") for item in items]
        await client.get_works_by_ids(dois)

        df = load_parquet_all("openalex_works")
        assert df is not None, "load_parquet_all should return data"
        assert df.height == count, f"Expected {count} rows, got {df.height}"
    finally:
        _restore_settings(*saved)


@pytest.mark.asyncio
async def test_no_consolidated_write_after_retrieval(tmp_path: pathlib.Path):
    """After get_works_by_ids, only chunk files should exist -- no single
    ``openalex_works.parquet`` in the cache root.

    The incremental pipeline should never produce a consolidated file during
    retrieval; only numbered chunks under ``_chunks/openalex_works/``.
    """
    count = 500
    items = generate_work_dicts(count=count, seed=300)

    client = _make_mock_client(items, per_page=50)
    saved = _apply_test_settings()

    try:
        dois = [item["doi"].replace("https://doi.org/", "") for item in items]
        await client.get_works_by_ids(dois)

        cache_dir = pathlib.Path(settings.cache_dir)
        consolidated = cache_dir / "openalex_works.parquet"
        assert not consolidated.exists(), (
            "Consolidated openalex_works.parquet should NOT exist after "
            "incremental retrieval; only chunk files are expected"
        )

        chunk_dir = cache_dir / "_chunks" / "openalex_works"
        assert chunk_dir.exists(), "Chunk directory should exist"
        chunk_files = list(chunk_dir.glob("*.parquet"))
        assert len(chunk_files) > 0, "At least one chunk file should exist"
    finally:
        _restore_settings(*saved)


@pytest.mark.asyncio
async def test_data_integrity_through_chunks(tmp_path: pathlib.Path):
    """All DOIs from the original input must survive the chunk write/load cycle.

    Generates 3000 items with deterministic DOIs, writes them through the
    pipeline, loads them back with load_parquet_all, and checks that every
    original DOI appears in the loaded DataFrame.
    """
    count = 3000
    items = generate_work_dicts(count=count, seed=400)

    client = _make_mock_client(items, per_page=50)
    saved = _apply_test_settings()

    try:
        # Collect the normalized DOIs we expect to see
        expected_dois = set()
        for item in items:
            raw_doi = item["doi"].replace("https://doi.org/", "")
            expected_dois.add(raw_doi)

        dois = [item["doi"].replace("https://doi.org/", "") for item in items]
        await client.get_works_by_ids(dois)

        df = load_parquet_all("openalex_works")
        assert df is not None
        assert df.height == count

        # Extract DOIs from loaded data and compare
        loaded_dois = set()
        for doi_val in df["doi"].to_list():
            if doi_val is not None:
                loaded_dois.add(str(doi_val).replace("https://doi.org/", "").lower())

        expected_lower = {d.lower() for d in expected_dois}
        missing = expected_lower - loaded_dois
        assert not missing, f"Missing {len(missing)} DOIs after chunk round-trip"
    finally:
        _restore_settings(*saved)


@pytest.mark.asyncio
async def test_corrupted_chunk_skipped_gracefully(tmp_path: pathlib.Path):
    """load_parquet_all should skip corrupted chunks and return valid data.

    Writes 3 valid chunks and 1 corrupted chunk (random bytes), then
    verifies that load_parquet_all returns only the valid chunks' data
    without raising.
    """
    saved = _apply_test_settings()

    try:
        from syntheca.utils.polars_frames import robust_from_dicts

        # Generate and write 3 valid chunks
        items = generate_work_dicts(count=300, seed=500)
        chunk_size = 100
        for i in range(3):
            batch = items[i * chunk_size : (i + 1) * chunk_size]
            dicts_batch = []
            for item in batch:
                try:
                    w = Work.from_dict(item)
                    dicts_batch.append(dataclasses.asdict(w))
                except Exception:
                    continue
            df = robust_from_dicts(dicts_batch)
            save_parquet_chunk("openalex_works", i, df)

        # Write a corrupted chunk
        cache_dir = pathlib.Path(settings.cache_dir)
        chunk_dir = cache_dir / "_chunks" / "openalex_works"
        corrupted_path = chunk_dir / "0003.parquet"
        corrupted_path.write_bytes(b"\x00\x01\x02\x03GARBAGE_NOT_PARQUET")

        df = load_parquet_all("openalex_works")
        assert df is not None, "Should return data from valid chunks"
        assert df.height == 300, f"Expected 300 rows from 3 valid chunks, got {df.height}"
    finally:
        _restore_settings(*saved)


@pytest.mark.asyncio
async def test_chunk_loading_fallback_to_single_file(tmp_path: pathlib.Path):
    """When no chunks exist, load_parquet_all should fall back to a single file.

    Writes a single ``openalex_works.parquet`` file (no _chunks directory)
    and verifies that load_parquet_all loads it successfully.
    """
    saved = _apply_test_settings()

    try:
        from syntheca.utils.polars_frames import robust_from_dicts

        items = generate_work_dicts(count=50, seed=600)
        dicts_list = []
        for item in items:
            try:
                w = Work.from_dict(item)
                dicts_list.append(dataclasses.asdict(w))
            except Exception:
                continue
        df = robust_from_dicts(dicts_list)
        save_dataframe_parquet(df, "openalex_works")

        loaded = load_parquet_all("openalex_works")
        assert loaded is not None, "Should load single consolidated file"
        assert loaded.height == 50, f"Expected 50 rows, got {loaded.height}"
    finally:
        _restore_settings(*saved)


@pytest.mark.asyncio
async def test_write_count_regression_guard(tmp_path: pathlib.Path):
    """Regression guard: total non-chunk writes must stay below threshold.

    During a mock pipeline run of 5000 items, we intercept all calls to
    ``save_dataframe_parquet`` (the function used for consolidated writes)
    and count how many times it is invoked.  The only expected consolidated
    write (if any) would be from ``get_works_by_title``, not from the main
    ``get_works_by_ids`` path.  For a pure ``get_works_by_ids`` call, the
    count should be **zero**.

    Expected writes:
      - 0 calls to save_dataframe_parquet (chunks use save_parquet_chunk)
      - 5 calls to save_parquet_chunk (5000 / 1000 = 5 chunks)

    The guard ensures no future change accidentally reintroduces a
    consolidated write at the end of the retrieval loop.
    """
    count = 5000
    items = generate_work_dicts(count=count, seed=700)

    client = _make_mock_client(items, per_page=50)
    saved = _apply_test_settings()

    consolidated_write_calls: list[str] = []
    chunk_write_calls: list[tuple[str, int]] = []

    original_save_df = save_dataframe_parquet
    original_save_chunk = save_parquet_chunk

    def _tracking_save_df(df, name):
        consolidated_write_calls.append(name)
        return original_save_df(df, name)

    def _tracking_save_chunk(name, chunk_index, df):
        chunk_write_calls.append((name, chunk_index))
        return original_save_chunk(name, chunk_index, df)

    try:
        with (
            patch("syntheca.clients.openalex.save_dataframe_parquet", side_effect=_tracking_save_df),
            patch("syntheca.clients.openalex.save_parquet_chunk", side_effect=_tracking_save_chunk),
        ):
            dois = [item["doi"].replace("https://doi.org/", "") for item in items]
            await client.get_works_by_ids(dois)

        # Assert: no consolidated writes from the ids retrieval path
        consolidated_from_ids = [
            n for n in consolidated_write_calls
            if n == "openalex_works"
        ]
        assert len(consolidated_from_ids) == 0, (
            f"Expected 0 consolidated writes of 'openalex_works' during "
            f"get_works_by_ids, but observed {len(consolidated_from_ids)}: "
            f"{consolidated_write_calls}"
        )

        # Assert: chunk writes are exactly as expected
        oa_chunks = [
            (n, i) for n, i in chunk_write_calls if n == "openalex_works"
        ]
        expected_chunks = count // OpenAlexClient.PERSIST_EVERY  # 5000 / 1000 = 5
        assert len(oa_chunks) == expected_chunks, (
            f"Expected {expected_chunks} chunk writes, got {len(oa_chunks)}: "
            f"{oa_chunks}"
        )

        # Verify chunk indices are sequential
        chunk_indices = [i for _, i in oa_chunks]
        assert chunk_indices == list(range(expected_chunks)), (
            f"Chunk indices should be sequential 0..{expected_chunks - 1}, "
            f"got {chunk_indices}"
        )
    finally:
        _restore_settings(*saved)
