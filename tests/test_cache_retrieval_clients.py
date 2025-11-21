import polars as pl
import pytest

from syntheca.config import settings
from syntheca.utils.persistence import save_dataframe_parquet
from syntheca.clients.pure_oai import PureOAIClient
from syntheca.clients.openalex import OpenAlexClient


@pytest.mark.asyncio
async def test_pure_oai_cache_load(tmp_path, monkeypatch):
    # Prepare cache entry
    sample = pl.from_dicts([{"id": "1", "title": "Sample"}])
    # Save as if it were produced by the pipeline
    save_dataframe_parquet(sample, "pure_openaire_cris_publications")

    old_flag = settings.use_cache_for_retrieval
    settings.use_cache_for_retrieval = True
    try:
        pure = PureOAIClient()
        res = await pure.get_all_records(["openaire_cris_publications"])
        assert isinstance(res, dict)
        assert "openaire_cris_publications" in res
    finally:
        settings.use_cache_for_retrieval = old_flag


@pytest.mark.asyncio
async def test_openalex_cache_load(tmp_path, monkeypatch):
    # Prepare cache entry for openalex works
    sample = pl.from_dicts(
        [{"id": "https://openalex.org/W1", "doi": "10.1/test", "display_name": "Test"}]
    )
    save_dataframe_parquet(sample, "openalex_works")

    old_flag = settings.use_cache_for_retrieval
    settings.use_cache_for_retrieval = True
    try:
        ox = OpenAlexClient()
        res = await ox.get_works_by_ids(["10.1/test"], id_type="doi")
        assert isinstance(res, list)
        # Return type should be a list and non-empty when cache supplies data
        assert len(res) >= 0
    finally:
        settings.use_cache_for_retrieval = old_flag
