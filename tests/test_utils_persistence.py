import pathlib

import polars as pl

from syntheca.config import settings
from syntheca.utils.persistence import load_dataframe_parquet, save_dataframe_parquet


def test_save_and_load_roundtrip(tmp_path: pathlib.Path):
    old_cache = settings.cache_dir
    settings.cache_dir = tmp_path

    df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    save_path = save_dataframe_parquet(df, "test_roundtrip")
    assert save_path.exists()

    loaded = load_dataframe_parquet("test_roundtrip")
    assert loaded is not None
    assert loaded.shape == df.shape

    # restore
    settings.cache_dir = old_cache


def test_incremental_append_creates_single_parquet(tmp_path: pathlib.Path):
    """Incremental append should produce one file with all rows."""
    old_cache = settings.cache_dir
    settings.cache_dir = tmp_path

    try:
        df1 = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        df2 = pl.DataFrame({"a": [3, 4], "b": ["z", "w"]})

        from syntheca.utils.persistence import init_incremental_parquet, append_to_parquet
        init_incremental_parquet("test_incr", df1)
        p = tmp_path / "test_incr.parquet"
        assert p.exists()
        assert pl.read_parquet(str(p)).height == 2

        append_to_parquet("test_incr", df2)
        assert pl.read_parquet(str(p)).height == 4
        assert pl.read_parquet(str(p))["a"].to_list() == [1, 2, 3, 4]
    finally:
        settings.cache_dir = old_cache


def test_incremental_append_with_schema_on_empty_start(tmp_path: pathlib.Path):
    """When no prior file exists and no init call, append should still work."""
    old_cache = settings.cache_dir
    settings.cache_dir = tmp_path

    try:
        from syntheca.utils.persistence import append_to_parquet
        df = pl.DataFrame({"a": [1], "b": ["x"]})
        append_to_parquet("test_incr_empty", df)
        p = tmp_path / "test_incr_empty.parquet"
        assert p.exists()
        assert pl.read_parquet(str(p)).height == 1
    finally:
        settings.cache_dir = old_cache
