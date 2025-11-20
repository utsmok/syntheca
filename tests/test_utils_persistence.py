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
