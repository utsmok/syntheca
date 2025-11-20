import pathlib

import polars as pl
import pytest

from syntheca.pipeline import Pipeline


@pytest.mark.asyncio
async def test_pipeline_merge_and_write(tmp_path: pathlib.Path):
    oils = pl.DataFrame(
        {
            "doi": ["10.1000/ABC", "10.2000/DEF"],
            "title": ["A study on X", "Another study"],
        }
    )
    full = pl.DataFrame(
        {
            "doi": ["https://doi.org/10.1000/abc"],
            "title": ["A study on X"],
            "extra": ["info"],
        }
    )

    p = Pipeline()
    merged = await p.run(oils_df=oils, full_df=full, output_dir=tmp_path)
    # Expect merged contains the 'extra' column and has merged rows
    assert "extra" in merged.columns
    assert (tmp_path / "merged.parquet").exists()
    assert (tmp_path / "merged.xlsx").exists()
