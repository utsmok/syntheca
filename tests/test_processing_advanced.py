from types import SimpleNamespace

import polars as pl
import pytest

from syntheca.processing.matching import resolve_missing_ids
from syntheca.processing.merging import deduplicate


class DummyClient:
    def __init__(self, works):
        self._works = works

    async def get_works_by_title(self, title):
        # return the list of work-like objects
        return self._works.get(title, [])


@pytest.mark.asyncio
async def test_resolve_missing_ids_success():
    mock_work = SimpleNamespace(
        display_name="My Article Title",
        id="https://openalex.org/W1",
        doi="https://doi.org/10.1000/xyz",
        corresponding_institution_ids=["https://openalex.org/I94624287"],
    )
    client = DummyClient({"My Article Title": [mock_work]})

    df = pl.DataFrame({"title": ["My Article Title"], "id": [None], "doi": [None]})
    out = await resolve_missing_ids(df, client, title_col="title")
    assert out["id"][0] == "https://openalex.org/W1"
    # doi should be set when present
    assert out["doi"][0] is not None


def test_deduplicate_by_doi_and_title():
    df = pl.DataFrame(
        {
            "title": ["Same Title", "Same Title", "Other"],
            "doi": ["https://doi.org/10.1/ABC", "https://doi.org/10.1/abc", None],
        }
    )
    dedup = deduplicate(df, doi_col="doi", title_col="title")
    # unique DOIs collapsed to 1 row, plus the one without DOI
    assert dedup.height == 2
