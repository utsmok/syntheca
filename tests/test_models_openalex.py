import json
import pathlib

from dacite import from_dict

from syntheca.models.adapters import openalex_work_to_canonical
from syntheca.models.openalex import Award, Work, WorkIds, production_config

FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures" / "openalex"


def _load_openalex_fixture(name: str) -> dict:
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


def test_dacite_config_available():
    assert production_config is not None


def test_from_dict_parses_workids():
    sample = {
        "openalex": "https://openalex.org/W123",
        "doi": "10.123/abc",
        "mag": 12345,
        "pmid": "PM123",
        "pmcid": "PMC123",
    }
    wi = from_dict(data_class=WorkIds, data=sample, config=production_config)
    assert wi.openalex == sample["openalex"]
    assert wi.doi == sample["doi"]


def test_work_from_dict_parses_live_like_awards_and_missing_grants():
    payload = _load_openalex_fixture("works_response_live_contract.json")

    work = Work.from_dict(payload["results"][0])

    assert work.id == "https://openalex.org/W4387332482"
    assert work.grants == []
    assert work.awards is not None
    assert isinstance(work.awards[0], Award)
    assert work.awards[0].funder_award_id == "EP/S019472/1"


def test_openalex_work_to_canonical_uses_unified_ut_corresponding_id():
    payload = _load_openalex_fixture("works_response_live_contract.json")

    work = Work.from_dict(payload["results"][0])
    canonical = openalex_work_to_canonical(work)

    assert canonical.ut_is_corresponding is True
    assert canonical.source_ids["openalex"] == work.id
