from dacite import from_dict

from syntheca.models.openalex import WorkIds, production_config


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
