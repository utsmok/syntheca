import json
import pathlib

from syntheca.config import settings


def test_settings_paths_exist():
    # publisher mapping and faculties/corrections must be present
    assert settings.publishers_mapping_path.exists()
    assert settings.faculties_mapping_path.exists()
    assert settings.corrections_mapping_path.exists()


def test_publishers_json_structure():
    with pathlib.Path(settings.publishers_mapping_path).open(encoding="utf8") as fh:
        data = json.load(fh)
    # Should be a dict of canonical -> list of variants
    assert isinstance(data, dict)


def test_faculties_json_structure():
    with pathlib.Path(settings.faculties_mapping_path).open(encoding="utf8") as fh:
        data = json.load(fh)
    assert "mapping" in data and "short_names" in data
