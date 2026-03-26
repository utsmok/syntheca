"""Tests for the UT institution profile configuration."""

from syntheca.config.ut_profile import UTProfile, ut_profile


def test_profile_loads_defaults():
    """The singleton ``ut_profile`` should load with all defaults populated."""
    assert ut_profile.openalex_institution_id == "https://openalex.org/I94624287"
    assert ut_profile.pure_oai_endpoint == "https://ris.utwente.nl/ws/oai"
    assert ut_profile.default_affiliation_uuid == "491145c6-1c9b-4338-aedd-98315c166d7e"


def test_mapping_paths_exist():
    """All mapping JSON paths referenced by the profile must exist on disk."""
    assert ut_profile.faculty_mapping_path.exists(), "faculties.json missing"
    assert ut_profile.publisher_mapping_path.exists(), "publishers.json missing"
    assert ut_profile.corrections_mapping_path.exists(), "corrections.json missing"


def test_mapping_paths_are_absolute():
    """Mapping paths should resolve to absolute paths (not relative)."""
    assert ut_profile.faculty_mapping_path.is_absolute()
    assert ut_profile.publisher_mapping_path.is_absolute()
    assert ut_profile.corrections_mapping_path.is_absolute()


def test_profile_instantiates_fresh():
    """Creating a new UTProfile should succeed with identical defaults."""
    fresh = UTProfile()
    assert fresh.openalex_institution_id == ut_profile.openalex_institution_id
    assert fresh.default_affiliation_uuid == ut_profile.default_affiliation_uuid


def test_profile_env_override(monkeypatch):
    """Environment variable overrides should work via the SYNTHECA_UT_ prefix."""
    monkeypatch.setenv("SYNTHECA_UT_PURE_OAI_ENDPOINT", "https://example.com/oai")
    overridden = UTProfile()
    assert overridden.pure_oai_endpoint == "https://example.com/oai"


def test_profile_openalex_id_matches_matching_module():
    """The profile ID should be consistent with the hardcoded constant in matching.py."""
    from syntheca.processing.matching import UT_OPENALEX_ID

    assert ut_profile.openalex_institution_id == UT_OPENALEX_ID


def test_profile_affiliation_uuid_format():
    """The affiliation UUID should be a valid UUID4 format."""
    import uuid

    # Should not raise
    uuid.UUID(ut_profile.default_affiliation_uuid, version=4)
