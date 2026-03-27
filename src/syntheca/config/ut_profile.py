"""Typed UT institution profile.

Collects every institution-specific constant in a single validated
Pydantic model so that scattered magic strings are replaced by
``ut_profile.<field>`` look-ups.
"""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_MAPPINGS_DIR = Path(__file__).parent / "mappings"


class UTProfile(BaseSettings):
    """University of Twente institutional profile.

    All values have sensible defaults derived from the codebase.
    Overrides are possible via environment variables prefixed with
    ``SYNTHECA_UT_``.
    """

    # OpenAlex full institution URL (used in ``corresponding_institution_ids`` checks)
    openalex_institution_id: str = "https://openalex.org/I94624287"

    # Pure OAI-PMH base endpoint
    pure_oai_endpoint: str = "https://ris.utwente.nl/ws/oai"

    # UUID used to filter UT-affiliated authors in Pure person records
    default_affiliation_uuid: str = "491145c6-1c9b-4338-aedd-98315c166d7e"

    # Paths to mapping JSON files shipped with the package
    faculty_mapping_path: Path = _MAPPINGS_DIR / "faculties.json"
    publisher_mapping_path: Path = _MAPPINGS_DIR / "publishers.json"
    corrections_mapping_path: Path = _MAPPINGS_DIR / "corrections.json"

    model_config = SettingsConfigDict(env_prefix="SYNTHECA_UT_")


ut_profile = UTProfile()
