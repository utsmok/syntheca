from __future__ import annotations

from pathlib import Path

from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Paths to mapping JSON files
    publishers_mapping_path: Path = Path(__file__).parent / "mappings" / "publishers.json"
    faculties_mapping_path: Path = Path(__file__).parent / "mappings" / "faculties.json"
    corrections_mapping_path: Path = Path(__file__).parent / "mappings" / "corrections.json"

    # Network and API defaults
    user_agent: str = "mailto:s.mok@utwente.nl"
    default_timeout: float = 10.0
    openalex_base_url: str = "https://api.openalex.org"
    cache_dir: Path = Path(__file__).parent.parent / ".cache"
    log_file: Path = Path(__file__).parent.parent / "logs" / "syntheca.log"
    # UI / behaviour toggles
    enable_progress: bool = True
    persist_intermediate: bool = True

    model_config = ConfigDict(env_prefix="SYNTHECA_")


settings = Settings()
