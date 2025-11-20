"""Application configuration and environment-aware settings.

This module defines a `Settings` class (pydantic `BaseSettings`) used for
loading environment-based configuration and default values for network,
paths and feature toggles used throughout the project.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Top-level pydantic Settings container for syntheca configuration.

    The class exposes default file system paths, network defaults, and toggles
    (such as `enable_progress`) which may be overridden via environment
    variables using the `SYNTHECA_` prefix.
    """

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
    # When enabled, client retrieval methods will attempt to load cached
    # data from the configured `cache_dir` (saved previously via
    # `save_dataframe_parquet`) before attempting network requests.
    use_cache_for_retrieval: bool = False

    model_config = ConfigDict(env_prefix="SYNTHECA_")


settings = Settings()
print(f"Loaded settings from environment with prefix SYNTHECA_: {settings}")
