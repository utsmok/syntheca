"""Typed models for external APIs such as OpenAlex.

Expose dataclass models and helper configs used across the project.
"""

from .openalex import production_config

__all__ = ["production_config"]
