"""Typed models for external APIs such as OpenAlex.

Expose dataclass models and helper configs used across the project,
as well as the canonical (Pydantic) normalized record layer.
"""

from .canonical import (
    CanonicalOrganization,
    CanonicalPerson,
    CanonicalWork,
    SourceAssertion,
    canonicals_to_polars,
)
from .openalex import production_config

__all__ = [
    "CanonicalOrganization",
    "CanonicalPerson",
    "CanonicalWork",
    "SourceAssertion",
    "canonicals_to_polars",
    "production_config",
]
