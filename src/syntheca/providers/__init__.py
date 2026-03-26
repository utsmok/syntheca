"""Provider protocol and stage orchestration for Syntheca data ingestion.

This package defines a lightweight :class:`DataProvider` protocol that all
data source wrappers implement, and a :class:`ProviderStage` orchestrator
that fans out fetches across registered providers.

Design: explicit composition, no registry, no DI framework.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from syntheca.config.source_precedence import Source
from syntheca.models.canonical import CanonicalOrganization, CanonicalPerson, CanonicalWork


@runtime_checkable
class DataProvider(Protocol):
    """Protocol for source data providers.

    Concrete implementations wrap a client + adapter pair so that
    the pipeline can treat all sources uniformly.
    """

    @property
    def source(self) -> Source:
        """The source this provider represents."""
        ...

    @property
    def capabilities(self) -> set[str]:
        """Entity types this provider supports (e.g. ``{'works', 'persons'}``)."""
        ...

    async def fetch(
        self, entity: str, **kwargs: object
    ) -> list[CanonicalWork] | list[CanonicalPerson] | list[CanonicalOrganization]:
        """Fetch and normalize records of the given entity type from this source."""
        ...


__all__ = [
    "CanonicalOrganization",
    "CanonicalPerson",
    "CanonicalWork",
    "DataProvider",
    "Source",
]
