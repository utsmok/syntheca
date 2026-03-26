"""Lightweight stage runner that orchestrates fetches across providers."""

from __future__ import annotations

from loguru import logger

from syntheca.config.source_precedence import Source
from syntheca.models.canonical import CanonicalOrganization, CanonicalPerson, CanonicalWork
from syntheca.providers import DataProvider


class ProviderStage:
    """Orchestrates data fetching across registered providers.

    Providers are iterated in the order they were supplied — this lets
    callers express source priority through ordering.
    """

    def __init__(self, providers: list[DataProvider]) -> None:
        """Initialize with an ordered list of providers."""
        self.providers = list(providers)

    async def fetch_all_works(self, **kwargs: object) -> dict[Source, list[CanonicalWork]]:
        """Fetch works from all providers that support it.

        Args:
            **kwargs: Forwarded to each provider's ``fetch(entity="works", ...)``.

        Returns:
            Mapping of source → list of canonical work records.
        """
        results: dict[Source, list[CanonicalWork]] = {}
        for provider in self.providers:
            if "works" not in provider.capabilities:
                continue
            try:
                works = await provider.fetch(entity="works", **kwargs)
                results[provider.source] = works  # type: ignore[assignment]
                logger.info(
                    "Provider {} returned {} works",
                    provider.source,
                    len(works),
                )
            except Exception as exc:
                logger.warning("Provider {} failed to fetch works: {}", provider.source, exc)
        return results

    async def fetch_all_persons(self, **kwargs: object) -> dict[Source, list[CanonicalPerson]]:
        """Fetch persons from all providers that support it.

        Args:
            **kwargs: Forwarded to each provider's ``fetch(entity="persons", ...)``.

        Returns:
            Mapping of source → list of canonical person records.
        """
        results: dict[Source, list[CanonicalPerson]] = {}
        for provider in self.providers:
            if "persons" not in provider.capabilities:
                continue
            try:
                persons = await provider.fetch(entity="persons", **kwargs)
                results[provider.source] = persons  # type: ignore[assignment]
                logger.info(
                    "Provider {} returned {} persons",
                    provider.source,
                    len(persons),
                )
            except Exception as exc:
                logger.warning("Provider {} failed to fetch persons: {}", provider.source, exc)
        return results

    async def fetch_all_organizations(
        self, **kwargs: object
    ) -> dict[Source, list[CanonicalOrganization]]:
        """Fetch organizations from all providers that support it.

        Args:
            **kwargs: Forwarded to each provider's ``fetch(entity="organizations", ...)``.

        Returns:
            Mapping of source → list of canonical organization records.
        """
        results: dict[Source, list[CanonicalOrganization]] = {}
        for provider in self.providers:
            if "organizations" not in provider.capabilities:
                continue
            try:
                orgs = await provider.fetch(entity="organizations", **kwargs)
                results[provider.source] = orgs  # type: ignore[assignment]
                logger.info(
                    "Provider {} returned {} organizations",
                    provider.source,
                    len(orgs),
                )
            except Exception as exc:
                logger.warning(
                    "Provider {} failed to fetch organizations: {}", provider.source, exc
                )
        return results
