"""Pure OAI-PMH data provider wrapping PureOAIClient + canonical adapters."""

from __future__ import annotations

from loguru import logger

from syntheca.clients.pure_oai import PureOAIClient
from syntheca.config.source_precedence import Source
from syntheca.models.adapters import (
    pure_orgunit_to_canonical,
    pure_person_to_canonical,
    pure_publication_to_canonical,
)
from syntheca.models.canonical import CanonicalOrganization, CanonicalPerson, CanonicalWork

# Mapping from entity name → Pure OAI collection identifier
_ENTITY_COLLECTIONS: dict[str, str] = {
    "works": "openaire_cris_publications",
    "persons": "openaire_cris_persons",
    "organizations": "openaire_cris_orgunits",
}


class PureProvider:
    """Provider that fetches Pure OAI-PMH records and converts to canonical form."""

    def __init__(self, client: PureOAIClient) -> None:
        """Initialize with a :class:`PureOAIClient` instance."""
        self._client = client

    @property
    def source(self) -> Source:
        """Return :attr:`Source.PURE`."""
        return Source.PURE

    @property
    def capabilities(self) -> set[str]:
        """Return supported entity types."""
        return {"works", "persons", "organizations"}

    async def fetch(
        self, entity: str, **kwargs: object
    ) -> list[CanonicalWork] | list[CanonicalPerson] | list[CanonicalOrganization]:
        """Fetch records from Pure OAI and convert via adapters.

        Args:
            entity: One of ``"works"``, ``"persons"``, ``"organizations"``.
            **kwargs: Passed through to ``PureOAIClient.get_all_records``
                (currently unused).

        Returns:
            List of canonical records for the requested entity type.

        Raises:
            ValueError: If *entity* is not in :attr:`capabilities`.
        """
        if entity not in self.capabilities:
            raise ValueError(f"PureProvider does not support entity {entity!r}")

        collection = _ENTITY_COLLECTIONS[entity]
        raw = await self._client.get_all_records([collection])
        records = raw.get(collection, [])

        if entity == "works":
            return _convert_works(records)
        if entity == "persons":
            return _convert_persons(records)
        return _convert_organizations(records)


def _convert_works(records: list[dict]) -> list[CanonicalWork]:
    out: list[CanonicalWork] = []
    for r in records:
        try:
            out.append(pure_publication_to_canonical(r))
        except Exception as exc:
            logger.debug("Pure canonical work conversion failed: {}", exc)
    return out


def _convert_persons(records: list[dict]) -> list[CanonicalPerson]:
    out: list[CanonicalPerson] = []
    for r in records:
        try:
            out.append(pure_person_to_canonical(r))
        except Exception as exc:
            logger.debug("Pure canonical person conversion failed: {}", exc)
    return out


def _convert_organizations(records: list[dict]) -> list[CanonicalOrganization]:
    out: list[CanonicalOrganization] = []
    for r in records:
        try:
            out.append(pure_orgunit_to_canonical(r))
        except Exception as exc:
            logger.debug("Pure canonical org conversion failed: {}", exc)
    return out
