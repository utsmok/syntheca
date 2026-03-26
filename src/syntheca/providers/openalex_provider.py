"""OpenAlex data provider wrapping OpenAlexClient + canonical adapters."""

from __future__ import annotations

from loguru import logger

from syntheca.clients.openalex import OpenAlexClient
from syntheca.config.source_precedence import Source
from syntheca.models.adapters import openalex_work_to_canonical
from syntheca.models.canonical import CanonicalWork


class OpenAlexProvider:
    """Provider that fetches OpenAlex works and converts to canonical form."""

    def __init__(self, client: OpenAlexClient) -> None:
        """Initialize with an :class:`OpenAlexClient` instance."""
        self._client = client

    @property
    def source(self) -> Source:
        """Return :attr:`Source.OPENALEX`."""
        return Source.OPENALEX

    @property
    def capabilities(self) -> set[str]:
        """Return supported entity types."""
        return {"works"}

    async def fetch(self, entity: str, **kwargs: object) -> list[CanonicalWork]:
        """Fetch works from OpenAlex and convert via adapters.

        Args:
            entity: Must be ``"works"``.
            **kwargs: Forwarded to :meth:`OpenAlexClient.get_works_by_ids`.
                Expected keyword arguments:

                * ``ids`` (``list[str]``): IDs (DOIs or OpenAlex IDs) to fetch.
                * ``id_type`` (``str``): ``"doi"`` (default) or ``"id"``.

        Returns:
            List of :class:`CanonicalWork` records.

        Raises:
            ValueError: If *entity* is not ``"works"``.
        """
        if entity not in self.capabilities:
            raise ValueError(f"OpenAlexProvider does not support entity {entity!r}")

        ids: list[str] = list(kwargs.get("ids") or [])  # type: ignore[arg-type]
        id_type: str = str(kwargs.get("id_type", "doi"))

        if not ids:
            logger.info("OpenAlexProvider.fetch called with no ids; returning empty list")
            return []

        works = await self._client.get_works_by_ids(ids, id_type=id_type)

        out: list[CanonicalWork] = []
        for w in works:
            try:
                out.append(openalex_work_to_canonical(w))
            except Exception as exc:
                logger.debug("OpenAlex canonical work conversion failed: {}", exc)
        return out
