"""OpenAIRE data provider wrapping OpenAIREClient + canonical adapters."""

from __future__ import annotations

from typing import Any

from loguru import logger

from syntheca.clients.openaire import OpenAIREClient
from syntheca.config.source_precedence import Source
from syntheca.models.adapters import openaire_org_to_canonical, openaire_product_to_canonical
from syntheca.models.canonical import CanonicalOrganization, CanonicalWork


class OpenAIREProvider:
    """Provider that fetches data from the OpenAIRE Graph API and converts to canonical form."""

    def __init__(self, client: OpenAIREClient) -> None:
        """Initialize with an :class:`OpenAIREClient` instance."""
        self._client = client

    @property
    def source(self) -> Source:
        """Return :attr:`Source.OPENAIRE`."""
        return Source.OPENAIRE

    @property
    def capabilities(self) -> set[str]:
        """Return supported entity types."""
        return {"works", "organizations"}

    async def fetch(
        self, entity: str, **kwargs: Any
    ) -> list[CanonicalWork] | list[CanonicalOrganization]:
        """Fetch and normalize records of the given entity type.

        Args:
            entity: ``"works"`` or ``"organizations"``.
            **kwargs: Forwarded to the corresponding client method.

                For *works*:
                    ``doi`` (``str | None``), ``title`` (``str | None``),
                    plus any Graph API filter params.

                For *organizations*:
                    ``name`` (``str | None``) plus any Graph API filter
                    params. Use ``precise=True`` and/or ``pid`` /
                    ``legalName`` / ``legalShortName`` when institution
                    resolution must avoid broad keyword search semantics.

        Returns:
            List of canonical records.

        Raises:
            ValueError: If *entity* is not in :attr:`capabilities`.
        """
        if entity not in self.capabilities:
            raise ValueError(f"OpenAIREProvider does not support entity {entity!r}")

        if entity == "works":
            return await self._fetch_works(**kwargs)
        return await self._fetch_organizations(**kwargs)

    # ------------------------------------------------------------------
    # Private fetch helpers
    # ------------------------------------------------------------------

    async def _fetch_works(self, **kwargs: Any) -> list[CanonicalWork]:
        doi = kwargs.get("doi")
        title = kwargs.get("title")
        # Forward remaining kwargs as extra filters
        extra: dict[str, Any] = {k: v for k, v in kwargs.items() if k not in ("doi", "title")}
        doi_text = str(doi) if doi is not None else None
        title_text = str(title) if title is not None else None

        products = await self._client.get_research_products(
            doi=doi_text,
            title=title_text,
            **extra,
        )

        out: list[CanonicalWork] = []
        for product in products:
            try:
                out.append(openaire_product_to_canonical(product))
            except Exception as exc:
                logger.debug("OpenAIRE canonical work conversion failed: {}", exc)
        return out

    async def _fetch_organizations(self, **kwargs: Any) -> list[CanonicalOrganization]:
        name = kwargs.get("name")
        extra: dict[str, Any] = {k: v for k, v in kwargs.items() if k != "name"}
        name_text = str(name) if name is not None else None

        orgs = await self._client.get_organizations(
            name=name_text,
            **extra,
        )

        out: list[CanonicalOrganization] = []
        for org in orgs:
            try:
                out.append(openaire_org_to_canonical(org))
            except Exception as exc:
                logger.debug("OpenAIRE canonical org conversion failed: {}", exc)
        return out
