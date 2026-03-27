"""OpenAIRE Graph API client.

Uses the **Graph API** (v2 for research products, v1 for organizations)
with cursor-based pagination for deep traversal.

Base URL: ``https://api.openaire.eu/graph/``

Reference docs: https://graph.openaire.eu/docs/apis/graph-api
"""

from __future__ import annotations

from typing import Any

from syntheca.clients.base import BaseClient
from syntheca.config import settings
from syntheca.models.openaire import (
    OpenAIREOrganization,
    OpenAIREResearchProduct,
    ResearchProductSearchResponse,
)

# Graph API base — intentionally without trailing version segment so each
# method can target the correct version (v2 for products, v1 for orgs).
_DEFAULT_BASE_URL = "https://api.openaire.eu/graph"

_DEFAULT_PAGE_SIZE = 50
_MAX_PAGES = 200  # safety cap to avoid infinite loops


class OpenAIREClient(BaseClient):
    """Async client for the OpenAIRE Graph API.

    Uses cursor-based pagination and the retry / backoff logic from
    :class:`BaseClient`.
    """

    def __init__(
        self,
        *,
        base_url: str | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> None:
        """Initialize with an optional custom base URL, headers, and timeout."""
        super().__init__(headers=headers, timeout=timeout)
        self.base_url = (
            base_url or getattr(settings, "openaire_base_url", None) or _DEFAULT_BASE_URL
        )

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    async def get_research_products(
        self,
        doi: str | None = None,
        title: str | None = None,
        *,
        page_size: int = _DEFAULT_PAGE_SIZE,
        **filters: Any,
    ) -> list[OpenAIREResearchProduct]:
        """Search for research products by DOI, title, or free filters.

        Args:
            doi: Persistent identifier (DOI) to search for.
            title: Main-title keyword search.
            page_size: Results per page (max 100).
            **filters: Extra query parameters forwarded verbatim
                (e.g. ``type="publication"``, ``fromPublicationDate="2023"``).

        Returns:
            Parsed :class:`OpenAIREResearchProduct` instances.
        """
        params: dict[str, Any] = {"pageSize": min(page_size, 100)}
        if doi:
            params["pid"] = doi
        if title:
            params["mainTitle"] = title
        params.update(filters)

        raw = await self._paginate(f"{self.base_url}/v2/researchProducts", params)
        return [
            ResearchProductSearchResponse.model_validate(
                {"header": {"numFound": 0}, "results": raw}
            ).results[i]
            if False
            else OpenAIREResearchProduct.model_validate(item)
            for i, item in enumerate(raw)
        ]

    async def get_organizations(
        self,
        name: str | None = None,
        *,
        precise: bool = False,
        page_size: int = _DEFAULT_PAGE_SIZE,
        **filters: Any,
    ) -> list[OpenAIREOrganization]:
        """Search for organizations by name or free filters.

        Args:
            name: Legal-name or keyword search string.
            precise: When ``True``, prefer exact-ish filters such as
                ``legalName`` instead of the broad ``search`` parameter.
            page_size: Results per page (max 100).
            **filters: Extra query parameters forwarded verbatim. Prefer
                ``legalName``, ``legalShortName``, or ``pid`` when precise
                institution resolution is required.

        Returns:
            Parsed :class:`OpenAIREOrganization` instances.
        """
        params: dict[str, Any] = {"pageSize": min(page_size, 100)}
        has_precise_filter = any(filters.get(key) for key in ("pid", "legalName", "legalShortName"))
        if name and (precise or has_precise_filter):
            params["legalName"] = name
        elif name:
            params["search"] = name
        params.update(filters)

        raw = await self._paginate(f"{self.base_url}/v1/organizations", params)
        return [OpenAIREOrganization.model_validate(item) for item in raw]

    # ------------------------------------------------------------------
    # Cursor-based pagination
    # ------------------------------------------------------------------

    async def _paginate(
        self,
        endpoint: str,
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Paginate through an OpenAIRE Graph endpoint using cursor tokens.

        The first request is sent with ``cursor=*``.  Subsequent pages use the
        ``nextCursor`` value returned in the response header.

        Args:
            endpoint: Full URL to the search endpoint.
            params: Query parameters (``pageSize`` should already be set).

        Returns:
            Flat list of raw result dicts across all pages.
        """
        all_results: list[dict[str, Any]] = []
        current_params = {**params, "cursor": "*"}

        for _ in range(_MAX_PAGES):
            resp = await self.request("GET", endpoint, params=current_params)
            body = resp.json()

            header = body.get("header", {})
            results = body.get("results") or []
            all_results.extend(results)

            next_cursor = header.get("nextCursor")
            if not next_cursor or not results:
                break

            # Guard against the API returning the same cursor we already sent
            if next_cursor == current_params.get("cursor"):
                break

            current_params["cursor"] = next_cursor
            # Drop page-based param if present to avoid conflict
            current_params.pop("page", None)

        self.logger.info(
            "OpenAIRE paginate {} → {} results",
            endpoint.split("/")[-1],
            len(all_results),
        )
        return all_results
