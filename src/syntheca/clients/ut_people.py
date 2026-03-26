"""UT People client used to search, scrape and parse faculty profiles.

This module exposes `UTPeopleClient` which provides person search through a
remote RPC interface and page-scraping helpers to enrich author metadata with
organization and department details.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin

import httpx
import polars as pl
from Levenshtein import ratio as levenshtein_ratio
from loguru import logger
from selectolax.parser import HTMLParser

from syntheca.clients.base import BaseClient
from syntheca.config import settings
from syntheca.utils.persistence import load_dataframe_parquet, save_dataframe_parquet

#: Minimum Levenshtein similarity to accept when ranking ambiguous candidates.
MIN_CANDIDATE_SIMILARITY: float = 0.55

#: Base URL used when converting relative profile paths to absolute URLs.
BASE_URL: str = "https://people.utwente.nl"


class UTPeopleClient(BaseClient):
    """Client for UT People RPC and profile scraping.

    This client provides a method to search persons by name via the RPC
    endpoint and a helper to scrape profile pages for detailed organization
    information.
    """

    RPC_URL = "https://people.utwente.nl/wh_services/utwente_ppp/rpc/"

    async def search_person(self, name: str, *, rank: bool = True) -> list[dict[str, Any]]:
        """Search the people RPC endpoint and return parsed candidate dicts.

        The RPC endpoint returns HTML; this function parses the search results
        into a list of candidate dictionaries with the keys:
            - found_name, email, people_page_url, main_orgs, role.

        When *rank* is ``True`` (default) and multiple candidates are returned,
        results are sorted by Levenshtein similarity to *name* and candidates
        below ``MIN_CANDIDATE_SIMILARITY`` are dropped.

        Args:
            name: Search query string (name) to send to the RPC API.
            rank: When ``True``, apply Levenshtein-based ranking and filtering.

        Returns:
            A list of candidate dictionaries; empty list when no matches.
        """
        # If cache retrieval is enabled, try to load cached results for this name
        if getattr(settings, "use_cache_for_retrieval", False):
            try:
                fname = name.lower().replace(" ", "_")[:64]
                df = load_dataframe_parquet(f"ut_people_search_{fname}")
                if df is not None and df.height:
                    return df.to_dicts()
            except (FileNotFoundError, OSError, pl.exceptions.ComputeError) as exc:
                logger.debug("Cache miss for UT People search '{}': {}", name, exc)

        # build payload similar to notebook
        payload = {
            "id": 1,
            "method": "SearchPersons",
            "params": [{"query": name, "page": 0, "resultsperpage": 20, "langcode": "en"}],
        }

        resp = await self.request("POST", self.RPC_URL, json=payload)
        data = resp.json()
        if not (data.get("result") and data["result"].get("resultshtml")):
            return []
        html_content = data["result"]["resultshtml"].replace("\\", "")
        tree = HTMLParser(html_content)
        people_tiles = tree.css("div.ut-person-tile")

        candidates: list[dict[str, Any]] = []

        for tile in people_tiles:
            name_node = tile.css_first("h3.ut-person-tile__title")
            email_node = tile.css_first("div.ut-person-tile__mail span.text")
            url_node = tile.css_first("div.ut-person-tile__profilelink a")
            role_node = tile.css_first("div.ut-person-tile__roles")

            found_name = name_node.text(strip=True) if name_node else None
            raw_url = url_node.attributes.get("href") if url_node else None
            profile_url = _normalize_profile_url(raw_url) if raw_url else None
            candidates.append(
                {
                    "found_name": found_name,
                    "role": role_node.text(strip=True) if role_node else None,
                    "email": email_node.text(strip=True) if email_node else None,
                    "people_page_url": profile_url,
                    "main_orgs": [
                        n.text(strip=True) for n in tile.css("div.ut-person-tile__orgs > div")
                    ]
                    or None,
                }
            )

        # Rank candidates by Levenshtein similarity when requested
        if rank and candidates:
            candidates = rank_candidates(name, candidates)

        # Persist search results if configured
        try:
            if settings.persist_intermediate and candidates:
                fname = name.lower().replace(" ", "_")[:64]
                save_dataframe_parquet(pl.from_dicts(candidates), f"ut_people_search_{fname}")
        except (OSError, pl.exceptions.ComputeError) as exc:
            logger.debug("Failed to persist UT People search for '{}': {}", name, exc)
        return candidates

    def _parse_org_text(self, text: str, split: bool = False) -> dict[str, str | None]:
        """Extract organization name and optional abbreviation from a string.

        Example: ``"Faculty of Science (ENS)"`` → ``{"name": "Faculty of Science", "abbr": "ENS"}``

        Args:
            text: Organization text; expected to contain a name and optional parentheses.
            split: When True and an abbreviation contains dashes, keep the last element.

        Returns:
            Dictionary with ``name`` and ``abbr`` keys.
        """
        match = re.search(r"(.+?)\s*\(([^)]+)\)$", text)
        if match:
            abbr = match.group(2).strip()
            if abbr and split:
                abbr = abbr.split("-")[-1].strip()
            return {"name": match.group(1).strip(), "abbr": abbr}
        return {"name": text.strip(), "abbr": None}

    def _parse_organization_details(self, html: str) -> list[dict[str, str | None]] | None:
        """Parse an organization listing widget HTML and extract hierarchy.

        Args:
            html: HTML content of a UT People profile page with organization listings.

        Returns:
            A list of organization dicts or ``None`` when no orgs found.
        """
        tree = HTMLParser(html)
        all_headings = tree.css("h2.heading2")
        org_heading = None
        for h in all_headings:
            if h.text(strip=True) == "Organisations":
                org_heading = h
                break
        if not org_heading:
            return None
        # Walk siblings to find the widget-linklist element (skip text nodes)
        org_widget = org_heading.next
        while org_widget is not None:
            if org_widget.tag != "-text" and "widget-linklist" in org_widget.attributes.get(
                "class", ""
            ):
                break
            org_widget = org_widget.next
        if not org_widget or "widget-linklist" not in org_widget.attributes.get("class", ""):
            return None
        list_items = org_widget.css("li.widget-linklist__item")
        organizations = []
        current_org = {}
        for item in list_items:
            text_node = item.css_first("span.widget-linklist__text")
            if not text_node:
                continue
            text = text_node.text(strip=True)
            item_class = item.attributes.get("class", "")
            if "widget-linklist__item--level1" in item_class:
                if current_org:
                    organizations.append(current_org)
                current_org = {
                    "faculty": self._parse_org_text(text),
                    "department": {"name": None, "abbr": None},
                    "group": {"name": None, "abbr": None},
                }
            elif "widget-linklist__item--level2" in item_class and current_org:
                current_org["department"] = self._parse_org_text(text, split=True)
            elif "widget-linklist__item--level3" in item_class and current_org:
                current_org["group"] = self._parse_org_text(text, split=True)
        if current_org:
            organizations.append(current_org)
        return organizations if organizations else None

    async def scrape_profile(self, url: str) -> list[dict[str, str | None]] | None:
        """Fetch and parse a UT People profile page to find organization details.

        Args:
            url: Absolute URL to the profile page to scrape.

        Returns:
            Extracted list of organization details or ``None`` on failure.
        """
        url = _normalize_profile_url(url)
        try:
            resp = await self.request("GET", url)
            resp.raise_for_status()
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            logger.warning("Failed to scrape UT People profile {}: {}", url, exc)
            return None
        return self._parse_organization_details(resp.text)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _normalize_profile_url(url: str) -> str:
    """Ensure a profile URL is absolute.

    Relative paths like ``/en/persons/alice`` are converted to
    ``https://people.utwente.nl/en/persons/alice``.
    """
    if url and not url.startswith(("http://", "https://")):
        return urljoin(BASE_URL + "/", url.lstrip("/"))
    return url


def rank_candidates(
    query_name: str,
    candidates: list[dict[str, Any]],
    *,
    threshold: float = MIN_CANDIDATE_SIMILARITY,
) -> list[dict[str, Any]]:
    """Rank and filter candidates by Levenshtein similarity to *query_name*.

    Candidates whose ``found_name`` is below *threshold* are dropped.  The
    remaining candidates are returned sorted best-match-first.

    Args:
        query_name: The original search name.
        candidates: List of candidate dicts (must have ``found_name`` key).
        threshold: Minimum Levenshtein ratio to keep a candidate.

    Returns:
        Filtered and sorted candidate list.
    """
    query_lower = query_name.strip().lower()
    scored: list[tuple[float, dict[str, Any]]] = []
    for c in candidates:
        found = (c.get("found_name") or "").strip().lower()
        if not found:
            continue
        sim = levenshtein_ratio(query_lower, found)
        if sim >= threshold:
            scored.append((sim, c))
    scored.sort(key=lambda t: t[0], reverse=True)
    return [c for _, c in scored]
