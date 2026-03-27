"""UT People client used to search, scrape and parse faculty profiles.

This module exposes `UTPeopleClient` which provides person search through a
remote RPC interface and page-scraping helpers to enrich author metadata with
organization and department details.
"""

from __future__ import annotations

import math
import re
from functools import lru_cache
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import httpx
from Levenshtein import ratio as levenshtein_ratio
from loguru import logger
from selectolax.parser import HTMLParser

from syntheca.clients.base import BaseClient
from syntheca.config import settings

#: Minimum Levenshtein similarity to accept when ranking ambiguous candidates.
MIN_CANDIDATE_SIMILARITY: float = 0.55

#: Base URL used when converting relative profile paths to absolute URLs.
BASE_URL: str = "https://people.utwente.nl"

#: Default number of UT People results requested per page.
DEFAULT_RESULTS_PER_PAGE: int = 20

#: Maximum number of extra result pages to inspect when candidate ranking is ambiguous.
MAX_ADDITIONAL_RESULT_PAGES: int = 2

#: Similarity threshold above which the best candidate is considered confident enough.
HIGH_CONFIDENCE_CANDIDATE_SIMILARITY: float = 0.92

#: Minimum score gap between the first and second candidate to treat the best hit as unambiguous.
AMBIGUOUS_CANDIDATE_MARGIN: float = 0.05


class UTPeopleClient(BaseClient):
    """Client for UT People RPC and profile scraping.

    This client provides a method to search persons by name via the RPC
    endpoint and a helper to scrape profile pages for detailed organization
    information.
    """

    RPC_URL = "https://people.utwente.nl/wh_services/utwente_ppp/rpc/"

    async def _fetch_search_page(
        self, name: str, *, page: int, results_per_page: int
    ) -> dict[str, Any]:
        """Fetch one UT People RPC search page."""
        payload = {
            "id": 1,
            "method": "SearchPersons",
            "params": [
                {
                    "query": name,
                    "page": page,
                    "resultsperpage": results_per_page,
                    "langcode": "en",
                }
            ],
        }
        resp = await self.request("POST", self.RPC_URL, json=payload)
        return resp.json()

    def _parse_search_results(self, data: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
        """Parse one RPC response page into candidates plus reported total count."""
        result = data.get("result") or {}
        html_content = (result.get("resultshtml") or "").replace("\\/", "/")
        if not html_content:
            return [], _coerce_totalcount(result.get("totalcount"), default=0)

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

        totalcount = _coerce_totalcount(result.get("totalcount"), default=len(candidates))
        return candidates, totalcount

    async def search_person(
        self,
        name: str,
        *,
        rank: bool = True,
        max_additional_pages: int = MAX_ADDITIONAL_RESULT_PAGES,
    ) -> list[dict[str, Any]]:
        """Search the people RPC endpoint and return parsed candidate dicts.

        The RPC endpoint returns HTML; this function parses the search results
        into a list of candidate dictionaries with the keys:
            - found_name, email, people_page_url, main_orgs, role.

        When *rank* is ``True`` (default) and multiple candidates are returned,
        results are sorted by Levenshtein similarity to *name* and candidates
        below ``MIN_CANDIDATE_SIMILARITY`` are dropped. When the first page is
        ambiguous and the RPC envelope reports additional pages via
        ``result.totalcount``, the client inspects a bounded number of extra
        pages before final ranking.

        Args:
            name: Search query string (name) to send to the RPC API.
            rank: When ``True``, apply Levenshtein-based ranking and filtering.
            max_additional_pages: Maximum number of extra RPC pages to inspect
                when the first page does not yield a confident best match.

        Returns:
            A list of candidate dictionaries; empty list when no matches.
        """
        data = await self._fetch_search_page(
            name,
            page=0,
            results_per_page=DEFAULT_RESULTS_PER_PAGE,
        )
        candidates, totalcount = self._parse_search_results(data)

        if rank and candidates and max_additional_pages > 0:
            available_additional_pages = max(
                0, math.ceil(totalcount / DEFAULT_RESULTS_PER_PAGE) - 1
            )
            pages_to_check = min(max_additional_pages, available_additional_pages)
            current_page = 1
            while current_page <= pages_to_check and _needs_additional_pages(
                name, candidates, totalcount
            ):
                next_page = await self._fetch_search_page(
                    name,
                    page=current_page,
                    results_per_page=DEFAULT_RESULTS_PER_PAGE,
                )
                next_candidates, _ = self._parse_search_results(next_page)
                if not next_candidates:
                    break
                candidates = _deduplicate_candidates([*candidates, *next_candidates])
                current_page += 1

        # Rank candidates by Levenshtein similarity when requested
        if rank and candidates:
            candidates = rank_candidates(name, candidates)

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

    def _parse_organization_details(self, html: str) -> list[dict[str, Any]] | None:
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
            org_widget_class = org_widget.attributes.get("class") or ""
            if org_widget.tag != "-text" and "widget-linklist" in org_widget_class:
                break
            org_widget = org_widget.next
        org_widget_class = org_widget.attributes.get("class") if org_widget is not None else None
        if not org_widget or "widget-linklist" not in (org_widget_class or ""):
            return None
        list_items = org_widget.css("li.widget-linklist__item")
        organizations: list[dict[str, Any]] = []
        current_hierarchy: list[dict[str, Any]] = []
        for item in list_items:
            text_node = item.css_first("span.widget-linklist__text")
            if not text_node:
                continue
            text = text_node.text(strip=True)
            item_class = item.attributes.get("class") or ""
            level = _extract_org_level(item_class)
            if level is None:
                continue

            node = {
                **self._parse_org_text(text),
                "level": level,
                "raw_text": text,
            }
            if level == 1:
                if current_hierarchy:
                    organizations.append(_build_org_entry(current_hierarchy))
                current_hierarchy = [node]
                continue
            if not current_hierarchy:
                current_hierarchy = [node]
                continue
            current_hierarchy.append(node)

        if current_hierarchy:
            organizations.append(_build_org_entry(current_hierarchy))
        return organizations if organizations else None

    async def scrape_profile(self, url: str) -> list[dict[str, Any]] | None:
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
    if not url:
        return url

    normalized_input = url.strip()
    if normalized_input.startswith("//"):
        normalized_input = f"https:{normalized_input}"

    absolute = urljoin(BASE_URL + "/", normalized_input.lstrip("/"))
    parsed = urlparse(absolute)
    netloc = parsed.netloc.lower()
    if netloc == "www.people.utwente.nl":
        netloc = "people.utwente.nl"

    scheme = "https" if netloc == "people.utwente.nl" else parsed.scheme or "https"
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")

    return urlunparse((scheme, netloc, path, "", "", ""))


def _coerce_totalcount(value: Any, *, default: int) -> int:
    """Coerce UT People ``totalcount`` values to an integer."""
    try:
        return int(value)
    except TypeError, ValueError:
        return default


def _extract_org_level(item_class: str) -> int | None:
    """Extract the numeric widget level from a UT People list item class string."""
    match = re.search(r"widget-linklist__item--level(\d+)", item_class)
    if not match:
        return None
    return int(match.group(1))


def _empty_org_slot() -> dict[str, str | None]:
    """Return the default empty organisation slot."""
    return {"name": None, "abbr": None}


@lru_cache(maxsize=1)
def _known_faculty_names() -> set[str]:
    """Load the configured faculty-like names used for conservative semantics."""
    path = settings.faculties_mapping_path
    if not path.exists():
        return set()
    try:
        import json

        data = json.loads(path.read_text(encoding="utf8"))
    except OSError, ValueError, TypeError:
        return set()
    mapping = data.get("mapping", {}) if isinstance(data, dict) else {}
    return {str(name).strip() for name in mapping}


def _is_faculty_like(name: str | None) -> bool:
    """Return whether a UT People hierarchy node is safely interpretable as a faculty-like unit."""
    if not name:
        return False
    return name.strip() in _known_faculty_names()


def _build_org_entry(hierarchy: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a conservative organisation entry from a raw UT People hierarchy branch."""
    cleaned_hierarchy = [
        {
            "name": node.get("name"),
            "abbr": node.get("abbr"),
            "level": node.get("level"),
            "raw_text": node.get("raw_text"),
        }
        for node in hierarchy
        if node.get("name")
    ]
    if not cleaned_hierarchy:
        return {
            "unit": _empty_org_slot(),
            "faculty": _empty_org_slot(),
            "department": _empty_org_slot(),
            "group": _empty_org_slot(),
            "hierarchy": [],
        }

    faculty_index = next(
        (
            index
            for index, node in enumerate(cleaned_hierarchy)
            if _is_faculty_like(node.get("name"))
        ),
        None,
    )
    faculty = (
        {
            "name": cleaned_hierarchy[faculty_index].get("name"),
            "abbr": cleaned_hierarchy[faculty_index].get("abbr"),
        }
        if faculty_index is not None
        else _empty_org_slot()
    )
    department = (
        {
            "name": cleaned_hierarchy[faculty_index + 1].get("name"),
            "abbr": cleaned_hierarchy[faculty_index + 1].get("abbr"),
        }
        if faculty_index is not None and len(cleaned_hierarchy) > faculty_index + 1
        else _empty_org_slot()
    )
    group = (
        {
            "name": cleaned_hierarchy[faculty_index + 2].get("name"),
            "abbr": cleaned_hierarchy[faculty_index + 2].get("abbr"),
        }
        if faculty_index is not None and len(cleaned_hierarchy) > faculty_index + 2
        else _empty_org_slot()
    )

    top_level = cleaned_hierarchy[0]
    return {
        "unit": {"name": top_level.get("name"), "abbr": top_level.get("abbr")},
        "faculty": faculty,
        "department": department,
        "group": group,
        "hierarchy": cleaned_hierarchy,
    }


def _deduplicate_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate candidate rows while preserving original order."""
    seen: set[tuple[str | None, str | None, str | None]] = set()
    deduplicated: list[dict[str, Any]] = []
    for candidate in candidates:
        key = (
            _normalize_profile_url(candidate.get("people_page_url") or "") or None,
            _normalize_person_name(candidate.get("found_name") or "") or None,
            (candidate.get("email") or "").strip().lower() or None,
        )
        if key in seen:
            continue
        seen.add(key)
        deduplicated.append(candidate)
    return deduplicated


def _normalize_person_name(name: str) -> str:
    """Normalize a person name for candidate matching."""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", name.lower())).strip()


def _score_candidates(
    query_name: str, candidates: list[dict[str, Any]]
) -> list[tuple[float, dict[str, Any]]]:
    """Score UT People candidates against a search name."""
    query_normalized = _normalize_person_name(query_name)
    scored: list[tuple[float, dict[str, Any]]] = []
    for candidate in candidates:
        found_name = _normalize_person_name(candidate.get("found_name") or "")
        if not found_name:
            continue
        scored.append((levenshtein_ratio(query_normalized, found_name), candidate))
    scored.sort(key=lambda item: item[0], reverse=True)
    return scored


def _needs_additional_pages(
    query_name: str,
    candidates: list[dict[str, Any]],
    totalcount: int,
) -> bool:
    """Return whether the first UT People page is ambiguous enough to inspect more results."""
    if totalcount <= len(candidates) or not candidates:
        return False

    scored = _score_candidates(query_name, candidates)
    if not scored:
        return False

    top_score, top_candidate = scored[0]
    if _normalize_person_name(top_candidate.get("found_name") or "") == _normalize_person_name(
        query_name
    ):
        return False

    if len(scored) == 1:
        return top_score < HIGH_CONFIDENCE_CANDIDATE_SIMILARITY

    second_score = scored[1][0]
    return (
        top_score < HIGH_CONFIDENCE_CANDIDATE_SIMILARITY
        or (top_score - second_score) < AMBIGUOUS_CANDIDATE_MARGIN
    )


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
    scored = _score_candidates(query_name, candidates)
    return [candidate for score, candidate in scored if score >= threshold]
