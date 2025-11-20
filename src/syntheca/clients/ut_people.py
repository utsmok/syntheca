"""UT People client used to search, scrape and parse faculty profiles.

This module exposes `UTPeopleClient` which provides person search through a
remote RPC interface and page-scraping helpers to enrich author metadata with
organization and department details.
"""

from __future__ import annotations

from typing import Any

from selectolax.parser import HTMLParser
import polars as pl

from syntheca.clients.base import BaseClient
from syntheca.config import settings
from syntheca.utils.persistence import load_dataframe_parquet, save_dataframe_parquet


class UTPeopleClient(BaseClient):
    """Client for UT People RPC and profile scraping.

    This client provides a method to search persons by name via the RPC
    endpoint and a helper to scrape profile pages for detailed organization
    information.
    """

    RPC_URL = "https://people.utwente.nl/wh_services/utwente_ppp/rpc/"

    async def search_person(self, name: str) -> list[dict[str, Any]]:
        """Search the people RPC endpoint and return parsed candidate dicts.

        The RPC endpoint returns HTML; this function parses the search results
        into a list of candidate dictionaries with the keys:
            - found_name, email, people_page_url, main_orgs, role.

        Args:
            name (str): Search query string (name) to send to the RPC API.

        Returns:
            list[dict[str, Any]]: A list of candidate dictionaries; empty list when
            no matches are returned.

        """
        # If cache retrieval is enabled, try to load cached results for this name
        if getattr(settings, "use_cache_for_retrieval", False):
            try:
                fname = name.lower().replace(" ", "_")[:64]
                df = load_dataframe_parquet(f"ut_people_search_{fname}")
                if df is not None and df.height:
                    return df.to_dicts()
            except Exception:
                # Fall through to live search
                pass

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
            candidates.append(
                {
                    "found_name": found_name,
                    "role": role_node.text(strip=True) if role_node else None,
                    "email": email_node.text(strip=True) if email_node else None,
                    "people_page_url": url_node.attributes.get("href") if url_node else None,
                    "main_orgs": [
                        n.text(strip=True) for n in tile.css("div.ut-person-tile__orgs > div")
                    ]
                    or None,
                }
            )

        # Persist search results if configured
        try:
            if settings.persist_intermediate and candidates:
                fname = name.lower().replace(" ", "_")[:64]
                save_dataframe_parquet(pl.from_dicts(candidates), f"ut_people_search_{fname}")
        except Exception:
            # best-effort persistence — ignore failures
            pass
        return candidates

    def _parse_org_text(self, text: str, split: bool = False) -> dict[str, str | None]:
        import re

        match = re.search(r"(.+?)\s*\(([^)]+)\)$", text)
        """Extract organization name and optional abbreviation from a string.

        Example: "Faculty of Science (ENS)" -> {"name": "Faculty of Science", "abbr": "ENS"}

        Args:
            text (str): Organization text; expected to contain a name and optional parentheses.
            split (bool): When True and an abbreviation contains dashes, keep the last element.
        Returns:
            dict[str, str | None]: Dictionary with `name` and `abbr` keys.
        """
        if match:
            abbr = match.group(2).strip()
            if abbr and split:
                abbr = abbr.split("-")[-1].strip()
            return {"name": match.group(1).strip(), "abbr": abbr}
        return {"name": text.strip(), "abbr": None}

    def _parse_organization_details(self, html: str) -> list[dict[str, str | None]] | None:
        tree = HTMLParser(html)
        all_headings = tree.css("h2.heading2")
        org_heading = None
        for h in all_headings:
            if h.text(strip=True) == "Organisations":
                org_heading = h
                break
        """Parse an organization listing widget HTML and extract hierarchy.

        Args:
            html (str): HTML content of a UT People profile page with organization listings.

        Returns:
            list[dict[str, str | None]] | None: A list of organization dicts or None when no orgs found.
        """
        if not org_heading:
            return None
        org_widget = org_heading.next
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
            url (str): Absolute URL to the profile page to scrape.

        Returns:
            list[dict[str, str | None]] | None: Extracted list of organization details or None on failure.

        """
        async with self.client as c:
            try:
                resp = await c.get(url)
                resp.raise_for_status()
            except Exception:
                return None
            return self._parse_organization_details(resp.text)
