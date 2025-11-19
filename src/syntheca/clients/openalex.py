from __future__ import annotations

import asyncio
from collections.abc import Iterable
from urllib.parse import quote

from dacite import from_dict

from syntheca.clients.base import BaseClient
from syntheca.config import settings
from syntheca.models.openalex import Work, production_config


class OpenAlexClient(BaseClient):
    """Client for querying the OpenAlex API and returning typed models."""

    BASE = settings.openalex_base_url
    PER_PAGE = 50

    @staticmethod
    def _chunks(iterable: Iterable[str], size: int):
        it = list(iterable)
        for i in range(0, len(it), size):
            yield it[i : i + size]

    async def get_works_by_ids(self, ids: list[str], id_type: str = "doi") -> list[Work]:
        id_type_param = "openalex" if id_type == "id" else id_type
        results: list[Work] = []
        for batch in self._chunks(ids, self.PER_PAGE):
            filter_value = "|".join([str(x).replace("doi:", "") for x in batch])
            params = {
                "filter": f"{id_type_param}:{filter_value}",
                "per-page": self.PER_PAGE,
            }
            url = f"{self.BASE}/works"
            resp = await self.request("GET", url, params=params)
            data = resp.json()
            items = data.get("results", [])
            for it in items:
                try:
                    results.append(from_dict(data_class=Work, data=it, config=production_config))
                except Exception:
                    # Skip items we can't parse; upstream will handle
                    continue
        return results

    async def get_works_by_title(self, title: str) -> list[Work]:
        url = f"{self.BASE}/autocomplete/works?q={quote(title)}"
        resp = await self.request("GET", url)
        data = resp.json()
        results = []
        # fetch details in parallel to speed up title lookups
        ids = [item.get("id") for item in data.get("results", []) if item.get("id")]
        coros = [self.request("GET", f"{self.BASE}/works/{quote(i)}") for i in ids]
        if coros:
            responses = await asyncio.gather(*coros, return_exceptions=True)
            for resp in responses:
                if not resp or isinstance(resp, Exception):
                    continue
                try:
                    work_data = resp.json()
                    results.append(
                        from_dict(data_class=Work, data=work_data, config=production_config)
                    )
                except Exception:
                    continue
        return results

    def clean_openalex_raw_data(self, works: list[dict]) -> list[dict]:
        """Return cleaned dictionaries for OpenAlex work data similar to the monolith implementation."""
        cleaned = []
        utwente_oa_id = "https://openalex.org/I94624287"
        for w in works:
            wclean = {}
            oa = w.get("open_access", {}) or {}
            wclean["is_oa"] = oa.get("is_oa")
            wclean["oa_color"] = oa.get("oa_status")
            wclean["in_repository"] = oa.get("any_repository_has_fulltext")
            wclean["oa_url"] = oa.get("oa_url")

            best = w.get("best_oa_location") or {}
            wclean["main_url"] = best.get("landing_page_url")
            wclean["oa_host_org"] = (best.get("source") or {}).get("host_organization_name")
            wclean["oa_host_name"] = (best.get("source") or {}).get("display_name")
            wclean["oa_host_type"] = (best.get("source") or {}).get("type")

            primary = w.get("primary_location") or {}
            wclean["primary_url"] = primary.get("landing_page_url")
            wclean["primary_host_org"] = (primary.get("source") or {}).get("host_organization_name")
            wclean["primary_host_name"] = (primary.get("source") or {}).get("display_name")
            wclean["primary_host_type"] = (primary.get("source") or {}).get("type")

            # all hosts
            locs = w.get("locations") or []
            hosts = []
            for loc in locs:
                src = (loc or {}).get("source") or {}
                ho = src.get("host_organization_name")
                if ho:
                    hosts.append(ho)
            wclean["all_host_orgs"] = list(dict.fromkeys(hosts))

            pt = w.get("primary_topic") or {}
            wclean["topic"] = pt.get("display_name")
            wclean["subfield"] = (pt.get("subfield") or {}).get("display_name")
            wclean["field"] = (pt.get("field") or {}).get("display_name")
            wclean["domain"] = (pt.get("domain") or {}).get("display_name")

            wclean["listed_apc_usd"] = (w.get("apc_list") or {}).get("value_usd")
            wclean["paid_apc_usd"] = (w.get("apc_paid") or {}).get("value_usd")
            wclean["ut_is_corresponding"] = utwente_oa_id in (
                w.get("corresponding_institution_ids") or []
            )

            # merge with basic fields
            wclean.update(
                {
                    "id": w.get("id"),
                    "display_name": w.get("display_name"),
                    "doi": w.get("doi"),
                    "publication_year": w.get("publication_year"),
                }
            )

            cleaned.append(wclean)

        return cleaned
