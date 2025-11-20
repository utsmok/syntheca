"""OpenAlex API client module.

Provides `OpenAlexClient`, an async client wrapper that fetches works and
converts API responses into typed `Work` dataclasses used by the pipeline.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Iterable
from urllib.parse import quote

import polars as pl
from dacite import from_dict
from tqdm import tqdm

from syntheca.clients.base import BaseClient
from syntheca.config import settings
from syntheca.models.openalex import Work, production_config
from syntheca.utils.persistence import load_dataframe_parquet, save_dataframe_parquet
from syntheca.utils.progress import get_next_position


class OpenAlexClient(BaseClient):
    """Client for querying the OpenAlex API and returning typed models."""

    BASE = settings.openalex_base_url
    PER_PAGE = 50

    @staticmethod
    def _chunks(iterable: Iterable[str], size: int):
        """Yield successive chunks from `iterable` of length `size`.

        Args:
            iterable (Iterable[str]): The iterable to chunk.
            size (int): The max size of each chunk.

        Yields:
            list[str]: Slices of the original iterable of length <= `size`.

        """
        it = list(iterable)
        for i in range(0, len(it), size):
            yield it[i : i + size]

    async def get_works_by_ids(
        self, ids: list[str], id_type: str = "doi", position: int | None = None
    ) -> list[Work]:
        """Retrieve works from OpenAlex for provided IDs and return typed models.

        This method performs batched requests to the OpenAlex `works` endpoint and
        converts results into typed `Work` dataclass instances when possible.

        Args:
            ids (list[str]): A list of IDs to fetch; typically DOIs or OpenAlex IDs.
            id_type (str): The id type ("doi" or "id") that defines the filter.
            position (int | None): Optional tqdm progress bar position. If `None`, a
                global position will be allocated.

        Returns:
            list[Work]: Parsed OpenAlex `Work` dataclass instances.

        """
        id_type_param = "openalex" if id_type == "id" else id_type
        # If cache retrieval is enabled, attempt to load a cached file first
        if settings.use_cache_for_retrieval:
            try:
                df = load_dataframe_parquet("openalex_works")
                if df is not None and df.height:
                    rows = df.to_dicts()
                    out: list[Work] = []
                    for r in rows:
                        try:
                            out.append(from_dict(data_class=Work, data=r, config=production_config))
                        except Exception:
                            # ignore row we can't parse into a dataclass
                            continue
                    if out:
                        return out
            except Exception:
                # Failed to load cache; fall back to API retrieval
                pass

        results: list[Work] = []
        raw_items: list[dict] = []
        bar = None
        if settings.enable_progress:
            pos = position if position is not None else get_next_position()
            bar = tqdm(total=len(ids), desc="openalex:ids", position=pos, unit="work")
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
                raw_items.append(it)
                try:
                    results.append(from_dict(data_class=Work, data=it, config=production_config))
                except Exception:
                    # Skip items we can't parse; upstream will handle
                    continue
            if bar is not None:
                bar.update(len(items))
        if bar is not None:
            bar.close()
        # Save raw items (fallback) and dataclass-converted rows if available
        if settings.persist_intermediate and (results or raw_items):
            # take dataclass instances and convert to dicts for saving
            rows = []
            for w in results:
                try:
                    if dataclasses.is_dataclass(w):
                        rows.append(dataclasses.asdict(w))
                    elif hasattr(w, "__dict__"):
                        rows.append({k: v for k, v in w.__dict__.items() if not k.startswith("_")})
                    else:
                        rows.append(w)
                except Exception:
                    rows.append(
                        {
                            "id": getattr(w, "id", None),
                            "display_name": getattr(w, "display_name", None),
                            "doi": getattr(w, "doi", None),
                        }
                    )
            # save converted dataclasses (if any) and save raw items as fallback
            try:
                if rows:
                    df = pl.from_dicts(rows)
                    save_dataframe_parquet(df, "openalex_works")
            except Exception:
                pass
            try:
                if raw_items:
                    rdf = pl.from_dicts(raw_items)
                    # Prefer saving converted rows as 'openalex_works', but if none exist, save raw as same name
                    save_dataframe_parquet(
                        rdf, "openalex_works" if not rows else "openalex_works_raw"
                    )
            except Exception:
                pass
        return results

    async def get_works_by_title(self, title: str) -> list[Work]:
        # Try to read cached title-based searches if enabled
        if settings.use_cache_for_retrieval:
            try:
                fname = (
                    title[:64]
                    .lower()
                    .replace(" ", "_")
                    .replace("/", "_")
                    .replace("\\", "_")
                )
                df = load_dataframe_parquet(f"openalex_title_{fname}")
                if df is not None and df.height:
                    out = []
                    for r in df.to_dicts():
                        try:
                            out.append(from_dict(data_class=Work, data=r, config=production_config))
                        except Exception:
                            continue
                    if out:
                        return out
            except Exception:
                pass
        """Automated title autocomplete + detail lookup for OpenAlex works.

        The method uses the OpenAlex autocomplete endpoint and then fetches full
        work entries in parallel for matching IDs.

        Args:
            title (str): The title text to send to the `autocomplete` endpoint.

        Returns:
            list[Work]: A list of `Work` dataclass instances matching the title.

        """
        url = f"{self.BASE}/autocomplete/works?q={quote(title)}"
        resp = await self.request("GET", url)
        data = resp.json()
        results = []
        # fetch details in parallel to speed up title lookups
        ids = [item.get("id") for item in data.get("results", []) if item.get("id")]
        coros = [self.request("GET", f"{self.BASE}/works/{quote(i)}") for i in ids]
        bar = None
        if settings.enable_progress and ids:
            bar = tqdm(
                total=len(ids), desc="openalex:title", position=get_next_position(), unit="work"
            )
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
                finally:
                    if bar:
                        bar.update(1)
        if bar is not None:
            bar.close()
        # Save title results if configured
        if settings.persist_intermediate and results:
            try:
                df = pl.from_dicts([dataclasses.asdict(w) for w in results])
                # sanitize title for file name
                fname = title[:64].lower().replace(" ", "_").replace("/", "_").replace("\\", "_")
                save_dataframe_parquet(df, f"openalex_title_{fname}")
            except Exception:
                pass
        return results

    def clean_openalex_raw_data(self, works: list[dict]) -> list[dict]:
        """Return cleaned dictionaries for OpenAlex raw work data.

        This helper inspects OpenAlex work records and extracts a small
        consistent set of fields used by downstream processing — it is inspired
        by the legacy monolith transformations but intentionally keeps the
        output compact and JSON-friendly.

        Args:
            works (list[dict]): A list of OpenAlex work result dictionaries.

        Returns:
            list[dict]: A list of simplified, normalized work dictionaries.

        """
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
