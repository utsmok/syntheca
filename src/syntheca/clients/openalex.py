"""OpenAlex API client module.

Provides `OpenAlexClient`, an async client wrapper that fetches works and
converts API responses into typed `Work` dataclasses used by the pipeline.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Iterable
from urllib.parse import quote

import httpx
from loguru import logger
from tenacity import RetryError
from tqdm import tqdm

from syntheca.clients.base import BaseClient
from syntheca.config import settings, ut_profile
from syntheca.models.openalex import Work
from syntheca.processing.cleaning import normalize_single_doi
from syntheca.utils.persistence import (
    append_to_parquet,
    init_incremental_parquet,
    save_dataframe_parquet,
)
from syntheca.utils.polars_frames import robust_from_dicts
from syntheca.utils.progress import get_next_position


class OpenAlexClient(BaseClient):
    """Client for querying the OpenAlex API and returning typed models."""

    BASE = settings.openalex_base_url
    PER_PAGE = 50
    PERSIST_EVERY = 1000
    _BATCH_RETRY_DELAYS = (1.0,)
    _SINGLE_ID_RETRY_DELAYS = (1.0, 2.0)

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

    @staticmethod
    def _normalize_work_id(value: str) -> str:
        """Return a normalized identifier value safe for OpenAlex filter queries."""
        return str(value).replace("doi:", "")

    @classmethod
    def _build_filter_value(cls, batch: list[str]) -> str:
        """Return the OpenAlex filter payload for an ID batch."""
        return "|".join(cls._normalize_work_id(item) for item in batch)

    async def _fetch_works_batch(self, batch: list[str], id_type_param: str) -> list[dict]:
        """Fetch a single OpenAlex batch and return raw result items."""
        params = {
            "filter": f"{id_type_param}:{self._build_filter_value(batch)}",
            "per-page": max(1, min(self.PER_PAGE, len(batch))),
        }
        resp = await self.request("GET", f"{self.BASE}/works", params=params)
        data = resp.json()
        items = data.get("results") or []
        if not isinstance(items, list):
            raise ValueError("OpenAlex response payload did not contain a list under 'results'")
        return items

    async def _sleep_before_retry(self, delay_seconds: float) -> None:
        """Sleep before a resilient retry attempt."""
        await asyncio.sleep(delay_seconds)

    async def _fetch_works_batch_resilient(
        self,
        batch: list[str],
        id_type_param: str,
    ) -> list[dict]:
        """Fetch an OpenAlex batch with retry, split, and skip fallbacks.

        Strategy:
        1. Try the batch as-is.
        2. Retry after a brief wait.
        3. If a multi-ID batch still fails, split it into smaller batches.
        4. If a single-ID request still fails after retries, skip that ID.

        This keeps retrieval progressing even when one request or one identifier
        causes the OpenAlex API to reject a larger batch.
        """
        retry_delays = self._SINGLE_ID_RETRY_DELAYS if len(batch) == 1 else self._BATCH_RETRY_DELAYS
        last_exc: Exception | None = None
        total_attempts = len(retry_delays) + 1

        for attempt in range(1, total_attempts + 1):
            if attempt > 1:
                delay = retry_delays[attempt - 2]
                logger.warning(
                    "Retrying OpenAlex batch of {} {} identifier(s) in {}s after failure: {}",
                    len(batch),
                    id_type_param,
                    delay,
                    last_exc,
                )
                await self._sleep_before_retry(delay)
            try:
                return await self._fetch_works_batch(batch, id_type_param)
            except (httpx.HTTPError, RetryError, ValueError) as exc:
                last_exc = exc
                logger.warning(
                    "OpenAlex batch request failed for {} {} identifier(s) on attempt {}/{}: {}",
                    len(batch),
                    id_type_param,
                    attempt,
                    total_attempts,
                    exc,
                )

        if len(batch) > 1:
            split_size = max(1, len(batch) // 2)
            logger.warning(
                "Splitting failing OpenAlex batch of {} {} identifier(s) into chunks of {}",
                len(batch),
                id_type_param,
                split_size,
            )
            recovered_items: list[dict] = []
            for subbatch in self._chunks(batch, split_size):
                recovered_items.extend(
                    await self._fetch_works_batch_resilient(list(subbatch), id_type_param)
                )
            return recovered_items

        logger.error(
            "Skipping OpenAlex {} identifier '{}' after {} failed attempt(s): {}",
            id_type_param,
            batch[0],
            total_attempts,
            last_exc,
        )
        return []

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
        if id_type_param == "doi":
            ids = [i for i in [normalize_single_doi(i) for i in ids] if i]
        results: list[Work] = []
        _pending: list[dict] = []  # rows waiting to be flushed to disk
        _persist_inited = False
        bar = None
        if settings.enable_progress:
            pos = position if position is not None else get_next_position()
            bar = tqdm(total=len(ids), desc="openalex:ids", position=pos, unit="work")

        def _flush_pending() -> None:
            nonlocal _persist_inited
            if not _pending:
                return
            try:
                chunk_df = robust_from_dicts(_pending)
                if not _persist_inited:
                    init_incremental_parquet("openalex_works", chunk_df)
                    _persist_inited = True
                else:
                    append_to_parquet("openalex_works", chunk_df)
            except Exception as exc:
                logger.warning("Failed to persist OpenAlex chunk ({} rows): {}", len(_pending), exc)
            _pending.clear()

        for batch in self._chunks(ids, self.PER_PAGE):
            items = await self._fetch_works_batch_resilient(list(batch), id_type_param)
            for it in items:
                try:
                    results.append(Work.from_dict(it))
                    _pending.append(dataclasses.asdict(results[-1]))
                except Exception:
                    continue
            if bar is not None:
                bar.update(len(batch))
            if settings.persist_intermediate and len(_pending) >= self.PERSIST_EVERY:
                _flush_pending()
        if bar is not None:
            bar.close()
        # Final flush for any remaining rows
        if settings.persist_intermediate:
            _flush_pending()
        return results

    async def get_works_by_title(self, title: str) -> list[Work]:
        """Search OpenAlex for works matching the given title."""
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
                if not isinstance(resp, httpx.Response):
                    continue
                try:
                    work_data = resp.json()
                    results.append(Work.from_dict(work_data))
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
                df = robust_from_dicts([dataclasses.asdict(w) for w in results])
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

        .. note::
            This method is **not** called automatically by ``Pipeline.run()``.
            The pipeline converts raw API responses to typed ``Work`` dataclasses
            via ``dacite.from_dict`` and uses those directly.  This method exists
            as a convenience for ad-hoc scripts or notebooks that need a flat,
            simplified dict representation of OpenAlex records (e.g., for export
            to CSV/Excel without the full dataclass schema).

        Args:
            works (list[dict]): A list of OpenAlex work result dictionaries.

        Returns:
            list[dict]: A list of simplified, normalized work dictionaries.

        """
        cleaned = []
        utwente_oa_id = ut_profile.openalex_institution_id
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

    # ------------------------------------------------------------------
    # Citing-works retrieval (cursor-paginated)
    # ------------------------------------------------------------------

    async def get_citing_works(
        self,
        openalex_id: str,
        *,
        per_page: int = 50,
        max_pages: int = 200,
    ) -> list[dict]:
        """Fetch all works that cite the given OpenAlex work ID.

        Uses the ``filter=cites:{id}`` parameter with cursor-based
        pagination to retrieve the full list of citing works.

        Args:
            openalex_id: Full OpenAlex work URL or short ID (e.g. ``W123``).
            per_page: Results per page (max 200 per OpenAlex docs).
            max_pages: Safety cap to avoid runaway loops.

        Returns:
            A flat list of raw OpenAlex work dicts.
        """
        url = f"{self.BASE}/works"
        params: dict[str, str | int] = {
            "filter": f"cites:{openalex_id}",
            "per-page": min(per_page, 200),
            "cursor": "*",
        }

        all_results: list[dict] = []
        for _ in range(max_pages):
            resp = await self.request("GET", url, params=params)
            data = resp.json()
            results = data.get("results") or []
            all_results.extend(results)

            meta = data.get("meta", {})
            next_cursor = meta.get("next_cursor")
            if not next_cursor or not results:
                break
            params["cursor"] = next_cursor

        return all_results
