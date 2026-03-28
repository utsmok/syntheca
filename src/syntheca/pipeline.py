"""Pipeline orchestrator for the Syntheca ETL processing.

This module exposes `Pipeline`, a small async orchestrator that wires data
ingestion, processing and reporting helpers together into a single `run`
convenience function designed for easy testing and scripted execution.
"""

from __future__ import annotations

import asyncio
import dataclasses
import pathlib
import time

import httpx
import polars as pl
from loguru import logger
from tenacity import RetryError
from tqdm import tqdm

from syntheca.clients.openaire import OpenAIREClient
from syntheca.clients.openalex import OpenAlexClient
from syntheca.clients.pure_oai import PureOAIClient, pure_publications_to_frame
from syntheca.clients.ut_people import UTPeopleClient
from syntheca.config import settings
from syntheca.config.output_contract import ensure_publication_contract
from syntheca.config.source_precedence import Source
from syntheca.models.canonical import CanonicalWork, canonicals_to_polars
from syntheca.processing import cleaning, enrichment, merging
from syntheca.processing.organizations import map_author_affiliations, resolve_org_hierarchy
from syntheca.processing.reconciliation import reconcile_works
from syntheca.providers.openaire_provider import OpenAIREProvider
from syntheca.reporting import export
from syntheca.utils.polars_frames import robust_from_dicts
from syntheca.utils.progress import get_next_position


class Pipeline:
    """Lightweight ETL pipeline orchestrator for Syntheca.

    This class provides a small, asynchronous orchestration layer that wires
    ingestion (optional clients), cleaning, enrichment, merging, and export
    stages together in a convenient `run` method.

    The pipeline is intentionally minimal and accepts prebuilt Polars DataFrames
    (to ease testing) but can also accept client instances to perform remote
    ingestion from OpenAlex, Pure OAI, and UT People. The `run` method returns
    the final merged `polars.DataFrame`.
    """

    def __init__(self) -> None:
        """Initialize a Pipeline instance.

        The constructor is intentionally lightweight; no state is kept on the
        instance. It exists primarily to provide a place for lifecycle
        management in the future.
        """
        pass

    async def run(
        self,
        pure_publications_df: pl.DataFrame | None = None,
        openalex_works_df: pl.DataFrame | None = None,
        authors_df: pl.DataFrame | None = None,
        org_units_df: pl.DataFrame | None = None,
        output_dir: pathlib.Path | str | None = None,
        *,
        orgunits_df: pl.DataFrame | None = None,
        allow_cached_orgunits_fallback: bool = False,
        pure_client: PureOAIClient | None = None,
        openalex_client: OpenAlexClient | None = None,
        openaire_client: OpenAIREClient | None = None,
        ut_people_client: UTPeopleClient | None = None,
        openalex_ids: list[str] | None = None,
        people_search_names: list[str] | None = None,
    ) -> pl.DataFrame:
        """Execute ETL steps and optionally export the results.

        The pipeline executes the following steps in order:
        1. Ingest publications (from provided `pure_publications_df` or via `pure_client`).
        2. Clean and normalize publication records.
        3. Optionally fetch and clean OpenAlex work data when `openalex_client`
           and `openalex_ids` are provided.
        4. Enrich authors with faculty/org details using `ut_people_client`.
        5. Merge the cleaned datasets and deduplicate the final set.
        6. Optionally write out to parquet and xlsx if `output_dir` is provided.

        Args:
            pure_publications_df (pl.DataFrame | None): Polars DataFrame of Pure OAI publications.
            openalex_works_df (pl.DataFrame | None): Polars DataFrame for OpenAlex works.
            authors_df (pl.DataFrame | None): Polars DataFrame of author/person records.
            org_units_df (pl.DataFrame | None): Polars DataFrame of organizational units.
                When provided, used for resolving author affiliation hierarchies.
            output_dir (pathlib.Path | str | None): Optional directory path to write
                parquet and Excel exports.
            orgunits_df (pl.DataFrame | None): Deprecated alias for ``org_units_df``.
                Use only for backward compatibility during the audit-remediation
                transition.
            allow_cached_orgunits_fallback (bool): When ``True``, explicitly allow
                loading cached ``openaire_cris_orgunits`` data if no org-unit
                DataFrame was supplied. The default ``False`` keeps org-unit
                behavior fully explicit.
            pure_client (PureOAIClient | None): Optional Pure OAI client to fetch data.
            openalex_client (OpenAlexClient | None): Optional OpenAlex client to fetch works.
            openaire_client (OpenAIREClient | None): Optional OpenAIRE Graph client
                used only for bounded runtime reconciliation supplements when
                unresolved merged fields would otherwise bypass later-wave
                correctness logic.
            ut_people_client (UTPeopleClient | None): Optional UT People client to search/enrich people.
            openalex_ids (list[str] | None): Optional list of OpenAlex/DOI IDs to fetch.
            people_search_names (list[str] | None): Optional list of person search names.

        Returns:
            pl.DataFrame: The merged and deduplicated DataFrame representing final publications.

        """
        if (
            pure_publications_df is None
            and openalex_works_df is None
            and pure_client is None
            and openalex_client is None
        ):
            raise ValueError(
                "At least one of pure_publications_df, openalex_works_df, "
                "pure_client, or openalex_client must be provided"
            )

        if org_units_df is not None and orgunits_df is not None:
            raise ValueError(
                "Use only one org-unit input parameter: prefer org_units_df over orgunits_df"
            )
        if org_units_df is None and orgunits_df is not None:
            logger.debug("Using deprecated Pipeline.run orgunits_df alias; prefer org_units_df")
            org_units_df = orgunits_df

        input_persons_df = authors_df
        input_org_units_df = org_units_df

        # Clean publications
        if pure_publications_df is None and pure_client is not None:
            raw = await pure_client.get_all_records(["openaire_cris_publications"])
            pure_publications_df = pure_publications_to_frame(
                raw.get("openaire_cris_publications", [])
            )
        pubs_clean = (
            cleaning.clean_publications(pure_publications_df)
            if pure_publications_df is not None
            else pl.DataFrame()
        )

        # If openalex_works_df is missing and we have an OpenAlex client, fetch via IDs
        if openalex_works_df is None and openalex_client is not None and openalex_ids:
            from syntheca.processing.cleaning import normalize_single_doi

            cached_oa: pl.DataFrame | None = None
            ids_to_fetch = list(openalex_ids)

            # Load cached works and compute remaining DOIs to fetch
            if settings.persist_intermediate:
                from syntheca.utils.persistence import (
                    load_parquet_all,
                    save_dataframe_parquet,
                )

                cached_oa = load_parquet_all("openalex_works")
                if cached_oa is not None and cached_oa.height:
                    cached_dois = set(
                        cached_oa.select(
                            pl.col("doi").str.to_lowercase().str.strip_chars()
                        )
                        .to_series()
                        .to_list()
                    )
                    cached_dois.discard(None)
                    cached_dois.discard("")
                    cached_dois = {d for d in cached_dois if d}

                    requested = {
                        nd
                        for i in openalex_ids
                        if (nd := normalize_single_doi(i))
                    }
                    missing = requested - cached_dois
                    ids_to_fetch = list(missing)

                    logger.info(
                        "Cache: {} works present, {} IDs requested, {} missing → fetching",
                        cached_oa.height,
                        len(requested),
                        len(ids_to_fetch),
                    )

            if ids_to_fetch:
                logger.info(
                    "Fetching {} OpenAlex IDs ({} cached works already present)",
                    len(ids_to_fetch),
                    cached_oa.height if cached_oa is not None else 0,
                )
                pos = get_next_position()
                _fetch_t0 = time.monotonic()
                try:
                    works = await openalex_client.get_works_by_ids(
                        ids_to_fetch, position=pos
                    )
                except Exception as exc:
                    logger.warning(
                        "OpenAlex retrieval failed after fallbacks; continuing pipeline with {} partial work(s): {}",
                        0,
                        exc,
                    )
                    works = []
                logger.info(
                    "Retrieved {} works from OpenAlex in {:.1f}s",
                    len(works),
                    time.monotonic() - _fetch_t0,
                )

                # Convert fetched works directly from the in-memory list.
                # The client already persisted chunks to disk for future
                # incremental runs — no need to reload them here (that
                # would load ALL previous chunks a second time).
                rows = []
                for w in works:
                    try:
                        rows.append(dataclasses.asdict(w))
                    except (TypeError, AttributeError) as exc:
                        logger.debug("Could not convert Work to dict: {}", exc)
                        rows.append(
                            {
                                "id": getattr(w, "id", None),
                                "doi": getattr(w, "doi", None),
                                "display_name": getattr(w, "display_name", None),
                                "publication_year": getattr(w, "publication_year", None),
                            }
                        )
                fetched_df = robust_from_dicts(rows) if rows else pl.DataFrame()
                logger.info(
                    "Converted {} fetched works to DataFrame ({} rows)",
                    len(works),
                    fetched_df.height,
                )
            else:
                fetched_df = pl.DataFrame()

            # Merge cached + freshly fetched into final dataframe
            if cached_oa is not None and cached_oa.height:
                openalex_works_df = pl.concat([cached_oa, fetched_df])
            elif fetched_df.height:
                openalex_works_df = fetched_df
            else:
                openalex_works_df = cached_oa if cached_oa is not None else pl.DataFrame()
            logger.info(
                "Merged OpenAlex data: {} total rows (cached={}, fetched={})",
                openalex_works_df.height if openalex_works_df is not None else 0,
                cached_oa.height if cached_oa is not None else 0,
                fetched_df.height,
            )

            # Persist the merged result so the cache grows each run.
            # The chunks ARE the cache now; load_parquet_all handles loading
            # from them.  No need to consolidate into a single file or clean
            # up chunks.
            if settings.persist_intermediate and openalex_works_df.height:
                logger.info(
                    "OpenAlex cache maintained via chunks ({} rows), skipping consolidated write",
                    openalex_works_df.height,
                )

        oa_clean = (
            cleaning.clean_publications(openalex_works_df)
            if openalex_works_df is not None
            else pl.DataFrame()
        )
        logger.info(
            "Cleaned OpenAlex data: {} rows", oa_clean.height if oa_clean is not None else 0
        )

        # -----------------------------------------------------------------
        # Canonical normalization step
        # Convert raw records to canonical form *alongside* the existing
        # DataFrames.  This does not replace downstream merge logic yet.
        # -----------------------------------------------------------------
        pure_canonical_works = _canonicalize_pure_rows(pubs_clean)
        openalex_canonical_works = _canonicalize_openalex_rows(oa_clean)
        canonical_works = [*pure_canonical_works, *openalex_canonical_works]
        if canonical_works:
            logger.info(
                "Canonical normalization produced {} work records ({} from Pure, {} from OpenAlex)",
                len(canonical_works),
                len(pure_canonical_works),
                len(openalex_canonical_works),
            )

        # Enrich authors
        # Build or append people_search_names by extracting names from `authors_df` when available.
        ut_affil_id = "491145c6-1c9b-4338-aedd-98315c166d7e"
        _authors_enriched: pl.DataFrame | None = None
        if authors_df is not None:
            logger.debug(
                "Extracting people search names from authors_df ({} rows)", authors_df.height
            )
            try:
                df_persons = authors_df
                # Try to filter to UT authors if possible
                if "is_ut" in df_persons.columns:
                    df_persons = df_persons.filter(pl.col("is_ut"))
                elif "affiliation_ids_pure" in df_persons.columns:
                    try:
                        df_persons = df_persons.filter(
                            pl.col("affiliation_ids_pure").list.contains(ut_affil_id)
                        )
                    except pl.exceptions.SchemaError, pl.exceptions.ComputeError:
                        df_persons = authors_df
                # Identify name columns and build full names
                built_names = []
                if "first_names" in df_persons.columns and "family_names" in df_persons.columns:
                    built_names = [
                        f"{r['first_names']} {r['family_names']}".strip()
                        for r in df_persons.select(["first_names", "family_names"]).to_dicts()
                        if r.get("first_names") or r.get("family_names")
                    ]
                elif "first_name" in df_persons.columns and "last_name" in df_persons.columns:
                    built_names = [
                        f"{r['first_name']} {r['last_name']}".strip()
                        for r in df_persons.select(["first_name", "last_name"]).to_dicts()
                        if r.get("first_name") or r.get("last_name")
                    ]
                elif "found_name" in df_persons.columns:
                    built_names = [
                        r.get("found_name")
                        for r in df_persons.select("found_name").to_dicts()
                        if r.get("found_name")
                    ]
                if built_names:
                    existing = people_search_names or []
                    people_search_names = list(dict.fromkeys(existing + built_names))
            except Exception as exc:
                logger.warning("Failed to extract people search names from authors_df: {}", exc)

        # UT People fallback: resolve unresolved UT authors via the
        # search_person → scrape_profile → parse chain.
        #
        # * When authors_df is ``None`` and names are available, perform a
        #   full search for every name (original behaviour).
        # * When authors_df exists but some UT authors lack affiliation data
        #   from Pure, run the chain *only* for those unresolved authors.
        if ut_people_client is not None and people_search_names:
            _need_search_names: list[str] = []
            if authors_df is None:
                # No author data at all - search everyone
                _need_search_names = list(people_search_names)
            else:
                # Identify UT authors whose Pure affiliation data is empty
                _has_affil_col = "affiliation_names_pure" in authors_df.columns
                for nm in people_search_names:
                    if _has_affil_col:
                        # Check if any row with this name already has filled-in affiliations
                        _name_rows = authors_df.filter(
                            pl.concat_str(
                                [
                                    pl.col(c)
                                    for c in ["first_names", "family_names"]
                                    if c in authors_df.columns
                                ],
                                separator=" ",
                            ).str.strip_chars()
                            == nm
                        )
                        if (
                            _name_rows.height == 0
                            or _name_rows["affiliation_names_pure"].list.len().sum() == 0
                        ):
                            _need_search_names.append(nm)
                    else:
                        _need_search_names.append(nm)

            if _need_search_names:
                cached_ut: pl.DataFrame | None = None
                names_to_fetch = list(_need_search_names)

                # Load cached UT People results and compute missing names
                if settings.persist_intermediate:
                    from syntheca.utils.persistence import (
                        load_dataframe_parquet,
                        save_dataframe_parquet,
                    )

                    cached_ut = load_dataframe_parquet("ut_people_results")
                    if cached_ut is not None and cached_ut.height:
                        cached_names = set(
                            cached_ut.select(
                                pl.col("found_name").str.to_lowercase().str.strip_chars()
                            )
                            .to_series()
                            .to_list()
                        )
                        cached_names.discard(None)
                        cached_names.discard("")
                        requested = {n.lower().strip() for n in _need_search_names}
                        missing = requested - cached_names
                        names_to_fetch = list(missing)

                        logger.info(
                            "UT People cache: {} results present, {} names requested, {} missing → fetching",
                            cached_ut.height,
                            len(requested),
                            len(names_to_fetch),
                        )

                candidates = []
                if names_to_fetch:
                    _ut_concurrency = 10
                    _ut_sem = asyncio.Semaphore(_ut_concurrency)
                    bar = (
                        tqdm(
                            names_to_fetch,
                            desc="ut-people",
                            disable=not settings.enable_progress,
                            position=get_next_position(),
                        )
                        if settings.enable_progress
                        else None
                    )

                    async def _resolve_one(name: str) -> dict | None:
                        try:
                            res = await ut_people_client.search_person(name)
                            if not res:
                                return None
                            best = res[0]
                            profile_url = best.get("people_page_url")
                            if profile_url:
                                org_details = await ut_people_client.scrape_profile(profile_url)
                                best["org_details_pp"] = org_details
                            return best
                        except (OSError, ValueError, KeyError, TypeError, httpx.HTTPStatusError, httpx.RequestError, RetryError) as exc:
                            logger.debug("UT People search failed for '{}': {}", name, exc)
                            return None
                        finally:
                            if bar is not None:
                                bar.update(1)

                    async def _bounded_resolve(name: str) -> dict | None:
                        async with _ut_sem:
                            return await _resolve_one(name)

                    results = await asyncio.gather(
                        *[_bounded_resolve(n) for n in names_to_fetch]
                    )
                    candidates = [r for r in results if r is not None]
                    if bar is not None:
                        bar.close()

                # Merge cached + freshly fetched
                fetched_df = robust_from_dicts(candidates) if candidates else pl.DataFrame()
                if cached_ut is not None and cached_ut.height:
                    pp_df = pl.concat([cached_ut, fetched_df])
                elif fetched_df.height:
                    pp_df = fetched_df
                else:
                    pp_df = cached_ut if cached_ut is not None else pl.DataFrame()

                # Persist the merged result so the cache grows each run
                if settings.persist_intermediate and pp_df.height:
                    save_dataframe_parquet(pp_df, "ut_people_results")

                if pp_df.height:
                    if authors_df is None:
                        authors_df = pp_df
                    else:
                        # Merge scraped enrichment back into the existing authors_df
                        # by joining on name, keeping new org_details_pp column
                        authors_df = _merge_ut_people_results(authors_df, pp_df)
                elif authors_df is None:
                    authors_df = pl.DataFrame()

        if authors_df is not None:
            # Enrich authors with scraped orgs -> parse org details
            _authors_enriched = enrichment.enrich_authors_with_faculties(authors_df)
            _authors_enriched = enrichment.parse_scraped_org_details(_authors_enriched)
            # Apply manual corrections from config
            _authors_enriched = enrichment.apply_manual_corrections(_authors_enriched)

            # Resolve org hierarchy and map affiliations.
            # Only use cached org-units when explicitly requested.
            _orgs_df = org_units_df
            if _orgs_df is None and allow_cached_orgunits_fallback:
                try:
                    from syntheca.utils.persistence import load_dataframe_parquet

                    _orgs_df = load_dataframe_parquet("openaire_cris_orgunits")
                    logger.debug(
                        "Loaded cached openaire_cris_orgunits ({} rows)",
                        _orgs_df.height if _orgs_df is not None else 0,
                    )
                except FileNotFoundError:
                    logger.info("No cached openaire_cris_orgunits found; skipping org mapping")
                    _orgs_df = pl.DataFrame()
                except Exception as exc:
                    logger.warning("Failed to load cached orgunits: {}", exc)
                    _orgs_df = pl.DataFrame()
            elif _orgs_df is None:
                logger.debug(
                    "No org_units_df supplied and cached org-unit fallback is disabled; skipping org mapping"
                )
                _orgs_df = pl.DataFrame()

            processed_orgs = (
                resolve_org_hierarchy(_orgs_df)
                if _orgs_df is not None and _orgs_df.height
                else pl.DataFrame()
            )
            if processed_orgs.height:
                _authors_enriched = map_author_affiliations(_authors_enriched, processed_orgs)
            # authors_enriched is persisted to the output dir by
            # _write_parity_support_artifacts — no separate cache write needed.

        # Optionally join author-level aggregated data to publications
        try:
            merged_with_authors = (
                merging.join_authors_and_publications(_authors_enriched, pubs_clean)
                if (
                    _authors_enriched is not None
                    and _authors_enriched.height
                    and pubs_clean is not None
                    and pubs_clean.height
                )
                else pubs_clean
            )
        except (
            KeyError,
            pl.exceptions.SchemaError,
            pl.exceptions.ComputeError,
            pl.exceptions.ColumnNotFoundError,
        ) as exc:
            logger.warning(
                "Author-publication join failed (pubs={} rows): {}",
                pubs_clean.height if pubs_clean is not None else 0,
                exc,
            )
            merged_with_authors = pubs_clean

        if not oa_clean.height:
            merged = merged_with_authors
        else:
            merged = merging.merge_datasets(merged_with_authors, oa_clean)

        # Deduplicate final set
        merged_final = ensure_publication_contract(merging.deduplicate(merged))

        reconciled_transition = await _build_reconciled_transition_output(
            legacy_merged=merged_final,
            pure_works=pure_canonical_works,
            openalex_works=openalex_canonical_works,
            openaire_client=openaire_client,
        )
        if reconciled_transition is not None and reconciled_transition.height:
            reconciled_transition = ensure_publication_contract(reconciled_transition)

        # Optionally write outputs
        if output_dir is not None:
            outdir = pathlib.Path(output_dir)
            outdir.mkdir(parents=True, exist_ok=True)
            _write_parity_support_artifacts(
                output_dir=outdir,
                pure_publications_clean=pubs_clean,
                pure_persons=input_persons_df,
                pure_org_units=input_org_units_df,
                openalex_works_clean=oa_clean,
                authors_enriched=_authors_enriched,
            )
            logger.info(
                "Writing merged.parquet ({} rows)", merged_final.height
            )
            parquet_path = outdir / "merged.parquet"
            _parquet_t0 = time.monotonic()
            export.write_parquet(merged_final, parquet_path)
            logger.info(
                "Saved merged.parquet in {:.1f}s",
                time.monotonic() - _parquet_t0,
            )
            xlsx_path = outdir / "merged.xlsx"
            logger.info(
                "Writing merged.xlsx ({} rows)", merged_final.height
            )
            _xlsx_t0 = time.monotonic()
            export.write_formatted_excel(merged_final, xlsx_path)
            logger.info(
                "Saved merged.xlsx in {:.1f}s",
                time.monotonic() - _xlsx_t0,
            )
            if reconciled_transition is not None and reconciled_transition.height:
                reconciled_parquet = outdir / "merged.reconciled.parquet"
                reconciled_xlsx = outdir / "merged.reconciled.xlsx"
                logger.info(
                    "Writing merged.reconciled.parquet ({} rows)",
                    reconciled_transition.height,
                )
                _rec_parquet_t0 = time.monotonic()
                export.write_parquet(reconciled_transition, reconciled_parquet)
                logger.info(
                    "Saved merged.reconciled.parquet in {:.1f}s",
                    time.monotonic() - _rec_parquet_t0,
                )
                logger.info(
                    "Writing merged.reconciled.xlsx ({} rows)",
                    reconciled_transition.height,
                )
                _rec_xlsx_t0 = time.monotonic()
                export.write_formatted_excel(reconciled_transition, reconciled_xlsx)
                logger.info(
                    "Saved merged.reconciled.xlsx in {:.1f}s",
                    time.monotonic() - _rec_xlsx_t0,
                )

        return merged_final


def _merge_ut_people_results(authors_df: pl.DataFrame, pp_df: pl.DataFrame) -> pl.DataFrame:
    """Merge scraped UT People data back into the existing authors DataFrame.

    For each row in *pp_df* with an ``org_details_pp`` value, look up the
    corresponding author by ``found_name`` and attach the scraped data.
    Columns present in *pp_df* but absent from *authors_df* are added.
    """
    if "found_name" not in pp_df.columns or "found_name" not in authors_df.columns:
        # Cannot join without a shared key; concatenate instead
        return pl.concat([authors_df, pp_df], how="diagonal_relaxed")

    # Only keep org_details_pp and found_name from pp_df to avoid column clashes
    keep_cols = ["found_name"]
    if "org_details_pp" in pp_df.columns:
        keep_cols.append("org_details_pp")
    pp_slim = pp_df.select(keep_cols).unique(subset=["found_name"])

    if "org_details_pp" not in authors_df.columns:
        authors_df = authors_df.with_columns(pl.lit(None).alias("org_details_pp"))

    merged = authors_df.join(
        pp_slim.rename({"org_details_pp": "_pp_org"}),
        on="found_name",
        how="left",
    )
    # Coalesce: prefer existing, fill with scraped
    merged = merged.with_columns(
        pl.coalesce(["org_details_pp", "_pp_org"]).alias("org_details_pp")
    ).drop("_pp_org")
    return merged


def _canonicalize_pure_rows(df: pl.DataFrame | None) -> list[CanonicalWork]:
    """Convert cleaned Pure publication rows to canonical work records."""
    if df is None or not df.height:
        return []

    from syntheca.models.adapters import pure_publication_to_canonical

    works: list[CanonicalWork] = []
    for row in df.to_dicts():
        try:
            works.append(pure_publication_to_canonical(row))
        except Exception as exc:
            logger.debug("Canonical conversion failed for Pure pub: {}", exc)
    return works


def _canonicalize_openalex_rows(df: pl.DataFrame | None) -> list[CanonicalWork]:
    """Convert cleaned OpenAlex rows to canonical work records."""
    if df is None or not df.height:
        return []

    from syntheca.models.adapters import openalex_work_to_canonical

    works: list[CanonicalWork] = []
    for row in df.to_dicts():
        try:
            works.append(openalex_work_to_canonical(row))
        except Exception as exc:
            logger.debug("Canonical conversion failed for OA work: {}", exc)
    return works


def _normalize_reconciliation_value(value: object) -> str | None:
    """Normalize DOI/title values for runtime reconciliation and overlay joins."""
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if text.startswith(prefix):
            text = text[len(prefix) :].strip()
            break
    return text or None


def _reconciliation_key_expr(*, doi_col: str = "doi", title_col: str = "title") -> pl.Expr:
    """Build a join key expression that prefers normalized DOI and falls back to title."""
    doi_expr = (
        pl.when(pl.col(doi_col).is_not_null())
        .then(
            pl.col(doi_col)
            .cast(pl.Utf8)
            .str.to_lowercase()
            .str.replace("https://doi.org/", "")
            .str.replace("http://doi.org/", "")
            .str.replace("doi:", "")
            .str.strip_chars()
        )
        .otherwise(None)
    )
    title_expr = (
        pl.when(pl.col(title_col).is_not_null())
        .then(pl.col(title_col).cast(pl.Utf8).str.to_lowercase().str.strip_chars())
        .otherwise(None)
    )
    return pl.coalesce([doi_expr, title_expr])


async def _fetch_openaire_supplemental_works(
    pure_works: list[CanonicalWork],
    openalex_works: list[CanonicalWork],
    openaire_client: OpenAIREClient | None,
) -> list[CanonicalWork]:
    """Fetch bounded OpenAIRE supplements for Pure DOI records not resolved by OpenAlex."""
    if openaire_client is None or not pure_works:
        return []

    openalex_dois = {
        normalized
        for work in openalex_works
        if (normalized := _normalize_reconciliation_value(work.doi)) is not None
    }

    provider = OpenAIREProvider(openaire_client)
    supplements: list[CanonicalWork] = []
    seen_dois: set[str] = set()

    for work in pure_works:
        doi = _normalize_reconciliation_value(work.doi)
        if doi is None or doi in openalex_dois or doi in seen_dois:
            continue
        seen_dois.add(doi)
        try:
            fetched = await provider.fetch("works", doi=doi)
        except Exception as exc:
            logger.debug("OpenAIRE supplement fetch failed for DOI {}: {}", doi, exc)
            continue

        exact_matches = [
            candidate
            for candidate in fetched
            if isinstance(candidate, CanonicalWork)
            and _normalize_reconciliation_value(candidate.doi) == doi
        ]
        if exact_matches:
            supplements.append(exact_matches[0])

    if supplements:
        logger.info(
            "Runtime reconciliation fetched {} bounded OpenAIRE work supplements",
            len(supplements),
        )
    return supplements


def _canonical_works_to_overlay_df(works: list[CanonicalWork]) -> pl.DataFrame:
    """Convert reconciled canonical works to a flat overlay DataFrame."""
    rows: list[dict[str, object]] = []
    for work in works:
        sources = sorted({assertion.source.value for assertion in work.provenance})
        rows.append(
            {
                "doi": work.doi,
                "title": work.title,
                "internal_repository_id": work.source_ids.get("pure"),
                "id": work.source_ids.get("openalex"),
                "publication_year": work.publication_year,
                "publication_date": work.publication_date,
                "type": work.type,
                "language": work.language,
                "is_oa": work.is_oa,
                "oa_color": work.oa_color,
                "cited_by_count": work.cited_by_count,
                "publisher": work.publisher,
                "primary_host_name": work.primary_host_name,
                "ut_is_corresponding": work.ut_is_corresponding,
                "access_right": work.access_right,
                "license": work.license,
                "abstract": work.abstract,
                "reconciled_sources": ",".join(sources),
                "reconciled_source_count": len(sources),
            }
        )
    return robust_from_dicts(rows) if rows else pl.DataFrame()


def _overlay_reconciled_work_fields(
    legacy_merged: pl.DataFrame,
    overlay_df: pl.DataFrame,
) -> pl.DataFrame:
    """Overlay reconciled canonical work fields onto the legacy merged export."""
    if overlay_df.is_empty():
        return legacy_merged

    overlay = (
        overlay_df.with_columns(_reconciliation_key_expr().alias("__reconcile_key"))
        .filter(pl.col("__reconcile_key").is_not_null())
        .unique(subset=["__reconcile_key"], keep="first")
    )
    if overlay.is_empty():
        return legacy_merged

    if legacy_merged.is_empty():
        return overlay.drop("__reconcile_key")

    legacy = legacy_merged.with_columns(_reconciliation_key_expr().alias("__reconcile_key"))
    renamed_overlay = overlay.rename(
        {
            column: f"{column}__reconciled"
            for column in overlay.columns
            if column != "__reconcile_key"
        }
    )
    joined = legacy.join(renamed_overlay, on="__reconcile_key", how="left")

    overlay_columns = [column for column in overlay.columns if column != "__reconcile_key"]
    update_exprs: list[pl.Expr] = []
    for column in overlay_columns:
        shadow = f"{column}__reconciled"
        if column in legacy_merged.columns:
            update_exprs.append(pl.coalesce([pl.col(shadow), pl.col(column)]).alias(column))
        else:
            update_exprs.append(pl.col(shadow).alias(column))

    return joined.with_columns(update_exprs).drop(
        "__reconcile_key",
        *[f"{column}__reconciled" for column in overlay_columns],
    )


async def _build_reconciled_transition_output(
    *,
    legacy_merged: pl.DataFrame,
    pure_works: list[CanonicalWork],
    openalex_works: list[CanonicalWork],
    openaire_client: OpenAIREClient | None,
) -> pl.DataFrame | None:
    """Build a bounded reconciled merged-output sidecar for transition review."""
    if not pure_works and not openalex_works:
        return None

    sources: dict[Source, list[CanonicalWork]] = {}
    if pure_works:
        sources[Source.PURE] = pure_works
    if openalex_works:
        sources[Source.OPENALEX] = openalex_works

    openaire_works = await _fetch_openaire_supplemental_works(
        pure_works=pure_works,
        openalex_works=openalex_works,
        openaire_client=openaire_client,
    )
    if openaire_works:
        sources[Source.OPENAIRE] = openaire_works

    reconciled_works, match_results, metrics = reconcile_works(sources)
    accepted_matches = sum(1 for result in match_results if result.accepted)
    logger.info(
        "Runtime reconciliation candidate produced {} merged works (accepted matches={}, unmatched={})",
        len(reconciled_works),
        accepted_matches,
        metrics.unmatched,
    )

    overlay_df = _canonical_works_to_overlay_df(reconciled_works)
    return _overlay_reconciled_work_fields(legacy_merged, overlay_df)


def _write_parity_support_artifacts(
    *,
    output_dir: pathlib.Path,
    pure_publications_clean: pl.DataFrame | None,
    pure_persons: pl.DataFrame | None,
    pure_org_units: pl.DataFrame | None,
    openalex_works_clean: pl.DataFrame | None,
    authors_enriched: pl.DataFrame | None,
) -> None:
    """Write parity-support artifacts alongside the main pipeline outputs."""
    artifacts: dict[str, pl.DataFrame | None] = {
        "pure_publications_clean.parquet": pure_publications_clean,
        "pure_persons.parquet": pure_persons,
        "pure_orgunits.parquet": pure_org_units,
        # openalex_works_clean is already persisted to the cache dir
        # via save_dataframe_parquet earlier in the pipeline; skip the
        # duplicate write here.
        "authors_enriched.parquet": authors_enriched,
    }

    for filename, df in artifacts.items():
        if df is None:
            continue
        logger.info("Writing parity artifact {} ({} rows)", filename, df.height)
        _artifact_t0 = time.monotonic()
        export.write_parquet(df, output_dir / filename)
        logger.info("Saved {} in {:.1f}s", filename, time.monotonic() - _artifact_t0)
