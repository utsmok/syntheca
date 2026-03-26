"""Pipeline orchestrator for the Syntheca ETL processing.

This module exposes `Pipeline`, a small async orchestrator that wires data
ingestion, processing and reporting helpers together into a single `run`
convenience function designed for easy testing and scripted execution.
"""

from __future__ import annotations

import dataclasses
import pathlib

import polars as pl
from loguru import logger
from tqdm import tqdm

from syntheca.clients.openalex import OpenAlexClient
from syntheca.clients.pure_oai import PureOAIClient
from syntheca.clients.ut_people import UTPeopleClient
from syntheca.config import settings
from syntheca.processing import cleaning, enrichment, merging
from syntheca.processing.organizations import map_author_affiliations, resolve_org_hierarchy
from syntheca.reporting import export
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
        orgunits_df: pl.DataFrame | None = None,
        output_dir: pathlib.Path | str | None = None,
        *,
        pure_client: PureOAIClient | None = None,
        openalex_client: OpenAlexClient | None = None,
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
            orgunits_df (pl.DataFrame | None): Polars DataFrame of organizational units.
                When provided, used for resolving author affiliation hierarchies.
                When ``None``, the pipeline will attempt to load a cached
                ``openaire_cris_orgunits`` parquet as a fallback.
            output_dir (pathlib.Path | str | None): Optional directory path to write
                parquet and Excel exports.
            pure_client (PureOAIClient | None): Optional Pure OAI client to fetch data.
            openalex_client (OpenAlexClient | None): Optional OpenAlex client to fetch works.
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

        # Clean publications
        if pure_publications_df is None and pure_client is not None:
            raw = await pure_client.get_all_records(["openaire_cris_publications"])
            pure_publications_df = pl.from_dicts(raw.get("openaire_cris_publications", []))
        pubs_clean = (
            cleaning.clean_publications(pure_publications_df)
            if pure_publications_df is not None
            else pl.DataFrame()
        )
        if settings.persist_intermediate and pubs_clean is not None and pubs_clean.height:
            try:
                from syntheca.utils.persistence import save_dataframe_parquet

                save_dataframe_parquet(pubs_clean, "pure_publications_clean")
            except Exception as exc:
                logger.warning(
                    "Failed to persist pure_publications_clean ({} rows): {}",
                    pubs_clean.height,
                    exc,
                )

        # If openalex_works_df is missing and we have an OpenAlex client, fetch via IDs
        if openalex_works_df is None and openalex_client is not None and openalex_ids:
            pos = get_next_position()
            works = await openalex_client.get_works_by_ids(openalex_ids, position=pos)
            rows = []
            for w in works:
                try:
                    rows.append(dataclasses.asdict(w))
                except (TypeError, AttributeError) as exc:
                    logger.debug("Could not convert Work to dict via dataclasses.asdict: {}", exc)
                    rows.append(
                        {
                            "id": getattr(w, "id", None),
                            "doi": getattr(w, "doi", None),
                            "display_name": getattr(w, "display_name", None),
                            "publication_year": getattr(w, "publication_year", None),
                        }
                    )
            openalex_works_df = pl.from_dicts(rows) if rows else pl.DataFrame()

        oa_clean = (
            cleaning.clean_publications(openalex_works_df)
            if openalex_works_df is not None
            else pl.DataFrame()
        )
        if settings.persist_intermediate and oa_clean is not None and oa_clean.height:
            try:
                from syntheca.utils.persistence import save_dataframe_parquet

                save_dataframe_parquet(oa_clean, "openalex_works_clean")
            except Exception as exc:
                logger.warning(
                    "Failed to persist openalex_works_clean ({} rows): {}",
                    oa_clean.height,
                    exc,
                )

        # -----------------------------------------------------------------
        # Canonical normalization step
        # Convert raw records to canonical form *alongside* the existing
        # DataFrames.  This does not replace downstream merge logic yet.
        # -----------------------------------------------------------------
        canonical_works: list = []
        try:
            from syntheca.models.adapters import (
                openalex_work_to_canonical,
                pure_publication_to_canonical,
            )
            from syntheca.models.canonical import canonicals_to_polars

            if pubs_clean is not None and pubs_clean.height:
                for row in pubs_clean.to_dicts():
                    try:
                        canonical_works.append(pure_publication_to_canonical(row))
                    except Exception as exc:
                        logger.debug("Canonical conversion failed for Pure pub: {}", exc)
            if oa_clean is not None and oa_clean.height:
                for row in oa_clean.to_dicts():
                    try:
                        canonical_works.append(openalex_work_to_canonical(row))
                    except Exception as exc:
                        logger.debug("Canonical conversion failed for OA work: {}", exc)
            if canonical_works:
                logger.info(
                    "Canonical normalization produced {} work records", len(canonical_works)
                )
                if settings.persist_intermediate:
                    try:
                        from syntheca.utils.persistence import save_dataframe_parquet

                        canonical_df = canonicals_to_polars(canonical_works)
                        save_dataframe_parquet(canonical_df, "canonical_works")
                    except Exception as exc:
                        logger.warning("Failed to persist canonical_works: {}", exc)
        except Exception as exc:
            logger.warning("Canonical normalization step failed: {}", exc)

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
                candidates = []
                iterable = (
                    tqdm(
                        _need_search_names,
                        desc="ut-people",
                        disable=not settings.enable_progress,
                        position=get_next_position(),
                    )
                    if settings.enable_progress
                    else _need_search_names
                )
                for name in iterable:
                    try:
                        res = await ut_people_client.search_person(name)
                        if not res:
                            continue
                        # Take best candidate (already ranked by Levenshtein)
                        best = res[0]
                        profile_url = best.get("people_page_url")
                        if profile_url:
                            org_details = await ut_people_client.scrape_profile(profile_url)
                            best["org_details_pp"] = org_details
                        candidates.append(best)
                    except (OSError, ValueError, KeyError, TypeError) as exc:
                        logger.debug("UT People search failed for '{}': {}", name, exc)
                        continue

                if candidates:
                    pp_df = pl.from_dicts(candidates)
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
            # Use explicitly provided orgunits_df; fall back to cached parquet.
            _orgs_df = orgunits_df
            if _orgs_df is None:
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

            processed_orgs = (
                resolve_org_hierarchy(_orgs_df)
                if _orgs_df is not None and _orgs_df.height
                else pl.DataFrame()
            )
            if processed_orgs.height:
                _authors_enriched = map_author_affiliations(_authors_enriched, processed_orgs)
            if settings.persist_intermediate:
                try:
                    from syntheca.utils.persistence import save_dataframe_parquet

                    save_dataframe_parquet(_authors_enriched, "authors_enriched")
                except Exception as exc:
                    logger.warning(
                        "Failed to persist authors_enriched ({} rows): {}",
                        _authors_enriched.height,
                        exc,
                    )

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
        merged_final = merging.deduplicate(merged)

        # Optionally write outputs
        if output_dir is not None:
            outdir = pathlib.Path(output_dir)
            outdir.mkdir(parents=True, exist_ok=True)
            parquet_path = outdir / "merged.parquet"
            xlsx_path = outdir / "merged.xlsx"
            export.write_parquet(merged_final, parquet_path)
            export.write_formatted_excel(merged_final, xlsx_path)

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
