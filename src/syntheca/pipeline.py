"""Pipeline orchestrator for the Syntheca ETL processing.

This module exposes `Pipeline`, a small async orchestrator that wires data
ingestion, processing and reporting helpers together into a single `run`
convenience function designed for easy testing and scripted execution.
"""

from __future__ import annotations

import dataclasses
import pathlib

import polars as pl
from tqdm import tqdm

from syntheca.clients.openalex import OpenAlexClient
from syntheca.clients.pure_oai import PureOAIClient
from syntheca.clients.ut_people import UTPeopleClient
from syntheca.config import settings
from syntheca.processing import cleaning, enrichment, merging
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
        oils_df: pl.DataFrame | None = None,
        full_df: pl.DataFrame | None = None,
        authors_df: pl.DataFrame | None = None,
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
        1. Ingest publications (from provided `oils_df` or via `pure_client`).
        2. Clean and normalize publication records.
        3. Optionally fetch and clean OpenAlex work data when `openalex_client`
           and `openalex_ids` are provided.
        4. Enrich authors with faculty/org details using `ut_people_client`.
        5. Merge the cleaned datasets and deduplicate the final set.
        6. Optionally write out to parquet and xlsx if `output_dir` is provided.

        Args:
            oils_df (pl.DataFrame | None): Polars DataFrame of Pure OAI publications.
            full_df (pl.DataFrame | None): Polars DataFrame for OpenAlex/other works.
            authors_df (pl.DataFrame | None): Polars DataFrame of author/person records.
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
        # output frame placeholder not used here — return merged_final

        if oils_df is None and full_df is None and pure_client is None and openalex_client is None:
            raise ValueError("At least one of oils_df or full_df must be provided")

        # Clean publications with defensive empty handling
        if oils_df is None and pure_client is not None:
            # Run a minimal ingestion of 'publications' collection
            raw = await pure_client.get_all_records(["publications"])
            oils_df = pl.from_dicts(raw.get("publications", []))
        
        oils_clean = (
            cleaning.clean_publications(oils_df)
            if oils_df is not None and not oils_df.is_empty()
            else pl.DataFrame(schema={"doi": pl.Utf8, "title": pl.Utf8, "pure_id": pl.Utf8})
        )
        if settings.persist_intermediate and oils_clean is not None and oils_clean.height:
            try:
                from syntheca.utils.persistence import save_dataframe_parquet

                save_dataframe_parquet(oils_clean, "oils_clean")
            except Exception:
                pass

        # If full_df is missing and we have an OpenAlex client, optionally fetch via IDs
        if full_df is None and openalex_client is not None and openalex_ids:
            # pass a position to the progress bar so it doesn't overwrite other bars
            pos = get_next_position()
            works = await openalex_client.get_works_by_ids(openalex_ids, position=pos)
            # Convert dataclass instances to dicts where possible
            rows = []
            for w in works:
                try:
                    rows.append(dataclasses.asdict(w))
                except Exception:
                    # Fallback: try attribute access
                    rows.append(
                        {
                            "id": getattr(w, "id", None),
                            "doi": getattr(w, "doi", None),
                            "display_name": getattr(w, "display_name", None),
                            "publication_year": getattr(w, "publication_year", None),
                        }
                    )
            full_df = pl.from_dicts(rows) if rows else pl.DataFrame()

        full_clean = (
            cleaning.clean_publications(full_df)
            if full_df is not None and not full_df.is_empty()
            else pl.DataFrame(schema={"doi": pl.Utf8, "title": pl.Utf8, "pure_id": pl.Utf8})
        )
        if settings.persist_intermediate and full_clean is not None and full_clean.height:
            try:
                from syntheca.utils.persistence import save_dataframe_parquet

                save_dataframe_parquet(full_clean, "full_clean")
            except Exception:
                pass

        # Extract author and funder names from nested structures (New Step)
        if oils_clean.height > 0:
            oils_clean = merging.extract_author_and_funder_names(oils_clean)
        if full_clean.height > 0:
            full_clean = merging.extract_author_and_funder_names(full_clean)

        # Enrich authors
        # Build or append people_search_names by extracting names from `authors_df` when available.
        ut_affil_id = "491145c6-1c9b-4338-aedd-98315c166d7e"
        print(authors_df)
        if authors_df is not None:
            print('checking for people search names from authors_df')
            try:
                df_persons = authors_df
                # Try to filter to UT authors if possible
                if "is_ut" in df_persons.columns:
                    df_persons = df_persons.filter(pl.col("is_ut"))
                elif "affiliation_ids_pure" in df_persons.columns:
                    try:
                        df_persons = df_persons.filter(pl.col("affiliation_ids_pure").list.contains(ut_affil_id))
                    except Exception:
                        # could be missing or different format; skip filtering
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
                    built_names = [r.get("found_name") for r in df_persons.select("found_name").to_dicts() if r.get("found_name")]
                if built_names:
                    # Append to existing list and keep unique order
                    existing = people_search_names or []
                    people_search_names = list(dict.fromkeys(existing + built_names))
            except Exception as e:
                # Don't halt the pipeline on extraction errors; leave people_search_names unchanged
                print(f' error: {e}')
                pass

        # If authors_df missing and we have a UT People client, optionally search by provided names
        if authors_df is None and ut_people_client is not None and people_search_names:
            candidates = []
            iterable = (
                tqdm(
                    people_search_names,
                    desc="ut-people",
                    disable=not settings.enable_progress,
                    position=get_next_position(),
                )
                if settings.enable_progress
                else people_search_names
            )
            for name in iterable:
                try:
                    res = await ut_people_client.search_person(name)
                    candidates.extend(res or [])
                except Exception:
                    continue
            authors_df = pl.from_dicts(candidates) if candidates else pl.DataFrame()

        if authors_df is not None and authors_df.height > 0:
            # Enrich authors with faculty mappings
            authors_enriched = enrichment.enrich_authors_with_faculties(authors_df)
            
            # Apply manual affiliation corrections (New Step)
            authors_enriched = merging.add_missing_affils(authors_enriched)
            
            # Join authors back to publications (New Step)
            if oils_clean.height > 0:
                oils_clean = enrichment.join_authors_and_publications(authors_enriched, oils_clean)
            if full_clean.height > 0:
                full_clean = enrichment.join_authors_and_publications(authors_enriched, full_clean)
            
            if settings.persist_intermediate:
                try:
                    from syntheca.utils.persistence import save_dataframe_parquet

                    save_dataframe_parquet(authors_enriched, "authors_enriched")
                except Exception:
                    pass

        # Merge datasets using appropriate merge strategy
        if full_clean.height > 0:
            if oils_clean.height > 0:
                # Use the OILS specialized merge when we have both datasets
                merged = merging.merge_oils_with_all(oils_clean, full_clean)
            else:
                merged = full_clean
        else:
            merged = oils_clean

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
