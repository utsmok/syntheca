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

        # Clean publications
        if oils_df is None and pure_client is not None:
            # Run a minimal ingestion of 'publications' collection
            raw = await pure_client.get_all_records(["publications"])
            oils_df = pl.from_dicts(raw.get("publications", []))
        oils_clean = cleaning.clean_publications(oils_df) if oils_df is not None else pl.DataFrame()
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

        full_clean = cleaning.clean_publications(full_df) if full_df is not None else pl.DataFrame()
        if settings.persist_intermediate and full_clean is not None and full_clean.height:
            try:
                from syntheca.utils.persistence import save_dataframe_parquet

                save_dataframe_parquet(full_clean, "full_clean")
            except Exception:
                pass

        # Enrich authors
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

        if authors_df is not None:
            _authors_enriched = enrichment.enrich_authors_with_faculties(authors_df)
            if settings.persist_intermediate:
                try:
                    from syntheca.utils.persistence import save_dataframe_parquet

                    save_dataframe_parquet(authors_df, "authors_enriched")
                except Exception:
                    pass

        if not full_clean.height:
            # Nothing to merge; return oils_clean
            merged = oils_clean
        else:
            merged = merging.merge_datasets(oils_clean, full_clean)

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
