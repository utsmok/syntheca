from __future__ import annotations

import dataclasses
import pathlib

import polars as pl

from syntheca.clients.openalex import OpenAlexClient
from syntheca.clients.pure_oai import PureOAIClient
from syntheca.clients.ut_people import UTPeopleClient
from syntheca.processing import cleaning, enrichment, merging
from syntheca.reporting import export


class Pipeline:
    """A lightweight async orchestrator for the syntheca ETL.

    The pipeline accepts DataFrames as inputs for testability but can also be
    extended to perform client-based ingestion when supplied with client instances.
    """

    def __init__(self) -> None:
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
        """Run the ETL pipeline.

        Steps (simple proof-of-concept implementation):
        1. Normalize DOIs and clean publications
        2. Enrich authors with faculties
        3. Merge datasets and deduplicate
        4. Write results to output directory if provided
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

        # If full_df is missing and we have an OpenAlex client, optionally fetch via IDs
        if full_df is None and openalex_client is not None and openalex_ids:
            works = await openalex_client.get_works_by_ids(openalex_ids)
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

        # Enrich authors
        # If authors_df missing and we have a UT People client, optionally search by provided names
        if authors_df is None and ut_people_client is not None and people_search_names:
            candidates = []
            for name in people_search_names:
                try:
                    res = await ut_people_client.search_person(name)
                    candidates.extend(res or [])
                except Exception:
                    continue
            authors_df = pl.from_dicts(candidates) if candidates else pl.DataFrame()

        if authors_df is not None:
            _authors_enriched = enrichment.enrich_authors_with_faculties(authors_df)

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
