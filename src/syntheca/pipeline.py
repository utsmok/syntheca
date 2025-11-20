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
from syntheca.processing import cleaning, enrichment, merging, organizations
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
        orgs_df: pl.DataFrame | None = None,
        output_dir: pathlib.Path | str | None = None,
        *,
        pure_client: PureOAIClient | None = None,
        openalex_client: OpenAlexClient | None = None,
        ut_people_client: UTPeopleClient | None = None,
        openalex_ids: list[str] | None = None,
        people_search_names: list[str] | None = None,
        enable_scraping: bool = False,
    ) -> pl.DataFrame:
        """Execute ETL steps and optionally export the results.

        The pipeline executes the following steps in order:
        1. Ingest publications, persons, and organizations (from provided DataFrames or via `pure_client`).
        2. Process organizational hierarchy and resolve parent relationships.
        3. Map author affiliations to organizations and set faculty flags.
        4. Optionally scrape UT People profiles to enrich author data with detailed org info.
        5. Parse scraped organizational details and apply manual corrections.
        6. Clean and normalize publication records.
        7. Optionally fetch and clean OpenAlex work data when `openalex_client` and `openalex_ids` are provided.
        8. Join authors and publications to aggregate faculty/org data at publication level.
        9. Merge the cleaned datasets and deduplicate the final set.
        10. Optionally write out to parquet and xlsx if `output_dir` is provided.

        Args:
            oils_df (pl.DataFrame | None): Polars DataFrame of Pure OAI publications.
            full_df (pl.DataFrame | None): Polars DataFrame for OpenAlex/other works.
            authors_df (pl.DataFrame | None): Polars DataFrame of author/person records.
            orgs_df (pl.DataFrame | None): Polars DataFrame of organization records.
            output_dir (pathlib.Path | str | None): Optional directory path to write
                parquet and Excel exports.
            pure_client (PureOAIClient | None): Optional Pure OAI client to fetch data.
            openalex_client (OpenAlexClient | None): Optional OpenAlex client to fetch works.
            ut_people_client (UTPeopleClient | None): Optional UT People client to search/enrich people.
            openalex_ids (list[str] | None): Optional list of OpenAlex/DOI IDs to fetch.
            people_search_names (list[str] | None): Optional list of person search names.
            enable_scraping (bool): Whether to enable scraping of UT People profiles. Default False.

        Returns:
            pl.DataFrame: The merged and deduplicated DataFrame representing final publications.

        """
        # output frame placeholder not used here — return merged_final

        if oils_df is None and full_df is None and pure_client is None and openalex_client is None:
            raise ValueError("At least one of oils_df or full_df must be provided")

        # Step 1: Ingest Pure data (publications, persons, organizations)
        if oils_df is None and pure_client is not None:
            # Fetch all three collections if available
            collections_to_fetch = ["publications"]
            if authors_df is None:
                collections_to_fetch.append("persons")
            if orgs_df is None:
                collections_to_fetch.append("organisationalUnits")

            raw = await pure_client.get_all_records(collections_to_fetch)
            oils_df = pl.from_dicts(raw.get("publications", []))

            if authors_df is None and "persons" in raw:
                authors_df = pl.from_dicts(raw.get("persons", []))

            if orgs_df is None and "organisationalUnits" in raw:
                orgs_df = pl.from_dicts(raw.get("organisationalUnits", []))

        # Step 2: Process organizational hierarchy
        orgs_processed = None
        if orgs_df is not None and orgs_df.height:
            orgs_processed = organizations.resolve_org_hierarchy(orgs_df)
            if settings.persist_intermediate:
                try:
                    from syntheca.utils.persistence import save_dataframe_parquet

                    save_dataframe_parquet(orgs_processed, "orgs_processed")
                except Exception:
                    pass

        # Step 3: Map author affiliations to organizations
        if authors_df is not None and orgs_processed is not None:
            authors_df = organizations.map_author_affiliations(authors_df, orgs_processed)
            if settings.persist_intermediate:
                try:
                    from syntheca.utils.persistence import save_dataframe_parquet

                    save_dataframe_parquet(authors_df, "authors_with_affils")
                except Exception:
                    pass

        # Clean publications

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

        # Step 4: Optionally scrape UT People profiles for detailed org info
        if enable_scraping and ut_people_client is not None and authors_df is not None:
            # Extract UT authors for scraping
            ut_affil_id = "491145c6-1c9b-4338-aedd-98315c166d7e"
            df_persons = authors_df

            # Try to filter to UT authors if possible
            if "is_ut" in df_persons.columns:
                df_persons = df_persons.filter(pl.col("is_ut"))
            elif "affiliation_ids_pure" in df_persons.columns:
                try:
                    df_persons = df_persons.filter(
                        pl.col("affiliation_ids_pure").list.contains(ut_affil_id)
                    )
                except Exception:
                    # could be missing or different format; skip filtering
                    df_persons = authors_df

            # Build search names from UT authors
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

            # Scrape profiles
            if built_names:
                scraped_data = []
                iterable = (
                    tqdm(
                        built_names,
                        desc="scraping-ut-people",
                        disable=not settings.enable_progress,
                        position=get_next_position(),
                    )
                    if settings.enable_progress
                    else built_names
                )
                for name in iterable:
                    try:
                        # Search for person
                        search_results = await ut_people_client.search_person(name)
                        if search_results:
                            # Get the first match's URL
                            url = (
                                search_results[0].get("people_page_url") if search_results else None
                            )
                            if url:
                                # Scrape the profile
                                profile = await ut_people_client.scrape_profile(url)
                                if profile and "org_details" in profile:
                                    scraped_data.append(
                                        {
                                            "name": name,
                                            "org_details_pp": profile["org_details"],
                                        }
                                    )
                    except Exception:
                        continue

                # Merge scraped data back into authors_df
                if scraped_data and "org_details_pp" not in authors_df.columns:
                    # For now, we just add the org_details_pp column if it doesn't exist
                    # A more sophisticated implementation would match and merge the data
                    authors_df = authors_df.with_columns(pl.lit(None).alias("org_details_pp"))

        # Step 5: Parse scraped org details and apply manual corrections
        if authors_df is not None:
            # Parse org details if available
            if "org_details_pp" in authors_df.columns:
                authors_df = enrichment.parse_scraped_org_details(authors_df)

            # Enrich with faculties from affiliation names
            authors_df = enrichment.enrich_authors_with_faculties(authors_df)

            if settings.persist_intermediate:
                try:
                    from syntheca.utils.persistence import save_dataframe_parquet

                    save_dataframe_parquet(authors_df, "authors_enriched")
                except Exception:
                    pass

        # Step 6: Join authors and publications to aggregate faculty/org data
        if authors_df is not None and oils_clean.height and "authors" in oils_clean.columns:
            # Apply manual corrections first
            oils_clean = enrichment.apply_manual_corrections(oils_clean)

            # Join authors and publications
            oils_clean = merging.join_authors_and_publications(oils_clean, authors_df)

            if settings.persist_intermediate:
                try:
                    from syntheca.utils.persistence import save_dataframe_parquet

                    save_dataframe_parquet(oils_clean, "oils_with_authors")
                except Exception:
                    pass

        # Step 7: Merge with OpenAlex data and deduplicate
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
