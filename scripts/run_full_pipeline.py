"""Example script to run a full end-to-end pipeline using the syntheca library.

This script demonstrates how to use the library to:
1. Retrieve data from Pure OAI-PMH collections
2. Clean and merge authors/orgs into publications
3. Enrich authors via UT People (RPC + profile scraping)
4. Lookup publications in OpenAlex by DOI
5. Run cleaning/enrichment/merging pipeline
6. Export the results to parquet and Excel

Use at your own risk; network calls are made and you must have an internet connection.
"""

from __future__ import annotations

import argparse
import asyncio
import pathlib
import sys

import polars as pl

from syntheca.clients.openalex import OpenAlexClient
from syntheca.clients.pure_oai import PureOAIClient
from syntheca.clients.ut_people import UTPeopleClient
from syntheca.pipeline import Pipeline
from syntheca.utils.logging import configure_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser("Run full syntheca pipeline scratch script")
    parser.add_argument("--output-dir", type=pathlib.Path, default=pathlib.Path("./output"))
    parser.add_argument(
        "--collections",
        nargs="+",
        default=[
            "openaire_cris_publications",
            "openaire_cris_persons",
            "openaire_cris_orgunits",
        ],
    )
    parser.add_argument(
        "--max-openalex", type=int, default=500, help="Max DOIs to fetch from OpenAlex (0=all)"
    )
    parser.add_argument(
        "--skip-people", action="store_true", help="Skip UT People enrichment calls"
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    outdir: pathlib.Path = pathlib.Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    configure_logging()

    # Instantiate clients
    async with (
        PureOAIClient() as pure_client,
        OpenAlexClient() as openalex_client,
        UTPeopleClient() as ut_client,
    ):
        print(
            "Retrieving data from Pure OAI-PMH concurrently: {}".format(",".join(args.collections))
        )
        # concurrently fetch all requested Pure OAI collections
        tasks = [pure_client.get_all_records([c]) for c in args.collections]
        results = await asyncio.gather(*tasks)
        raw = {}
        for r in results:
            if isinstance(r, dict):
                raw.update(r)

        publications = pl.from_dicts(raw.get("openaire_cris_publications", []) or [])
        persons = pl.from_dicts(raw.get("openaire_cris_persons", []) or [])
        orgs = pl.from_dicts(raw.get("openaire_cris_orgunits", []) or [])

        print(
            f"Loaded: publications={publications.height}, persons={persons.height}, orgs={orgs.height}"
        )

        # Build DOIs to fetch from OpenAlex
        doi_col = "doi"
        if doi_col in publications.columns:
            doi_series = (
                publications.select(doi_col)
                .with_columns(
                    pl.col(doi_col)
                    .str.replace("https://doi.org/", "")
                    .str.to_lowercase()
                    .str.strip_chars()
                )
                .to_series()
            )
            all_dois = [d for d in doi_series.unique().to_list() if d]
        else:
            all_dois = []

        if args.max_openalex and args.max_openalex > 0:
            openalex_ids = all_dois[: args.max_openalex]
        else:
            openalex_ids = all_dois

        # Setup people names to search (if persons DataFrame exists), use "first_names" + family name
        people_search_names: list[str] = []
        if not args.skip_people and (
            "first_names" in persons.columns and "family_names" in persons.columns
        ):
            # build unique search names, keep 'firstname lastname'
            people_search_names = [
                (f"{a['first_names']} {a['family_names']}".strip())
                for a in persons.select(["first_names", "family_names"]).to_dicts()
                if a.get("first_names") or a.get("family_names")
            ]
            people_search_names = list(dict.fromkeys(people_search_names))  # stable dedupe

        print("Starting pipeline run — this may take a while for large datasets")
        pipeline = Pipeline()
        merged = await pipeline.run(
            oils_df=publications,
            full_df=None,
            authors_df=persons,
            output_dir=outdir,
            pure_client=None,
            openalex_client=openalex_client,
            ut_people_client=(None if args.skip_people else ut_client),
            openalex_ids=openalex_ids if openalex_ids else None,
            people_search_names=people_search_names if people_search_names else None,
        )

        # Basic notification and write output (pipeline writes already, but ensure we save here too)
        print(f"Pipeline finished; result: rows={merged.height}, cols={len(merged.columns)}")
        # As an extra convenience write the same output with explicit functions
        parquet_path = outdir / "merged.explicit.parquet"
        xlsx_path = outdir / "merged.explicit.xlsx"
        from syntheca.reporting import export

        export.write_parquet(merged, parquet_path)
        export.write_formatted_excel(merged, xlsx_path)

        print("Exported to:")
        print(parquet_path)
        print(xlsx_path)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Aborted by user")
        sys.exit(1)
