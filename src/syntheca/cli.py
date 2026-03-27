"""Supported command-line interface for Syntheca.

The CLI exposes one authoritative surface for supported workflows:

* ``syntheca run`` — execute the bounded ETL path and write core outputs.
* ``syntheca compare-scopus`` — compare a local Scopus/SciVal export with an
  internal Parquet output and emit the documented comparison artifacts.
"""

from __future__ import annotations

import argparse
import asyncio
import pathlib
from collections.abc import Sequence
from typing import Any, cast

import polars as pl

from syntheca.clients.openaire import OpenAIREClient
from syntheca.clients.openalex import OpenAlexClient
from syntheca.clients.pure_oai import PureOAIClient, pure_publications_to_frame
from syntheca.clients.ut_people import UTPeopleClient
from syntheca.comparison.scopus import ScopusComparison, ScopusExportReader
from syntheca.pipeline import Pipeline
from syntheca.reporting import export
from syntheca.reporting.parity import (
    assess_baseline_readiness,
    check_parity,
    compute_regression_metrics,
    load_baseline,
)
from syntheca.utils.logging import configure_logging
from syntheca.utils.polars_frames import robust_from_dicts

logger = __import__("loguru").logger

_BASELINE_PATH = pathlib.Path(__file__).resolve().parents[2] / "tests" / "regression_baseline.json"


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level CLI parser."""
    parser = argparse.ArgumentParser(prog="syntheca")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run the supported ETL pipeline")
    run_parser.add_argument("--output-dir", type=pathlib.Path, default=pathlib.Path("./output"))
    run_parser.add_argument(
        "--collections",
        nargs="+",
        default=[
            "openaire_cris_publications",
            "openaire_cris_persons",
            "openaire_cris_orgunits",
        ],
    )
    run_parser.add_argument(
        "--max-openalex", type=int, default=0, help="Max DOIs to fetch from OpenAlex (0=all)"
    )
    run_parser.add_argument(
        "--skip-people", action="store_true", help="Skip UT People enrichment calls"
    )
    run_parser.add_argument(
        "--skip-openaire",
        action="store_true",
        help="Disable bounded OpenAIRE reconciliation supplements",
    )
    run_parser.add_argument(
        "--check-parity",
        action="store_true",
        help="Run regression-metric comparison against the committed baseline after export",
    )

    compare_parser = subparsers.add_parser(
        "compare-scopus",
        help="Compare a local Scopus/SciVal export with an internal Parquet output",
    )
    compare_parser.add_argument("export_path", type=pathlib.Path)
    compare_parser.add_argument("--internal-parquet", required=True, type=pathlib.Path)
    compare_parser.add_argument("--output-dir", type=pathlib.Path, default=pathlib.Path("./output"))

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Syntheca CLI and return a process-style exit code."""
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    configure_logging()

    if args.command == "run":
        asyncio.run(_run_pipeline_command(args))
        return 0
    if args.command == "compare-scopus":
        _run_compare_scopus_command(args)
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2


async def _run_pipeline_command(args: argparse.Namespace) -> None:
    """Execute the supported ETL pipeline command."""
    outdir = pathlib.Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    async with (
        PureOAIClient() as pure_client_base,
        OpenAlexClient() as openalex_client_base,
        OpenAIREClient() as openaire_client_base,
        UTPeopleClient() as ut_client_base,
    ):
        pure_client = cast(PureOAIClient, pure_client_base)
        openalex_client = cast(OpenAlexClient, openalex_client_base)
        openaire_client = cast(OpenAIREClient, openaire_client_base)
        ut_client = cast(UTPeopleClient, ut_client_base)
        logger.info(
            "Retrieving data from Pure OAI-PMH concurrently: {}", ",".join(args.collections)
        )
        results = await asyncio.gather(
            *[pure_client.get_all_records([c]) for c in args.collections]
        )

        raw: dict[str, Any] = {}
        for result in results:
            if isinstance(result, dict):
                raw.update(result)

        publications = pure_publications_to_frame(
            cast(list[dict[str, Any]], raw.get("openaire_cris_publications") or [])
        )
        persons = robust_from_dicts(
            cast(list[dict[str, Any]], raw.get("openaire_cris_persons") or [])
        )
        org_units = robust_from_dicts(
            cast(list[dict[str, Any]], raw.get("openaire_cris_orgunits") or [])
        )

        logger.info(
            "Loaded: publications={}, persons={}, orgs={}",
            publications.height,
            persons.height,
            org_units.height,
        )

        openalex_ids = _extract_openalex_ids(publications, args.max_openalex)
        people_search_names = _extract_people_search_names(persons, args.skip_people)

        merged = await Pipeline().run(
            pure_publications_df=publications,
            openalex_works_df=None,
            authors_df=persons,
            org_units_df=org_units,
            output_dir=outdir,
            pure_client=None,
            openalex_client=openalex_client,
            openaire_client=(None if args.skip_openaire else openaire_client),
            ut_people_client=(None if args.skip_people else ut_client),
            openalex_ids=openalex_ids if openalex_ids else None,
            people_search_names=people_search_names if people_search_names else None,
        )

        explicit_parquet = outdir / "merged.explicit.parquet"
        explicit_xlsx = outdir / "merged.explicit.xlsx"
        export.write_parquet(merged, explicit_parquet)
        export.write_formatted_excel(merged, explicit_xlsx)
        logger.info(
            "Pipeline finished; result: rows={}, cols={}", merged.height, len(merged.columns)
        )

    if args.check_parity:
        _run_parity_check(outdir)


def _run_compare_scopus_command(args: argparse.Namespace) -> None:
    """Execute the export-first Scopus/SciVal comparison command."""
    outdir = pathlib.Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    scopus_df = ScopusExportReader.read_export(args.export_path)
    internal_df = pl.read_parquet(args.internal_parquet)
    result = ScopusComparison.compare(scopus_df, internal_df)
    written = export.save_comparison_result(result, outdir)
    logger.info("Wrote {} comparison artifacts to {}", len(written), outdir / "comparison")


def _extract_openalex_ids(publications: pl.DataFrame, max_openalex: int) -> list[str]:
    """Return normalized DOI identifiers for OpenAlex enrichment."""
    if "doi" not in publications.columns:
        return []

    doi_series = (
        publications.select("doi")
        .with_columns(
            pl.col("doi").str.replace("https://doi.org/", "").str.to_lowercase().str.strip_chars()
        )
        .to_series()
    )
    all_dois = [doi for doi in doi_series.unique().to_list() if doi]
    if max_openalex and max_openalex > 0:
        return all_dois[:max_openalex]
    return all_dois


def _extract_people_search_names(persons: pl.DataFrame, skip_people: bool) -> list[str]:
    """Return deduplicated person names for UT People fallback enrichment."""
    if skip_people or "first_names" not in persons.columns or "family_names" not in persons.columns:
        return []

    names = [
        f"{row['first_names']} {row['family_names']}".strip()
        for row in persons.select(["first_names", "family_names"]).to_dicts()
        if row.get("first_names") or row.get("family_names")
    ]
    return list(dict.fromkeys(names))


def _run_parity_check(output_dir: pathlib.Path) -> None:
    """Run regression-metric comparison against the committed baseline."""
    if not _BASELINE_PATH.exists():
        logger.warning("Regression baseline not found at {}; skipping parity check", _BASELINE_PATH)
        return

    logger.info("Computing regression metrics from {}", output_dir)
    current = compute_regression_metrics(output_dir)
    baseline = load_baseline(_BASELINE_PATH)
    readiness = assess_baseline_readiness(_BASELINE_PATH)

    if readiness.release_ready:
        logger.info("Regression baseline is release-ready")
    else:
        preview = ", ".join(readiness.missing_metrics[:5]) or "none"
        if len(readiness.missing_metrics) > 5:
            preview += ", ..."
        logger.warning(
            "Regression baseline status '{}' is not release-ready; parity results are informational only. {} Missing baselines: {}",
            readiness.status,
            readiness.claim_rule,
            preview,
        )

    results = check_parity(current, baseline)
    passed = sum(1 for ok in results.values() if ok)
    failed = sum(1 for ok in results.values() if not ok)
    logger.info("Parity check: {}/{} metrics passed", passed, passed + failed)

    for metric, ok in sorted(results.items()):
        status = "PASS" if ok else "FAIL"
        logger.info(
            "  [{}] {} — current={}, baseline={}",
            status,
            metric,
            current.get(metric),
            baseline.get(metric),
        )

    if failed:
        logger.warning("{} parity checks failed — review output before publishing", failed)
