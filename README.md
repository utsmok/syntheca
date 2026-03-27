# Syntheca

A modern ETL pipeline for retrieving, processing, and enriching institutional academic metadata.

## Requirements

- **Python 3.14** or newer
- **uv** for dependency management

## Installation

```bash
# Install dependencies
uv sync
```

## Running Syntheca today

Syntheca now has one authoritative supported CLI surface, plus a repo-local
compatibility wrapper.

### Supported primary entrypoint

Run the installed CLI from the repository root:

```bash
uv run syntheca run --output-dir ./output
```

Equivalent module form:

```bash
uv run python -m syntheca run --output-dir ./output
```

Compatibility wrapper:

```bash
python scripts/run_full_pipeline.py --output-dir ./output
```

Options:
- `--output-dir DIR` — where to write outputs (default: `./output`)
- `--collections ...` — Pure OAI-PMH collections to fetch (default: `openaire_cris_publications`, `openaire_cris_persons`, `openaire_cris_orgunits`)
- `--max-openalex N` — limit OpenAlex DOI lookups (0 = all)
- `--skip-people` — skip UT People enrichment
- `--skip-openaire` — disable bounded OpenAIRE reconciliation supplements
- `--check-parity` — run regression-metric comparison against the committed frozen offline baseline

### Supported opt-in command

Scopus/SciVal comparison remains export-first and is exposed as an explicit opt-in CLI mode:

```bash
uv run syntheca compare-scopus path/to/export.xlsx --internal-parquet output/merged.reconciled.parquet --output-dir ./output
```

### Not currently supported as a user CLI

- `python -m syntheca.pipeline` is **not** a supported user-facing CLI entrypoint.
- Co-authorship and policy-citation workflows remain documented library paths, not default or bundled CLI commands.

### Reference-only surfaces

- `current_marimo_monolith.py` is retained for historical/reference purposes only.
- It should not be treated as the current supported runtime path.

## Implemented capabilities

- **Default supported run path**: Pure OAI-PMH ingestion, OpenAlex enrichment, bounded OpenAIRE reconciliation sidecar, UT People fallback enrichment, merged export writing, regression parity comparison
- **Library-level modules present in package code**: OpenAIRE Graph client/provider support, Scopus/SciVal comparison utilities, co-authorship analysis, policy-citation investigation
- **Stack**: Python 3.14, `polars`, `httpx`, `pydantic`
- **Architecture**: async I/O, type-safe canonical data models, robust error handling

### Source precedence

When a record appears in multiple sources, the pipeline applies these precedence rules:
1. **Pure OAI-PMH** — primary authoritative source for UT publications and persons
2. **OpenAlex** — enrichment layer for citations, OA status, topics, identifiers
3. **UT People pages** — faculty/department affiliation enrichment

See `src/syntheca/processing/merging.py` and `src/syntheca/models/adapters.py` for details.

## Product boundary

The installable product surface is `src/syntheca/`.  Every module inside this
tree satisfies quality gates (typing, linting, tests).

## Outputs

### Default run outputs today

The supported run path writes the merged-output family plus bounded transition/parity artifacts.

| File | Description |
|---|---|
| `merged.parquet` | Merged and deduplicated publications (Parquet) |
| `merged.xlsx` | Same data as a formatted Excel workbook |
| `merged.reconciled.parquet` | Bounded reconciliation sidecar used for side-by-side review during transition |
| `merged.reconciled.xlsx` | Excel form of the bounded reconciliation sidecar |
| `merged.explicit.parquet` | Explicit duplicate export written by the script wrapper |
| `merged.explicit.xlsx` | Explicit duplicate Excel export written by the script wrapper |
| `pure_publications_clean.parquet` | Parity-support artifact for Pure publication counts |
| `pure_persons.parquet` | Parity-support artifact for Pure person counts |
| `pure_orgunits.parquet` | Parity-support artifact for Pure org-unit counts |
| `openalex_works_clean.parquet` | Parity-support artifact for OpenAlex hit-rate checks |
| `authors_enriched.parquet` | Parity-support artifact for org-mapping coverage and unresolved-person checks |

Stable publication columns are defined in `src/syntheca/config/output_contract.py`.

### Output groups defined in code, but not emitted automatically by the default run path

The package defines richer output groups in `src/syntheca/reporting/output_groups.py`, but the default runnable path above does **not** currently generate them automatically.

- **Comparison** — Scopus/SciVal comparison outputs from `syntheca.comparison.scopus`
- **Co-authorship** — edge tables and collaboration rollups from `syntheca.analysis.coauthorship`
- **Policy citations** — candidate/review exports from `syntheca.analysis.policy_citations`

Treat co-authorship and policy-citation outputs as **implemented library capabilities**, and comparison outputs as a **supported opt-in CLI path**, not as files guaranteed by the default `syntheca run` command.

### Scopus/SciVal comparison input boundary

The comparison path remains **export-first**.

Supported comparison inputs for `syntheca.comparison.scopus` are local, document-level export files in `.csv`, `.xlsx`, or `.xls` form, including:

- Scopus document exports
- SciVal publication-detail exports whose columns normalize into document fields such as DOI, title, year, document type, and source title
- observed SciVal header variants such as `publication_type` / `Publication type` and `scopus_source_title` / `Scopus Source title`

Out of scope for this product surface:

- direct Scopus or Elsevier API access, API-key handling, or export-dashboard automation
- source-list or journal-list workbooks that are not document exports, such as `scopus_ext_list_May_2025.xlsx`
- treating authenticated vendor export UX behavior as part of the offline comparison contract

## Parity status

The frozen offline regression pack now has a **real committed baseline**.

- `tests/regression_baseline.json` is marked `_baseline_status: "real"` and carries non-null tracked metrics for the frozen offline pack.
- `--check-parity` compares current exported artifacts against that baseline and should fail meaningfully when quality regresses.
- This parity gate covers the offline regression pack only; live endpoint smoke checks remain a separate release-signoff step.
- Safe cutover still requires both green parity and successful live smoke verification.

## Configuration

Environment-variable examples are provided in `.env.example`.

Supported prefixes:

- `SYNTHECA_` for general runtime settings
- `SYNTHECA_UT_` for University of Twente institutional overrides

## Development

```bash
# Linting
uvx ruff check src/
uvx ruff format src/

# Type checking
uv run ty check
```

### Tests

Tests are **offline by default** — no network access required.

```bash
# Run default (offline) tests
uv run pytest -x --tb=short

# Include live tests (requires network)
uv run pytest -m live
```

Markers:
- `live` — tests that hit real external services
- `network` — tests that require network access
- `slow` — tests too slow for routine runs

## Reference-only files

The following files are kept for **historical context only** and must
**not** be imported at runtime:

| File | Purpose |
|---|---|
| `current_marimo_monolith.py` | Legacy single-file Marimo notebook that preceded the library extraction. |
| `archive/openalex_data_models.py` | Original standalone OpenAlex dataclass definitions. The canonical versions now live in `src/syntheca/models/openalex.py`. |

### Archived clients

`src/syntheca/clients/archive/pure_oai_lxml.py` — the lxml-based Pure
OAI-PMH parser has been superseded by the xmltodict-based `pure_oai.py`.
It is kept in the `archive/` directory for historical reference only.

The installable product surface is `src/syntheca/` — see the package docstring
in `src/syntheca/__init__.py` for details.
