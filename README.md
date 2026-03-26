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

## Running the pipeline

### Full pipeline script

The primary entrypoint is the pipeline script:

```bash
python scripts/run_full_pipeline.py --output-dir ./output
```

Options:
- `--output-dir DIR` — where to write outputs (default: `./output`)
- `--collections ...` — Pure OAI-PMH collections to fetch (default: publications, persons, orgunits)
- `--max-openalex N` — limit OpenAlex DOI lookups (0 = all)
- `--skip-people` — skip UT People enrichment
- `--check-parity` — validate outputs against the regression baseline after pipeline completes

### Module entrypoint

```bash
python -m syntheca.pipeline  # planned
```

## Features

- **Sources**: OAI-PMH (Pure), OpenAlex API, UT People page scraping, Scopus exports (comparison only)
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

## Output groups

The pipeline produces outputs organised into four stable groups.

### Core Data (`output/`)

Normalized, merged publications — the main pipeline output.

| File | Description |
|---|---|
| `merged.parquet` | Merged and deduplicated publications (Parquet) |
| `merged.xlsx` | Same data as a formatted Excel workbook |

Stable columns are defined in `src/syntheca/config/output_contract.py`.

### Comparison (`output/comparison/`)

Scopus/SciVal export comparison results (produced when a Scopus file is provided).

| File | Description |
|---|---|
| `scopus_matched.parquet` | Records in both Scopus and internal data |
| `scopus_only.parquet` | Records only in the Scopus export |
| `internal_only.parquet` | Records only in internal data |
| `scopus_mismatches.parquet` | Matched records with field-level differences |

### Co-authorship (`output/coauthorship/`)

Co-authorship edge tables and collaboration rollups.

| File | Description |
|---|---|
| `author_publication_links.parquet` | Publication-to-author link table |
| `coauthor_edges.parquet` | Co-author pair edges with shared-work counts |
| `ut_vs_external.parquet` | UT-internal vs external collaboration summary |
| `university_rollup.parquet` | Edges by university-type affiliation |
| `company_rollup.parquet` | Edges involving company-affiliated authors |
| `country_rollup.parquet` | Edges by country pairs |

### Policy Citations (`output/policy_citations/`)

Policy-citation candidates and human-review queue.

| File | Description |
|---|---|
| `policy_candidates.csv` | Policy-citation candidates sorted by confidence |
| `policy_review_queue.xlsx` | Human-review queue for borderline candidates |

Output groups and file contracts are defined in `src/syntheca/reporting/output_groups.py`.

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