# Refactoring Master Plan: Syntheca Library

This document outlines the step-by-step plan to refactor the current monolithic Marimo notebook into a modern, modular Python 3.14 library (`syntheca`).
Be sure to adhere to the instructions in `.github/copilot-instructions.md` plus the details in this document when implementing each step.

## Project Overview
*   **Goal**: Create a robust ETL pipeline for academic metadata.
*   **Stack**: Python 3.14, `uv`, `ty` (Type Checking), `ruff` (Linting), `polars`, `httpx`, `marimo`.
*   **Architecture**: Async I/O, Type-safe Data Models (Dacite), JSON-based Configuration.

## Existing code to be refactored/incorporated

These files can be found in the root of the repository, and are used as reference for porting logic into the new modular structure.
* `current_marimo_monolith.py`: The existing Marimo notebook with all logic in one place.
* `openalex_data_models.py`: The existing OpenAlex data models using Dataclasses.

---

## Summary of Steps
Completed? | Step | Module | Description |
| :--- | :--- | :--- | :--- |
| YES | **1** | **Config & Models** | ✓ Completed — Set up Pydantic settings, externalized hardcoded data (publishers, faculties) to JSON, and implemented OpenAlex dataclasses. |
| YES | **2** | **Infrastructure** | ✓ Completed — Built `loguru` logging config, async file cache, and `BaseClient` with tenacity retries and httpx lifecycle handling. |
| YES | **3** | **Clients** | ✓ Completed — Implemented `PureOAIClient`, `OpenAlexClient`, and `UTPeopleClient` including robust XML helpers, `dacite` typed parsing and chunked OpenAlex queries. |
| YES | **4** | **Processing (Core)** | ✓ Completed — added basic `cleaning`, `matching`, `enrichment` (string-based), and `merging` logic. |
| YES | **5a** | **Pipeline (Basic)** | ✓ Completed — Basic orchestrator structure implemented. |
| YES | **5b** | **Enrichment Gap Fill** | ✓ Completed — Implemented organizational hierarchy resolution, author-org mapping, scraped data parsing, manual corrections, and author-publication aggregation with full test coverage. |
| NO  | **6** | **Frontend** | Create the clean `app.py` Marimo notebook that serves as the UI. |

---

## Repo overview

```markdown

syntheca/                           # Root dir/repo
├── .github/
|    ├── workflows/                 # CI/CD
|    │   └── ci.yml                 # Run tests, lint, type check
|    └── copilot-instructions.md    # LLM coding instructions
│    └── refactor-plan.md           # This file
├── src/syntheca/                   # Main library code
│    ├── __init__.py
│    ├── config/
│    │   ├── __init__.py
│    │   ├── settings.py            # Pydantic settings (Env vars, API keys)
│    │   └── mappings/              # JSON data
│    │       ├── publishers.json    # The massive publisher dict
│    │       ├── corrections.json   # Author affiliation fixes
│    │       └── faculties.json     # Faculty names & UUIDs
│    ├── models/
│    │   ├── __init__.py
│    │   └── openalex.py            # dataclasses for OpenAlex entities
│    ├── clients/
│    │   ├── __init__.py
│    │   ├── base.py                # Async BaseClient (Tenacity + Cache + Loguru)
│    │   ├── openalex.py            # Uses models/openalex.py for parsing
│    │   ├── pure_oai.py            # Async XML harvester
│    │   └── ut_people.py           # RPC & Scraper
│    ├── processing/
│    │   ├── __init__.py
│    │   ├── cleaning.py            # Polars transformations
│    │   ├── enrichment.py          # Joining authors <-> faculties
│    │   ├── matching.py            # Fuzzy matching logic (Title/Name)
│    │   └── merging.py             # OILS & Deduplication logic
│    ├── reporting/
│    │   ├── __init__.py
│    │   └── export.py              # Excel formatting & Parquet writing
│    └── pipeline.py                # Main Async Orchestrator
│  # Root Level
├──app.py                         # Marimo Notebook (UI Only)
├──pyproject.toml                 # Dependencies
├──current_marimo_monolith.py     # Existing monolithic code (for reference)
└──openalex_data_models.py        # Existing OpenAlex models (for reference)
```

---

## Detailed Instructions

Note 1: most code should be taken from `current_marimo_monolith.py` with modifications as needed; unless otherwise specified.
Note 2: Ensure to always add correct type hints and docstrings to all functions and classes.
Note 3: Add basic unit tests for each module in `tests/` during implementation. This is not detailed here but is expected as part of the development workflow. We use `pytest` for testing, with `pytest-asyncio` for async tests. Make use of fixtures and mocks as needed, and follow best practices for test organization and implementation.Run the tests using `uv`, e.g., `uv run pytest`.

---

## Completed steps

### Step 1: Configuration & Data Models [Completed]
**Goal**: Remove hardcoded data and establish type safety.

1.  **`src/syntheca/config/mappings/`**:
    *   Create `publishers.json`: Extract the massive publisher dictionary from the old `clean_publications` function.
    *   Create `faculties.json`: Store Faculty names, abbreviations, and the hardcoded "University of Twente" UUID.
    *   Create `corrections.json`: Extract the list of hardcoded author affiliations from `add_missing_affils`.
2.  **`src/syntheca/config/settings.py`**:
    *   Use `pydantic_settings`.
    *   Define `Settings` class to load paths to the JSON files above, default API timeouts, and User-Agent headers.
    *   Expose a `settings` instance.
3.  **`src/syntheca/models/openalex.py`**:
    *   **Action**: Copy the provided Dataclasses code from `openalex_data_models.py`.
    *   **Modification**: ensure a `dacite.Config` object at the module level with `strict=False`, `check_types=True`, and `cast=[int, float]` to handle API inconsistencies.

Notes (differences / clarifications):
- `publishers.json`, `faculties.json`, and `corrections.json` were created with values taken from `current_marimo_monolith.py` rather than being fully comprehensive; these files can be expanded in Step 4 while porting `clean_publications` and `add_missing_affils`.
- Tests for settings & models were added. The models `openalex.py` copy is comprehensive (closely mirrors `openalex_data_models.py`) so downstream typed clients can be implemented in Step 3.
- `faculties.json` includes `ut_uuid` and `openalex_ut_id` so that processing functions can reference them without hardcoding.


### Step 2: Infrastructure (Utils & Base Client)
**Goal**: Reusable plumbing for network requests and logging.

1.  **`src/syntheca/utils/logging.py`**:
    *   Configure `loguru` to write to `stderr` (formatted) and a rotating file log.
2.  **`src/syntheca/utils/caching.py`**:
    *   Implement a decorator (e.g., `@file_cache`) using `pickle` or `fsspec` to cache API results locally based on function arguments.
3.  **`src/syntheca/clients/base.py`**:
    *   Create `BaseClient`.
    *   **Attributes**: `httpx.AsyncClient`, `logger`.
    *   **Features**:
        *   Implement automatic retries using `tenacity` (handle 429s and 5xx errors).
        *   Integrate the caching logic.
        *   Implement `__aenter__` and `__aexit__` for context management.

    Notes (changes & clarifications):
    - `src/syntheca/utils/logging.py` configures `loguru` to log to stderr (INFO) and to a rotating logfile at `settings.log_file` (DEBUG). This replicates the intended persistent logging behavior and uses `settings` for the log path.
    - `src/syntheca/utils/caching.py` implements `@file_cache` that supports both sync and async functions. Async wrapper awaits the decorated coroutine and writes/reads pickled results to/from disk. The cache key is a blake2b hash of the function qualname and repr(args/kwargs).
    - `src/syntheca/clients/base.py` provides `BaseClient` with `httpx.AsyncClient`, a `request()` helper decorated with `tenacity.retry` to retry on `httpx.RequestError` and HTTP 429/5xx. `tenacity.before_sleep_log` is used with `loguru` to surface retry messages. `__aexit__` ensures the underlying `AsyncClient` is closed to avoid leaking resources.
    - Tests added: `tests/test_utils_caching.py` (async+sync caching) and `tests/test_clients_base.py` (context manager + retry on 429) to validate the implementation.

### Step 3: API Clients
**Goal**: Implement specific logic for each data source.

1.  **`src/syntheca/clients/pure_oai.py`**:
    *   Inherit from `BaseClient`.
    *   **Method**: `get_all_records(collections: list[str]) -> dict`.
    *   **Logic**: Handle OAI-PMH resumption tokens (pagination).
    *   **Parsing**: Use `xmltodict`. Implement specific parsers (`parse_pub`, `parse_person`, `parse_org`) that return flat dictionaries suitable for Polars.
2.  **`src/syntheca/clients/openalex.py`**:
    *   Inherit from `BaseClient`.
    *   **Method**: `get_works_by_ids(ids: list[str]) -> list[models.Work]`.
    *   **Method**: `get_works_by_title(title: str)`.
    *   **Logic**: Use the `dacite.from_dict` config from Step 1 to parse JSON into objects.
3.  **`src/syntheca/clients/ut_people.py`**:
    *   Inherit from `BaseClient`.
    *   **Method**: `search_person(name: str)`.
    *   **Method**: `scrape_profile(url: str)`.
    *   **Logic**: Use `selectolax` for HTML parsing (CSS selectors). Port the regex logic for parsing "Faculty (Abbr)".

Notes (changes & clarifications):
- `PureOAIClient` includes `_ensure_list`, `_get_text`, `_safe_get`, `_parse_publication`, `_parse_person`, and `_parse_orgunit` for robust XML parsing. Resumption token logic handles XML pagination.
- `OpenAlexClient` implements chunking of IDs (50 per batch) in `get_works_by_ids` and resolves titles via `autocomplete/works` then fetches full work details. It uses `dacite.from_dict` with `production_config` to create typed `Work` instances.
 - `OpenAlexClient` implements chunking of IDs (50 per batch) in `get_works_by_ids` and resolves titles via `autocomplete/works` then fetches full work details in parallel (asyncio.gather). It uses `dacite.from_dict` with `production_config` to create typed `Work` instances. This prevents sequential slowdowns when many title matches are returned.
- `UTPeopleClient` now exposes `search_person` for RPC queries and `scrape_profile` for detail pages and includes `_parse_organization_details` that mirrors the logic in the original notebook. The heavy fuzzy matching is intentionally left for the `processing/matching.py` module as the clients should focus on I/O.
- All clients' I/O methods are unit-tested using `httpx.MockTransport` so tests do not call external networks.

### Step 4: Processing Logic (Polars)
**Goal**: Pure functions that take DataFrames and return DataFrames.

1.  **`src/syntheca/processing/cleaning.py`**:
    *   `clean_publications(df)`: Date standardization, Publisher normalization (load `publishers.json`).
    *   `clean_persons(df)`: Name standardization.
2.  **`src/syntheca/processing/matching.py`**:
    *   `calculate_fuzzy_match(series_a, series_b)`: Use `levenshtein`.
    *   `resolve_missing_ids(df, client)`: Logic to isolate rows missing DOIs, call the OpenAlex client for title search, and merge results.
3.  **`src/syntheca/processing/enrichment.py`**:
    *   `enrich_authors_with_faculties(authors, pubs, orgs)`: The complex logic linking authors -> orgs -> faculties.
    *   `apply_manual_corrections(df)`: Apply the rules from `corrections.json`.
4.  **`src/syntheca/processing/merging.py`**:
    *   `merge_datasets(pure_df, openalex_df, oils_df)`: High-level joins.
    *   `deduplicate(df)`: Logic to handle duplicate entries based on DOI/Title.

### Step 5: Reporting & Orchestration
**Goal**: Tie it all together and produce output.

1.  **`src/syntheca/reporting/export.py`**:
    *   `write_formatted_excel(df, path)`: Use `xlsxwriter` via Polars. Apply column widths and date formats.
    *   `write_parquet(df, path)`.
2.  **`src/syntheca/pipeline.py`**:
    *   Create `class Pipeline`.
    *   **Method**: `run(...)` (Async).
    *   **Logic**:
        1.  Load Config.
        2.  Initialize Clients.
        3.  **Ingest**: Fetch Pure (OAI), OpenAlex (Bulk), scrape People Pages (Async gather).
        4.  **Transform**: Call `processing` functions.
        5.  **Report**: Call `export` functions.
---

## Steps to be implemented

### Step 5b: Advanced Enrichment & Merging (Gap Filling)
**Goal**: Restore the deep enrichment and aggregation logic from the legacy monolith to ensure data completeness.

1.  **`src/syntheca/processing/organizations.py` (New Module)**:
    *   **Port logic from**: `clean_and_enrich_persons_data` (specifically the org data cleaning parts).
    *   `resolve_org_hierarchy(orgs_df: pl.DataFrame) -> pl.DataFrame`:
        *   Resolve `part_of` relationships to find the "Parent Org" for every unit.
        *   Map names to short-codes (e.g., "Faculty of ..." -> "tnw") using `faculties.json`.
    *   `map_author_affiliations(authors_df: pl.DataFrame, processed_orgs_df: pl.DataFrame) -> pl.DataFrame`:
        *   Join authors to organizations on `affiliation_id`.
        *   Flag "is_ut" based on the UUID.
        *   Assign boolean faculty flags based on the hierarchy resolution.

2.  **Update `src/syntheca/processing/enrichment.py`**:
    *   **Port logic from**: `enrich_employee_data` and `parse_org_details`.
    *   `parse_scraped_org_details(authors_df: pl.DataFrame) -> pl.DataFrame`:
        *   Process the data structure returned by `scrape_profile` (which will need to be added to the DF).
        *   Extract `department`, `group`, `faculty` strings and create boolean flags for institutes (dsi, mesa, etc.).
    *   `apply_manual_corrections(df: pl.DataFrame) -> pl.DataFrame`:
        *   Load `corrections.json`.
        *   Update affiliations/boolean flags for specific authors found in the mapping.

3.  **Update `src/syntheca/processing/merging.py`**:
    *   **Port logic from**: `join_authors_and_publications`.
    *   `join_authors_and_publications(publications_df: pl.DataFrame, authors_df: pl.DataFrame) -> pl.DataFrame`:
        *   Explode `publications_df` by author ID.
        *   Join with the fully enriched `authors_df`.
        *   **Aggregation**:
            *   Boolean cols (tnw, eemcs, etc.): Use `any()` (if any author is TNW, the pub is TNW).
            *   List cols (groups, depts): Flatten and `unique()`.
            *   Metadata (orcids): Collect unique.
        *   Join aggregated data back to the original `publications_df`.

4.  **Update `src/syntheca/pipeline.py`**:
    *   Update `run()` to execute the full flow:
        1.  **Ingest**: Fetch `publications`, `persons`, `orgs` from Pure (ensure all are cached to parquet).
        2.  **Process Orgs**: Call `resolve_org_hierarchy`.
        3.  **Process Persons (Pure)**: Call `map_author_affiliations`.
        4.  **Process Persons (Scraper)**:
            *   Filter for UT authors.
            *   Orchestrate `search_person` -> `scrape_profile` (async loop).
            *   Update persons DF with scraped data.
            *   Call `parse_scraped_org_details`.
        5.  **Corrections**: Call `apply_manual_corrections`.
        6.  **Merge**: Call `join_authors_and_publications` to fuse persons into publications.
        7.  **Finalize**: Merge with OILS/OpenAlex (existing logic) and Export.

### Step 6: Frontend (Marimo)
**Goal**: User Interface.

1.  **`app.py`**:
    *   Imports: `syntheca.pipeline`, `syntheca.config`.
    *   **UI**: `mo.ui.file_browser`, `mo.ui.checkbox`, `mo.ui.range_slider`.
    *   **Action**: Button click triggers `await Pipeline().run(...)`.
    *   **Display**: Show progress bars and final DataFrame sample.


---

## Development Workflow

For each step:
1.  Write the code in `src/`.
2.  Run `uv run ruff check --fix` and `uv run ruff format`.
3.  Run `uv run ty check`.
