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

| Step | Module | Description |
| :--- | :--- | :--- |
| **1** | **Config & Models** | Set up Pydantic settings, externalize hardcoded data (publishers, faculties) to JSON, and implement OpenAlex data models. |
| **2** | **Infrastructure** | Build the foundation: Logging (`loguru`), Caching (`utils`), and a robust `BaseAsyncClient` with retries (`tenacity`). |
| **3** | **Clients** | Implement specific API clients: `PureOAIClient` (XML), `OpenAlexClient` (Typed), and `UTPeopleClient` (Scraper). |
| **4** | **Processing** | Port Polars logic: Cleaning, Fuzzy Matching, Enrichment (Faculty linking), and Merging (OILS). |
| **5** | **Reporting & Pipeline** | Create the Export module (Excel/Parquet) and the main Async Orchestrator (`pipeline.py`). |
| **6** | **Frontend** | Create the clean `app.py` Marimo notebook that serves as the UI. |

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

### Step 1: Configuration & Data Models
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
