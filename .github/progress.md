2025-11-19 - Completed Step 1: Configuration & Models

Summary:
- Created `src/syntheca/config/settings.py` using `pydantic-settings` and exposed `settings` from `src/syntheca/config/__init__.py`.
- Added mapping JSONs under `src/syntheca/config/mappings/`:
	- `publishers.json` (canonical publisher keys & variants)
	- `faculties.json` (faculty fullname -> short name mapping, University of Twente UUID & OpenAlex UT id)
	- `corrections.json` (manual author affiliation corrections)
- Ported the full OpenAlex dataclasses to `src/syntheca/models/openalex.py` with `production_config` (dacite Config) per plan.
- Added unit tests:
	- `tests/test_config_settings.py` to check mapping paths and JSON structure
	- `tests/test_models_openalex.py` to validate dacite parsing and dataclass use

Notes:
- Mappings were extracted from `current_marimo_monolith.py`. The `publishers.json` contains a subset of the mapping variants used in the notebook — we can expand later.
- `faculties.json` includes `ut_uuid` and `openalex_ut_id` to remain compatible with existing logic in the notebook.

2025-11-19T00:20:00Z - Completed Step 3: Clients

Summary:
- Implemented `PureOAIClient` (`src/syntheca/clients/pure_oai.py`) with robust xml parsing helpers copied/adapted from `current_marimo_monolith.py`: `_ensure_list`, `_get_text`, `_safe_get`, `_parse_publication`, `_parse_person`, `_parse_orgunit`, and resumption token handling.
- Implemented `OpenAlexClient` (`src/syntheca/clients/openalex.py`) with batch retrieval in `get_works_by_ids` (50 ids per call), and `get_works_by_title` using the autocomplete API followed by a detail fetch. Responses are converted into typed `Work` dataclasses using `dacite.production_config`.
 - Implemented `OpenAlexClient` (`src/syntheca/clients/openalex.py`) with batch retrieval in `get_works_by_ids` (50 ids per call), and `get_works_by_title` using the autocomplete API followed by a detail fetch. Responses are converted into typed `Work` dataclasses using `dacite.production_config`.
 - Added `clean_openalex_raw_data` which produces a simplified flattened dict per work with fields like `is_oa`, `oa_color`, `main_url`, `primary_url`, `all_host_orgs`, `topic`, `listed_apc_usd`, and `ut_is_corresponding`.
- Implemented `UTPeopleClient` (`src/syntheca/clients/ut_people.py`) for search RPC and scraping of profile pages. It returns candidate dicts for matching and includes `scrape_profile` and `_parse_organization_details` to parse the 'Organisations' widget.

Tests:
- `tests/test_clients_pure_oai.py` validates XML parsing and `get_all_records` with `MockTransport`.
- `tests/test_clients_openalex.py` validates chunked retrieval and title autocomplete/detail fetching with `MockTransport`.
- `tests/test_clients_ut_people.py` validates `search_person` and `scrape_profile` parsing logic.

Notes:
- The clients deliberately don't perform fuzzy matching — this was moved into the processing layer to keep clients I/O-only. The processing step will implement fuzzy matching and candidate resolution.
 - The clients deliberately don't perform fuzzy matching — this was moved into the processing layer to keep clients I/O-only. The processing step will implement fuzzy matching and candidate resolution. However, parsing logic that flattens nested structures from Pure and OpenAlex has been ported into the clients to avoid duplication and to provide ready-to-consume dicts for processing.

2025-11-19T00:30:00Z - OpenAlex title search optimization

Summary:
- Parallelized the detailed work fetch in `OpenAlexClient.get_works_by_title` using `asyncio.gather` to fetch multiple work details concurrently. This prevents slow serial fetches when the autocomplete returns multiple matches.
- Added a unit test to ensure that one failing detail request doesn't break the entire operation (error-tolerant behaviour).

2025-11-19T00:25:00Z - Pydantic v2 config migration

Summary:
- Replaced deprecated Pydantic class Config with `model_config = ConfigDict(...)` in `src/syntheca/config/settings.py` to address the Pydantic V2 deprecation warning. This change follows the Pydantic V2 migration guide and avoids the PydanticDeprecatedSince20 warning.

- All client calls in tests are mocked to avoid external API calls.


2025-11-19 - Completed Step 2: Infrastructure

Summary:
- Implemented `loguru` configuration in `src/syntheca/utils/logging.py` that emits to stderr and a rotating file at `settings.log_file`.
- Implemented `@file_cache` in `src/syntheca/utils/caching.py`, which supports both async coroutine functions and sync functions. Key generation uses blake2b on the function qualname + repr(args/kwargs). Cached results are pickled to `settings.cache_dir`.
- Implemented `BaseClient` in `src/syntheca/clients/base.py` with `httpx.AsyncClient`, retry logic using `tenacity` (retries on network errors, 429 and 5xx responses), `before_sleep_log` for informative retry logging via `loguru`, and explicit `client.aclose()` in `__aexit__`.

Tests:
- `tests/test_utils_caching.py` ensures sync and async caching works and produces cache files.
- `tests/test_clients_base.py` covers context manager cleanup and retry behavior where the first call returns 429 and a following call returns 200 (using `httpx.MockTransport`).

Notes:
- The `file_cache` decorator is intentionally simple: no TTL, no invalidation or complex key normalization. It's good for deterministic inputs (titles/DOIs) but can be updated later.
- `BaseClient` implements a `request` helper that raises `HTTPStatusError`; the retry predicate handles these errors and determines when to retry. We use `before_sleep_log` to tie `tenacity` logging into `loguru`.



2025-11-19 - Started/Completed Step 4: Processing (initial core functions)

Summary:
- Added Polars-based processing modules in `src/syntheca/processing/`:
	- `cleaning.py`: Implements `normalize_doi()` (lowercase, strip `https://doi.org/`, trim) and a light `clean_publications()` used in pipeline tests.
	- `matching.py`: Implements `calculate_fuzzy_match()` using `Levenshtein.ratio` called via `pl.struct(...).map_elements(...)` to remain vectorized.
	- `enrichment.py`: Implements `enrich_authors_with_faculties()` which loads `faculties.json` from `settings` and creates boolean faculty columns when affiliation names match.
	- `merging.py`: Implements `merge_datasets()` which normalizes DOIs in both frames using `normalize_doi()` and joins on `_norm_doi`.

Tests:
- `tests/test_processing_cleaning.py` verifies DOI normalization and year parsing.
- `tests/test_processing_matching.py` verifies fuzzy matching for exact and partial matches.
- `tests/test_processing_enrichment.py` verifies faculty boolean columns are added from `faculties.json`.
- `tests/test_processing_merging.py` verifies DOIs are normalized and used for the join.

Notes:
- `calculate_fuzzy_match` uses `Levenshtein.ratio` (dependency in `pyproject.toml`) and `map_elements` — this keeps the computation row-level while remaining expressional.
- `enrich_authors_with_faculties` intentionally expects the column `affiliation_names_pure` and is defensive — it returns the original DataFrame if the column is missing.
- This completes an initial surface area of Step 4; next we'll implement deeper matching / enrichment flows and robust deduplication.
 - Implemented `resolve_missing_ids()` in `src/syntheca/processing/matching.py`. This async helper uses the `OpenAlexClient.get_works_by_title` API to fetch candidate matches and selects the best match using `Levenshtein.ratio`. It prefers results where the OpenAlex candidate lists UT as a corresponding institution by adding a small score boost. Successful matches populate the `id` and `doi` columns where missing.
 - Implemented `deduplicate()` in `src/syntheca/processing/merging.py`. Strategy:
	 - Normalize DOIs and drop duplicate DOI rows while preserving the first occurrence.
	 - For rows without DOIs, deduplicate by a normalized title (lowercased and trimmed). This mirrors the notebook's dedup strategy and can be extended later.
 - Added tests: `tests/test_processing_advanced.py` covers `resolve_missing_ids` (mocking OpenAlex results) and `deduplicate`.

Notes:
- `resolve_missing_ids` is conservative: only matches that exceed a default `threshold=0.9` are used. The function also provides a small boost for UT-affiliated OpenAlex works to help resolve ambiguous titles.
- `deduplicate` is intentionally simple and returns a stable, deterministic subset. Later versions can implement more complex title-similarity or ID heuristics.


2025-11-20 - Completed Step 5: Reporting & Pipeline

Summary:
- Implemented `src/syntheca/reporting/export.py` with `write_parquet` and `write_formatted_excel` using Polars' IO (`write_parquet` / `write_excel`). `write_formatted_excel` uses `autofit` and `dtype_formats` for basic date formatting.
- Implemented `src/syntheca/pipeline.py` with an async `Pipeline` that:
	- Accepts `oils_df`, `full_df`, and `authors_df` directly for deterministic unit testing.
	- Accepts optional client instances (`pure_client`, `openalex_client`, `ut_people_client`) for client-based ingestion.
	- Supports `openalex_ids` to bulk fetch OpenAlex works when `full_df` is not supplied.
	- Supports `people_search_names` to call `UTPeopleClient.search_person` when `authors_df` is missing.
	- Integrates cleaning, enrichment, merging, and deduplication.
	- Writes `merged.parquet` and `merged.xlsx` to `output_dir` if provided.

Tests:
- Added `tests/test_reporting_export.py` for exports.
- Added `tests/test_pipeline.py` and `tests/test_pipeline_clients.py` (with fake clients) to validate pipeline logic and client ingestion flows.
- Fixed `cleaning.normalize_doi` to create a Null `_norm_doi` when DOI column missing to avoid `ColumnNotFoundError` during merges/deduplication.

Notes:
- Pipeline client ingestion is minimal and intended for controlled testable ingestion paths. Client lifecycle (async context manager) and production-ready ingestion flows (pagination, rate-limits, and large-batch downloads) can be added in subsequent iterations.
- `write_formatted_excel` uses Polars' `write_excel` to avoid adding a hard `pandas` dependency and to leverage `xlsxwriter` formatting features.
- All tests pass locally (31 passed, 0 failed) after these changes.


2025-11-20 - Enhancements: Progress bars & Intermediate persistence

Summary:
- Added `tqdm`-based progress bars to client retrieval functions (`PureOAIClient.get_all_records`, `OpenAlexClient.get_works_by_ids`, and `OpenAlexClient.get_works_by_title`), made compatible with `asyncio`. Bars can be displayed concurrently through a global position allocator (`syntheca.utils.progress.get_next_position`). The default behaviour is enabled via `settings.enable_progress = True`.
- Added `persist_intermediate` boolean to `settings` (default True), and a set of persistence helpers in `src/syntheca/utils/persistence.py` to save/load Parquet files in `settings.cache_dir`.
- Adjusted clients and `Pipeline` to persist intermediate datasets when enabled (Raw Pure collection data, OpenAlex results and cleaned frames like `oils_clean`, `full_clean`, and `authors_enriched`).
- Added tests verifying persistence and concurrent progress bar behavior: `tests/test_utils_persistence.py`, `tests/test_concurrent_progress_bars.py`, updates to `tests/test_clients_*` to check saving of parquet files when configured.


2025-11-21 - Added integration tests for pipeline

Summary:
- Added `tests/test_pipeline_integration.py` with:
	- A mock-based end-to-end pipeline integration test (`test_pipeline_integration_mock_end_to_end`) that uses fake clients and persisted `openaire_cris_orgunits` to validate full ingestion, enrichment, and join behavior.
	- An live test (`test_pipeline_integration_live_minimal`) which performs a minimal OpenAlex fetch (single DOI) to validate real-world API behavior.
- Integration tests ensure the end-to-end pipeline path (ingest -> cleaning -> enrichment -> joining -> merge) behaves as expected with both synthetic and minimal real data.

Notes:
- The mock test uses `save_dataframe_parquet` to ensure the pipeline can load `openaire_cris_orgunits` and map authors correctly.
- There are no live tests for the other APIs yet.

2025-11-21 - Added validation utilities & schema normalization

Summary:
- Added `src/syntheca/utils/validation.py` with helpers to normalize string columns (`normalize_str_column`), ensure required columns (`ensure_columns`) and a specific `normalize_orgs_df` helper to normalize organization dataframes used across the pipeline and clients.
- Replaced manual defensive list-to-scalar conversions in `src/syntheca/processing/organizations.py` with a normalized approach using `normalize_orgs_df` and ensured `map_author_affiliations` uses the normalized org schema.
- Added unit tests in `tests/test_utils_validation.py` covering the new utilities and adapted organization tests to verify behavior with list-based fields.



2025-11-21 - Completed Step 5b: Advanced Enrichment & Merging (Gap Fill)

Summary:
- Implemented `src/syntheca/processing/organizations.py` with `resolve_org_hierarchy` and `map_author_affiliations` to map Pure org units and author affiliation IDs to faculty boolean flags and convenience columns.
- Extended `src/syntheca/processing/enrichment.py` with `parse_scraped_org_details` to parse nested scraped org details into `faculty`/`department`/`group` cols and boolean flags, and added `apply_manual_corrections` to load `corrections.json` and apply fixes to author rows.
- Extended `src/syntheca/processing/merging.py` with `join_authors_and_publications` to aggregate author flags and list columns and join them back onto publications by `pure_id`.
- Integrated the above into `src/syntheca/pipeline.py` to run the full enrichment & joining flow, with persistence-aware org loading.
- Added unit tests for organizations parsing (`tests/test_processing_organizations.py`), advanced enrichment parsing & corrections (`tests/test_processing_enrichment_advanced.py`), and join logic (`tests/test_processing_merging.py`).

Notes:
- The enrich & join functions rely on `faculties.json` and `corrections.json` stored in `src/syntheca/config/mappings/`.
- The pipeline will attempt to load processed `openaire_cris_orgunits` via persistence utilities if present; otherwise fall back to no organization mapping.
- More robust matching and hierarchical resolution can be added; these functions focus on deterministic behavior and unit testability.
