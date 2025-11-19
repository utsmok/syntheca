---

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

---

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

---

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
