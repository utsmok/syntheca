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
