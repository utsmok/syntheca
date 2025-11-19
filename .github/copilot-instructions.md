# Copilot / AI agent instructions for the syntheca repository

This file documents the precise, project-specific patterns an AI coding agent should follow when making changes in this codebase. Read it before editing or adding code.

Core constraints
- Python 3.14. Use language features accordingly.
- Use the project manager `uv` for runs (do not use pip). Example: `uv run pytest`.
- Async HTTP must use `httpx.AsyncClient`. Do not add `requests`.
- Dataframes must use `polars`, not `pandas`.
- Type checking uses `ty` (Astral). Ensure types are correct.
- Linting and fixes use `ruff`.

Key repository patterns (what you must follow)
- Keep clients async: files under `src/syntheca/clients/` must expose `async def` methods and use `httpx.AsyncClient`.
- Pure functions for transformations: modules under `src/syntheca/processing/` take and return `polars.DataFrame` objects (no side effects). This keeps logic testable.
- Models live in `src/syntheca/models/openalex.py` and should be used (via dacite) instead of raw dicts for API responses.
- Configuration and mappings must be loaded from `src/syntheca/config/` (use `settings` from `src/syntheca/config/settings.py`). Do not hardcode mappings, UUIDs, or API keys.
- Use `pathlib.Path` for filesystem paths and modern typing (e.g. `list[str | None]`).

Important files to consult
- `current_marimo_monolith.py` — the legacy notebook; useful reference for business logic and transformation sequences.
- `openalex_data_models.py` — original dataclasses for OpenAlex used to create `src/syntheca/models/openalex.py`.
- `src/syntheca/config/mappings/` — JSON files (publishers.json, faculties.json, corrections.json) are the single source of truth for static mappings.
- `src/syntheca/clients/base.py` — BaseClient should implement tenacity retries, caching decorator hooks, and context manager support.
- `src/syntheca/pipeline.py` — the orchestrator that wires ingestion, processing, and reporting.

Patterns and examples
- Client example: implement `async def get_works_by_title(self, title: str) -> list[models.Work]` that returns typed dataclasses via dacite.from_dict using a module-level dacite.Config (strict=False, check_types=True, cast=[int,float]).
- Processing functions: `def clean_publications(df: pl.DataFrame) -> pl.DataFrame` — do not mutate in place; return a new dataframe.
- Testing: write small pure-unit tests for processing functions using `pytest` / `pytest-asyncio` for async client logic.

Development commands (PowerShell / Windows)
- Run tests: uv run pytest -q
- Run ruff lint/fix: uv run ruff check --fix
- Format with ruff: uv run ruff format
- Run type checks: uv run ty check

Caching, logging, and retries
- Logging: use `loguru` configured under `src/syntheca/utils/logging.py`.
- Caching: high-level functions that call external APIs should be decorated with the repository caching decorator (e.g., `file_cache`) found/implemented in `src/syntheca/utils/caching.py`.
- Retries: `BaseClient` must use `tenacity` to retry on 429/5xx and respect Retry-After headers.

Tests and CI
- Tests should live under `tests/` alongside modules they exercise (e.g., `tests/processing/test_cleaning.py`).
- Use fixtures and mocks for network calls (pytest-asyncio + httpx.MockTransport or respx).
- CI runs: the repo's GitHub workflows (see `.github/workflows/ci.yml`) expect `ruff`, `ty`, and `pytest` to run.

What to preserve from the refactor plan
- Follow the refactor plan in `.github/refactor-plan.md` when adding new modules — it contains the intended package layout and responsibilities (config, models, clients, processing, reporting, pipeline).
- Prefer small, focused commits that add one module or unit of work at a time. Link back to corresponding sections in the refactor plan in PR descriptions.

When in doubt
- Search for similar implementations in the repo before adding new helpers (e.g., how caching or logging is wired).
- Keep functions small and typed. If you need to change public APIs, update the `py.typed` indication and add type stubs if necessary.

Quick checklist for PRs by an AI agent
1. Ensure settings and static mappings are loaded from `src/syntheca/config/`.
2. Clients are async and return typed models where applicable.
3. Processing modules accept and return polars DataFrames (no side effects).
4. Add or update unit tests (pytest / pytest-asyncio) covering new logic.
5. Run: `uv run ruff check --fix && uv run ty check && uv run pytest -q` and fix issues reported.

If anything in these instructions is unclear or you want me to expand examples for a particular module, tell me which file and I'll update this guidance.
