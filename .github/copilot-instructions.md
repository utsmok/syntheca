# Project Instructions for syntheca

You are an expert Python developer working on the `syntheca` library.

## Core Stack
- **Python**: 3.14
- **Manager**: `uv` (NEVER use pip).
- **Data**: `polars` (NEVER use `pandas`).
- **HTTP Client**: `httpx` (async) (NEVER use `requests`).
- **Type Checking**: `ty` (Astral). ensure code is correctly typed.
- **Linting, Formatting, Fixing**: `ruff`.

## Conventions
1.  **Type Hints**:
    - Use modern syntax: `list[str | None]` (not `List`, not `Optional`).
    - Use `pathlib.Path` for files.
2.  **Models**:
    - Use the provided `dacite` models in `models/openalex.py` for API responses.
    - Don't use raw dicts for complex objects.
3.  **Async**:
    - All clients in `clients/` must be `async`.
4.  **Config**:
    - Load all constants from `config.settings` or JSON files. NEVER hardcode UUIDs or lists.

When generating code, ensure it passes `ruff check` and `ty check`.
