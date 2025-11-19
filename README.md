# Academic Tracker

A modern ETL pipeline for retrieving, processing, and enriching academic metadata.

## Features
- Sources: OAI-PMH, OpenAlex API, manual file uploades, and specific page scraping. More to come.
- Stack: Python 3.14, `polars`, `httpx`, `marimo`.
- Architecture: async I/O, type-safe data models, robust error handling.

## Installation

This project uses `uv` for dependency management.

```bash
# Install dependencies
uv sync

# Run the frontend (Marimo)
uv run marimo edit app.py
```

# Development
linting: `ruff`
type checking: `ty`

```
uv run ruff check src/
uv run ruff fix src/
uv run ruff format src/
uv run ty check
```