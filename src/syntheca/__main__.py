"""Module entrypoint for ``python -m syntheca``."""

from __future__ import annotations

from syntheca.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
