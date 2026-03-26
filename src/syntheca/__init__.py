"""Top-level package for the syntheca library.

This package exposes the library's high-level modules and provides a simple
integration point for CLI and scripts to import `syntheca` as a package.

Product boundary
================
``syntheca/src/syntheca/`` is the **only** product runtime surface.  Every
module inside this directory tree is considered part of the installable
library and must satisfy quality gates (typing, linting, tests).

Files that live outside this tree — in particular ``current_marimo_monolith.py``
and ``archive/openalex_data_models.py`` — are **reference-only** artefacts
kept for historical context.  They must **not** be imported at runtime and
are not covered by the project's test suite.
"""
