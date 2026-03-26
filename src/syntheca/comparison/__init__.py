"""Comparison utilities for reconciling Syntheca data against external exports.

This package provides readers and comparison logic for licensed export files
(e.g. Scopus/SciVal Excel or CSV exports) so that institutions can verify
coverage and detect discrepancies between their internal metadata pipeline
and the records available in proprietary databases.

No runtime API calls are made — this works entirely with local export files.
"""

from __future__ import annotations

from syntheca.comparison.scopus import ComparisonResult, ScopusComparison, ScopusExportReader

__all__ = ["ComparisonResult", "ScopusComparison", "ScopusExportReader"]
