"""Scopus/SciVal export comparison utilities.

This module reads licensed Scopus or SciVal export files (Excel / CSV) and
compares them against an internal Syntheca DataFrame to identify:

* **Matched** records — present in both Scopus export and internal data.
* **Scopus-only** records — in the Scopus export but absent internally.
* **Internal-only** records — in internal data but absent from Scopus.
* **Field mismatches** — matched by DOI but with divergent field values.

Design decisions
----------------
* No Scopus API calls — works purely with local export files.
* Supported inputs are document-level Scopus exports and SciVal publication
    detail exports already produced outside the product.
* Source-list or journal-list workbooks are intentionally out of scope.
* DOI is the primary matching key.  Matching reuses
  :func:`syntheca.processing.cleaning.normalize_doi`.
* Column-name matching is case-insensitive to tolerate different Scopus
  export versions and regional locale differences.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from syntheca.processing.cleaning import normalize_doi

# ---------------------------------------------------------------------------
# Expected column names in Scopus exports (canonical lower-case form)
# ---------------------------------------------------------------------------

#: Mapping of canonical lower-case column name → list of known alternatives.
#: Used for case-insensitive / variant matching when ingesting an export file.
SCOPUS_COLUMN_ALIASES: dict[str, list[str]] = {
    "doi": ["doi"],
    "title": ["title", "document title"],
    "authors": ["authors", "author names", "author(s)"],
    "source_title": ["source title", "source", "scopus_source_title", "scopus source title"],
    "year": ["year", "publication year"],
    "document_type": ["document type", "type", "publication_type", "publication type"],
    "eid": ["eid"],
    "cited_by": ["cited by", "cited by count", "citation count"],
    "abstract": ["abstract"],
    "author_keywords": ["author keywords"],
    "index_keywords": ["index keywords"],
    "affiliations": ["affiliations"],
    "publisher": ["publisher"],
    "issn": ["issn"],
    "language": ["language of original document", "language"],
    "open_access": ["open access"],
}


def _normalize_header_key(value: str) -> str:
    """Normalize a raw header or alias into a comparison-friendly key."""
    return re.sub(r"[^0-9a-z]+", "_", value.strip().lower()).strip("_")


def _build_alias_lookup() -> dict[str, str]:
    """Return a mapping of ``alias_lowercase → canonical_name``."""
    lookup: dict[str, str] = {}
    for canonical, aliases in SCOPUS_COLUMN_ALIASES.items():
        for alias in aliases:
            lookup[_normalize_header_key(alias)] = canonical
    return lookup


_ALIAS_LOOKUP = _build_alias_lookup()


# ---------------------------------------------------------------------------
# ScopusExportReader
# ---------------------------------------------------------------------------


class ScopusExportReader:
    """Read and normalize a Scopus/SciVal export file.

    Supports ``.xlsx``, ``.xls``, and ``.csv`` extensions.  Column names are
    normalized to the canonical lower-case forms defined in
    :data:`SCOPUS_COLUMN_ALIASES`.  DOIs are normalized using the pipeline's
    existing :func:`~syntheca.processing.cleaning.normalize_doi`.

    This reader is deliberately export-first: it accepts local document-export
    files, not live Scopus API responses and not source-list workbooks.
    """

    @staticmethod
    def read_export(path: Path | str) -> pl.DataFrame:
        """Read a Scopus/SciVal export file and return a normalized DataFrame.

        Args:
            path: File path to an Excel (``.xlsx`` / ``.xls``) or CSV (``.csv``) export.

        Returns:
            A Polars DataFrame with canonical column names and normalized DOIs.

        Raises:
            ValueError: If the file extension is unsupported.
            FileNotFoundError: If *path* does not exist.
        """
        path = Path(path)
        if not path.exists():
            msg = f"Export file not found: {path}"
            raise FileNotFoundError(msg)

        suffix = path.suffix.lower()
        if suffix in (".xlsx", ".xls"):
            df = pl.read_excel(path, engine="openpyxl")
        elif suffix == ".csv":
            df = pl.read_csv(path, infer_schema_length=10_000)
        else:
            msg = f"Unsupported file format: {suffix!r}. Expected .xlsx, .xls, or .csv."
            raise ValueError(msg)

        df = _normalize_column_names(df)

        # Normalize DOIs if a doi column is present
        if "doi" in df.columns:
            df = normalize_doi(df, "doi")

        return df


# ---------------------------------------------------------------------------
# Column-name normalization
# ---------------------------------------------------------------------------


def _normalize_column_names(df: pl.DataFrame) -> pl.DataFrame:
    """Rename columns to canonical forms using case-insensitive alias matching.

    Columns that do not match any alias are kept with their original names
    lower-cased and with spaces replaced by underscores.
    """
    rename_map: dict[str, str] = {}
    for col in df.columns:
        normalized = _normalize_header_key(col)
        if normalized in _ALIAS_LOOKUP:
            rename_map[col] = _ALIAS_LOOKUP[normalized]
        else:
            # Fall back to a sanitized lower-case version
            rename_map[col] = normalized

    return df.rename(rename_map)


# ---------------------------------------------------------------------------
# ComparisonResult
# ---------------------------------------------------------------------------


@dataclass
class ComparisonResult:
    """Result of comparing a Scopus export against an internal DataFrame.

    Attributes:
        matched: Records present in both sources (joined on DOI).
        scopus_only: Records only in the Scopus export.
        internal_only: Records only in the internal data.
        mismatch_details: Matched records with at least one field-level difference.
        summary: Aggregate counts and statistics.
    """

    matched: pl.DataFrame
    scopus_only: pl.DataFrame
    internal_only: pl.DataFrame
    mismatch_details: pl.DataFrame
    summary: dict[str, object] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# ScopusComparison
# ---------------------------------------------------------------------------

#: Fields compared when detecting mismatches between matched records.
_MISMATCH_FIELDS: list[tuple[str, str]] = [
    # (scopus_column, internal_column)
    ("title", "title"),
    ("document_type", "type"),
    ("year", "publication_year"),
]


class ScopusComparison:
    """Compare a Scopus export DataFrame with the internal Syntheca DataFrame.

    The comparison is **DOI-based**: records are matched on their normalized
    DOI values.  Records without a DOI on either side are classified as
    ``scopus_only`` or ``internal_only`` respectively.
    """

    @staticmethod
    def compare(
        scopus_df: pl.DataFrame,
        internal_df: pl.DataFrame,
        *,
        scopus_doi_col: str = "doi",
        internal_doi_col: str = "doi",
    ) -> ComparisonResult:
        """Run the comparison and return a :class:`ComparisonResult`.

        Args:
            scopus_df: Normalized Scopus export (output of
                :meth:`ScopusExportReader.read_export`).
            internal_df: Internal Syntheca DataFrame.  Expected to contain at
                least a ``doi`` column (already normalized).
            scopus_doi_col: Column name for DOIs in *scopus_df*.
            internal_doi_col: Column name for DOIs in *internal_df*.

        Returns:
            A :class:`ComparisonResult` with matched, scopus-only, internal-only,
            and mismatch-detail DataFrames.
        """
        # Ensure DOI columns exist; create null placeholders if missing
        if scopus_doi_col not in scopus_df.columns:
            scopus_df = scopus_df.with_columns(pl.lit(None).cast(pl.Utf8).alias(scopus_doi_col))
        if internal_doi_col not in internal_df.columns:
            internal_df = internal_df.with_columns(
                pl.lit(None).cast(pl.Utf8).alias(internal_doi_col)
            )

        # Filter to rows with non-null, non-empty DOIs for matching
        scopus_with_doi = scopus_df.filter(
            pl.col(scopus_doi_col).is_not_null() & (pl.col(scopus_doi_col) != "")
        )
        internal_with_doi = internal_df.filter(
            pl.col(internal_doi_col).is_not_null() & (pl.col(internal_doi_col) != "")
        )

        # Rows without a usable DOI are automatically placed in the *_only sets
        scopus_no_doi = scopus_df.filter(
            pl.col(scopus_doi_col).is_null() | (pl.col(scopus_doi_col) == "")
        )
        internal_no_doi = internal_df.filter(
            pl.col(internal_doi_col).is_null() | (pl.col(internal_doi_col) == "")
        )

        # DOI sets
        scopus_dois = set(scopus_with_doi[scopus_doi_col].to_list())
        internal_dois = set(internal_with_doi[internal_doi_col].to_list())

        matched_dois = scopus_dois & internal_dois
        scopus_only_dois = scopus_dois - internal_dois
        internal_only_dois = internal_dois - scopus_dois

        # Build result DataFrames
        matched_scopus = scopus_with_doi.filter(pl.col(scopus_doi_col).is_in(matched_dois))
        matched_internal = internal_with_doi.filter(pl.col(internal_doi_col).is_in(matched_dois))

        # Prefix columns to avoid collision before joining
        matched_scopus_prefixed = matched_scopus.rename(
            {c: f"scopus_{c}" for c in matched_scopus.columns}
        )
        matched_internal_prefixed = matched_internal.rename(
            {c: f"internal_{c}" for c in matched_internal.columns}
        )

        matched_joined = matched_scopus_prefixed.join(
            matched_internal_prefixed,
            left_on=f"scopus_{scopus_doi_col}",
            right_on=f"internal_{internal_doi_col}",
            how="inner",
        )

        # Scopus-only and internal-only
        scopus_only = pl.concat(
            [
                scopus_with_doi.filter(pl.col(scopus_doi_col).is_in(scopus_only_dois)),
                scopus_no_doi,
            ],
            how="diagonal",
        )
        internal_only = pl.concat(
            [
                internal_with_doi.filter(pl.col(internal_doi_col).is_in(internal_only_dois)),
                internal_no_doi,
            ],
            how="diagonal",
        )

        # --- Mismatch detection ---
        mismatch_details = _detect_mismatches(matched_joined)

        summary = {
            "total_scopus": len(scopus_df),
            "total_internal": len(internal_df),
            "matched": len(matched_dois),
            "scopus_only": len(scopus_only),
            "internal_only": len(internal_only),
            "mismatched_records": len(mismatch_details),
        }

        return ComparisonResult(
            matched=matched_joined,
            scopus_only=scopus_only,
            internal_only=internal_only,
            mismatch_details=mismatch_details,
            summary=summary,
        )


def _detect_mismatches(matched_joined: pl.DataFrame) -> pl.DataFrame:
    """Detect field-level mismatches between matched Scopus and internal records.

    Returns a DataFrame with one row per matched DOI that has at least one
    divergent field, with columns ``doi``, ``field``, ``scopus_value``, and
    ``internal_value``.
    """
    if matched_joined.is_empty():
        return pl.DataFrame(
            schema={
                "doi": pl.Utf8,
                "field": pl.Utf8,
                "scopus_value": pl.Utf8,
                "internal_value": pl.Utf8,
            }
        )

    rows: list[dict[str, str | None]] = []

    # Resolve the DOI column name in the joined DataFrame
    doi_col = "scopus_doi" if "scopus_doi" in matched_joined.columns else None
    if doi_col is None:
        # Fallback: find any column ending with _doi prefixed with scopus_
        for c in matched_joined.columns:
            if c.startswith("scopus_") and c.endswith("doi"):
                doi_col = c
                break
    if doi_col is None:
        return pl.DataFrame(
            schema={
                "doi": pl.Utf8,
                "field": pl.Utf8,
                "scopus_value": pl.Utf8,
                "internal_value": pl.Utf8,
            }
        )

    for scopus_field, internal_field in _MISMATCH_FIELDS:
        s_col = f"scopus_{scopus_field}"
        i_col = f"internal_{internal_field}"
        if s_col not in matched_joined.columns or i_col not in matched_joined.columns:
            continue

        for row in matched_joined.iter_rows(named=True):
            s_val = str(row.get(s_col) or "").strip().lower()
            i_val = str(row.get(i_col) or "").strip().lower()
            if s_val and i_val and s_val != i_val:
                rows.append(
                    {
                        "doi": row[doi_col],
                        "field": f"{scopus_field}_mismatch",
                        "scopus_value": str(row.get(s_col, "")),
                        "internal_value": str(row.get(i_col, "")),
                    }
                )

    if not rows:
        return pl.DataFrame(
            schema={
                "doi": pl.Utf8,
                "field": pl.Utf8,
                "scopus_value": pl.Utf8,
                "internal_value": pl.Utf8,
            }
        )

    return pl.from_dicts(rows)
