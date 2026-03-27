"""Canonical normalized record models with provenance tracking.

This module defines the **canonical layer** — Pydantic models that represent
source-agnostic, normalized views of works, persons and organizations.
Every field value is accompanied by a :class:`SourceAssertion` recording
*which* source provided the value and at what confidence.

Design decisions
----------------
* **Pydantic** is used consistently for the canonical layer (the project
  already depends on pydantic for Settings).
* The canonical models are intentionally *flat* — they are designed to
  convert easily to Polars rows for downstream processing.
* Provenance is stored as a list of :class:`SourceAssertion` objects
  attached to each canonical record and is preserved through
  ``model_dump`` / ``model_validate`` round-trips.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import polars as pl
from pydantic import BaseModel, Field

from syntheca.config.source_precedence import Source
from syntheca.utils.polars_frames import robust_from_dicts

# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------


class SourceAssertion(BaseModel):
    """Provenance record for a single field value from a single source.

    Attributes:
        source: The upstream data source that provided this value.
        field_name: Canonical field name this assertion refers to.
        value: The raw value as reported by the source.
        confidence: Confidence score in [0.0, 1.0]; defaults to 1.0.
        timestamp: When the assertion was recorded (optional).
    """

    source: Source
    field_name: str
    value: Any = None
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    timestamp: datetime | None = None


# ---------------------------------------------------------------------------
# Canonical Work
# ---------------------------------------------------------------------------


class CanonicalWork(BaseModel):
    """Normalized work / publication record.

    Fields are drawn from the output contract (``config.output_contract``)
    and enriched with source-level provenance.
    """

    # --- Identity ---
    internal_id: str = Field(
        description="Pure internal_repository_id when available, else best available ID."
    )
    doi: str | None = None
    title: str

    # --- Bibliographic core ---
    publication_year: int | None = None
    publication_date: str | None = None
    type: str | None = None
    language: str | None = None

    # --- Authorship ---
    authors: list[str] = Field(
        default_factory=list, description="Flat list of author display names."
    )

    # --- Cross-source IDs ---
    source_ids: dict[str, str] = Field(
        default_factory=dict,
        description="Mapping of source name → source-specific ID, e.g. {'openalex': 'W123', 'pure': 'uuid'}.",
    )

    # --- Open Access ---
    is_oa: bool | None = None
    oa_color: str | None = None

    # --- Citation ---
    cited_by_count: int | None = None
    fwci: float | None = None

    # --- Publisher / venue ---
    publisher: str | None = None
    primary_host_name: str | None = None

    # --- UT-specific ---
    ut_is_corresponding: bool | None = None

    # --- Access / license (from Pure) ---
    access_right: str | None = None
    license: str | None = None

    # --- Keywords / topics (from OpenAlex) ---
    keywords: list[str] = Field(default_factory=list)

    # --- Abstract ---
    abstract: str | None = None

    # --- Provenance ---
    provenance: list[SourceAssertion] = Field(default_factory=list)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def to_flat_dict(self) -> dict[str, Any]:
        """Return a flat dict suitable for Polars ``from_dicts``.

        Nested provenance is serialized as a JSON-compatible list of dicts
        so that it can be stored in a Polars Utf8 column.
        """
        d = self.model_dump()
        # Flatten provenance to a JSON-safe string representation
        d["provenance"] = [a.model_dump(mode="json") for a in self.provenance]
        return d


# ---------------------------------------------------------------------------
# Canonical Person
# ---------------------------------------------------------------------------


class CanonicalPerson(BaseModel):
    """Normalized person record."""

    internal_id: str = Field(description="Pure internal_repository_id.")
    name: str
    orcid: str | None = None
    scopus_author_id: str | None = None
    affiliations: list[dict[str, Any]] = Field(default_factory=list)
    provenance: list[SourceAssertion] = Field(default_factory=list)

    def to_flat_dict(self) -> dict[str, Any]:
        """Return a flat dict suitable for Polars ``from_dicts``."""
        d = self.model_dump()
        d["provenance"] = [a.model_dump(mode="json") for a in self.provenance]
        return d


# ---------------------------------------------------------------------------
# Canonical Organization
# ---------------------------------------------------------------------------


class CanonicalOrganization(BaseModel):
    """Normalized organization record."""

    internal_id: str
    name: str
    type: str | None = None
    parent_id: str | None = None
    provenance: list[SourceAssertion] = Field(default_factory=list)

    def to_flat_dict(self) -> dict[str, Any]:
        """Return a flat dict suitable for Polars ``from_dicts``."""
        d = self.model_dump()
        d["provenance"] = [a.model_dump(mode="json") for a in self.provenance]
        return d


# ---------------------------------------------------------------------------
# Collection helper
# ---------------------------------------------------------------------------


def canonicals_to_polars(
    records: list[CanonicalWork | CanonicalPerson | CanonicalOrganization],
) -> pl.DataFrame:
    """Convert a list of canonical records to a Polars DataFrame.

    This preserves provenance as a nested list-of-dicts column that
    can be further processed or serialized as needed.
    """
    if not records:
        return pl.DataFrame()
    return robust_from_dicts([r.to_flat_dict() for r in records])
