"""Source precedence rules for multi-source metadata merging.

These rules codify *which* upstream source is considered authoritative
for every semantic category of metadata.  They can be imported by
processing code to make conflict-resolution logic explicit rather than
ad-hoc.

Design decisions
----------------
* **Work identity** — DOI is the primary deduplication key; title serves
  as fallback.  Pure ``internal_repository_id`` is the institutional
  canonical ID.  OpenAlex ``id`` is the citation-graph canonical ID.
* **Person identity** — Pure ``internal_repository_id`` is authoritative
  for UT persons.  ORCID is the preferred external key.  UT People
  ``people_page_url`` is supplementary only.
* **Organization identity** — Pure org-unit hierarchy is authoritative.
  UT People org structure is used only when Pure affiliation data is
  absent.
* **Field-level conflicts** — see :class:`FieldPrecedence` members.
"""

from __future__ import annotations

from enum import StrEnum


class Source(StrEnum):
    """Upstream data source identifiers."""

    PURE = "pure"
    OPENALEX = "openalex"
    OPENAIRE = "openaire"
    UT_PEOPLE = "ut_people"
    MANUAL = "manual"


class FieldPrecedence(StrEnum):
    """Field-level precedence declarations.

    Each member encodes ``<field_group>:<preferred_source>[|<fallback>]``.
    Processing code can parse the value or simply compare against the
    known :class:`Source` constants.

    Rules
    -----
    * OA status (``is_oa``, ``oa_color``) → OpenAlex
    * Access right / license → Pure
    * Funding data → OpenAIRE when available, else OpenAlex
    * Publisher name → Pure  (institutional curation is more reliable)
    * Citation metrics (``cited_by_count``, ``fwci``) → OpenAlex
    * Publication type → Pure
    * Abstract → OpenAlex (inverted-index availability)
    * Keywords / topics → OpenAlex
    """

    OA_STATUS = "oa_status:openalex"
    ACCESS_RIGHT = "access_right:pure"
    LICENSE = "license:pure"
    FUNDING = "funding:openaire|openalex"
    PUBLISHER_NAME = "publisher_name:pure"
    CITATION_METRICS = "citation_metrics:openalex"
    PUBLICATION_TYPE = "publication_type:pure"
    ABSTRACT = "abstract:openalex"
    KEYWORDS = "keywords:openalex"

    # ------------------------------------------------------------------
    # Helper API
    # ------------------------------------------------------------------

    @property
    def preferred_source(self) -> Source:
        """Return the preferred :class:`Source` for this field group."""
        raw = self.value.split(":")[1].split("|")[0]
        return Source(raw)

    @property
    def fallback_source(self) -> Source | None:
        """Return the fallback :class:`Source`, or ``None`` if there is no fallback."""
        parts = self.value.split(":")[1].split("|")
        if len(parts) > 1:
            return Source(parts[1])
        return None


# -----------------------------------------------------------------------
# Identity precedence (work / person / org) — expressed as plain dicts
# so that they are easy to inspect but still importable as code.
# -----------------------------------------------------------------------

WORK_IDENTITY_KEYS: dict[str, Source] = {
    "doi": Source.PURE,  # primary dedup key
    "title": Source.PURE,  # fallback dedup key
    "internal_repository_id": Source.PURE,  # institutional canonical ID
    "id": Source.OPENALEX,  # citation-graph canonical ID
}

PERSON_IDENTITY_KEYS: dict[str, Source] = {
    "internal_repository_id": Source.PURE,  # authoritative for UT persons
    "orcid": Source.OPENALEX,  # preferred external key
    "people_page_url": Source.UT_PEOPLE,  # supplementary
}

ORGANIZATION_IDENTITY_KEYS: dict[str, Source] = {
    "org_hierarchy": Source.PURE,  # authoritative
    "org_structure_fallback": Source.UT_PEOPLE,  # used only when Pure is absent
}
