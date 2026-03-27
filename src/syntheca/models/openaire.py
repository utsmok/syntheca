"""Pydantic models for OpenAIRE Graph API response structures.

These models map directly to the JSON shapes returned by the OpenAIRE
Graph API (v2 for research products, v1 for organizations).  They are
intentionally thin — just enough structure to parse and forward to the
canonical adapter layer.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Shared / envelope
# ---------------------------------------------------------------------------


class SearchHeader(BaseModel):
    """Pagination header returned by all OpenAIRE Graph search endpoints."""

    num_found: int = Field(0, alias="numFound")
    max_score: float | None = Field(None, alias="maxScore")
    query_time: int | None = Field(None, alias="queryTime")
    page: int | None = None
    page_size: int | None = Field(None, alias="pageSize")
    next_cursor: str | None = Field(None, alias="nextCursor")

    model_config = {"populate_by_name": True}


# ---------------------------------------------------------------------------
# Research product
# ---------------------------------------------------------------------------


class AuthorPidSchemeValue(BaseModel):
    """Scheme/value pair for an author PID."""

    scheme: str | None = None
    value: str | None = None


class AuthorPidWrapper(BaseModel):
    """Wrapper around an author persistent identifier."""

    id: AuthorPidSchemeValue | None = None


class Author(BaseModel):
    """Author record within a research product."""

    full_name: str | None = Field(None, alias="fullName")
    name: str | None = None
    surname: str | None = None
    rank: int | None = None
    pid: AuthorPidWrapper | None = None

    model_config = {"populate_by_name": True}


class Language(BaseModel):
    """Language code/label pair."""

    code: str | None = None
    label: str | None = None


class BestAccessRight(BaseModel):
    """Best access right across all instances of a product."""

    code: str | None = None
    label: str | None = None
    scheme: str | None = None


class AccessRight(BaseModel):
    """Access right for a specific instance."""

    code: str | None = None
    label: str | None = None
    scheme: str | None = None
    open_access_route: str | None = Field(None, alias="openAccessRoute")

    model_config = {"populate_by_name": True}


class ResultPid(BaseModel):
    """Persistent identifier for a result (DOI, PMID, etc.)."""

    scheme: str | None = None
    value: str | None = None


class KeyValue(BaseModel):
    """Generic key/value pair used for hostedBy and collectedFrom."""

    key: str | None = None
    value: str | None = None


class Instance(BaseModel):
    """A single 'instance' of a research product (one per source/host)."""

    pids: list[ResultPid] = Field(default_factory=list)
    license: str | None = None
    access_right: AccessRight | None = Field(None, alias="accessRight")
    type: str | None = None
    urls: list[str] = Field(default_factory=list)
    publication_date: str | None = Field(None, alias="publicationDate")
    refereed: str | None = None
    hosted_by: KeyValue | None = Field(None, alias="hostedBy")
    collected_from: KeyValue | None = Field(None, alias="collectedFrom")

    model_config = {"populate_by_name": True}


class Container(BaseModel):
    """Journal or hosting container metadata."""

    name: str | None = None
    issn_printed: str | None = Field(None, alias="issnPrinted")
    issn_online: str | None = Field(None, alias="issnOnline")

    model_config = {"populate_by_name": True}


class SubjectSchemeValue(BaseModel):
    """Scheme/value pair for a subject keyword."""

    scheme: str | None = None
    value: str | None = None


class Subject(BaseModel):
    """Subject/keyword entry."""

    subject: SubjectSchemeValue | None = None


class BipIndicators(BaseModel):
    """Bibliometric Indicator Platform (BIP) scores."""

    citation_count: float | None = Field(None, alias="citationCount")
    influence: float | None = None
    popularity: float | None = None
    impulse: float | None = None

    model_config = {"populate_by_name": True}


class CitationImpact(BipIndicators):
    """Current live citation-impact payload returned by the Graph API."""

    citation_class: str | None = Field(None, alias="citationClass")
    influence_class: str | None = Field(None, alias="influenceClass")
    impulse_class: str | None = Field(None, alias="impulseClass")
    popularity_class: str | None = Field(None, alias="popularityClass")

    model_config = {"populate_by_name": True}


class UsageCounts(BaseModel):
    """Download and view counters."""

    downloads: int | None = None
    views: int | None = None


class Indicators(BaseModel):
    """Container for current and legacy bibliometric indicators."""

    citation_impact: CitationImpact | None = Field(None, alias="citationImpact")
    bip_indicators: BipIndicators | None = Field(None, alias="bipIndicators")
    usage_counts: UsageCounts | None = Field(None, alias="usageCounts")

    model_config = {"populate_by_name": True}

    @property
    def citation_metrics(self) -> CitationImpact | BipIndicators | None:
        """Return the active citation-like metrics payload.

        Live Graph responses currently use ``citationImpact`` while older
        saved fixtures still carry ``bipIndicators``.
        """
        return self.citation_impact or self.bip_indicators

    @property
    def citation_count(self) -> float | None:
        """Return the citation-count signal from either supported shape."""
        metrics = self.citation_metrics
        return metrics.citation_count if metrics is not None else None


class OpenAIREResearchProduct(BaseModel):
    """A research product record from the OpenAIRE Graph API."""

    id: str | None = None
    main_title: str | None = Field(None, alias="mainTitle")
    sub_title: str | None = Field(None, alias="subTitle")
    type: str | None = None
    publication_date: str | None = Field(None, alias="publicationDate")
    publisher: str | None = None
    language: Language | None = None
    authors: list[Author] = Field(default_factory=list)
    pids: list[ResultPid] = Field(default_factory=list)
    best_access_right: BestAccessRight | None = Field(None, alias="bestAccessRight")
    open_access_color: str | None = Field(None, alias="openAccessColor")
    container: Container | None = None
    subjects: list[Subject] = Field(default_factory=list)
    descriptions: list[str] = Field(default_factory=list)
    indicators: Indicators | None = None
    instances: list[Instance] = Field(default_factory=list)
    sources: list[str] = Field(default_factory=list)
    collected_from: list[KeyValue] = Field(default_factory=list, alias="collectedFrom")
    is_green: bool | None = Field(None, alias="isGreen")
    is_in_diamond_journal: bool | None = Field(None, alias="isInDiamondJournal")
    publicly_funded: bool | None = Field(None, alias="publiclyFunded")
    original_ids: list[str] = Field(default_factory=list, alias="originalIds")
    projects: list[dict] = Field(default_factory=list)
    organizations: list[dict] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


class ResearchProductSearchResponse(BaseModel):
    """Envelope for ``GET /v2/researchProducts`` responses."""

    header: SearchHeader
    results: list[OpenAIREResearchProduct] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Organization
# ---------------------------------------------------------------------------


class Country(BaseModel):
    """Country code/label pair."""

    code: str | None = None
    label: str | None = None


class OrganizationPid(BaseModel):
    """Persistent identifier for an organization (ROR, etc.)."""

    scheme: str | None = None
    value: str | None = None


class OpenAIREOrganization(BaseModel):
    """An organization record from the OpenAIRE Graph API."""

    id: str | None = None
    legal_short_name: str | None = Field(None, alias="legalShortName")
    legal_name: str | None = Field(None, alias="legalName")
    website_url: str | None = Field(None, alias="websiteUrl")
    alternative_names: list[str] | None = Field(None, alias="alternativeNames")
    country: Country | None = None
    pids: list[OrganizationPid] | None = None
    original_ids: list[str] = Field(default_factory=list, alias="originalIds")

    model_config = {"populate_by_name": True}


class OrganizationSearchResponse(BaseModel):
    """Envelope for ``GET /v1/organizations`` responses."""

    header: SearchHeader
    results: list[OpenAIREOrganization] = Field(default_factory=list)
