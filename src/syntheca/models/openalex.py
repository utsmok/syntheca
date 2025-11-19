"""
OpenAlex dataclasses and small helpers.

Ported from repository-level `openalex_data_models.py`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Self, TypeVar

from dacite import Config, from_dict

production_config = Config(
    strict=False,
    check_types=True,
    cast=[int, float],
)


# --------------------------------------------------------------------------------------------------
# Type aliases
# --------------------------------------------------------------------------------------------------

WorkType = Literal[
    "article",
    "book-chapter",
    "book-section",
    "book",
    "dataset",
    "database",
    "dissertation",
    "editorial",
    "erratum",
    "grant",
    "letter",
    "libguides",
    "other",
    "paratext",
    "peer-review",
    "preprint",
    "reference-entry",
    "report",
    "report-component",
    "retraction",
    "review",
    "software",
    "standard",
    "supplementary-materials",
]

WorkTypeCrossref = Literal[
    "book-section",
    "monograph",
    "report-component",
    "report",
    "peer-review",
    "book-track",
    "journal-article",
    "book-part",
    "other",
    "book",
    "journal-volume",
    "book-set",
    "reference-entry",
    "proceedings-article",
    "journal",
    "component",
    "book-chapter",
    "proceedings-series",
    "report-series",
    "proceedings",
    "database",
    "standard",
    "reference-book",
    "posted-content",
    "journal-issue",
    "dissertation",
    "grant",
    "dataset",
    "book-series",
    "edited-book",
]

SourceType = Literal[
    "journal",
    "repository",
    "conference",
    "ebook platform",
    "book series",
    "metadata",
    "other",
]

InstitutionType = Literal[
    "education",
    "healthcare",
    "company",
    "archive",
    "nonprofit",
    "government",
    "facility",
    "other",
    "funder",
]


@dataclass
class BaseOpenAlex:
    """
    Base class for OpenAlex entities with an id.
    All entity classes should inherit from this.
    """

    id: str | None
    display_name: str | None
    relevance_score: float | None
    created_date: str | None
    updated_date: str | None
    score: float | None
    cited_by_count: int | None
    works_count: int | None
    works_api_url: str | None

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        return from_dict(data_class=cls, data=data, config=production_config)


@dataclass
class WorkIds:
    openalex: str
    doi: str | None
    mag: int | str | None
    pmid: str | None
    pmcid: str | None


@dataclass
class AuthorIds:
    openalex: str
    orcid: str | None
    scopus: str | None
    twitter: str | None
    wikipedia: str | None


@dataclass
class SourceIds:
    openalex: str
    fatcat: str | None
    issn: list[str | None] | None
    issn_l: str | None
    mag: int | str | None
    wikidata: str | None


@dataclass
class InstitutionIds:
    openalex: str
    ror: str | None
    grid: str | None
    mag: int | str | None
    wikidata: str | None
    wikipedia: str | None


@dataclass
class TopicIds:
    openalex: str
    wikipedia: str | None


@dataclass
class PublisherIds:
    openalex: str
    ror: str | None
    wikidata: str | None


@dataclass
class FunderIds:
    openalex: str
    doi: str | None
    crossref: str | None
    ror: str | None
    wikidata: str | None


@dataclass
class ConceptIds:
    openalex: str
    mag: int | str | None
    umls_cui: list[str] | None
    umls_aui: list[str] | None
    wikidata: str | None
    wikipedia: str | None


@dataclass
class Affiliation:
    raw_affiliation_string: str
    institution_ids: list[str | None]


@dataclass
class DehydratedAuthor(BaseOpenAlex):
    orcid: str | None


@dataclass
class DehydratedInstitution(BaseOpenAlex):
    country_code: str | None
    lineage: list[str | None] | None
    ror: str | None
    type: InstitutionType | None


@dataclass
class RelatedInstitution(DehydratedInstitution):
    relationship: Literal["parent", "child", "related", "successor"] | None


@dataclass
class DehydratedInstitutionWithYear:
    institution: DehydratedInstitution
    years: list[int | None]


@dataclass
class DehydratedSource(BaseOpenAlex):
    is_core: bool
    is_in_doaj: bool
    is_oa: bool
    is_indexed_in_scopus: bool | None
    type: SourceType
    issn_l: str | None
    issn: list[str] | None
    host_organization: str | None
    host_organization_lineage: list[str | None]
    host_organization_name: str | None
    host_organization_lineage_names: list[str | None] | None
    raw_type: str | None


@dataclass
class Repository(BaseOpenAlex):
    host_organization: str | None
    host_organization_lineage: list[str | None]
    host_organization_name: str | None


@dataclass
class SimpleDehydratedConcept(BaseOpenAlex):
    level: int | None
    wikidata: str | None


@dataclass
class DehydratedConcept(SimpleDehydratedConcept):
    score: float


@dataclass
class Authorship:
    author: DehydratedAuthor
    raw_author_name: str
    is_corresponding: bool
    countries: list[str]
    author_position: Literal["first", "middle", "last"] | None
    affiliations: list[Affiliation]
    institutions: list[DehydratedInstitution]
    raw_affiliation_strings: list[str]


@dataclass
class APCData:
    value: int | None
    currency: str | None
    value_usd: int | None
    provenance: str | None


@dataclass
class APCEntry:
    price: int
    currency: str


@dataclass
class Biblio:
    volume: str | None
    issue: str | None
    first_page: str | None
    last_page: str | None


@dataclass
class Mesh:
    descriptor_ui: str
    descriptor_name: str
    is_major_topic: bool
    qualifier_ui: str | None
    qualifier_name: str | None


@dataclass
class Location:
    is_accepted: bool | None
    is_oa: bool
    is_published: bool | None
    landing_page_url: str | None
    pdf_url: str | None
    license: str | None
    license_id: str | None

    source: DehydratedSource | None
    version: Literal["publishedVersion", "acceptedVersion", "submittedVersion"] | None

    raw_source_name: str | None
    id: str | None


@dataclass
class OpenAccess:
    is_oa: bool
    oa_status: Literal["diamond", "gold", "green", "hybrid", "bronze", "closed"]
    oa_url: str | None
    any_repository_has_fulltext: bool


@dataclass
class Grant:
    funder: str | None
    funder_display_name: str | None
    award_id: str | None


@dataclass
class DehydratedFunder:
    id: str | None
    display_name: str | None
    ror: str | None


@dataclass
class Domain(BaseOpenAlex): ...


@dataclass
class Field(BaseOpenAlex): ...


@dataclass
class Subfield(BaseOpenAlex): ...


@dataclass
class TopicMinimal(BaseOpenAlex): ...


@dataclass
class DehydratedTopic(BaseOpenAlex):
    score: float
    subfield: Subfield
    field: Field
    domain: Domain


@dataclass
class TopicCount(BaseOpenAlex):
    count: int
    score: float | None
    subfield: Subfield
    field: Field
    domain: Domain


@dataclass
class TopicShare(BaseOpenAlex):
    value: float
    subfield: Subfield
    field: Field
    domain: Domain


@dataclass
class SDG(BaseOpenAlex):
    score: float


@dataclass
class DehydratedKeyword(BaseOpenAlex):
    score: float


@dataclass
class CitationNormalizedPercentile:
    value: float
    is_in_top_1_percent: bool
    is_in_top_10_percent: bool


@dataclass
class YearCountBasic:
    year: int | None
    cited_by_count: int | None


@dataclass
class YearCount:
    year: int | None
    cited_by_count: int | None
    works_count: int | None
    oa_works_count: int | None


@dataclass
class SummaryStats:
    two_yr_mean_citedness: float
    h_index: int
    i10_index: int


@dataclass
class Society:
    url: str | None
    organization: str | None


@dataclass
class Geo:
    city: str | None
    geonames_city_id: str | None
    region: str | None
    country_code: str | None
    country: str | None
    latitude: float | None
    longitude: float | None


@dataclass
class Role:
    role: Literal["funder", "publisher", "institution"]
    id: str
    works_count: int | None


@dataclass
class International:
    display_name: dict[str, str] | None
    description: dict[str, str] | None


@dataclass
class HasContent:
    pdf: bool
    grobid_xml: bool


@dataclass
class Keyword(BaseOpenAlex): ...


@dataclass
class Topic(BaseOpenAlex):
    description: str
    ids: TopicIds
    keywords: list[str]
    subfield: Subfield
    field: Field
    domain: Domain
    siblings: list[TopicMinimal]


@dataclass
class Author(BaseOpenAlex):
    ids: AuthorIds
    orcid: str | None

    summary_stats: dict[str, float | int]

    affiliations: list[DehydratedInstitutionWithYear | None]
    counts_by_year: list[YearCount | None]
    display_name_alternatives: list[str | None]
    last_known_institutions: list[DehydratedInstitution | None]
    x_concepts: list[DehydratedConcept | None]

    topics: list[TopicCount | None]
    topic_share: list[TopicShare | None]


@dataclass
class Source(BaseOpenAlex):
    ids: SourceIds
    is_core: bool
    is_in_doaj: bool
    is_oa: bool
    summary_stats: dict[str, float | int]
    type: SourceType

    abbreviated_title: str | None
    alternate_titles: list[str | None] | None
    apc_prices: list[APCEntry | None] | None
    apc_usd: int | None
    country_code: str | None
    counts_by_year: list[YearCount | None]
    homepage_url: str | None
    host_organization: str | None
    host_organization_lineage: list[str | None]
    host_organization_name: str | None
    issn: list[str | None] | None
    issn_l: str | None
    societies: list[Society | None] | None
    x_concepts: list[DehydratedConcept | None] | None

    is_indexed_in_scopus: bool | None
    topics: list[TopicCount | None]
    topic_share: list[TopicShare | None]
    relevance_score: float | None
    oa_flip_year: int | None
    is_high_oa_rate: bool | None
    is_ojs: bool | None
    is_in_scielo: bool | None
    is_high_oa_rate_since_year: int | None
    is_in_doaj_since_year: int | None
    oa_works_count: int | None
    last_publication_year: int | None
    first_publication_year: int | None


@dataclass
class Institution(BaseOpenAlex):
    ids: InstitutionIds
    is_super_system: bool
    summary_stats: dict[str, float | int]
    type: InstitutionType

    associated_institutions: list[RelatedInstitution | None] | None
    country_code: str | None
    counts_by_year: list[YearCount | None]
    display_name_acronyms: list[str | None]
    display_name_alternatives: list[str | None]
    geo: Geo | None
    homepage_url: str | None
    image_thumbnail_url: str | None
    image_url: str | None
    international: International | None

    lineage: list[str | None] | None

    repositories: list[Repository | None]
    roles: list[Role | None]
    ror: str | None

    x_concepts: list[DehydratedConcept | None] | None

    type_id: str | None
    topics: list[TopicCount | None]
    topic_share: list[TopicShare | None]


@dataclass
class Publisher(BaseOpenAlex):
    hierarchy_level: int | None
    ids: PublisherIds
    sources_api_url: str
    summary_stats: dict[str, float | int] | None

    alternate_titles: list[str | None]
    country_codes: list[str | None] | None

    counts_by_year: list[YearCount | None]
    image_thumbnail_url: str | None
    image_url: str | None

    lineage: list[str | None] | None

    parent_publisher: BaseOpenAlex | None
    roles: list[Role | None] | None
    homepage_url: str | None


@dataclass
class Funder(BaseOpenAlex):
    ids: FunderIds
    grants_count: int
    summary_stats: dict[str, float | int | None]
    alternate_titles: list[str | None]
    country_code: str | None
    counts_by_year: list[YearCount | None]
    description: str | None
    homepage_url: str | None
    image_thumbnail_url: str | None
    image_url: str | None
    roles: list[Role | None]


@dataclass
class Concept(BaseOpenAlex):
    ids: ConceptIds
    level: int
    summary_stats: dict[str, float | int] | None
    wikidata: str

    counts_by_year: list[YearCount | None] | None
    description: str | None

    international: International | None
    related_concepts: list[DehydratedConcept | None] | None
    image_url: str | None
    image_thumbnail_url: str | None
    ancestors: list[SimpleDehydratedConcept | None] | None


@dataclass
class Work(BaseOpenAlex):
    title: str | None
    publication_year: int
    publication_date: str
    doi: str | None
    ids: WorkIds
    type: WorkType
    open_access: OpenAccess

    has_fulltext: bool | None
    is_paratext: bool
    is_retracted: bool

    locations_count: int
    cited_by_count: int
    countries_distinct_count: int
    institutions_distinct_count: int

    institution_assertions: list[str | None] | None
    funders: list[DehydratedFunder | None] | None
    institutions: list[DehydratedInstitution | None] | None
    is_xpac: bool | None
    awards: list[str | None] | None

    abstract_inverted_index: dict[str, list[int]] | None
    authorships: list[Authorship | None]
    apc_list: APCData | None
    apc_paid: APCData | None
    best_oa_location: Location | None
    biblio: Biblio | None
    citation_normalized_percentile: CitationNormalizedPercentile | None
    cited_by_api_url: str | None
    concepts: list[DehydratedConcept | None]
    corresponding_author_ids: list[str | None]
    corresponding_institution_ids: list[str | None]
    counts_by_year: list[YearCountBasic | None]
    fulltext_origin: Literal["pdf", "ngrams"] | None
    fwci: float | None
    grants: list[Grant | None]
    indexed_in: list[Literal["arxiv", "crossref", "doaj", "pubmed", "datacite"] | None]
    keywords: list[DehydratedKeyword | None]
    language: str | None
    license: str | None
    locations: list[Location | None]
    mesh: list[Mesh | None]
    primary_location: Location | None
    primary_topic: DehydratedTopic | None
    referenced_works: list[str | None]
    related_works: list[str | None]
    sustainable_development_goals: list[SDG | None]
    topics: list[DehydratedTopic | None]
    type_crossref: WorkTypeCrossref | None
    has_content: HasContent | None

    cited_by_percentile_year: dict[str, int] | None
    datasets: list | None
    versions: list[str | None] | None
    referenced_works_count: int | None


T = TypeVar("T", bound=BaseOpenAlex)


@dataclass
class Meta:
    count: int
    db_response_time_ms: int
    page: int
    per_page: int
    groups_count: int | None
    next_cursor: str | None

    @classmethod
    def from_dict(cls, data: dict) -> Meta:
        return from_dict(data_class=cls, data=data, config=production_config)


@dataclass
class Response[T: BaseOpenAlex]:
    meta: Meta
    results: list[T | None]

    @classmethod
    def from_dict(cls, data: dict, result_type: type[T] | None) -> Response[T]:
        raw_meta = data.get("meta")
        if raw_meta is None:
            raise ValueError("Missing 'meta' field in response data")
        meta = Meta.from_dict(raw_meta)
        raw_results = data.get("results", [])
        if result_type:
            parsed = [
                None
                if r is None
                else from_dict(data_class=result_type, data=r, config=production_config)
                for r in raw_results
            ]
            return cls(meta=meta, results=parsed)
        return from_dict(data_class=cls, data=data, config=production_config)
