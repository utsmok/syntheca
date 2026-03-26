"""Stable output groups and file contracts for Syntheca pipeline outputs.

Each output group defines a logical set of related files produced by the
pipeline.  The file contracts enumerate expected filenames and their column
schemas so that downstream consumers can rely on a stable interface.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class OutputGroupName(StrEnum):
    """Enum of recognised output groups."""

    CORE_DATA = "core_data"
    COMPARISON = "comparison"
    COAUTHORSHIP = "coauthorship"
    POLICY_CITATIONS = "policy_citations"


@dataclass(frozen=True)
class FileContract:
    """Contract for a single output file within a group.

    Attributes:
        filename: Expected file name (relative to the group subdirectory).
        required_columns: Columns that MUST be present in the output.
        description: Human-readable description of the file contents.
    """

    filename: str
    required_columns: list[str] = field(default_factory=list)
    description: str = ""


@dataclass(frozen=True)
class OutputGroup:
    """Definition of an output group, its files, and documentation pointers.

    Attributes:
        name: Group identifier (matches :class:`OutputGroupName`).
        description: Human-readable description of the group.
        subdirectory: Subdirectory under the output root where files are written.
            An empty string means files live directly in the output root.
        files: File contracts belonging to this group.
        schema_reference: Pointer to the module or constant defining schema details.
    """

    name: OutputGroupName
    description: str
    subdirectory: str
    files: list[FileContract] = field(default_factory=list)
    schema_reference: str = ""


# ---------------------------------------------------------------------------
# File contracts per group
# ---------------------------------------------------------------------------

_CORE_FILES = [
    FileContract(
        filename="merged.parquet",
        required_columns=[
            "doi",
            "title",
            "publication_year",
            "type",
        ],
        description="Merged and deduplicated publications in Parquet format.",
    ),
    FileContract(
        filename="merged.xlsx",
        required_columns=[
            "doi",
            "title",
            "publication_year",
            "type",
        ],
        description="Merged and deduplicated publications as a formatted Excel workbook.",
    ),
]

_COMPARISON_FILES = [
    FileContract(
        filename="scopus_matched.parquet",
        required_columns=["doi"],
        description="Records matched between Scopus export and internal data.",
    ),
    FileContract(
        filename="scopus_only.parquet",
        required_columns=["doi"],
        description="Records present only in the Scopus export.",
    ),
    FileContract(
        filename="internal_only.parquet",
        required_columns=["doi"],
        description="Records present only in the internal Syntheca data.",
    ),
    FileContract(
        filename="scopus_mismatches.parquet",
        required_columns=["doi"],
        description="Matched records with field-level mismatches.",
    ),
]

_COAUTHORSHIP_FILES = [
    FileContract(
        filename="author_publication_links.parquet",
        required_columns=[
            "work_id",
            "work_doi",
            "author_name",
            "author_position",
        ],
        description="Publication-to-author link table.",
    ),
    FileContract(
        filename="coauthor_edges.parquet",
        required_columns=[
            "author_a_name",
            "author_b_name",
            "shared_works_count",
        ],
        description="Co-author pair edge table with shared-work counts.",
    ),
    FileContract(
        filename="ut_vs_external.parquet",
        required_columns=["collab_type", "edge_count"],
        description="UT-internal vs external collaboration summary.",
    ),
    FileContract(
        filename="university_rollup.parquet",
        required_columns=["org_type", "edge_count"],
        description="Edges grouped by university-type org affiliation.",
    ),
    FileContract(
        filename="company_rollup.parquet",
        required_columns=["org_type", "edge_count"],
        description="Edges involving company-affiliated authors.",
    ),
    FileContract(
        filename="country_rollup.parquet",
        required_columns=["country_a", "country_b", "edge_count"],
        description="Edges grouped by author country pairs.",
    ),
]

_POLICY_CITATIONS_FILES = [
    FileContract(
        filename="policy_candidates.csv",
        required_columns=[
            "openalex_id",
            "title",
            "confidence",
            "needs_review",
        ],
        description="Policy-citation candidates sorted by confidence.",
    ),
    FileContract(
        filename="policy_review_queue.xlsx",
        required_columns=[
            "openalex_id",
            "title",
            "confidence",
            "needs_review",
            "review_status",
        ],
        description="Human-review queue for borderline policy candidates.",
    ),
]

# ---------------------------------------------------------------------------
# Group definitions
# ---------------------------------------------------------------------------

CORE_DATA = OutputGroup(
    name=OutputGroupName.CORE_DATA,
    description=("Normalized publications, persons, and organizations — the main pipeline output."),
    subdirectory="",
    files=_CORE_FILES,
    schema_reference="syntheca.config.output_contract.STABLE_COLUMNS",
)

COMPARISON = OutputGroup(
    name=OutputGroupName.COMPARISON,
    description="Scopus/SciVal export comparison results.",
    subdirectory="comparison",
    files=_COMPARISON_FILES,
    schema_reference="syntheca.comparison.scopus.ComparisonResult",
)

COAUTHORSHIP = OutputGroup(
    name=OutputGroupName.COAUTHORSHIP,
    description="Co-authorship edge tables and collaboration rollups.",
    subdirectory="coauthorship",
    files=_COAUTHORSHIP_FILES,
    schema_reference="syntheca.analysis.coauthorship.CoauthorshipReport",
)

POLICY_CITATIONS = OutputGroup(
    name=OutputGroupName.POLICY_CITATIONS,
    description="Policy-citation candidates and human-review queue.",
    subdirectory="policy_citations",
    files=_POLICY_CITATIONS_FILES,
    schema_reference="syntheca.analysis.policy_citations.PolicyCitationReport",
)

ALL_GROUPS: list[OutputGroup] = [CORE_DATA, COMPARISON, COAUTHORSHIP, POLICY_CITATIONS]

GROUP_REGISTRY: dict[OutputGroupName, OutputGroup] = {g.name: g for g in ALL_GROUPS}
