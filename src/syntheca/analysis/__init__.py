"""Analysis modules for Syntheca metadata outputs.

This package contains analytical functions that operate on canonical
records and produce structured report artefacts (DataFrames, summaries).
"""

from syntheca.analysis.coauthorship import (
    CoauthorshipReport,
    build_author_publication_links,
    build_coauthor_edges,
    build_collaboration_rollups,
    generate_coauthorship_report,
)
from syntheca.analysis.policy_citations import (
    PolicyCitationInvestigator,
    PolicyCitationReport,
    PolicyClassifier,
    PolicyDocumentCandidate,
    export_review_queue,
)

__all__ = [
    "CoauthorshipReport",
    "PolicyCitationInvestigator",
    "PolicyCitationReport",
    "PolicyClassifier",
    "PolicyDocumentCandidate",
    "build_author_publication_links",
    "build_coauthor_edges",
    "build_collaboration_rollups",
    "export_review_queue",
    "generate_coauthorship_report",
]
