"""Policy-document citation investigation.

Identifies citing works that may be policy documents — government reports,
standards, parliamentary records, international-organisation publications —
and produces a human-reviewable queue of candidates.

The classification pipeline is intentionally *recall-oriented*: it is better
to surface a borderline candidate for manual review than to miss a genuine
policy citation.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from syntheca.clients.openaire import OpenAIREClient
from syntheca.clients.openalex import OpenAlexClient
from syntheca.utils.polars_frames import robust_from_dicts

# ---------------------------------------------------------------------------
# Classification configuration — all thresholds are explicit & configurable
# ---------------------------------------------------------------------------

#: OpenAlex work types considered inherently policy-relevant.
DEFAULT_POLICY_WORK_TYPES: set[str] = {
    "report",
    "standard",
    "government-document",
    "regulation",
    "legal-case",
    "bill",
    "grant",
    "statute",
}

#: Venue name substrings that signal policy context (case-insensitive).
DEFAULT_VENUE_KEYWORDS: set[str] = {
    "policy",
    "government",
    "parliament",
    "congressional",
    "ministry",
    "commission",
    "regulation",
    "gazette",
    "legislative",
    "white paper",
    "green paper",
    "official journal",
    "federal register",
    "staatscourant",
    "staatsblad",
    "eur-lex",
}

#: Publisher name substrings that signal policy context (case-insensitive).
DEFAULT_PUBLISHER_KEYWORDS: set[str] = {
    "european commission",
    "european parliament",
    "world health organization",
    "united nations",
    "oecd",
    "world bank",
    "imf",
    "international monetary fund",
    "amnesty international",
    "ministry",
    "government",
    "parliament",
    "nwo",
    "rijksoverheid",
    "who",
    "nato",
    "unesco",
    "gao",
    "national audit",
    "public health",
    "ngo",
}

#: Title substrings that contribute to policy signal (case-insensitive).
DEFAULT_TITLE_KEYWORDS: set[str] = {
    "policy",
    "government",
    "parliament",
    "regulation",
    "legislation",
    "guideline",
    "white paper",
    "green paper",
    "recommendation",
    "directive",
    "national strategy",
    "impact assessment",
    "public consultation",
    "ministerial",
    "official report",
    "ngo",
}

# ---------------------------------------------------------------------------
# Scoring weights — how much each signal contributes to confidence
# ---------------------------------------------------------------------------

DEFAULT_SCORE_WEIGHTS: dict[str, float] = {
    "work_type": 0.45,
    "venue": 0.20,
    "publisher": 0.25,
    "title": 0.10,
}

#: Minimum aggregate score to classify a work as a policy candidate.
DEFAULT_MIN_CONFIDENCE: float = 0.20

#: Candidates below this confidence are flagged for manual review.
DEFAULT_REVIEW_THRESHOLD: float = 0.50

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class PolicyDocumentCandidate(BaseModel):
    """A citing work classified as a potential policy document."""

    openalex_id: str
    doi: str | None = None
    title: str
    type: str
    venue: str | None = None
    publisher: str | None = None
    publication_year: int | None = None
    cited_work_id: str = Field(description="The UT publication that was cited.")
    evidence: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    needs_review: bool = True
    review_status: str = "pending"


# ---------------------------------------------------------------------------
# Report dataclass
# ---------------------------------------------------------------------------


@dataclass
class PolicyCitationReport:
    """Aggregated results of a policy-citation investigation run."""

    candidates: list[PolicyDocumentCandidate] = field(default_factory=list)
    total_citing_works_checked: int = 0
    total_candidates_found: int = 0
    needs_review_count: int = 0
    summary: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Refresh derived counts after initialization."""
        self._refresh_counts()

    def _refresh_counts(self) -> None:
        self.total_candidates_found = len(self.candidates)
        self.needs_review_count = sum(1 for c in self.candidates if c.needs_review)


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------


class PolicyClassifier:
    """Heuristic classifier that decides whether a citing work is policy-related.

    All keyword sets, score weights, and thresholds are configurable via
    constructor arguments.
    """

    def __init__(
        self,
        *,
        policy_work_types: set[str] | None = None,
        venue_keywords: set[str] | None = None,
        publisher_keywords: set[str] | None = None,
        title_keywords: set[str] | None = None,
        score_weights: dict[str, float] | None = None,
        min_confidence: float = DEFAULT_MIN_CONFIDENCE,
        review_threshold: float = DEFAULT_REVIEW_THRESHOLD,
    ) -> None:
        """Initialize with configurable keyword sets and thresholds."""
        self.policy_work_types = policy_work_types or DEFAULT_POLICY_WORK_TYPES
        self.venue_keywords = venue_keywords or DEFAULT_VENUE_KEYWORDS
        self.publisher_keywords = publisher_keywords or DEFAULT_PUBLISHER_KEYWORDS
        self.title_keywords = title_keywords or DEFAULT_TITLE_KEYWORDS
        self.score_weights = score_weights or DEFAULT_SCORE_WEIGHTS
        self.min_confidence = min_confidence
        self.review_threshold = review_threshold

    # -- helpers ----------------------------------------------------------

    @staticmethod
    def _matches_any(text: str | None, keywords: set[str]) -> list[str]:
        """Return list of matched keywords found in *text* (case-insensitive)."""
        if not text:
            return []
        lower = text.lower()
        return [kw for kw in keywords if kw in lower]

    def _extract_venue(self, work: dict) -> str | None:
        loc = work.get("primary_location") or {}
        src = loc.get("source") or {}
        return src.get("display_name")

    def _extract_publisher(self, work: dict) -> str | None:
        loc = work.get("primary_location") or {}
        src = loc.get("source") or {}
        return src.get("host_organization_name")

    # -- main classify method ---------------------------------------------

    def classify(self, work: dict, *, cited_work_id: str = "") -> PolicyDocumentCandidate | None:
        """Classify a single citing-work dict as a policy candidate or *None*.

        Args:
            work: Raw OpenAlex work dict.
            cited_work_id: The OpenAlex ID of the UT publication that was cited.

        Returns:
            A :class:`PolicyDocumentCandidate` if the work passes the
            minimum confidence threshold, otherwise ``None``.
        """
        evidence: list[str] = []
        scores: dict[str, float] = {}

        work_type = (work.get("type") or "").lower()
        if work_type in self.policy_work_types:
            evidence.append(f"work_type={work_type}")
            scores["work_type"] = 1.0

        venue = self._extract_venue(work)
        venue_hits = self._matches_any(venue, self.venue_keywords)
        if venue_hits:
            evidence.append(f"venue matches: {', '.join(venue_hits)}")
            scores["venue"] = 1.0

        publisher = self._extract_publisher(work)
        pub_hits = self._matches_any(publisher, self.publisher_keywords)
        if pub_hits:
            evidence.append(f"publisher matches: {', '.join(pub_hits)}")
            scores["publisher"] = 1.0

        title = work.get("title") or work.get("display_name") or ""
        title_hits = self._matches_any(title, self.title_keywords)
        if title_hits:
            evidence.append(f"title matches: {', '.join(title_hits)}")
            scores["title"] = 1.0

        # weighted aggregate confidence
        confidence = sum(scores.get(k, 0.0) * w for k, w in self.score_weights.items())
        confidence = round(min(confidence, 1.0), 4)

        if confidence < self.min_confidence:
            return None

        needs_review = confidence < self.review_threshold

        return PolicyDocumentCandidate(
            openalex_id=work.get("id", ""),
            doi=work.get("doi"),
            title=title,
            type=work_type,
            venue=venue,
            publisher=publisher,
            publication_year=work.get("publication_year"),
            cited_work_id=cited_work_id,
            evidence=evidence,
            confidence=confidence,
            needs_review=needs_review,
            review_status="pending",
        )


# ---------------------------------------------------------------------------
# Investigator
# ---------------------------------------------------------------------------


class PolicyCitationInvestigator:
    """Orchestrates citing-work retrieval and policy classification.

    Uses :class:`OpenAlexClient` to fetch citing works and runs each through
    the :class:`PolicyClassifier`.  An optional :class:`OpenAIREClient` is
    used for secondary lookup on low-confidence candidates.
    """

    def __init__(
        self,
        openalex_client: OpenAlexClient,
        classifier: PolicyClassifier,
        openaire_client: OpenAIREClient | None = None,
    ) -> None:
        """Initialize with required OpenAlex client, classifier, and optional OpenAIRE client."""
        self.openalex = openalex_client
        self.classifier = classifier
        self.openaire = openaire_client

    async def investigate(self, openalex_ids: list[str]) -> PolicyCitationReport:
        """Investigate citing works for a list of UT publication OpenAlex IDs.

        Args:
            openalex_ids: OpenAlex work IDs (full URLs or short IDs).

        Returns:
            A :class:`PolicyCitationReport` with all candidates.
        """
        all_candidates: list[PolicyDocumentCandidate] = []
        total_checked = 0

        for oa_id in openalex_ids:
            citing_works = await self.openalex.get_citing_works(oa_id)
            total_checked += len(citing_works)

            for cw in citing_works:
                candidate = self.classifier.classify(cw, cited_work_id=oa_id)
                if candidate is not None:
                    all_candidates.append(candidate)

        # Optional OpenAIRE secondary lookup for low-confidence candidates
        if self.openaire is not None:
            for candidate in all_candidates:
                if candidate.needs_review and candidate.doi:
                    try:
                        products = await self.openaire.get_research_products(doi=candidate.doi)
                        if products:
                            candidate.evidence.append("openaire: found in OpenAIRE Graph")
                    except Exception:
                        # secondary lookup is best-effort
                        pass

        report = PolicyCitationReport(
            candidates=all_candidates,
            total_citing_works_checked=total_checked,
        )
        report._refresh_counts()
        report.summary = {
            "openalex_ids_investigated": len(openalex_ids),
            "total_citing_works_checked": total_checked,
            "total_candidates_found": report.total_candidates_found,
            "needs_review_count": report.needs_review_count,
            "high_confidence_count": sum(1 for c in all_candidates if not c.needs_review),
        }
        return report


# ---------------------------------------------------------------------------
# Review-queue export
# ---------------------------------------------------------------------------


def export_review_queue(report: PolicyCitationReport, output_path: Path) -> None:
    """Export candidates as a CSV (or Excel) file for human review.

    Candidates are sorted by confidence ascending (lowest-confidence first)
    so that the items most needing review appear at the top.

    The format is chosen based on the file extension:
    - ``.csv`` -- CSV
    - ``.xlsx`` -- Excel (requires *openpyxl*)
    - anything else -- CSV

    Args:
        report: The investigation report.
        output_path: Destination file path.
    """
    sorted_candidates = sorted(report.candidates, key=lambda c: c.confidence)

    rows = [c.model_dump() for c in sorted_candidates]
    # flatten evidence list to a semicolon-separated string
    for row in rows:
        row["evidence"] = "; ".join(row.get("evidence", []))

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.suffix == ".xlsx":
        try:
            import openpyxl  # noqa: F401 - presence check

            df = robust_from_dicts(rows)
            df.write_excel(output_path)
        except ImportError:
            # fall back to CSV if openpyxl is unavailable
            _write_csv(rows, output_path.with_suffix(".csv"))
    else:
        _write_csv(rows, output_path)


def _write_csv(rows: list[dict], path: Path) -> None:
    """Write rows to a CSV file."""
    if not rows:
        path.write_text("")
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
