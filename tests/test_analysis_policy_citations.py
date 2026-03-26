"""Tests for policy-citation investigation module."""

from __future__ import annotations

import json
import pathlib
from unittest.mock import AsyncMock

import pytest

from syntheca.analysis.policy_citations import (
    PolicyCitationInvestigator,
    PolicyCitationReport,
    PolicyClassifier,
    PolicyDocumentCandidate,
    export_review_queue,
)
from syntheca.clients.openalex import OpenAlexClient

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "openalex"


@pytest.fixture
def citing_works() -> list[dict]:
    """Load the citing-works fixture and return the results list."""
    data = json.loads((FIXTURES / "citing_works_response.json").read_text())
    return data["results"]


@pytest.fixture
def classifier() -> PolicyClassifier:
    return PolicyClassifier()


# -----------------------------------------------------------------------
# PolicyDocumentCandidate model
# -----------------------------------------------------------------------


class TestPolicyDocumentCandidate:
    def test_create_minimal(self):
        c = PolicyDocumentCandidate(
            openalex_id="https://openalex.org/W1",
            title="Test",
            type="report",
            cited_work_id="W0",
            confidence=0.5,
        )
        assert c.review_status == "pending"
        assert c.needs_review is True

    def test_confidence_bounds(self):
        """Confidence must be in [0.0, 1.0]."""
        c = PolicyDocumentCandidate(
            openalex_id="W1",
            title="X",
            type="report",
            cited_work_id="W0",
            confidence=0.0,
        )
        assert c.confidence == 0.0

        c2 = PolicyDocumentCandidate(
            openalex_id="W1",
            title="X",
            type="report",
            cited_work_id="W0",
            confidence=1.0,
        )
        assert c2.confidence == 1.0

        with pytest.raises(ValueError):
            PolicyDocumentCandidate(
                openalex_id="W1",
                title="X",
                type="report",
                cited_work_id="W0",
                confidence=1.5,
            )

        with pytest.raises(ValueError):
            PolicyDocumentCandidate(
                openalex_id="W1",
                title="X",
                type="report",
                cited_work_id="W0",
                confidence=-0.1,
            )

    def test_evidence_field(self):
        c = PolicyDocumentCandidate(
            openalex_id="W1",
            title="X",
            type="report",
            cited_work_id="W0",
            confidence=0.5,
            evidence=["work_type=report", "venue matches: policy"],
        )
        assert len(c.evidence) == 2


# -----------------------------------------------------------------------
# PolicyClassifier
# -----------------------------------------------------------------------


class TestPolicyClassifier:
    def test_regular_article_not_classified(self, classifier: PolicyClassifier):
        work = {
            "id": "https://openalex.org/W1",
            "title": "A great peer-reviewed study on optics",
            "type": "article",
            "primary_location": {
                "source": {
                    "display_name": "Nature Photonics",
                    "host_organization_name": "Springer Nature",
                }
            },
        }
        assert classifier.classify(work, cited_work_id="W0") is None

    def test_report_type_classified(self, classifier: PolicyClassifier):
        work = {
            "id": "https://openalex.org/W2",
            "title": "Annual Report on Research Trends",
            "type": "report",
            "primary_location": {
                "source": {
                    "display_name": "Some Repository",
                    "host_organization_name": "Acme Inc",
                }
            },
        }
        result = classifier.classify(work, cited_work_id="W0")
        assert result is not None
        assert "work_type=report" in result.evidence

    def test_government_venue_classified(self, classifier: PolicyClassifier):
        work = {
            "id": "https://openalex.org/W3",
            "title": "Something",
            "type": "article",
            "primary_location": {
                "source": {
                    "display_name": "Government Policy Gazette",
                    "host_organization_name": "Publisher X",
                }
            },
        }
        result = classifier.classify(work, cited_work_id="W0")
        assert result is not None
        assert any("venue" in e for e in result.evidence)

    def test_government_publisher_classified(self, classifier: PolicyClassifier):
        work = {
            "id": "https://openalex.org/W4",
            "title": "Technical Annex",
            "type": "article",
            "primary_location": {
                "source": {
                    "display_name": "Document Series",
                    "host_organization_name": "European Commission DG RTD",
                }
            },
        }
        result = classifier.classify(work, cited_work_id="W0")
        assert result is not None
        assert any("publisher" in e for e in result.evidence)

    def test_title_keyword_classified(self):
        """Title keywords alone (weight 0.10) are below default threshold.

        Use a lower min_confidence to verify title-keyword detection works.
        """
        lenient = PolicyClassifier(min_confidence=0.05)
        work = {
            "id": "https://openalex.org/W5",
            "title": "National Strategy for Open Science Policy Implementation",
            "type": "article",
            "primary_location": {
                "source": {
                    "display_name": "Random Journal",
                    "host_organization_name": "Random Publisher",
                }
            },
        }
        result = lenient.classify(work, cited_work_id="W0")
        assert result is not None
        assert any("title" in e for e in result.evidence)

    def test_combined_signals_boost_confidence(self, classifier: PolicyClassifier):
        work = {
            "id": "https://openalex.org/W6",
            "title": "Policy Recommendation on Research Data",
            "type": "report",
            "primary_location": {
                "source": {
                    "display_name": "Government Reports",
                    "host_organization_name": "European Commission",
                }
            },
        }
        result = classifier.classify(work, cited_work_id="W0")
        assert result is not None
        assert result.confidence > 0.5

    def test_custom_thresholds(self):
        strict = PolicyClassifier(min_confidence=0.90)
        work = {
            "id": "W7",
            "title": "Some policy discussion",
            "type": "article",
            "primary_location": {
                "source": {
                    "display_name": "Journal A",
                    "host_organization_name": "Pub A",
                }
            },
        }
        # Only a title-keyword hit → 0.10 confidence → below 0.90
        assert strict.classify(work, cited_work_id="W0") is None

    def test_custom_keywords(self):
        custom = PolicyClassifier(title_keywords={"custom-keyword-xyz"}, min_confidence=0.05)
        work = {
            "id": "W8",
            "title": "Analysis of custom-keyword-xyz implications",
            "type": "article",
            "primary_location": {"source": {}},
        }
        result = custom.classify(work, cited_work_id="W0")
        assert result is not None

    def test_empty_work_not_classified(self, classifier: PolicyClassifier):
        assert classifier.classify({}, cited_work_id="W0") is None

    def test_needs_review_flag(self, classifier: PolicyClassifier):
        """Works at or above review_threshold should not need review."""
        work = {
            "id": "W9",
            "title": "Policy recommendation on directive compliance",
            "type": "report",
            "primary_location": {
                "source": {
                    "display_name": "Government reports",
                    "host_organization_name": "European Commission",
                }
            },
        }
        result = classifier.classify(work, cited_work_id="W0")
        assert result is not None
        # Has type + venue + publisher + title signals → high confidence
        assert not result.needs_review

    def test_fixture_classification(self, classifier: PolicyClassifier, citing_works: list[dict]):
        """Run classifier on fixture data and verify expected behaviour."""
        candidates = []
        for cw in citing_works:
            c = classifier.classify(cw, cited_work_id="W_UT")
            if c is not None:
                candidates.append(c)

        # The fixture has several policy-type works
        assert len(candidates) >= 3
        for c in candidates:
            assert 0.0 <= c.confidence <= 1.0
            assert len(c.evidence) > 0


# -----------------------------------------------------------------------
# PolicyCitationReport
# -----------------------------------------------------------------------


class TestPolicyCitationReport:
    def test_empty_report(self):
        r = PolicyCitationReport()
        assert r.total_candidates_found == 0
        assert r.needs_review_count == 0

    def test_report_counts(self):
        cands = [
            PolicyDocumentCandidate(
                openalex_id="W1",
                title="A",
                type="report",
                cited_work_id="W0",
                confidence=0.3,
                needs_review=True,
            ),
            PolicyDocumentCandidate(
                openalex_id="W2",
                title="B",
                type="report",
                cited_work_id="W0",
                confidence=0.9,
                needs_review=False,
            ),
        ]
        r = PolicyCitationReport(candidates=cands, total_citing_works_checked=10)
        assert r.total_candidates_found == 2
        assert r.needs_review_count == 1
        assert r.total_citing_works_checked == 10


# -----------------------------------------------------------------------
# Review-queue export
# -----------------------------------------------------------------------


class TestExportReviewQueue:
    def test_export_csv(self, tmp_path: pathlib.Path):
        cands = [
            PolicyDocumentCandidate(
                openalex_id="W1",
                title="High-conf",
                type="report",
                cited_work_id="W0",
                confidence=0.9,
                evidence=["work_type=report"],
                needs_review=False,
            ),
            PolicyDocumentCandidate(
                openalex_id="W2",
                title="Low-conf",
                type="other",
                cited_work_id="W0",
                confidence=0.25,
                evidence=["venue matches: policy"],
                needs_review=True,
            ),
        ]
        report = PolicyCitationReport(candidates=cands, total_citing_works_checked=50)
        out = tmp_path / "review.csv"
        export_review_queue(report, out)
        assert out.exists()
        content = out.read_text()
        lines = content.strip().splitlines()
        # header + 2 data rows
        assert len(lines) == 3
        # lowest confidence first
        assert "Low-conf" in lines[1]
        assert "High-conf" in lines[2]

    def test_export_empty_report(self, tmp_path: pathlib.Path):
        report = PolicyCitationReport()
        out = tmp_path / "empty.csv"
        export_review_queue(report, out)
        assert out.exists()

    def test_export_xlsx(self, tmp_path: pathlib.Path):
        cands = [
            PolicyDocumentCandidate(
                openalex_id="W1",
                title="Test",
                type="report",
                cited_work_id="W0",
                confidence=0.5,
                evidence=["work_type=report"],
            ),
        ]
        report = PolicyCitationReport(candidates=cands, total_citing_works_checked=5)
        out = tmp_path / "review.xlsx"
        export_review_queue(report, out)
        # should create the xlsx (openpyxl is a project dependency)
        assert out.exists()


# -----------------------------------------------------------------------
# Citing-works retrieval (OpenAlexClient.get_citing_works)
# -----------------------------------------------------------------------


class TestGetCitingWorks:
    @pytest.mark.asyncio
    async def test_single_page(self):
        fixture = json.loads((FIXTURES / "citing_works_response.json").read_text())

        async def handler(request):
            from httpx import Response

            return Response(200, json=fixture)

        from httpx import MockTransport

        transport = MockTransport(handler)
        client = OpenAlexClient()
        client.client = client.client.__class__(transport=transport)

        results = await client.get_citing_works("https://openalex.org/W1000000001")
        assert len(results) == 7

    @pytest.mark.asyncio
    async def test_cursor_pagination(self):
        """Simulate two pages of cursor-paginated results."""
        page1 = {
            "meta": {"next_cursor": "abc123"},
            "results": [{"id": "W1", "title": "Page 1 result", "type": "article"}],
        }
        page2 = {
            "meta": {"next_cursor": None},
            "results": [{"id": "W2", "title": "Page 2 result", "type": "article"}],
        }
        pages = iter([page1, page2])

        async def handler(request):
            from httpx import Response

            return Response(200, json=next(pages))

        from httpx import MockTransport

        transport = MockTransport(handler)
        client = OpenAlexClient()
        client.client = client.client.__class__(transport=transport)

        results = await client.get_citing_works("W_TEST")
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_empty_results(self):
        empty = {"meta": {"next_cursor": None}, "results": []}

        async def handler(request):
            from httpx import Response

            return Response(200, json=empty)

        from httpx import MockTransport

        transport = MockTransport(handler)
        client = OpenAlexClient()
        client.client = client.client.__class__(transport=transport)

        results = await client.get_citing_works("W_NOCITES")
        assert results == []


# -----------------------------------------------------------------------
# PolicyCitationInvestigator
# -----------------------------------------------------------------------


class TestPolicyCitationInvestigator:
    @pytest.mark.asyncio
    async def test_investigate_basic(self, citing_works: list[dict]):
        client = OpenAlexClient()
        client.get_citing_works = AsyncMock(return_value=citing_works)

        classifier = PolicyClassifier()
        investigator = PolicyCitationInvestigator(client, classifier)

        report = await investigator.investigate(["https://openalex.org/W1000000001"])
        assert isinstance(report, PolicyCitationReport)
        assert report.total_citing_works_checked == len(citing_works)
        assert report.total_candidates_found >= 1
        assert "openalex_ids_investigated" in report.summary

    @pytest.mark.asyncio
    async def test_investigate_multiple_ids(self):
        client = OpenAlexClient()
        client.get_citing_works = AsyncMock(return_value=[])

        classifier = PolicyClassifier()
        investigator = PolicyCitationInvestigator(client, classifier)

        report = await investigator.investigate(["W1", "W2", "W3"])
        assert report.total_citing_works_checked == 0
        assert report.total_candidates_found == 0
        assert report.summary["openalex_ids_investigated"] == 3

    @pytest.mark.asyncio
    async def test_investigate_with_openaire(self, citing_works: list[dict]):
        """OpenAIRE secondary lookup adds evidence to candidates."""
        from syntheca.clients.openaire import OpenAIREClient

        oa_client = OpenAlexClient()
        oa_client.get_citing_works = AsyncMock(return_value=citing_works)

        openaire_client = OpenAIREClient()
        openaire_client.get_research_products = AsyncMock(return_value=["dummy"])

        classifier = PolicyClassifier()
        investigator = PolicyCitationInvestigator(
            oa_client, classifier, openaire_client=openaire_client
        )

        report = await investigator.investigate(["W1"])
        # Check that openaire evidence was added to candidates that need review and have a DOI
        for c in report.candidates:
            if c.needs_review and c.doi:
                assert any("openaire" in e for e in c.evidence)
