"""Tests for the OpenAIRE Graph API client, models, adapters, and provider."""

from __future__ import annotations

import json
import pathlib

import pytest
from httpx import MockTransport, Response

from syntheca.clients.openaire import OpenAIREClient
from syntheca.config.source_precedence import Source
from syntheca.models.adapters import openaire_org_to_canonical, openaire_product_to_canonical
from syntheca.models.canonical import CanonicalOrganization, CanonicalWork
from syntheca.models.openaire import (
    OpenAIREOrganization,
    OpenAIREResearchProduct,
    OrganizationSearchResponse,
    ResearchProductSearchResponse,
    SearchHeader,
)
from syntheca.providers import DataProvider
from syntheca.providers.openaire_provider import OpenAIREProvider

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "openaire"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


# ---------------------------------------------------------------------------
# Model parsing
# ---------------------------------------------------------------------------


class TestOpenAIREModels:
    """Validate that Pydantic models parse the Graph API JSON correctly."""

    def test_search_header(self):
        raw = {"numFound": 5, "maxScore": 3.2, "queryTime": 10, "pageSize": 10, "nextCursor": "abc"}
        header = SearchHeader.model_validate(raw)
        assert header.num_found == 5
        assert header.next_cursor == "abc"

    def test_research_product_response(self):
        fixture = _load_fixture("research_product_response.json")
        resp = ResearchProductSearchResponse.model_validate(fixture)
        assert resp.header.num_found == 2
        assert len(resp.results) == 2

        first = resp.results[0]
        assert first.main_title == "Assembly theory explains and quantifies selection and evolution"
        assert first.type == "publication"
        assert first.publisher == "Springer Science and Business Media LLC"
        assert first.publication_date == "2023-10-04"
        assert len(first.authors) == 2
        assert first.authors[0].full_name == "Abhishek Sharma"
        assert first.open_access_color == "hybrid"
        assert first.best_access_right is not None
        assert first.best_access_right.label == "OPEN"

    def test_research_product_pids(self):
        fixture = _load_fixture("research_product_response.json")
        resp = ResearchProductSearchResponse.model_validate(fixture)
        pids = resp.results[0].pids
        doi_pids = [p for p in pids if p.scheme == "doi"]
        assert len(doi_pids) == 1
        assert doi_pids[0].value == "10.1038/s41586-023-06600-9"

    def test_research_product_subjects(self):
        fixture = _load_fixture("research_product_response.json")
        resp = ResearchProductSearchResponse.model_validate(fixture)
        subjects = resp.results[0].subjects
        assert len(subjects) == 2
        assert subjects[0].subject.value == "Assembly theory"

    def test_research_product_indicators(self):
        fixture = _load_fixture("research_product_response.json")
        resp = ResearchProductSearchResponse.model_validate(fixture)
        indicators = resp.results[0].indicators
        assert indicators is not None
        assert indicators.bip_indicators.citation_count == 150.0
        assert indicators.usage_counts.downloads == 5200

    def test_organization_response(self):
        raw = {
            "header": {"numFound": 1, "maxScore": 10.0, "queryTime": 50, "page": 1, "pageSize": 10},
            "results": [
                {
                    "id": "pending_org_::abc123",
                    "legalShortName": "UT",
                    "legalName": "University of Twente",
                    "websiteUrl": "https://www.utwente.nl",
                    "alternativeNames": ["Universiteit Twente"],
                    "country": {"code": "NL", "label": "Netherlands"},
                    "pids": [{"scheme": "ROR", "value": "https://ror.org/006hf6230"}],
                    "originalIds": ["pending_org_::abc123"],
                }
            ],
        }
        resp = OrganizationSearchResponse.model_validate(raw)
        assert len(resp.results) == 1
        org = resp.results[0]
        assert org.legal_name == "University of Twente"
        assert org.country.code == "NL"
        assert org.pids[0].scheme == "ROR"

    def test_minimal_product_parses(self):
        """A product with many null/empty fields should still parse."""
        fixture = _load_fixture("research_product_response.json")
        second = fixture["results"][1]
        product = OpenAIREResearchProduct.model_validate(second)
        assert product.main_title == "A second test publication record"
        assert product.indicators is None
        assert product.container is None


# ---------------------------------------------------------------------------
# Adapter tests
# ---------------------------------------------------------------------------


class TestAdapters:
    """Adapter conversion to canonical records."""

    def test_product_to_canonical_typed(self):
        fixture = _load_fixture("research_product_response.json")
        product = OpenAIREResearchProduct.model_validate(fixture["results"][0])
        work = openaire_product_to_canonical(product)

        assert isinstance(work, CanonicalWork)
        assert work.title == "Assembly theory explains and quantifies selection and evolution"
        assert work.doi == "10.1038/s41586-023-06600-9"
        assert work.publication_year == 2023
        assert work.publisher == "Springer Science and Business Media LLC"
        assert work.is_oa is True
        assert work.oa_color == "hybrid"
        assert work.primary_host_name == "Nature"
        assert "Assembly theory" in work.keywords
        assert work.cited_by_count == 150
        assert work.abstract is not None
        assert work.authors == ["Abhishek Sharma", "Leroy Cronin"]
        assert work.source_ids["openaire"] == "doi_dedup___::abc123def456"
        assert work.source_ids["doi"] == "10.1038/s41586-023-06600-9"

    def test_product_to_canonical_dict(self):
        fixture = _load_fixture("research_product_response.json")
        work = openaire_product_to_canonical(fixture["results"][0])
        assert isinstance(work, CanonicalWork)
        assert work.doi == "10.1038/s41586-023-06600-9"

    def test_product_to_canonical_provenance(self):
        fixture = _load_fixture("research_product_response.json")
        work = openaire_product_to_canonical(fixture["results"][0])
        assert len(work.provenance) > 0
        sources = {a.source for a in work.provenance}
        assert sources == {Source.OPENAIRE}

    def test_product_minimal_fields(self):
        """Second fixture record has minimal data — adapter should handle gracefully."""
        fixture = _load_fixture("research_product_response.json")
        work = openaire_product_to_canonical(fixture["results"][1])
        assert work.title == "A second test publication record"
        assert work.is_oa is False  # RESTRICTED → not OPEN
        assert work.cited_by_count is None

    def test_org_to_canonical_typed(self):
        org = OpenAIREOrganization.model_validate(
            {
                "id": "pending_org_::abc123",
                "legalName": "University of Twente",
                "legalShortName": "UT",
                "country": {"code": "NL", "label": "Netherlands"},
            }
        )
        canonical = openaire_org_to_canonical(org)
        assert isinstance(canonical, CanonicalOrganization)
        assert canonical.name == "University of Twente"
        assert canonical.internal_id == "pending_org_::abc123"

    def test_org_to_canonical_dict(self):
        canonical = openaire_org_to_canonical(
            {
                "id": "org_123",
                "legalName": "Example Org",
            }
        )
        assert isinstance(canonical, CanonicalOrganization)
        assert canonical.name == "Example Org"

    def test_org_provenance(self):
        canonical = openaire_org_to_canonical({"id": "org_1", "legalName": "Org"})
        assert len(canonical.provenance) > 0
        assert all(a.source == Source.OPENAIRE for a in canonical.provenance)


# ---------------------------------------------------------------------------
# Client tests (mocked HTTP)
# ---------------------------------------------------------------------------


class TestOpenAIREClient:
    """Test the OpenAIRE client against mock responses."""

    @pytest.mark.asyncio
    async def test_get_research_products(self):
        fixture = _load_fixture("research_product_response.json")
        # Remove nextCursor so pagination stops after one page
        fixture["header"]["nextCursor"] = None

        async def handler(request):
            return Response(200, json=fixture)

        transport = MockTransport(handler)
        client = OpenAIREClient(base_url="https://api.openaire.eu/graph")
        client.client = client.client.__class__(transport=transport)

        products = await client.get_research_products(doi="10.1038/s41586-023-06600-9")
        assert len(products) == 2
        assert products[0].main_title is not None

    @pytest.mark.asyncio
    async def test_get_organizations(self):
        org_response = {
            "header": {"numFound": 1, "maxScore": 5.0, "queryTime": 20, "pageSize": 50},
            "results": [
                {
                    "id": "org_1",
                    "legalName": "University of Twente",
                    "legalShortName": "UT",
                    "country": {"code": "NL", "label": "Netherlands"},
                }
            ],
        }

        async def handler(request):
            return Response(200, json=org_response)

        transport = MockTransport(handler)
        client = OpenAIREClient(base_url="https://api.openaire.eu/graph")
        client.client = client.client.__class__(transport=transport)

        orgs = await client.get_organizations(name="University of Twente")
        assert len(orgs) == 1
        assert orgs[0].legal_name == "University of Twente"

    @pytest.mark.asyncio
    async def test_cursor_pagination_multi_page(self):
        """Simulate two pages of results via cursor pagination."""
        page1 = {
            "header": {
                "numFound": 3,
                "maxScore": 5.0,
                "queryTime": 10,
                "pageSize": 2,
                "nextCursor": "page2_cursor_token",
            },
            "results": [
                {"id": "prod_1", "mainTitle": "Paper A", "type": "publication", "pids": []},
                {"id": "prod_2", "mainTitle": "Paper B", "type": "publication", "pids": []},
            ],
        }
        page2 = {
            "header": {
                "numFound": 3,
                "maxScore": 5.0,
                "queryTime": 10,
                "pageSize": 2,
                "nextCursor": None,
            },
            "results": [
                {"id": "prod_3", "mainTitle": "Paper C", "type": "publication", "pids": []},
            ],
        }

        call_count = 0

        async def handler(request):
            nonlocal call_count
            call_count += 1
            url = str(request.url)
            if "page2_cursor_token" in url:
                return Response(200, json=page2)
            return Response(200, json=page1)

        transport = MockTransport(handler)
        client = OpenAIREClient(base_url="https://api.openaire.eu/graph")
        client.client = client.client.__class__(transport=transport)

        products = await client.get_research_products(title="test", page_size=2)
        assert len(products) == 3
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_cursor_stops_on_same_cursor(self):
        """If the API keeps returning the same cursor, pagination should stop."""
        body = {
            "header": {
                "numFound": 1,
                "pageSize": 10,
                "nextCursor": "same_cursor",
            },
            "results": [{"id": "p1", "mainTitle": "X", "type": "publication", "pids": []}],
        }

        async def handler(request):
            return Response(200, json=body)

        transport = MockTransport(handler)
        client = OpenAIREClient(base_url="https://api.openaire.eu/graph")
        client.client = client.client.__class__(transport=transport)

        # First call uses cursor=*, second returns same_cursor again.
        # The guard should stop after 2 requests at most.
        products = await client.get_research_products(title="test")
        # Should get results from only the first call after detecting duplicate cursor
        assert len(products) >= 1

    @pytest.mark.asyncio
    async def test_empty_results(self):
        body = {"header": {"numFound": 0, "pageSize": 10}, "results": []}

        async def handler(request):
            return Response(200, json=body)

        transport = MockTransport(handler)
        client = OpenAIREClient(base_url="https://api.openaire.eu/graph")
        client.client = client.client.__class__(transport=transport)

        products = await client.get_research_products(doi="10.0000/nope")
        assert products == []


# ---------------------------------------------------------------------------
# Provider protocol test
# ---------------------------------------------------------------------------


class TestOpenAIREProvider:
    """Verify the provider implements the DataProvider protocol."""

    def test_implements_protocol(self):
        client = OpenAIREClient()
        provider = OpenAIREProvider(client)
        assert isinstance(provider, DataProvider)

    def test_source(self):
        provider = OpenAIREProvider(OpenAIREClient())
        assert provider.source == Source.OPENAIRE

    def test_capabilities(self):
        provider = OpenAIREProvider(OpenAIREClient())
        assert "works" in provider.capabilities
        assert "organizations" in provider.capabilities

    @pytest.mark.asyncio
    async def test_fetch_works(self):
        fixture = _load_fixture("research_product_response.json")
        fixture["header"]["nextCursor"] = None

        async def handler(request):
            return Response(200, json=fixture)

        transport = MockTransport(handler)
        client = OpenAIREClient(base_url="https://api.openaire.eu/graph")
        client.client = client.client.__class__(transport=transport)

        provider = OpenAIREProvider(client)
        works = await provider.fetch("works", doi="10.1038/s41586-023-06600-9")
        assert len(works) == 2
        assert all(isinstance(w, CanonicalWork) for w in works)
        assert works[0].doi == "10.1038/s41586-023-06600-9"

    @pytest.mark.asyncio
    async def test_fetch_organizations(self):
        org_response = {
            "header": {"numFound": 1, "pageSize": 50},
            "results": [{"id": "org_1", "legalName": "University of Twente"}],
        }

        async def handler(request):
            return Response(200, json=org_response)

        transport = MockTransport(handler)
        client = OpenAIREClient(base_url="https://api.openaire.eu/graph")
        client.client = client.client.__class__(transport=transport)

        provider = OpenAIREProvider(client)
        orgs = await provider.fetch("organizations", name="University of Twente")
        assert len(orgs) == 1
        assert all(isinstance(o, CanonicalOrganization) for o in orgs)

    @pytest.mark.asyncio
    async def test_fetch_unsupported_entity(self):
        provider = OpenAIREProvider(OpenAIREClient())
        with pytest.raises(ValueError, match="does not support"):
            await provider.fetch("journals")
