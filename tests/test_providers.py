"""Tests for the provider protocol, concrete providers, and stage runner."""

from __future__ import annotations

from typing import cast

import pytest

from syntheca.clients.openalex import OpenAlexClient
from syntheca.clients.pure_oai import PureOAIClient
from syntheca.clients.ut_people import UTPeopleClient
from syntheca.config.source_precedence import Source
from syntheca.models.canonical import CanonicalOrganization, CanonicalPerson, CanonicalWork
from syntheca.providers import DataProvider
from syntheca.providers.openalex_provider import OpenAlexProvider
from syntheca.providers.pure_provider import PureProvider
from syntheca.providers.stage import ProviderStage
from syntheca.providers.ut_people_provider import UTPeopleProvider

# ---------------------------------------------------------------------------
# Helpers: minimal mock clients
# ---------------------------------------------------------------------------


class _MockPureClient:
    """Minimal stand-in for PureOAIClient."""

    def __init__(self, records: dict[str, list[dict]] | None = None) -> None:
        self._records = records or {}

    async def get_all_records(self, collections: list[str]) -> dict[str, list[dict]]:
        return {c: self._records.get(c, []) for c in collections}


class _MockOpenAlexClient:
    """Minimal stand-in for OpenAlexClient."""

    def __init__(self, works: list | None = None) -> None:
        self._works = works or []

    async def get_works_by_ids(self, ids: list[str], **kwargs) -> list:
        return self._works


class _MockUTPeopleClient:
    """Minimal stand-in for UTPeopleClient."""

    def __init__(self, results: dict[str, list[dict]] | None = None) -> None:
        # name → candidates
        self._results = results or {}

    async def search_person(self, name: str, **kwargs) -> list[dict]:
        return self._results.get(name, [])


# ---------------------------------------------------------------------------
# A simple DataProvider-conformant stub for stage-runner tests
# ---------------------------------------------------------------------------


class _StubProvider:
    """Fully in-memory provider that satisfies the DataProvider protocol."""

    def __init__(
        self,
        source: Source,
        capabilities: set[str],
        data: dict[str, list],
    ) -> None:
        self._source = source
        self._capabilities = capabilities
        self._data = data

    @property
    def source(self) -> Source:
        return self._source

    @property
    def capabilities(self) -> set[str]:
        return self._capabilities

    async def fetch(self, entity: str, **kwargs):
        return self._data.get(entity, [])


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


class TestProtocolConformance:
    """Verify that each concrete provider satisfies the DataProvider protocol."""

    def test_pure_provider_is_data_provider(self):
        provider = PureProvider(client=cast(PureOAIClient, _MockPureClient()))
        assert isinstance(provider, DataProvider)

    def test_openalex_provider_is_data_provider(self):
        provider = OpenAlexProvider(client=cast(OpenAlexClient, _MockOpenAlexClient()))
        assert isinstance(provider, DataProvider)

    def test_ut_people_provider_is_data_provider(self):
        provider = UTPeopleProvider(client=cast(UTPeopleClient, _MockUTPeopleClient()))
        assert isinstance(provider, DataProvider)

    def test_stub_provider_is_data_provider(self):
        provider = _StubProvider(Source.PURE, {"works"}, {})
        assert isinstance(provider, DataProvider)


# ---------------------------------------------------------------------------
# Concrete provider properties
# ---------------------------------------------------------------------------


class TestProviderProperties:
    def test_pure_source(self):
        p = PureProvider(client=cast(PureOAIClient, _MockPureClient()))
        assert p.source == Source.PURE

    def test_pure_capabilities(self):
        p = PureProvider(client=cast(PureOAIClient, _MockPureClient()))
        assert p.capabilities == {"works", "persons", "organizations"}

    def test_openalex_source(self):
        p = OpenAlexProvider(client=cast(OpenAlexClient, _MockOpenAlexClient()))
        assert p.source == Source.OPENALEX

    def test_openalex_capabilities(self):
        p = OpenAlexProvider(client=cast(OpenAlexClient, _MockOpenAlexClient()))
        assert p.capabilities == {"works"}

    def test_ut_people_source(self):
        p = UTPeopleProvider(client=cast(UTPeopleClient, _MockUTPeopleClient()))
        assert p.source == Source.UT_PEOPLE

    def test_ut_people_capabilities(self):
        p = UTPeopleProvider(client=cast(UTPeopleClient, _MockUTPeopleClient()))
        assert p.capabilities == {"persons"}


# ---------------------------------------------------------------------------
# Concrete provider fetch
# ---------------------------------------------------------------------------


class TestPureProviderFetch:
    @pytest.mark.asyncio
    async def test_fetch_works_returns_canonical(self):
        records = {
            "openaire_cris_publications": [
                {"id": "p1", "title": "Test Paper", "doi": "10.1234/test"},
            ]
        }
        provider = PureProvider(client=cast(PureOAIClient, _MockPureClient(records)))
        result = await provider.fetch("works")
        assert len(result) == 1
        assert isinstance(result[0], CanonicalWork)
        assert result[0].title == "Test Paper"

    @pytest.mark.asyncio
    async def test_fetch_persons_returns_canonical(self):
        records = {
            "openaire_cris_persons": [
                {"id": "per1", "first_names": "Alice", "family_names": "Smith"},
            ]
        }
        provider = PureProvider(client=cast(PureOAIClient, _MockPureClient(records)))
        result = await provider.fetch("persons")
        assert len(result) == 1
        assert isinstance(result[0], CanonicalPerson)
        assert result[0].name == "Alice Smith"

    @pytest.mark.asyncio
    async def test_fetch_organizations_returns_canonical(self):
        records = {
            "openaire_cris_orgunits": [
                {"id": "org1", "name": "Faculty of Science"},
            ]
        }
        provider = PureProvider(client=cast(PureOAIClient, _MockPureClient(records)))
        result = await provider.fetch("organizations")
        assert len(result) == 1
        assert isinstance(result[0], CanonicalOrganization)
        assert result[0].name == "Faculty of Science"

    @pytest.mark.asyncio
    async def test_fetch_unsupported_entity_raises(self):
        provider = PureProvider(client=cast(PureOAIClient, _MockPureClient()))
        with pytest.raises(ValueError, match="does not support"):
            await provider.fetch("funding")

    @pytest.mark.asyncio
    async def test_fetch_empty_collection(self):
        provider = PureProvider(client=cast(PureOAIClient, _MockPureClient()))
        result = await provider.fetch("works")
        assert result == []


class TestOpenAlexProviderFetch:
    @pytest.mark.asyncio
    async def test_fetch_works_with_dict_works(self):
        raw_works = [
            {
                "id": "https://openalex.org/W1",
                "doi": "10.1234/oa1",
                "display_name": "OA Paper",
                "publication_year": 2025,
            }
        ]
        provider = OpenAlexProvider(client=cast(OpenAlexClient, _MockOpenAlexClient(raw_works)))
        result = await provider.fetch("works", ids=["10.1234/oa1"])
        assert len(result) == 1
        assert isinstance(result[0], CanonicalWork)

    @pytest.mark.asyncio
    async def test_fetch_with_no_ids_returns_empty(self):
        provider = OpenAlexProvider(client=cast(OpenAlexClient, _MockOpenAlexClient()))
        result = await provider.fetch("works")
        assert result == []

    @pytest.mark.asyncio
    async def test_fetch_unsupported_entity_raises(self):
        provider = OpenAlexProvider(client=cast(OpenAlexClient, _MockOpenAlexClient()))
        with pytest.raises(ValueError, match="does not support"):
            await provider.fetch("persons")


class TestUTPeopleProviderFetch:
    @pytest.mark.asyncio
    async def test_fetch_persons_returns_canonical(self):
        results = {
            "Alice Smith": [
                {"found_name": "Alice Smith", "people_page_url": "https://people.utwente.nl/alice"}
            ]
        }
        provider = UTPeopleProvider(client=cast(UTPeopleClient, _MockUTPeopleClient(results)))
        result = await provider.fetch("persons", names=["Alice Smith"])
        assert len(result) == 1
        assert isinstance(result[0], CanonicalPerson)
        assert result[0].name == "Alice Smith"

    @pytest.mark.asyncio
    async def test_fetch_with_no_names_returns_empty(self):
        provider = UTPeopleProvider(client=cast(UTPeopleClient, _MockUTPeopleClient()))
        result = await provider.fetch("persons")
        assert result == []

    @pytest.mark.asyncio
    async def test_fetch_unsupported_entity_raises(self):
        provider = UTPeopleProvider(client=cast(UTPeopleClient, _MockUTPeopleClient()))
        with pytest.raises(ValueError, match="does not support"):
            await provider.fetch("works")


# ---------------------------------------------------------------------------
# Stage runner
# ---------------------------------------------------------------------------


class TestProviderStage:
    @pytest.mark.asyncio
    async def test_fetch_all_works_from_multiple_providers(self):
        w1 = CanonicalWork(internal_id="p1", title="Pure Paper")
        w2 = CanonicalWork(internal_id="oa1", title="OA Paper")

        stage = ProviderStage(
            [
                _StubProvider(Source.PURE, {"works", "persons"}, {"works": [w1]}),
                _StubProvider(Source.OPENALEX, {"works"}, {"works": [w2]}),
            ]
        )

        results = await stage.fetch_all_works()
        assert Source.PURE in results
        assert Source.OPENALEX in results
        assert len(results[Source.PURE]) == 1
        assert len(results[Source.OPENALEX]) == 1
        assert results[Source.PURE][0].title == "Pure Paper"
        assert results[Source.OPENALEX][0].title == "OA Paper"

    @pytest.mark.asyncio
    async def test_fetch_all_works_skips_non_capable(self):
        """UT People provider has no 'works' — should be skipped."""
        w1 = CanonicalWork(internal_id="p1", title="A Paper")
        stage = ProviderStage(
            [
                _StubProvider(Source.PURE, {"works"}, {"works": [w1]}),
                _StubProvider(Source.UT_PEOPLE, {"persons"}, {}),
            ]
        )

        results = await stage.fetch_all_works()
        assert Source.PURE in results
        assert Source.UT_PEOPLE not in results

    @pytest.mark.asyncio
    async def test_fetch_all_persons(self):
        per = CanonicalPerson(internal_id="per1", name="Alice")
        stage = ProviderStage(
            [
                _StubProvider(Source.PURE, {"works", "persons"}, {"persons": [per]}),
                _StubProvider(Source.OPENALEX, {"works"}, {}),
            ]
        )

        results = await stage.fetch_all_persons()
        assert Source.PURE in results
        assert Source.OPENALEX not in results
        assert len(results[Source.PURE]) == 1

    @pytest.mark.asyncio
    async def test_fetch_all_organizations(self):
        org = CanonicalOrganization(internal_id="org1", name="Faculty")
        stage = ProviderStage(
            [
                _StubProvider(Source.PURE, {"organizations"}, {"organizations": [org]}),
            ]
        )

        results = await stage.fetch_all_organizations()
        assert Source.PURE in results
        assert results[Source.PURE][0].name == "Faculty"

    @pytest.mark.asyncio
    async def test_provider_ordering_is_preserved(self):
        """Iteration order of results dict matches provider list order."""
        w1 = CanonicalWork(internal_id="w1", title="First")
        w2 = CanonicalWork(internal_id="w2", title="Second")

        stage = ProviderStage(
            [
                _StubProvider(Source.PURE, {"works"}, {"works": [w1]}),
                _StubProvider(Source.OPENALEX, {"works"}, {"works": [w2]}),
            ]
        )

        results = await stage.fetch_all_works()
        keys = list(results.keys())
        assert keys == [Source.PURE, Source.OPENALEX]

    @pytest.mark.asyncio
    async def test_provider_failure_does_not_block_others(self):
        """A failing provider should not prevent others from returning data."""

        class _FailingProvider:
            @property
            def source(self) -> Source:
                return Source.OPENAIRE

            @property
            def capabilities(self) -> set[str]:
                return {"works"}

            async def fetch(self, entity: str, **kwargs):
                raise RuntimeError("simulated failure")

        w = CanonicalWork(internal_id="w1", title="Healthy")
        stage = ProviderStage(
            [
                _FailingProvider(),
                _StubProvider(Source.PURE, {"works"}, {"works": [w]}),
            ]
        )

        results = await stage.fetch_all_works()
        assert Source.OPENAIRE not in results
        assert Source.PURE in results
        assert len(results[Source.PURE]) == 1

    @pytest.mark.asyncio
    async def test_empty_providers_list(self):
        stage = ProviderStage([])
        assert await stage.fetch_all_works() == {}
        assert await stage.fetch_all_persons() == {}
        assert await stage.fetch_all_organizations() == {}
