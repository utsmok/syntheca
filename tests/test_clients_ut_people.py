import json
import pathlib

import pytest
from httpx import MockTransport, Response

from syntheca.clients.ut_people import (
    BASE_URL,
    UTPeopleClient,
    _normalize_profile_url,
    rank_candidates,
)

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "ut_people"


# ---------------------------------------------------------------------------
# Fixture-based tests
# ---------------------------------------------------------------------------


def _load_rpc_fixture() -> dict:
    return json.loads((FIXTURES / "rpc_response.json").read_text(encoding="utf8"))


def _load_profile_fixture() -> str:
    return (FIXTURES / "profile_page.html").read_text(encoding="utf8")


# ---------------------------------------------------------------------------
# search_person
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_person_parse_fixture():
    """Parse the project-local RPC fixture and validate candidates."""
    rpc_json = _load_rpc_fixture()

    async def handler(request):
        return Response(200, json=rpc_json)

    transport = MockTransport(handler)
    client = UTPeopleClient()
    client.client = client.client.__class__(transport=transport)

    results = await client.search_person("Alice Researcher", rank=False)
    assert isinstance(results, list)
    assert len(results) == 2
    # fixture contains relative hrefs - client must return absolute URLs
    for r in results:
        assert r["people_page_url"].startswith("https://")


@pytest.mark.asyncio
async def test_search_person_normalizes_relative_urls():
    """Relative profile hrefs must be normalised to absolute URLs."""
    html = (
        '<div class="ut-person-tile">'
        '<h3 class="ut-person-tile__title">Test User</h3>'
        '<div class="ut-person-tile__profilelink">'
        '<a href="/en/persons/test-user">Profile</a></div>'
        '<div class="ut-person-tile__orgs"><div>Faculty X</div></div>'
        "</div>"
    )
    rpc_json = {"result": {"resultshtml": html}}

    async def handler(request):
        return Response(200, json=rpc_json)

    transport = MockTransport(handler)
    client = UTPeopleClient()
    client.client = client.client.__class__(transport=transport)

    results = await client.search_person("Test User", rank=False)
    assert results[0]["people_page_url"] == f"{BASE_URL}/en/persons/test-user"


@pytest.mark.asyncio
async def test_search_person_already_absolute_url():
    """Already-absolute URLs must not be double-prefixed."""
    html = (
        '<div class="ut-person-tile">'
        '<h3 class="ut-person-tile__title">Test User</h3>'
        '<div class="ut-person-tile__profilelink">'
        '<a href="https://people.utwente.nl/en/persons/test-user">Profile</a></div>'
        '<div class="ut-person-tile__orgs"><div>Faculty X</div></div>'
        "</div>"
    )
    rpc_json = {"result": {"resultshtml": html}}

    async def handler(request):
        return Response(200, json=rpc_json)

    transport = MockTransport(handler)
    client = UTPeopleClient()
    client.client = client.client.__class__(transport=transport)

    results = await client.search_person("Test User", rank=False)
    assert results[0]["people_page_url"] == "https://people.utwente.nl/en/persons/test-user"


# ---------------------------------------------------------------------------
# scrape_profile  +  _parse_organization_details
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scrape_profile_fixture():
    """Parse the project-local profile_page.html fixture."""
    profile_html = _load_profile_fixture()

    async def handler(request):
        return Response(200, content=profile_html)

    transport = MockTransport(handler)
    client = UTPeopleClient()
    client.client = client.client.__class__(transport=transport)

    parsed = await client.scrape_profile("https://people.utwente.nl/en/persons/alice-researcher")
    assert parsed is not None
    assert isinstance(parsed, list)
    assert parsed[0]["faculty"]["abbr"] == "TNW"
    assert parsed[0]["department"]["abbr"] == "CS"


@pytest.mark.asyncio
async def test_scrape_profile_missing_organisations_section():
    """HTML without an 'Organisations' heading should return None."""
    html = "<h2 class='heading2'>Research profiles</h2><p>No orgs here</p>"

    async def handler(request):
        return Response(200, content=html)

    transport = MockTransport(handler)
    client = UTPeopleClient()
    client.client = client.client.__class__(transport=transport)

    parsed = await client.scrape_profile("https://people.utwente.nl/en/persons/no-org")
    assert parsed is None


@pytest.mark.asyncio
async def test_scrape_profile_empty_linklist():
    """Organisations heading present but widget-linklist empty."""
    html = '<h2 class="heading2">Organisations</h2><ul class="widget-linklist"></ul>'

    async def handler(request):
        return Response(200, content=html)

    transport = MockTransport(handler)
    client = UTPeopleClient()
    client.client = client.client.__class__(transport=transport)

    parsed = await client.scrape_profile("https://people.utwente.nl/en/persons/empty-org")
    assert parsed is None


@pytest.mark.asyncio
async def test_scrape_profile_http_error():
    """Non-200 status should return None without raising."""

    async def handler(request):
        return Response(404)

    transport = MockTransport(handler)
    client = UTPeopleClient()
    client.client = client.client.__class__(transport=transport)

    parsed = await client.scrape_profile("https://people.utwente.nl/en/persons/gone")
    assert parsed is None


# ---------------------------------------------------------------------------
# _normalize_profile_url
# ---------------------------------------------------------------------------


def test_normalize_relative_url():
    assert _normalize_profile_url("/en/persons/alice") == f"{BASE_URL}/en/persons/alice"


def test_normalize_already_absolute():
    url = "https://people.utwente.nl/en/persons/alice"
    assert _normalize_profile_url(url) == url


def test_normalize_http_url_untouched():
    url = "http://people.utwente.nl/en/persons/alice"
    assert _normalize_profile_url(url) == url


# ---------------------------------------------------------------------------
# rank_candidates (Levenshtein-based)
# ---------------------------------------------------------------------------


def test_rank_candidates_best_first():
    """Best match by Levenshtein should come first."""
    candidates = [
        {"found_name": "Alice B. Researcher"},
        {"found_name": "Alice Researcher"},
        {"found_name": "Bob Scientist"},
    ]
    ranked = rank_candidates("Alice Researcher", candidates)
    assert ranked[0]["found_name"] == "Alice Researcher"


def test_rank_candidates_filters_low_similarity():
    """Candidates below the threshold should be dropped."""
    candidates = [
        {"found_name": "Completely Different Name"},
        {"found_name": "Alice Researcher"},
    ]
    ranked = rank_candidates("Alice Researcher", candidates, threshold=0.5)
    names = [c["found_name"] for c in ranked]
    assert "Alice Researcher" in names


def test_rank_candidates_empty_input():
    assert rank_candidates("Test", []) == []


def test_rank_candidates_no_found_name():
    """Candidates without found_name should be silently skipped."""
    candidates = [{"found_name": None}, {"found_name": "Alice Researcher"}]
    ranked = rank_candidates("Alice Researcher", candidates)
    assert len(ranked) == 1
    assert ranked[0]["found_name"] == "Alice Researcher"


def test_rank_candidates_ambiguous_similar_names():
    """Ambiguous candidates with very similar names should all be kept."""
    candidates = [
        {"found_name": "van den Berg, A. (Anna)"},
        {"found_name": "van den Berg, A.B. (Anna B.)"},
        {"found_name": "van der Berg, C. (Carl)"},
    ]
    ranked = rank_candidates("Anna van den Berg", candidates, threshold=0.4)
    # All three have >0.4 similarity to "Anna van den Berg"
    assert len(ranked) >= 2
