import json
import pathlib

import polars as pl
import pytest
from httpx import MockTransport, Response

from syntheca.clients.ut_people import (
    BASE_URL,
    UTPeopleClient,
    _normalize_profile_url,
    rank_candidates,
)
from syntheca.processing.enrichment import parse_scraped_org_details

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "ut_people"


# ---------------------------------------------------------------------------
# Fixture-based tests
# ---------------------------------------------------------------------------


def _load_rpc_fixture() -> dict:
    return json.loads((FIXTURES / "rpc_response.json").read_text(encoding="utf8"))


def _load_profile_fixture() -> str:
    return (FIXTURES / "profile_page.html").read_text(encoding="utf8")


def _load_live_rpc_fixture() -> dict:
    return json.loads((FIXTURES / "rpc_live_response.json").read_text(encoding="utf8"))


def _load_live_profile_fixture() -> str:
    html = (FIXTURES / "profile_live_page.html").read_text(encoding="utf8")
    if '<h2 class="heading2">' not in html:
        html = f'<h2 class="heading2">{html}'
    return html


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


@pytest.mark.asyncio
async def test_search_person_parses_live_rpc_envelope_fixture():
    """Live-like UT People RPC envelopes should parse without dropping absolute profile URLs."""
    rpc_json = _load_live_rpc_fixture()

    async def handler(request):
        return Response(200, json=rpc_json)

    transport = MockTransport(handler)
    client = UTPeopleClient()
    client.client = client.client.__class__(transport=transport)

    results = await client.search_person("Sam Mok", rank=False)
    assert results
    assert results[0]["people_page_url"] == "https://people.utwente.nl/s.mok"
    normalized_urls = [r["people_page_url"] for r in results if r.get("people_page_url")]
    assert normalized_urls
    assert all(url.startswith("https://people.utwente.nl/") for url in normalized_urls)


@pytest.mark.asyncio
async def test_search_person_fetches_bounded_additional_page_for_ambiguity():
    """Ambiguous first-page results should inspect a bounded number of extra pages using totalcount."""
    page_requests = []
    responses = {
        0: {
            "result": {
                "options": {
                    "query": "Alice Researcher",
                    "page": 0,
                    "resultsperpage": 20,
                    "langcode": "en",
                },
                "totalcount": 21,
                "resultshtml": (
                    '<div class="ut-person-tile">'
                    '<h3 class="ut-person-tile__title">Alice Q. Researcher</h3>'
                    '<div class="ut-person-tile__profilelink"><a href="/alice-q">Profile</a></div>'
                    '<div class="ut-person-tile__orgs"><div>Faculty X</div></div>'
                    "</div>"
                    '<div class="ut-person-tile">'
                    '<h3 class="ut-person-tile__title">Alicia Researcher</h3>'
                    '<div class="ut-person-tile__profilelink"><a href="/alicia">Profile</a></div>'
                    '<div class="ut-person-tile__orgs"><div>Faculty X</div></div>'
                    "</div>"
                ),
            }
        },
        1: {
            "result": {
                "options": {
                    "query": "Alice Researcher",
                    "page": 1,
                    "resultsperpage": 20,
                    "langcode": "en",
                },
                "totalcount": 21,
                "resultshtml": (
                    '<div class="ut-person-tile">'
                    '<h3 class="ut-person-tile__title">Alice Researcher</h3>'
                    '<div class="ut-person-tile__profilelink"><a href="/alice-researcher">Profile</a></div>'
                    '<div class="ut-person-tile__orgs"><div>Faculty X</div></div>'
                    "</div>"
                ),
            }
        },
    }

    async def handler(request):
        payload = json.loads(request.content.decode("utf8"))
        page = payload["params"][0]["page"]
        page_requests.append(page)
        return Response(200, json=responses[page])

    transport = MockTransport(handler)
    client = UTPeopleClient()
    client.client = client.client.__class__(transport=transport)

    results = await client.search_person("Alice Researcher", max_additional_pages=1)
    assert page_requests == [0, 1]
    assert results[0]["found_name"] == "Alice Researcher"
    assert results[0]["people_page_url"] == f"{BASE_URL}/alice-researcher"


@pytest.mark.asyncio
async def test_search_person_skips_additional_pages_for_confident_exact_match():
    """Exact first-page matches should not trigger needless pagination even when totalcount is higher."""
    page_requests = []
    rpc_json = {
        "result": {
            "options": {
                "query": "Alice Researcher",
                "page": 0,
                "resultsperpage": 20,
                "langcode": "en",
            },
            "totalcount": 25,
            "resultshtml": (
                '<div class="ut-person-tile">'
                '<h3 class="ut-person-tile__title">Alice Researcher</h3>'
                '<div class="ut-person-tile__profilelink"><a href="/alice-researcher">Profile</a></div>'
                '<div class="ut-person-tile__orgs"><div>Faculty X</div></div>'
                "</div>"
                '<div class="ut-person-tile">'
                '<h3 class="ut-person-tile__title">Alice Q. Researcher</h3>'
                '<div class="ut-person-tile__profilelink"><a href="/alice-q">Profile</a></div>'
                '<div class="ut-person-tile__orgs"><div>Faculty X</div></div>'
                "</div>"
            ),
        }
    }

    async def handler(request):
        payload = json.loads(request.content.decode("utf8"))
        page_requests.append(payload["params"][0]["page"])
        return Response(200, json=rpc_json)

    transport = MockTransport(handler)
    client = UTPeopleClient()
    client.client = client.client.__class__(transport=transport)

    results = await client.search_person("Alice Researcher")
    assert page_requests == [0]
    assert results[0]["found_name"] == "Alice Researcher"


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
async def test_scrape_profile_live_fixture_preserves_nonfaculty_hierarchy():
    """Live-like profile markup should preserve raw hierarchy without forcing level-1 units into faculty semantics."""
    profile_html = _load_live_profile_fixture()

    async def handler(request):
        return Response(200, content=profile_html)

    transport = MockTransport(handler)
    client = UTPeopleClient()
    client.client = client.client.__class__(transport=transport)

    parsed = await client.scrape_profile("https://people.utwente.nl/s.mok")
    assert parsed is not None
    assert parsed[0]["unit"]["name"] == "Library, ICT-Services & Archive"
    assert parsed[0]["faculty"]["name"] is None
    assert parsed[0]["department"]["name"] is None
    assert parsed[0]["hierarchy"][1]["abbr"] == "LISA-EIS"
    assert parsed[1]["faculty"]["abbr"] == "EEMCS"

    authors_df = pl.DataFrame({"org_details_pp": [parsed]})
    enriched = parse_scraped_org_details(authors_df)
    assert (
        enriched["faculty"][0]
        == "Faculty of Electrical Engineering, Mathematics and Computer Science"
    )
    assert enriched["department"][0] is None
    assert enriched["eemcs"][0] is True


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
    assert _normalize_profile_url(url) == "https://people.utwente.nl/en/persons/alice"


def test_normalize_url_canonicalizes_www_query_and_fragment():
    url = "https://www.people.utwente.nl/en/persons/alice/?foo=bar#section"
    assert _normalize_profile_url(url) == "https://people.utwente.nl/en/persons/alice"


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
