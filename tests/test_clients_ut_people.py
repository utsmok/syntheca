from httpx import MockTransport, Response
import pytest

from syntheca.clients.ut_people import UTPeopleClient


@pytest.mark.asyncio
async def test_search_person_parse(monkeypatch):
    # Simulate RPC response JSON with result.resultshtml containing person tiles
    html = '<div class="ut-person-tile"><h3 class="ut-person-tile__title">Doe, J. (John)</h3>'
    html += '<div class="ut-person-tile__profilelink"><a href="/profile/1">Profile</a></div>'
    html += '<div class="ut-person-tile__mail"><span class="text">john.doe@example.com</span></div>'
    html += '<div class="ut-person-tile__roles">Researcher</div>'
    html += (
        '<div class="ut-person-tile__orgs"><div>Faculty of Something</div></div></div>'
    )

    rpc_json = {"result": {"resultshtml": html}}

    async def handler(request):
        return Response(200, json=rpc_json)

    transport = MockTransport(handler)
    client = UTPeopleClient()
    client.client = client.client.__class__(transport=transport)

    results = await client.search_person("John Doe")
    assert isinstance(results, list)
    assert results[0]["people_page_url"] == "/profile/1"


@pytest.mark.asyncio
async def test_scrape_profile_and_parse(monkeypatch):
    # Build an HTML sample that includes the 'Organisations' section
    html = '<h2 class="heading2">Organisations</h2>'
    html += '<ul class="widget-linklist"><li class="widget-linklist__item widget-linklist__item--level1"><span class="widget-linklist__text">Faculty of Science and Technology (TNW)</span></li>'
    html += '<li class="widget-linklist__item widget-linklist__item--level2"><span class="widget-linklist__text">Department of Something (SOM)</span></li></ul>'

    async def handler(request):
        return Response(200, content=html)

    transport = MockTransport(handler)
    client = UTPeopleClient()
    client.client = client.client.__class__(transport=transport)

    parsed = await client.scrape_profile("https://people.utwente.nl/profile/1")
    assert parsed is not None
    assert isinstance(parsed, list)
    # Expect the top-level faculty name and abbreviation
    assert parsed[0]["faculty"]["abbr"] == "TNW"
