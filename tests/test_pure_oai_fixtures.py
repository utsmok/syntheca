"""Fixture-based tests for Pure OAI-PMH client parsing.

These tests verify that the xmltodict-based parser correctly extracts
person, org-unit, and publication fields from project-local XML fixtures,
including the fields restored from the legacy monolith (scopus_author_id,
researcher_id, affiliations, org type, identifier, part_of).
"""

from __future__ import annotations

import pathlib

import pytest
import xmltodict
from httpx import MockTransport, Response

from syntheca.clients.pure_oai import PureOAIClient

FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures" / "pure"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


def _parse_oai_records(xml_text: str) -> list[dict]:
    """Return the list of raw record/metadata dicts from OAI XML."""
    parsed = xmltodict.parse(xml_text)
    records = parsed["OAI-PMH"]["ListRecords"]["record"]
    if not isinstance(records, list):
        records = [records]
    return [r["metadata"] for r in records]


# ---------------------------------------------------------------------------
# Publication fixture tests
# ---------------------------------------------------------------------------


class TestPublicationFixture:
    @pytest.fixture()
    def client(self):
        return PureOAIClient()

    @pytest.fixture()
    def publications(self, client):
        metas = _parse_oai_records(_load_fixture("publication_page.xml"))
        parsed = []
        for meta in metas:
            pub_data = meta.get("cerif:Publication", meta)
            parsed.append(client._parse_publication(pub_data))
        return parsed

    def test_two_publications_parsed(self, publications):
        assert len(publications) == 2

    def test_first_pub_fields(self, publications):
        pub = publications[0]
        assert pub["id"] == "aaa-111"
        assert pub["title"] == "Advances in Metadata Retrieval: A Synthetic Approach"
        assert pub["doi"] == "10.1234/synth.2025.001"
        assert pub["publication_date"] == "2025-01-15"
        assert pub["language"] == "en"
        assert pub["volume"] == "42"
        assert pub["issue"] == "3"
        assert pub["start_page"] == "100"
        assert pub["end_page"] == "115"
        assert pub["publisher_name"] == "Elsevier"
        assert "metadata" in pub["keywords"]
        assert "1234-5678" in pub["issn"]

    def test_first_pub_authors(self, publications):
        authors = publications[0]["authors"]
        assert len(authors) == 2
        assert authors[0]["family_names"] == "Researcher"
        assert authors[0]["first_names"] == "Alice"
        assert authors[0]["person_id"] == "p-100"
        assert authors[0]["affiliation_id"] == "org-10"
        assert authors[0]["affiliation_name"] == "Faculty of Science and Technology"

    def test_first_pub_published_in(self, publications):
        pub = publications[0]
        assert pub["published_in_id"] == "journal-001"
        assert pub["published_in_title"] == "Journal of Metadata Studies"

    def test_first_pub_file_locations(self, publications):
        files = publications[0]["file_locations"]
        assert len(files) == 1
        assert files[0]["uri"] == "https://ris.utwente.nl/files/aaa-111/manuscript.pdf"
        assert files[0]["mime_type"] == "application/pdf"


# ---------------------------------------------------------------------------
# Person fixture tests
# ---------------------------------------------------------------------------


class TestPersonFixture:
    @pytest.fixture()
    def client(self):
        return PureOAIClient()

    @pytest.fixture()
    def persons(self, client):
        metas = _parse_oai_records(_load_fixture("persons_page.xml"))
        parsed = []
        for meta in metas:
            pers_data = meta.get("cerif:Person", meta)
            parsed.append(client._parse_person(pers_data))
        return parsed

    def test_three_persons_parsed(self, persons):
        assert len(persons) == 3

    def test_alice_basic_fields(self, persons):
        alice = persons[0]
        assert alice["id"] == "p-100"
        assert alice["family_names"] == "Researcher"
        assert alice["first_names"] == "Alice"
        assert alice["orcid"] == "https://orcid.org/0000-0001-0000-0001"

    def test_alice_scopus_author_id(self, persons):
        assert persons[0]["scopus_author_id"] == "55512345678"

    def test_alice_researcher_id(self, persons):
        assert persons[0]["researcher_id"] == "A-1234-2020"

    def test_alice_affiliations(self, persons):
        affiliations = persons[0]["affiliations"]
        assert len(affiliations) == 1
        assert affiliations[0]["affiliation_id"] == "org-10"
        assert affiliations[0]["affiliation_name"] == "Faculty of Science and Technology"

    def test_bob_multiple_affiliations(self, persons):
        bob = persons[1]
        assert bob["scopus_author_id"] == "55587654321"
        affiliations = bob["affiliations"]
        assert len(affiliations) == 2
        ids = {a["affiliation_id"] for a in affiliations}
        assert ids == {"org-20", "org-30"}

    def test_charlie_missing_optional_fields(self, persons):
        charlie = persons[2]
        assert charlie["id"] == "p-300"
        assert charlie["scopus_author_id"] is None
        assert charlie["researcher_id"] is None
        assert charlie["orcid"] is None
        assert charlie["affiliations"] == []


# ---------------------------------------------------------------------------
# OrgUnit fixture tests
# ---------------------------------------------------------------------------


class TestOrgUnitFixture:
    @pytest.fixture()
    def client(self):
        return PureOAIClient()

    @pytest.fixture()
    def orgunits(self, client):
        metas = _parse_oai_records(_load_fixture("orgunits_page.xml"))
        parsed = []
        for meta in metas:
            org_data = meta.get("cerif:OrgUnit", meta)
            parsed.append(client._parse_orgunit(org_data))
        return parsed

    def test_three_orgunits_parsed(self, orgunits):
        assert len(orgunits) == 3

    def test_tnw_faculty(self, orgunits):
        tnw = orgunits[0]
        assert tnw["id"] == "org-10"
        assert tnw["name"] == "Faculty of Science and Technology"
        assert tnw["acronym"] == "TNW"
        assert tnw["type"] == "Faculty"
        assert tnw["identifier"] == "https://ror.org/006hf6230"
        assert tnw["identifier_type"] == "ORCID"
        assert tnw["part_of_org_id"] is None

    def test_et_faculty_no_identifier(self, orgunits):
        et = orgunits[1]
        assert et["id"] == "org-20"
        assert et["type"] == "Faculty"
        assert et["identifier"] is None
        assert et["identifier_type"] is None
        assert et["part_of_org_id"] is None

    def test_cs_department_with_parent(self, orgunits):
        cs = orgunits[2]
        assert cs["id"] == "org-30"
        assert cs["name"] == "Department of Computer Science"
        assert cs["type"] == "Department"
        assert cs["identifier"] == "abc-def-123"
        assert cs["identifier_type"] == "UUID"
        assert cs["part_of_org_id"] == "org-20"
        assert cs["part_of_name"] == "Faculty of Engineering Technology"


# ---------------------------------------------------------------------------
# CERIF namespace 1.2 handling test
# ---------------------------------------------------------------------------


CERIF_NS_1_2_PERSON_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <ListRecords>
    <record>
      <header>
        <identifier>oai:test:person/1</identifier>
        <datestamp>2025-01-01</datestamp>
      </header>
      <metadata>
        <cerif:Person xmlns:cerif="https://www.openaire.eu/cerif-profile/1.2/" id="ns12-person">
          <cerif:PersonName>
            <cerif:FamilyNames>Namespace</cerif:FamilyNames>
            <cerif:FirstNames>Test</cerif:FirstNames>
          </cerif:PersonName>
          <cerif:ORCID>0000-0003-0000-0003</cerif:ORCID>
          <cerif:ScopusAuthorID>99999</cerif:ScopusAuthorID>
        </cerif:Person>
      </metadata>
    </record>
  </ListRecords>
</OAI-PMH>
"""


def test_cerif_namespace_1_2_person_parsing():
    """Verify CERIF namespace 1.2 payloads parse without error via xmltodict."""
    client = PureOAIClient()
    parsed = xmltodict.parse(CERIF_NS_1_2_PERSON_XML)
    meta = parsed["OAI-PMH"]["ListRecords"]["record"]["metadata"]
    # The namespace prefix differs (1.2 vs 1.6), but xmltodict uses the prefix
    # so the key will be cerif:Person regardless of namespace URI
    person_data = meta.get("cerif:Person", meta)
    result = client._parse_person(person_data)
    assert result["id"] == "ns12-person"
    assert result["family_names"] == "Namespace"
    assert result["first_names"] == "Test"
    assert result["orcid"] == "0000-0003-0000-0003"
    assert result["scopus_author_id"] == "99999"


CERIF_NS_1_2_ORGUNIT_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <ListRecords>
    <record>
      <header>
        <identifier>oai:test:org/1</identifier>
        <datestamp>2025-01-01</datestamp>
      </header>
      <metadata>
        <cerif:OrgUnit xmlns:cerif="https://www.openaire.eu/cerif-profile/1.2/" id="ns12-org">
          <cerif:Type>Institute</cerif:Type>
          <cerif:Identifier type="ROR">https://ror.org/example</cerif:Identifier>
          <cerif:Name>Test Institute</cerif:Name>
          <cerif:Acronym>TI</cerif:Acronym>
        </cerif:OrgUnit>
      </metadata>
    </record>
  </ListRecords>
</OAI-PMH>
"""


def test_cerif_namespace_1_2_orgunit_parsing():
    """Verify CERIF namespace 1.2 org-unit payloads parse without error."""
    client = PureOAIClient()
    parsed = xmltodict.parse(CERIF_NS_1_2_ORGUNIT_XML)
    meta = parsed["OAI-PMH"]["ListRecords"]["record"]["metadata"]
    org_data = meta.get("cerif:OrgUnit", meta)
    result = client._parse_orgunit(org_data)
    assert result["id"] == "ns12-org"
    assert result["type"] == "Institute"
    assert result["identifier"] == "https://ror.org/example"
    assert result["identifier_type"] == "ROR"
    assert result["name"] == "Test Institute"


# ---------------------------------------------------------------------------
# Resumption token paging mock test
# ---------------------------------------------------------------------------

PAGE_1_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <ListRecords>
    <record>
      <metadata>
        <cerif:Person xmlns:cerif="urn:xmlns:org:eurocris:cerif-1.6-2" id="paged-1">
          <cerif:PersonName>
            <cerif:FamilyNames>Page1</cerif:FamilyNames>
            <cerif:FirstNames>Person</cerif:FirstNames>
          </cerif:PersonName>
        </cerif:Person>
      </metadata>
    </record>
    <resumptionToken>token-page-2</resumptionToken>
  </ListRecords>
</OAI-PMH>
"""

PAGE_2_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <ListRecords>
    <record>
      <metadata>
        <cerif:Person xmlns:cerif="urn:xmlns:org:eurocris:cerif-1.6-2" id="paged-2">
          <cerif:PersonName>
            <cerif:FamilyNames>Page2</cerif:FamilyNames>
            <cerif:FirstNames>Person</cerif:FirstNames>
          </cerif:PersonName>
        </cerif:Person>
      </metadata>
    </record>
  </ListRecords>
</OAI-PMH>
"""


@pytest.mark.asyncio
async def test_resumption_token_paging():
    """Test that the client follows resumption tokens across pages."""
    call_count = 0

    async def handler(request):
        nonlocal call_count
        call_count += 1
        if "resumptionToken=token-page-2" in str(request.url):
            return Response(200, content=PAGE_2_XML)
        return Response(200, content=PAGE_1_XML)

    transport = MockTransport(handler)
    client = PureOAIClient()
    client.client = client.client.__class__(transport=transport)

    result = await client.get_all_records(["openaire_cris_persons"])
    persons = result["openaire_cris_persons"]

    assert call_count == 2, f"Expected 2 requests (two pages), got {call_count}"
    assert len(persons) == 2
    assert persons[0]["family_names"] == "Page1"
    assert persons[1]["family_names"] == "Page2"


# ---------------------------------------------------------------------------
# Full fixture integration: parse via get_all_records mock
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_all_records_persons_fixture():
    """Parse the persons fixture through the full get_all_records path."""
    fixture_xml = _load_fixture("persons_page.xml")

    async def handler(request):
        return Response(200, content=fixture_xml)

    transport = MockTransport(handler)
    client = PureOAIClient()
    client.client = client.client.__class__(transport=transport)

    result = await client.get_all_records(["openaire_cris_persons"])
    persons = result["openaire_cris_persons"]
    assert len(persons) == 3
    # Verify new fields survive the full round-trip
    assert persons[0]["scopus_author_id"] == "55512345678"
    assert persons[0]["researcher_id"] == "A-1234-2020"
    assert len(persons[0]["affiliations"]) == 1


@pytest.mark.asyncio
async def test_get_all_records_orgunits_fixture():
    """Parse the orgunits fixture through the full get_all_records path."""
    fixture_xml = _load_fixture("orgunits_page.xml")

    async def handler(request):
        return Response(200, content=fixture_xml)

    transport = MockTransport(handler)
    client = PureOAIClient()
    client.client = client.client.__class__(transport=transport)

    result = await client.get_all_records(["openaire_cris_orgunits"])
    orgs = result["openaire_cris_orgunits"]
    assert len(orgs) == 3
    assert orgs[0]["type"] == "Faculty"
    assert orgs[2]["part_of_org_id"] == "org-20"
