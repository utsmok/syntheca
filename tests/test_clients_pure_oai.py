import pathlib

import pytest
import xmltodict
from httpx import MockTransport, Response

from syntheca.clients.pure_oai import PureOAIClient
from syntheca.config import settings
from syntheca.utils.persistence import load_dataframe_parquet


def sample_oai_xml():
    return """
    <OAI-PMH>
      <ListRecords>
        <record>
          <metadata>
            <cerif:Publication xmlns:cerif="uri">
              <cerif:Title>#text</cerif:Title>
              <cerif:DOI>10.123/sometest</cerif:DOI>
              <cerif:PublicationDate>2020-01-02</cerif:PublicationDate>
              <cerif:Language>en</cerif:Language>
              <cerif:Keyword>#text</cerif:Keyword>
              <cerif:Publishers><cerif:Publisher><cerif:OrgUnit><cerif:Name>Elsevier</cerif:Name></cerif:OrgUnit></cerif:Publisher></cerif:Publishers>
            </cerif:Publication>
          </metadata>
        </record>
      </ListRecords>
    </OAI-PMH>
    """


@pytest.mark.asyncio
async def test_parse_publication():
    client = PureOAIClient()
    parsed = xmltodict.parse(sample_oai_xml())
    rec = parsed["OAI-PMH"]["ListRecords"]["record"]["metadata"]["cerif:Publication"]
    out = client._parse_publication(rec)
    assert out["doi"] == "10.123/sometest"
    assert out["publication_date"] == "2020-01-02"
    assert out["language"] == "en"
    assert out["keywords"] == ["#text"]
    assert out["publisher_name"] == "Elsevier"


@pytest.mark.asyncio
async def test_parse_person_and_org():
    client = PureOAIClient()
    person_xml = '<cerif:Person xmlns:cerif="uri"><cerif:PersonName><cerif:FamilyNames>Doe</cerif:FamilyNames><cerif:FirstNames>John</cerif:FirstNames></cerif:PersonName><cerif:ORCID>0000-0000</cerif:ORCID></cerif:Person>'
    pers = xmltodict.parse(person_xml)["cerif:Person"]
    parsed_person = client._parse_person(pers)
    assert parsed_person["family_names"] == "Doe"
    assert parsed_person["first_names"] == "John"

    org_xml = '<cerif:OrgUnit xmlns:cerif="uri"><cerif:Name>Dept</cerif:Name><cerif:Acronym>D</cerif:Acronym></cerif:OrgUnit>'
    org = xmltodict.parse(org_xml)["cerif:OrgUnit"]
    parsed_org = client._parse_orgunit(org)
    assert parsed_org["name"] == "Dept"
    assert parsed_org["acronym"] == "D"


@pytest.mark.asyncio
async def test_get_all_records_mock(monkeypatch):
    # Mock request to return the sample xml
    async def handler(request):
        return Response(200, content=sample_oai_xml())

    transport = MockTransport(handler)
    client = PureOAIClient()
    # patch underlying httpx client with one that uses transport
    client.client = client.client.__class__(transport=transport)

    result = await client.get_all_records(["openaire_cris_publications"])
    assert "openaire_cris_publications" in result
    assert len(result["openaire_cris_publications"]) == 1


@pytest.mark.asyncio
async def test_persistence_of_collections(tmp_path: pathlib.Path):
    old_cache = settings.cache_dir
    settings.cache_dir = tmp_path
    settings.persist_intermediate = True

    async def handler(request):
        return Response(200, content=sample_oai_xml())

    transport = MockTransport(handler)
    client = PureOAIClient()
    client.client = client.client.__class__(transport=transport)
    await client.get_all_records(["openaire_cris_publications"])
    # file should be written to cache dir
    df = load_dataframe_parquet("pure_openaire_cris_publications")
    assert df is not None
    assert df.height == 1

    # restore
    settings.persist_intermediate = False
    settings.cache_dir = old_cache
