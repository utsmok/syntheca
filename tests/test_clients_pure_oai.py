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
                <cerif:URL>https://example.com/pub/1</cerif:URL>
                <cerif:Abstract>Sample abstract</cerif:Abstract>
                <cerif:Status>published</cerif:Status>
              <cerif:PublicationDate>2020-01-02</cerif:PublicationDate>
              <cerif:Language>en</cerif:Language>
              <cerif:Keyword>#text</cerif:Keyword>
              <cerif:Publishers><cerif:Publisher><cerif:OrgUnit><cerif:Name>Elsevier</cerif:Name></cerif:OrgUnit></cerif:Publisher></cerif:Publishers>
              <cerif:FileLocations>
                <cerif:Medium>
                  <cerif:Type>#text</cerif:Type>
                  <cerif:Title>memo2048.pdf</cerif:Title>
                  <cerif:URI>https://example.com/files/memo2048.pdf</cerif:URI>
                  <cerif:MimeType>application/pdf</cerif:MimeType>
                  <cerif:Size>4096</cerif:Size>
                </cerif:Medium>
              </cerif:FileLocations>
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
    assert out["url"] == "https://example.com/pub/1"
    assert out["abstract"] == "Sample abstract"
    assert isinstance(out["file_locations"], list)
    assert out["file_locations"][0]["uri"] == "https://example.com/files/memo2048.pdf"
    assert out["status"] == "published"


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


def test_parse_wrapped_person_and_orgunit():
  client = PureOAIClient()
  sample_person_wrapped = {
    "cerif:Person": {
      "@id": "824eae9b-2185-4532-a56f-269c6b0e2f13",
      "cerif:PersonName": {"cerif:FamilyNames": "Vaheoja", "cerif:FirstNames": "Monika"},
      "cerif:ORCID": "https://orcid.org/0000-0002-1540-8565",
    }
  }
  parsed = client._parse_person(sample_person_wrapped)
  assert parsed["id"] == "824eae9b-2185-4532-a56f-269c6b0e2f13"
  assert parsed["family_names"] == "Vaheoja"
  assert parsed["first_names"] == "Monika"

  sample_org_wrapped = {
    "cerif:OrgUnit": {
      "@id": "0e8be171-fec6-476b-8c6f-383a181d3632",
      "cerif:Name": {"#text": "Chemical Science and Engineering"},
      "cerif:Acronym": {"#text": "CSE"},
    }
  }
  parsed_org = client._parse_orgunit(sample_org_wrapped)
  assert parsed_org["id"] == "0e8be171-fec6-476b-8c6f-383a181d3632"
  assert parsed_org["name"] == "Chemical Science and Engineering"
  assert parsed_org["acronym"] == "CSE"


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
