from __future__ import annotations

from typing import Any

import xmltodict

from syntheca.clients.base import BaseClient


class PureOAIClient(BaseClient):
    """Client for retrieving OAI-PMH records from Pure / OAI endpoints.

    This client exposes `get_all_records` which handles resumptionTokens
    and returns a dict of collection -> list[parsed records].
    """

    BASEURL = "https://ris.utwente.nl/ws/oai"
    SCHEMA = "oai_cerif_openaire"

    # Helper utilities
    @staticmethod
    def _ensure_list(value: Any) -> list:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]

    @staticmethod
    def _get_text(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, dict):
            return value.get("#text")
        return str(value)

    @staticmethod
    def _safe_get(data: dict, keys: list[str], default=None):
        cur = data
        for k in keys:
            if not isinstance(cur, dict) or k not in cur:
                return default
            cur = cur[k]
        return cur

    def _parse_publication(self, pub: dict) -> dict:
        # Based on notebook helpers: reduce to a flat shape (extended)
        return {
            "id": self._safe_get(pub, ["@id"]),
            "type": self._parse_enum(pub.get("pubt:Type")),
            "language": self._get_text(pub.get("cerif:Language")),
            "title": self._get_text(pub.get("cerif:Title")),
            "publication_date": self._get_text(pub.get("cerif:PublicationDate")),
            "doi": self._get_text(pub.get("cerif:DOI")),
            "authors": [
                {
                    "person_id": self._safe_get(a, ["cerif:Person", "@id"]),
                    "family_names": self._get_text(
                        self._safe_get(a, ["cerif:Person", "cerif:PersonName", "cerif:FamilyNames"])
                    ),
                    "first_names": self._get_text(
                        self._safe_get(a, ["cerif:Person", "cerif:PersonName", "cerif:FirstNames"])
                    ),
                }
                for a in self._ensure_list(self._safe_get(pub, ["cerif:Authors", "cerif:Author"]))
            ],
            "editors": [
                {
                    "person_id": self._safe_get(a, ["cerif:Person", "@id"]),
                    "family_names": self._get_text(
                        self._safe_get(a, ["cerif:Person", "cerif:PersonName", "cerif:FamilyNames"])
                    ),
                }
                for a in self._ensure_list(self._safe_get(pub, ["cerif:Editors", "cerif:Editor"]))
            ],
            "keywords": [
                self._get_text(kw)
                for kw in self._ensure_list(pub.get("cerif:Keyword"))
                if self._get_text(kw)
            ],
            "isbn": [
                self._get_text(i)
                for i in self._ensure_list(pub.get("cerif:ISBN"))
                if self._get_text(i)
            ],
            "issn": [
                self._get_text(i)
                for i in self._ensure_list(pub.get("cerif:ISSN"))
                if self._get_text(i)
            ],
            "publisher_name": self._get_text(
                self._safe_get(
                    pub,
                    [
                        "cerif:Publishers",
                        "cerif:Publisher",
                        "cerif:OrgUnit",
                        "cerif:Name",
                    ],
                )
            ),
        }

    def _parse_person(self, pers: dict) -> dict:
        return {
            "id": self._safe_get(pers, ["@id"]),
            "family_names": self._get_text(
                self._safe_get(pers, ["cerif:PersonName", "cerif:FamilyNames"])
            ),
            "first_names": self._get_text(
                self._safe_get(pers, ["cerif:PersonName", "cerif:FirstNames"])
            ),
            "orcid": self._get_text(pers.get("cerif:ORCID")),
        }

    def _parse_enum(self, value: str | dict | None) -> str | None:
        """Parses a CERIF controlled vocab value: dict with #text or a URL string."""
        if value is None:
            return None
        if isinstance(value, dict):
            text_val = value.get("#text")
            return text_val.strip() if text_val else None
        if isinstance(value, str) and ("/" in value or "#" in value):
            return value.split("/")[-1].split("#")[-1]
        return str(value)

    def _parse_person_name(self, name_dict: dict | None) -> tuple[str | None, str | None]:
        if not isinstance(name_dict, dict):
            return None, None
        family = self._get_text(name_dict.get("cerif:FamilyNames"))
        first = self._get_text(name_dict.get("cerif:FirstNames"))
        return family, first

    def _parse_contributors(self, contrib_list: list | None) -> list[dict] | None:
        if not contrib_list:
            return None
        parsed_list = []
        for item in contrib_list:
            person_data = self._safe_get(item, ["cerif:Person"])
            if not person_data:
                continue
            family_names, first_names = self._parse_person_name(person_data.get("cerif:PersonName"))
            affiliation_data = self._safe_get(item, ["cerif:Affiliation", "cerif:OrgUnit"]) or {}
            parsed_list.append(
                {
                    "person_id": self._safe_get(person_data, ["@id"]),
                    "family_names": family_names,
                    "first_names": first_names,
                    "affiliation_id": self._safe_get(affiliation_data, ["@id"]),
                    "affiliation_name": self._get_text(
                        self._safe_get(affiliation_data, ["cerif:Name"])
                    ),
                }
            )
        return parsed_list if parsed_list else None

    def _parse_orgunit(self, org: dict) -> dict:
        return {
            "id": self._safe_get(org, ["@id"]),
            "name": self._get_text(org.get("cerif:Name")),
            "acronym": self._get_text(org.get("cerif:Acronym")),
        }

    async def get_all_records(self, collections: list[str]) -> dict[str, list[dict]]:
        results = {}

        async def get_collection_data(collection: str):
            url = f"{self.BASEURL}?verb=ListRecords&metadataPrefix={self.SCHEMA}&set={collection}"
            resume_url = url.split("&metadataPrefix", maxsplit=1)[0]

            col_records: list[dict] = []
            while url:
                resp = await self.request("GET", url)
                parsed = xmltodict.parse(resp.text)
                records = parsed.get("OAI-PMH", {}).get("ListRecords", {})
                recs = records.get("record")
                if not isinstance(recs, list):
                    recs = [recs] if recs else []
                for r in recs:
                    meta = r.get("metadata", {})
                    # Publication is nested at metadata->cerif:Publication
                    pub = (
                        self._safe_get(meta, ["cerif:Publication"])
                        or self._safe_get(meta, ["openaire_cris:publication"])
                        or meta
                    )
                    if pub:
                        # Choose parser based on collection type
                        if "person" in collection.lower():
                            col_records.append(self._parse_person(pub))
                        elif "orgunit" in collection.lower() or "orgs" in collection.lower():
                            col_records.append(self._parse_orgunit(pub))
                        else:
                            col_records.append(self._parse_publication(pub))
                # resumption token
                token = records.get("resumptionToken")
                if token:
                    token_text = token.get("#text")
                    if token_text:
                        url = f"{resume_url}&resumptionToken={token_text}"
                        continue
                url = None
            return {collection: col_records}

        for collection in collections:
            results.update(await get_collection_data(collection))

        return results
