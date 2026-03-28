"""Pure OAI-PMH client integration for the syntheca project.

This module provides `PureOAIClient`, which wraps Pure's OAI-PMH endpoints
and parses CERIF/OAI XML into flattened dictionaries for downstream
processing and analysis.
"""

from __future__ import annotations

from typing import Any

import polars as pl
import xmltodict
from loguru import logger
from tqdm import tqdm

from syntheca.clients.base import BaseClient
from syntheca.config import settings
from syntheca.utils.polars_frames import robust_from_dicts
from syntheca.utils.progress import get_next_position

_PURE_PUBLICATION_STRING_FIELDS: tuple[str, ...] = ("volume", "issue", "start_page", "end_page")


def pure_publications_to_frame(records: list[dict[str, Any]] | None) -> pl.DataFrame:
    """Materialize parsed Pure publication records into a Polars DataFrame.

    Pure OAI bibliographic fields such as volume, issue, and page ranges may
    arrive with mixed scalar types across records. Keep these columns stable as
    strings before handing them to Polars so schema inference does not fail when
    a later row changes type.

    Args:
        records (list[dict[str, Any]] | None): Parsed Pure publication records.

    Returns:
        pl.DataFrame: Publication DataFrame with stable string bibliographic fields.

    """
    if not records:
        return pl.DataFrame()

    normalized_records: list[dict[str, Any]] = []
    for record in records:
        normalized = dict(record)
        for field in _PURE_PUBLICATION_STRING_FIELDS:
            value = normalized.get(field)
            if value is None or isinstance(value, str):
                continue
            if isinstance(value, dict):
                text_value = value.get("#text")
                normalized[field] = None if text_value is None else str(text_value)
                continue
            normalized[field] = str(value)
        normalized_records.append(normalized)

    return robust_from_dicts(
        normalized_records,
        schema_overrides={field: pl.Utf8 for field in _PURE_PUBLICATION_STRING_FIELDS},
    )


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
        """Return a list for `value`, converting None to empty list.

        Args:
            value (Any): The value that should be treated as a list.

        Returns:
            list: `value` if it's already a list, [`value`] if scalar, or [] when None.

        """
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]

    @staticmethod
    def _get_text(value: Any) -> str | None:
        """Extract the text content of a possibly nested element.

        Many XML-parsing helpers return a dictionary where text content is under
        the key `#text`. This helper extracts that text or coerces scalars to
        strings. Returns `None` for `None` input.

        Args:
            value (Any): The parsed XML node value to extract text from.

        Returns:
            str | None: Extracted string or None if not present.

        """
        if value is None:
            return None
        if isinstance(value, dict):
            return value.get("#text")
        return str(value)

    @staticmethod
    def _safe_get(data: dict, keys: list[str], default=None):
        """Traverse `data` by the supplied list of `keys` and return nested value.

        Args:
            data (dict): A nested dict to traverse.
            keys (list[str]): The ordered list of keys defining the path.
            default (Any): The fallback to return when path can't be traversed.

        Returns:
            Any: The nested value or `default` if not found.

        """
        cur = data
        for k in keys:
            if not isinstance(cur, dict) or k not in cur:
                return default
            cur = cur[k]
        return cur

    def _parse_publication(self, pub: dict) -> dict:
        """Parse a CERIF publication XML dictionary to a flat dict.

        This mirrors the notebook helpers by flattening and extracting common
        fields from a CERIF/OAI-PMH publication representation.

        Args:
            pub (dict): Raw parsed publication dictionary from xmltodict.

        Returns:
            dict: A flattened dictionary with common keys (id, title, doi, authors, etc.).

        """
        if isinstance(pub, dict) and "cerif:Publication" in pub:
            pub = pub.get("cerif:Publication") or pub
        elif isinstance(pub, dict) and "openaire_cris:publication" in pub:
            pub = pub.get("openaire_cris:publication") or pub

        result = {
            "id": self._safe_get(pub, ["@id"]),
            "type": self._parse_enum(pub.get("pubt:Type")),
            "language": self._get_text(pub.get("cerif:Language")),
            "title": self._get_text(pub.get("cerif:Title")),
            "publication_date": self._get_text(pub.get("cerif:PublicationDate")),
            "doi": self._get_text(pub.get("cerif:DOI")),
            "url": self._get_text(pub.get("cerif:URL")),
            "abstract": self._get_text(pub.get("cerif:Abstract")),
            "volume": self._get_text(pub.get("cerif:Volume")),
            "issue": self._get_text(pub.get("cerif:Issue")),
            "start_page": self._get_text(pub.get("cerif:StartPage")),
            "end_page": self._get_text(pub.get("cerif:EndPage")),
            "status": self._parse_enum(pub.get("cerif:Status")),
            "access_right": self._parse_enum(pub.get("ar:Access")),
            "license": self._parse_enum(pub.get("cerif:License")),
            "authors": self._parse_contributors(
                self._ensure_list(self._safe_get(pub, ["cerif:Authors", "cerif:Author"]))
            ),
            "editors": self._parse_contributors(
                self._ensure_list(self._safe_get(pub, ["cerif:Editors", "cerif:Editor"]))
            ),
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
        # Published in / Part of relationships
        published_in = self._safe_get(pub, ["cerif:PublishedIn", "cerif:Publication"]) or {}
        result["published_in_id"] = self._safe_get(published_in, ["@id"]) if published_in else None
        result["published_in_title"] = self._get_text(self._safe_get(published_in, ["cerif:Title"]))
        part_of = self._safe_get(pub, ["cerif:PartOf", "cerif:Publication"]) or {}
        result["part_of_id"] = self._safe_get(part_of, ["@id"]) if part_of else None
        result["part_of_title"] = self._get_text(self._safe_get(part_of, ["cerif:Title"]))

        # Event information
        event = self._safe_get(pub, ["cerif:PresentedAt", "cerif:Event"]) or {}
        result["event_name"] = self._get_text(self._safe_get(event, ["cerif:Name"]))
        result["event_acronym"] = self._get_text(self._safe_get(event, ["cerif:Acronym"]))

        # File locations
        result["file_locations"] = self._parse_file_locations(pub.get("cerif:FileLocations"))

        # References
        result["references"] = self._parse_references(pub.get("cerif:References"))

        # Return parsed mapping
        return result

    def _parse_person(self, pers: dict) -> dict:
        """Parse a CERIF person element into a flat dictionary.

        Args:
            pers (dict): Raw parsed person dictionary.

        Returns:
            dict: Normalized person dictionary with identification and affiliation fields.

        """
        # Some responses include a wrapper key like 'cerif:Person' or 'openaire_cris:person'
        if isinstance(pers, dict) and "cerif:Person" in pers:
            pers = pers.get("cerif:Person") or pers
        elif isinstance(pers, dict) and "openaire_cris:person" in pers:
            pers = pers.get("openaire_cris:person") or pers

        # Parse affiliations list
        affiliations = self._parse_person_affiliations(
            self._ensure_list(pers.get("cerif:Affiliation"))
        )

        result = {
            "id": self._safe_get(pers, ["@id"]),
            "family_names": self._get_text(
                self._safe_get(pers, ["cerif:PersonName", "cerif:FamilyNames"])
            ),
            "first_names": self._get_text(
                self._safe_get(pers, ["cerif:PersonName", "cerif:FirstNames"])
            ),
            "orcid": self._get_text(pers.get("cerif:ORCID")),
            "scopus_author_id": self._get_text(pers.get("cerif:ScopusAuthorID")),
            "researcher_id": self._get_text(pers.get("cerif:ResearcherID")),
            "affiliations": affiliations,
        }
        return result

    def _parse_person_affiliations(self, affil_list: list) -> list[dict]:
        """Parse a list of person affiliation nodes into dicts.

        Each affiliation wraps a ``cerif:OrgUnit`` with id, name and acronym.

        Args:
            affil_list (list): List of affiliation dicts from CERIF XML.

        Returns:
            list[dict]: Parsed affiliation entries.

        """
        out: list[dict] = []
        for affil in affil_list:
            if not isinstance(affil, dict):
                continue
            org = affil.get("cerif:OrgUnit") or {}
            out.append(
                {
                    "affiliation_id": self._safe_get(org, ["@id"]),
                    "affiliation_name": self._get_text(org.get("cerif:Name")),
                }
            )
        return out

    def _parse_orgunit_identifiers(self, identifier_raw: Any) -> list[dict[str, str | None]]:
        """Parse one or more ``cerif:Identifier`` elements into structured pairs.

        Pure CERIF org-unit records may expose a single identifier or repeated
        identifiers. This helper preserves each identifier with its associated
        ``type`` attribute so downstream consumers can distinguish scalar
        convenience fields from the full identifier list.

        Args:
            identifier_raw (Any): Raw ``cerif:Identifier`` value from xmltodict.

        Returns:
            list[dict[str, str | None]]: Structured ``value`` / ``type`` pairs.

        """
        identifiers: list[dict[str, str | None]] = []
        for item in self._ensure_list(identifier_raw):
            value = self._get_text(item)
            identifier_type = item.get("@type") if isinstance(item, dict) else None
            if value is None and identifier_type is None:
                continue
            identifiers.append({"value": value, "type": identifier_type})
        return identifiers

    def _parse_file_locations(self, file_locations: dict | None) -> list[dict] | None:
        """Parse the `cerif:FileLocations` node into a list of medium dicts.

        Args:
            file_locations (dict | None): The parsed file locations node.

        Returns:
            list[dict] | None: List of parsed medium dicts or None when empty.

        """
        if not file_locations:
            return []
        mediums = self._ensure_list(self._safe_get(file_locations, ["cerif:Medium"]))
        out = []
        for m in mediums:
            out.append(
                {
                    "type": self._get_text(self._safe_get(m, ["cerif:Type"])),
                    "title": self._get_text(self._safe_get(m, ["cerif:Title"])),
                    "uri": self._get_text(m.get("cerif:URI")),
                    "mime_type": self._get_text(m.get("cerif:MimeType")),
                    "size": self._get_text(m.get("cerif:Size")),
                    "access": self._parse_enum(m.get("ar:Access")),
                }
            )
        return out

    def _parse_references(self, refs: dict | None) -> list[dict] | None:
        """Parse `cerif:References` into a list of publication references.

        Args:
            refs (dict | None): The `cerif:References` node.

        Returns:
            list[dict] | None: List of referenced publication dictionaries.

        """
        if not refs:
            return []
        pubs = self._ensure_list(self._safe_get(refs, ["cerif:Publication"]))
        out = []
        for p in pubs:
            out.append(
                {
                    "id": self._safe_get(p, ["@id"]),
                    "type": self._parse_enum(p.get("pubt:Type")),
                    "title": self._get_text(self._safe_get(p, ["cerif:Title"])),
                }
            )
        return out

    def _parse_enum(self, value: str | dict | None) -> str | None:
        """Parse a CERIF controlled vocabulary element to its string ID.

        CERIF vocab values may be returned as a dict with `#text` or as a
        URL-like string; this helper extracts a clean ID when possible.

        Args:
            value (str | dict | None): The raw controlled-vocab element.

        Returns:
            str | None: A trimmed ID string or None when input is missing.

        """
        if value is None:
            return None
        if isinstance(value, dict):
            text_val = value.get("#text")
            return text_val.strip() if text_val else None
        if isinstance(value, str) and ("/" in value or "#" in value):
            return value.split("/")[-1].split("#")[-1]
        return str(value)

    def _parse_person_name(self, name_dict: dict | None) -> tuple[str | None, str | None]:
        """Extract `family` and `first` name from a CERIF name dictionary.

        Args:
            name_dict (dict | None): Name node representing `cerif:PersonName`.

        Returns:
            tuple[str | None, str | None]: Tuple of (family_names, first_names).

        """
        if not isinstance(name_dict, dict):
            return None, None
        family = self._get_text(name_dict.get("cerif:FamilyNames"))
        first = self._get_text(name_dict.get("cerif:FirstNames"))
        return family, first

    def _parse_contributors(self, contrib_list: list | None) -> list[dict] | None:
        """Parse a list of contributor nodes to a list of dictionaries.

        This helper is used to extract authors/editors and their affiliation
        details into a compact structure friendly for DF conversion.

        Args:
            contrib_list (list | None): A list of contributor nodes from CERIF.

        Returns:
            list[dict] | None: Normalized list of contributors, or None when empty.

        """
        if not contrib_list:
            return []
        parsed_list = []
        for item in contrib_list:
            person_data = self._safe_get(item, ["cerif:Person"])
            if not person_data:
                continue
            family_names, first_names = self._parse_person_name(person_data.get("cerif:PersonName"))
            affiliations = self._parse_person_affiliations(
                self._ensure_list(item.get("cerif:Affiliation"))
            )
            primary_affiliation = affiliations[0] if affiliations else {}
            parsed_list.append(
                {
                    "person_id": self._safe_get(person_data, ["@id"]),
                    "family_names": family_names,
                    "first_names": first_names,
                    "affiliation_id": primary_affiliation.get("affiliation_id"),
                    "affiliation_name": primary_affiliation.get("affiliation_name"),
                    "affiliations": affiliations,
                }
            )
        return parsed_list

    def _parse_orgunit(self, org: dict) -> dict:
        """Parse an organization unit entry into a dictionary.

        Args:
            org (dict): Raw organization entry from CERIF XML.

        Returns:
            dict: Normalized organization unit with hierarchy and identifier fields.

        """
        # Unwrap if server returned a wrapper like 'cerif:OrgUnit' or 'openaire_cris:orgunit'
        if isinstance(org, dict) and "cerif:OrgUnit" in org:
            org = org.get("cerif:OrgUnit") or org
        elif isinstance(org, dict) and "openaire_cris:orgunit" in org:
            org = org.get("openaire_cris:orgunit") or org

        # Parse cerif:Identifier which may be a single node or repeated nodes.
        identifiers = self._parse_orgunit_identifiers(org.get("cerif:Identifier"))
        primary_identifier = identifiers[0] if identifiers else {}

        # Parse cerif:PartOf -> cerif:OrgUnit -> @id
        part_of = self._safe_get(org, ["cerif:PartOf", "cerif:OrgUnit"]) or {}
        part_of_org_id = self._safe_get(part_of, ["@id"]) if part_of else None
        part_of_name = self._get_text(part_of.get("cerif:Name")) if part_of else None

        result = {
            "id": self._safe_get(org, ["@id"]),
            "type": self._get_text(org.get("cerif:Type")),
            "identifier": primary_identifier.get("value"),
            "identifier_type": primary_identifier.get("type"),
            "identifiers": identifiers,
            "name": self._get_text(org.get("cerif:Name")),
            "acronym": self._get_text(org.get("cerif:Acronym")),
            "part_of_org_id": part_of_org_id,
            "part_of_name": part_of_name,
        }
        return result

    async def get_all_records(self, collections: list[str]) -> dict[str, list[dict]]:
        """Retrieve all records for a list of OAI-PMH `collections`.

        This method iterates through the collections provided, handling
        resumption tokens internally, parsing records and returning a dict
        mapping collection name to a list of parsed record dictionaries.

        Args:
            collections (list[str]): A list of OAI collection identifiers to fetch.

        Returns:
            dict[str, list[dict]]: Mapping of collection name to parsed records.

        """
        results = {}

        async def get_collection_data(collection: str, position: int | None = None):
            """Fetch a single OAI collection resumption loop and return records.

            Args:
                collection (str): The collection key to fetch.
                position (int | None): Optional tqdm `position` for progress bar.

            Returns:
                dict[str, list[dict]]: Mapping of the collection to the list of parsed records.

            """
            url = f"{self.BASEURL}?verb=ListRecords&metadataPrefix={self.SCHEMA}&set={collection}"
            resume_url = url.split("&metadataPrefix", maxsplit=1)[0]

            """Fetch a single OAI collection resumption loop and return records.

            Args:
                collection (str): The collection key to fetch.
                position (int | None): Optional tqdm `position` for progress bar.

            Returns:
                dict[str, list[dict]]: Mapping of the collection to the list of parsed records.
            """
            col_records: list[dict] = []
            bar = None
            if settings.enable_progress:
                # create a progress bar that updates with number of records fetched; obtain global position for concurrency
                pos = position if position is not None else get_next_position()
                bar = tqdm(desc=f"{collection}", unit="rec", position=pos)
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
                # update progress bar with how many were fetched in this page
                if bar is not None:
                    bar.update(len(recs))
                # resumption token
                token = records.get("resumptionToken")
                if token:
                    # xmltodict returns a dict when attributes exist, plain str otherwise
                    token_text = token.get("#text") if isinstance(token, dict) else token
                    if token_text:
                        url = f"{resume_url}&resumptionToken={token_text}"
                        continue
                url = None

            if bar is not None:
                bar.close()
            final = {collection: col_records}
            return final

        for collection in collections:
            # Do not pass enumerated positions — allocate unique positions globally using get_next_position
            results.update(await get_collection_data(collection, position=None))

        return results
