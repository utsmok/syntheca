"""Pure OAI-PMH client integration for the syntheca project.

This module provides `PureOAIClient`, which wraps Pure's OAI-PMH endpoints
and parses CERIF/OAI XML into flattened dictionaries for downstream
processing and analysis.
"""

from __future__ import annotations

from typing import Any

import polars as pl
import xmltodict
from tqdm import tqdm

from syntheca.clients.base import BaseClient
from syntheca.config import settings
from syntheca.utils.persistence import load_dataframe_parquet
from syntheca.utils.persistence import save_dataframe_parquet
from syntheca.utils.progress import get_next_position

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
            "authors": self._parse_contributors(self._ensure_list(self._safe_get(pub, ["cerif:Authors", "cerif:Author"]))),
            "editors": self._parse_contributors(self._ensure_list(self._safe_get(pub, ["cerif:Editors", "cerif:Editor"]))),
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
            )
        }
        # Published in / Part of relationships
        published_in = self._safe_get(pub, ["cerif:PublishedIn", "cerif:Publication"]) or {}
        result["published_in_id"] = self._safe_get(published_in, ["@id"]) if published_in else None
        result["published_in_title"] = self._get_text(
            self._safe_get(published_in, ["cerif:Title"])
        )
        part_of = self._safe_get(pub, ["cerif:PartOf", "cerif:Publication"]) or {}
        result["part_of_id"] = self._safe_get(part_of, ["@id"]) if part_of else None
        result["part_of_title"] = self._get_text(
            self._safe_get(part_of, ["cerif:Title"])
        )

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
            dict: Normalized person dictionary with `id`, `family_names`, `first_names`, and `orcid`.

        """
        # Some responses include a wrapper key like 'cerif:Person' or 'openaire_cris:person'
        if isinstance(pers, dict) and "cerif:Person" in pers:
            pers = pers.get("cerif:Person") or pers
        elif isinstance(pers, dict) and "openaire_cris:person" in pers:
            pers = pers.get("openaire_cris:person") or pers
        result = {
            "id": self._safe_get(pers, ["@id"]),
            "family_names": self._get_text(
                self._safe_get(pers, ["cerif:PersonName", "cerif:FamilyNames"])
            ),
            "first_names": self._get_text(
                self._safe_get(pers, ["cerif:PersonName", "cerif:FirstNames"])
            ),
            "orcid": self._get_text(pers.get("cerif:ORCID")),
        }
        return result

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
        return parsed_list

    def _parse_orgunit(self, org: dict) -> dict:
        """Parse an organization unit entry into a dictionary.

        Args:
            org (dict): Raw organization entry from CERIF XML.

        Returns:
            dict: Normalized organization unit with `id`, `name`, and `acronym`.

        """
        # Unwrap if server returned a wrapper like 'cerif:OrgUnit' or 'openaire_cris:orgunit'
        if isinstance(org, dict) and "cerif:OrgUnit" in org:
            org = org.get("cerif:OrgUnit") or org
        elif isinstance(org, dict) and "openaire_cris:orgunit" in org:
            org = org.get("openaire_cris:orgunit") or org

        result = {
            "id": self._safe_get(org, ["@id"]),
            "name": self._get_text(org.get("cerif:Name")),
            "acronym": self._get_text(org.get("cerif:Acronym")),
        }

        # Debug logs removed: parsing returns `result` mapping
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
                break
                # resumption token
                token = records.get("resumptionToken")
                if token:
                    token_text = token.get("#text")
                    if token_text:
                        url = f"{resume_url}&resumptionToken={token_text}"
                        continue
                url = None

            if bar is not None:
                bar.close()
            final = {collection:col_records}
            return final

        for collection in collections:
            # If cache-for-retrieval is enabled, try to load the cached parquet file for this collection
            if settings.use_cache_for_retrieval:
                try:
                    df = load_dataframe_parquet(f"pure_{collection}")
                    if df is not None and df.height:
                        results[collection] = df.to_dicts()
                        continue
                except Exception:
                    # fall back to live retrieval if cache load fails
                    pass
            # Do not pass enumerated positions — allocate unique positions globally using get_next_position
            results.update(await get_collection_data(collection, position=None))

        # persist intermediate results if configured
        if settings.persist_intermediate:
            # Save each collection as parquet for quick inspection
            for col, recs in results.items():
                if recs:
                    try:
                        df = pl.from_dicts(recs)
                        save_dataframe_parquet(df, f"pure_{col}")
                    except Exception:
                        # ignore saving errors
                        pass

        return results
