from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

# import timing functions to measure performance
from time import perf_counter

import polars as pl
from lxml import etree

from syntheca.clients.base import BaseClient

# Define namespaces globally for lxml performance
NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "cerif": "https://www.openaire.eu/cerif-profile/1.1/",
    "openaire_cris": "https://www.openaire.eu/cerif-profile/1.1/",
    "pubt": "http://www.openarchives.org/OAI/2.0/",  # Adjust based on actual XML if needed
    "ar": "http://www.openarchives.org/OAI/2.0/",  # Adjust based on actual XML if needed
}


def generate_date_chunks(
    start_year: int, end_year: int, chunk_size_days: int = 30
) -> list[tuple[str, str]]:
    """Generate (from, until) date strings.

    Args:
        start_year: The starting year (inclusive).
        end_year: The ending year (exclusive).
        chunk_size_days: Number of days per chunk.

    Returns:
        List of (from, until) date string tuples in 'YYYY-MM-DD' format.

    """
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    chunks = []

    current = start_date
    while current < end_date:
        # Define the end of this chunk
        chunk_end = current + timedelta(days=chunk_size_days)
        if chunk_end > end_date:
            chunk_end = end_date

        # Format as YYYY-MM-DD (Standard OAI format)
        fmt = "%Y-%m-%d"
        chunks.append((current.strftime(fmt), chunk_end.strftime(fmt)))

        # Move start to next day
        current = chunk_end + timedelta(days=1)
    for chunk in chunks:
        print(f"{chunk[0]:<10} -- {chunk[1]:<10}")
    return chunks


class PureOAIClient(BaseClient):
    BASEURL = "https://ris.utwente.nl/ws/oai"
    SCHEMA = "oai_cerif_openaire"

    # --- Helper Methods for lxml ---
    @staticmethod
    def _xpath_text(node: etree._Element, xpath_query: str) -> str | None:
        """Efficiently extract first text result from an xpath query."""
        # lxml xpath returns a list of results (strings or nodes)
        res = node.xpath(xpath_query, namespaces=NS)
        if not res:
            return None
        # if the result is a node, get .text, if it's already a string (from /text()), use it
        val = res[0]
        return str(val).strip() if val else None

    @staticmethod
    def _xpath_list_text(node: etree._Element, xpath_query: str) -> list[str]:
        """Extract all text results from an xpath query."""
        res = node.xpath(xpath_query, namespaces=NS)
        return [str(r).strip() for r in res if r]

    # --- Parsing Logic (Translated to lxml) ---

    def _parse_publication(self, pub: etree._Element) -> dict:
        """Parse a CERIF publication XML element to a flat dict using XPath."""
        # Note: XPath handles the 'if exists' logic automatically.
        # If the path doesn't exist, it returns [], which _xpath_text converts to None.

        return {
            "id": self._xpath_text(pub, "@id"),
            "type": self._xpath_text(pub, "pubt:Type/text()"),
            "language": self._xpath_text(pub, "cerif:Language/text()"),
            "title": self._xpath_text(pub, "cerif:Title/text()"),
            "publication_date": self._xpath_text(pub, "cerif:PublicationDate/text()"),
            "doi": self._xpath_text(pub, "cerif:DOI/text()"),
            "url": self._xpath_text(pub, "cerif:URL/text()"),
            "abstract": self._xpath_text(pub, "cerif:Abstract/text()"),
            "volume": self._xpath_text(pub, "cerif:Volume/text()"),
            "issue": self._xpath_text(pub, "cerif:Issue/text()"),
            "start_page": self._xpath_text(pub, "cerif:StartPage/text()"),
            "end_page": self._xpath_text(pub, "cerif:EndPage/text()"),
            "status": self._xpath_text(pub, "cerif:Status/text()"),
            "access_right": self._xpath_text(pub, "ar:Access/text()"),
            # Relationships
            "published_in_id": self._xpath_text(pub, "cerif:PublishedIn/cerif:Publication/@id"),
            "published_in_title": self._xpath_text(
                pub, "cerif:PublishedIn/cerif:Publication/cerif:Title/text()"
            ),
            "part_of_id": self._xpath_text(pub, "cerif:PartOf/cerif:Publication/@id"),
            "part_of_title": self._xpath_text(
                pub, "cerif:PartOf/cerif:Publication/cerif:Title/text()"
            ),
            "event_name": self._xpath_text(pub, "cerif:PresentedAt/cerif:Event/cerif:Name/text()"),
            # Complex nested structures
            "authors": self._parse_contributors(
                pub.xpath("cerif:Authors/cerif:Author", namespaces=NS)
            ),
            "editors": self._parse_contributors(
                pub.xpath("cerif:Editors/cerif:Editor", namespaces=NS)
            ),
            "keywords": self._xpath_list_text(pub, "cerif:Keyword/text()"),
            "isbn": self._xpath_list_text(pub, "cerif:ISBN/text()"),
            "issn": self._xpath_list_text(pub, "cerif:ISSN/text()"),
            "publisher_name": self._xpath_text(
                pub, "cerif:Publishers/cerif:Publisher/cerif:OrgUnit/cerif:Name/text()"
            ),
            "file_locations": self._parse_file_locations(
                pub.find("cerif:FileLocations", namespaces=NS)
            ),
            "references": self._parse_references(pub.find("cerif:References", namespaces=NS)),
        }

    def _parse_contributors(self, nodes: list[etree._Element]) -> list[dict]:
        out = []
        for item in nodes:
            # Relative paths inside the contributor node
            p_node = item.find("cerif:Person", namespaces=NS)
            if p_node is None:
                continue

            aff_node = item.find("cerif:Affiliation/cerif:OrgUnit", namespaces=NS)

            out.append(
                {
                    "person_id": p_node.get("id")
                    or p_node.get(
                        "{http://www.w3.org/XML/1998/namespace}id"
                    ),  # Handle xml:id or @id
                    "family_names": self._xpath_text(
                        p_node, "cerif:PersonName/cerif:FamilyNames/text()"
                    ),
                    "first_names": self._xpath_text(
                        p_node, "cerif:PersonName/cerif:FirstNames/text()"
                    ),
                    "affiliation_id": aff_node.get("id") if aff_node is not None else None,
                    "affiliation_name": self._xpath_text(aff_node, "cerif:Name/text()")
                    if aff_node is not None
                    else None,
                }
            )
        return out

    def _parse_file_locations(self, node: etree._Element | None) -> list[dict]:
        if node is None:
            return []
        out = []
        for m in node.xpath("cerif:Medium", namespaces=NS):
            out.append(
                {
                    "type": self._xpath_text(m, "cerif:Type/text()"),
                    "title": self._xpath_text(m, "cerif:Title/text()"),
                    "uri": self._xpath_text(m, "cerif:URI/text()"),
                    "mime_type": self._xpath_text(m, "cerif:MimeType/text()"),
                    "size": self._xpath_text(m, "cerif:Size/text()"),
                    "access": self._xpath_text(m, "ar:Access/text()"),
                }
            )
        return out

    def _parse_references(self, node: etree._Element | None) -> list[dict]:
        if node is None:
            return []
        out = []
        for p in node.xpath("cerif:Publication", namespaces=NS):
            out.append(
                {
                    "id": p.get("id"),
                    "type": self._xpath_text(p, "pubt:Type/text()"),
                    "title": self._xpath_text(p, "cerif:Title/text()"),
                }
            )
        return out

    def _parse_person(self, person: etree._Element) -> dict:
        """Parse a CERIF person XML element to a flat dict using XPath."""
        return {
            "id": self._xpath_text(person, "@id"),
            "family_names": self._xpath_text(person, "cerif:PersonName/cerif:FamilyNames/text()"),
            "first_names": self._xpath_text(person, "cerif:PersonName/cerif:FirstNames/text()"),
            "orcid": self._xpath_text(person, "cerif:ORCID/text()"),
        }

    def _parse_orgunit(self, org: etree._Element) -> dict:
        """Parse a CERIF orgunit XML element to a flat dict using XPath."""
        return {
            "id": self._xpath_text(org, "@id"),
            "name": self._xpath_text(org, "cerif:Name/text()"),
            "acronym": self._xpath_text(org, "cerif:Acronym/text()"),
        }

    async def get_all_records(self, collections: list[str]) -> dict[str, list[dict]]:
        results = {}
        semaphore = asyncio.Semaphore(10)

        async def harvest_worker(coll, d_from, d_until):
            async with semaphore:
                return await self._harvest_collection_concurrent(
                    coll, date_from=d_from, date_until=d_until
                )

        for collection in collections:
            print(f"Starting parallel harvest for: {collection}")
            # 1. Generate Date Chunks (e.g., last 20 years in 1-year chunks)
            # Adjust start year based on your institution's history
            chunks = generate_date_chunks(2015, 2025)
            tasks = []
            for d_from, d_until in chunks:
                tasks.append(harvest_worker(collection, d_from, d_until))
            # 2. Run all date ranges in parallel
            chunk_results = await asyncio.gather(*tasks)
            # 3. Flatten results
            all_records = []
            for res in chunk_results:
                all_records.extend(res)
            results[collection] = all_records
        return results

    async def _harvest_collection_concurrent(
        self, collection: str, date_from: str | None = None, date_until: str | None = None
    ) -> list[dict]:
        """Concurrent Fetcher and Parser with Date Filters."""
        queue = asyncio.Queue(maxsize=10)
        final_records = []

        # Build initial URL with date params
        base_params = (
            f"?verb=ListRecords&metadataPrefix={self.SCHEMA}&set=openaire_cris_{collection}"
        )
        if date_from:
            base_params += f"&from={date_from}"
        if date_until:
            base_params += f"&until={date_until}"

        initial_url = f"{self.BASEURL}{base_params}"

        # We need a resume_base that DOES NOT include the date params,
        # because the resumptionToken implies the original params.
        # Note: OAI spec says usually you just do ?verb=ListRecords&resumptionToken=XXXX
        # You do NOT re-append &from/&until/&set when using a token.
        resume_url_base = f"{self.BASEURL}?verb=ListRecords"

        print(f"[{date_from} to {date_until}] Starting...")

        async def producer():
            url = initial_url
            while url:
                try:
                    resp = await self.request("GET", url)

                    # Handle "No Records Match" (Common in date splitting if a year was empty)
                    if b"noRecordsMatch" in resp.content:
                        print(f"[{date_from}] No records found.")
                        url = None
                        continue

                    parser = etree.XMLParser(recover=True, huge_tree=True)
                    root = etree.fromstring(resp.content, parser=parser)

                    token_node = root.find(".//oai:resumptionToken", namespaces=NS)
                    token = token_node.text if token_node is not None else None

                    await queue.put(root)

                    url = f"{resume_url_base}&resumptionToken={token}" if token else None
                except Exception as e:
                    print(f"Error fetching {url}: {e}")
                    url = None

            await queue.put(None)

        async def consumer():
            """Parse XML Tree into dicts."""
            print("Consumer started, waiting")
            start_t = perf_counter()
            while True:
                root = await queue.get()
                if root is None:
                    print("Consumer received termination signal, exiting.")
                    break
                # Find all record metadata blocks
                # The path depends on your exact OAI structure. Usually:
                # OAI-PMH -> ListRecords -> record -> metadata -> (Payload)
                records = root.xpath(".//oai:record", namespaces=NS)

                batch_data = []
                for rec in records:
                    # Check for deleted records
                    header_status = rec.find("oai:header", namespaces=NS).get("status")
                    if header_status == "deleted":
                        continue

                    # Find the payload inside metadata
                    # Note: We look for ANY child of metadata, then filter by tag/type
                    meta = rec.find("oai:metadata", namespaces=NS)
                    if meta is not None and len(meta):
                        # Get the first child of metadata (the actual content)
                        payload = meta[0]

                        # Determine parser based on payload tag or collection name
                        tag = payload.tag

                        parsed_item = None
                        if "Person" in tag or "person" in collection:
                            parsed_item = self._parse_person(
                                payload
                            )  # You need to implement the lxml version
                        elif "OrgUnit" in tag or "org" in collection:
                            parsed_item = self._parse_orgunit(
                                payload
                            )  # You need to implement the lxml version
                        else:
                            parsed_item = self._parse_publication(payload)

                        if parsed_item:
                            batch_data.append(parsed_item)

                if batch_data:
                    final_records.extend(batch_data)
                    print(
                        f"[{collection}][{date_from} - {date_until}][{len(final_records)} items][{perf_counter() - start_t:.2f}s]"
                    )

                # Cleanup: Clear the tree from memory
                root.clear()
                queue.task_done()

        # Run Producer and Consumer concurrently
        t_prod = asyncio.create_task(producer())
        t_cons = asyncio.create_task(consumer())

        await asyncio.gather(t_prod, t_cons)

        # # Optional: Save intermediate result immediately
        # if settings.persist_intermediate and final_records:
        if final_records:
            try:
                pl.from_dicts(final_records).write_parquet(
                    f"pure_oai_{collection}_{date_from}_to_{date_until}.parquet"
                )
            except Exception as e:
                print(
                    f"Failed to save intermediate parquet for {collection} {date_from} to {date_until}: {e}"
                )
                pass

        return final_records


async def main():
    client = PureOAIClient()
    async with client:
        data = await client.get_all_records(["publications"])
        for coll, records in data.items():
            print(f"Collection: {coll}, Records fetched: {len(records)}")


if __name__ == "__main__":
    asyncio.run(main())
