"""Generate ``scopus_export_sample.xlsx`` fixture for comparison tests.

Run this script once to create the fixture file.  The file is then committed
alongside the tests and does not need to be regenerated.
"""

from __future__ import annotations

import pathlib

import polars as pl

FIXTURE_DIR = pathlib.Path(__file__).parent

ROWS = [
    # 1-3: Will MATCH internal records by DOI
    {
        "DOI": "10.1234/synth.2025.001",
        "Title": "Advances in Metadata Retrieval",
        "Authors": "Researcher, A.",
        "Source title": "Journal of Metadata",
        "Year": 2025,
        "Document Type": "Article",
        "EID": "2-s2.0-00000000001",
        "Cited by": 12,
        "Abstract": "A study on metadata retrieval.",
        "Language of Original Document": "English",
    },
    {
        "DOI": "10.1234/synth.2025.002",
        "Title": "A Survey of Open Access Policies",
        "Authors": "Scientist, B.",
        "Source title": "OA Review",
        "Year": 2025,
        "Document Type": "Review",
        "EID": "2-s2.0-00000000002",
        "Cited by": 5,
        "Abstract": "An overview of OA policies.",
        "Language of Original Document": "English",
    },
    {
        "DOI": "https://doi.org/10.1234/SYNTH.2025.003",  # has prefix + uppercase → tests normalization
        "Title": "Deep Learning for Scholarly Graphs",
        "Authors": "Engineer, C.",
        "Source title": "AI Quarterly",
        "Year": 2024,
        "Document Type": "Article",
        "EID": "2-s2.0-00000000003",
        "Cited by": 30,
        "Abstract": "DL applied to citation graphs.",
        "Language of Original Document": "English",
    },
    # 4-5: SCOPUS-ONLY (DOIs not in internal set)
    {
        "DOI": "10.9999/scopus.only.001",
        "Title": "Scopus Exclusive Paper A",
        "Authors": "Extra, D.",
        "Source title": "Exclusive Journal",
        "Year": 2025,
        "Document Type": "Conference Paper",
        "EID": "2-s2.0-00000000004",
        "Cited by": 0,
        "Abstract": "Not in internal data.",
        "Language of Original Document": "English",
    },
    {
        "DOI": "10.9999/scopus.only.002",
        "Title": "Scopus Exclusive Paper B",
        "Authors": "Extra, E.",
        "Source title": "Exclusive Journal",
        "Year": 2024,
        "Document Type": "Article",
        "EID": "2-s2.0-00000000005",
        "Cited by": 2,
        "Abstract": None,
        "Language of Original Document": "English",
    },
    # 6: FIELD MISMATCH — matches by DOI but title and type differ
    {
        "DOI": "10.1234/synth.2025.004",
        "Title": "Scopus Title for Mismatch Check",  # internal has different title
        "Authors": "Mismatch, F.",
        "Source title": "Mismatch Journal",
        "Year": 2025,
        "Document Type": "Conference Paper",  # internal has "Article"
        "EID": "2-s2.0-00000000006",
        "Cited by": 7,
        "Abstract": "Mismatch test record.",
        "Language of Original Document": "English",
    },
    # 7: NO DOI — always scopus-only
    {
        "DOI": None,
        "Title": "Record Without DOI",
        "Authors": "NoDoi, G.",
        "Source title": "No DOI Journal",
        "Year": 2023,
        "Document Type": "Editorial",
        "EID": "2-s2.0-00000000007",
        "Cited by": 0,
        "Abstract": None,
        "Language of Original Document": "English",
    },
    # 8-10: More matched-by-DOI rows with clean data
    {
        "DOI": "10.1234/synth.2025.005",
        "Title": "Scalable Repository Harvesting",
        "Authors": "Harvest, H.",
        "Source title": "Repository Today",
        "Year": 2025,
        "Document Type": "Article",
        "EID": "2-s2.0-00000000008",
        "Cited by": 3,
        "Abstract": "Scalable approaches to harvesting.",
        "Language of Original Document": "English",
    },
    {
        "DOI": "10.1234/synth.2025.006",
        "Title": "FAIR Data Practices in Institutions",
        "Authors": "Fair, I.",
        "Source title": "Data Governance",
        "Year": 2024,
        "Document Type": "Review",
        "EID": "2-s2.0-00000000009",
        "Cited by": 15,
        "Abstract": "Review of FAIR practices.",
        "Language of Original Document": "English",
    },
    {
        "DOI": "10.1234/synth.2025.007",
        "Title": "Persistent Identifiers for Research",
        "Authors": "Persist, J.",
        "Source title": "ID Standards",
        "Year": 2025,
        "Document Type": "Article",
        "EID": "2-s2.0-00000000010",
        "Cited by": 8,
        "Abstract": "PID ecosystem review.",
        "Language of Original Document": "English",
    },
]


def generate() -> None:
    """Write the fixture to ``scopus_export_sample.xlsx``."""
    df = pl.DataFrame(ROWS)
    out = FIXTURE_DIR / "scopus_export_sample.xlsx"
    df.write_excel(out)
    print(f"Wrote {len(df)} rows → {out}")


if __name__ == "__main__":
    generate()
