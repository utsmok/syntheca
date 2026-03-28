"""Generate synthetic OpenAlex work fixtures for load testing.

Produces realistic Work dataclass instances with varied nested structures
to stress-test schema inference and incremental persistence.

The generator creates items that exercise the exact same schema surface
as the real ``Work`` dataclass, but with controlled variation in which
nested fields are populated vs. all-None.  This variation is essential
for testing schema-consistency bugs: when different batches produce
different Polars struct layouts, ``pl.concat`` can fail unless schemas
are aligned via the ``_chunk_schema`` memo in ``get_works_by_ids``.

Usage::

    from tests.fixtures.openalex.generate_large_fixture import (
        generate_work_dicts,
        generate_api_response_page,
    )

    # Produce 5000 raw dicts matching the Work schema
    items = generate_work_dicts(count=5000, seed=42)

    # Produce a paginated OpenAlex API response envelope for 50 items
    page = generate_api_response_page(items[:50], total_count=5000)
"""

from __future__ import annotations

import random
from typing import Any

# Deterministic choice pools --------------------------------------------------

_FIRST_NAMES = [
    "Abhishek", "Anna", "Ben", "Clara", "David", "Emma", "Fatima", "George",
    "Hannah", "Ivan", "Julia", "Kai", "Lena", "Marco", "Nadia", "Omar",
    "Priya", "Quentin", "Rosa", "Sam", "Tatiana", "Uma", "Victor", "Wei",
    "Xia", "Yuki", "Zara", "Ahmed", "Bianca", "Carlos",
]

_LAST_NAMES = [
    "Sharma", "Mueller", "Johnson", "Garcia", "Kim", "Novak", "Silva",
    "Patel", "Williams", "Tanaka", "Jansen", "Alvarez", "Singh", "Brown",
    "Chen", "Rodriguez", "Park", "Ivanov", "Lopez", "Lee",
]

_INSTITUTIONS = [
    ("I94624287", "University of Twente", "NL"),
    ("I136293718", "MIT", "US"),
    ("I185605682", "University of Oxford", "GB"),
    ("I2802298767", "ETH Zurich", "CH"),
    ("I4210104726", "TU Delft", "NL"),
    ("I201448701", "Stanford University", "US"),
    ("I14147328", "University of Tokyo", "JP"),
    ("I84862036", "University of Melbourne", "AU"),
    ("I204699606", "University of Toronto", "CA"),
    ("I4210111043", "Technical University of Munich", "DE"),
]

_WORK_TYPES = [
    "article", "book-chapter", "preprint", "review", "book", "dataset",
    "dissertation", "report", "software", "editorial", "letter", "retraction",
    "paratext", "peer-review", "reference-entry", "standard",
    "supplementary-materials", "book-section", "report-component",
    "database", "grant",
]

_WORK_TYPES_CROSSREF = [
    "journal-article", "book-chapter", "posted-content", "book",
    "dataset", "dissertation", "report", "reference-entry", "component",
    "proceedings-article", "monograph", "book-track", "book-part",
    "journal-volume", "book-set", "proceedings-series", "report-series",
    "proceedings", "database", "standard", "reference-book", "journal",
    "journal-issue", "book-series", "edited-book", "grant",
]

_OA_STATUSES = ["diamond", "gold", "green", "hybrid", "bronze", "closed"]

_JOURNALS = [
    ("S137773608", "Nature", "journal"),
    ("S4300406915", "PLOS ONE", "journal"),
    ("S51057483", "Science", "journal"),
    ("S20572221", "arXiv", "repository"),
    ("S15295830", "IEEE Access", "journal"),
    ("S9947111", "Springer Nature", "ebook platform"),
]

_TOPICS = [
    ("T10100", "Physics", "Quantum Mechanics", "Physical Sciences", "Natural Sciences"),
    ("T10234", "Machine Learning", "Artificial Intelligence", "Computer Science", "Natural Sciences"),
    ("T10456", "Materials Science", "Nanotechnology", "Engineering", "Natural Sciences"),
    ("T10678", "Ecology", "Environmental Science", "Agricultural and Biological Sciences", "Life Sciences"),
    ("T10890", "Chemistry", "Organic Chemistry", "Chemical Engineering", "Natural Sciences"),
    ("T11012", "Medicine", "Clinical Medicine", "Health Sciences", "Health Sciences"),
    ("T10045", "Mathematics", "Applied Mathematics", "Mathematics", "Natural Sciences"),
    ("T10367", "Economics", "Econometrics", "Economics", "Social Sciences"),
    ("T10589", "Sociology", "Social Psychology", "Social Sciences", "Social Sciences"),
    ("T10723", "Engineering", "Mechanical Engineering", "Engineering", "Natural Sciences"),
]


def _pick(rng: random.Random, pool: list, k: int = 1) -> list:
    """Return *k* unique items from *pool*, or fewer if pool is too small."""
    return rng.sample(pool, min(k, len(pool)))


def _make_work_ids(rng: random.Random, idx: int) -> dict[str, Any]:
    return {
        "openalex": f"https://openalex.org/W{idx:012d}",
        "doi": f"https://doi.org/10.{rng.randint(1000, 9999)}/synth.{rng.randint(100000, 999999)}",
        "mag": None,
        "pmid": f"https://pubmed.ncbi.nlm.nih.gov/{rng.randint(10000000, 39999999)}" if rng.random() > 0.4 else None,
        "pmcid": f"PMC{rng.randint(1000000, 9999999)}" if rng.random() > 0.7 else None,
    }


def _make_author(rng: random.Random, idx: int) -> dict[str, Any]:
    first = rng.choice(_FIRST_NAMES)
    last = rng.choice(_LAST_NAMES)
    aid = rng.randint(5000000, 9999999)
    return {
        "id": f"https://openalex.org/A{aid}",
        "display_name": f"{first} {last}",
        "relevance_score": None,
        "created_date": "2020-01-01",
        "updated_date": "2025-01-01",
        "score": None,
        "cited_by_count": rng.randint(0, 5000),
        "works_count": rng.randint(1, 200),
        "works_api_url": f"https://api.openalex.org/works?filter=author.id:A{aid}",
        "orcid": f"https://orcid.org/0000-{rng.randint(1000,9999)}-{rng.randint(1000,9999)}-{rng.randint(1000,9999)}X" if rng.random() > 0.5 else None,
    }


def _make_institution(rng: random.Random, inst_tuple: tuple[str, str, str]) -> dict[str, Any]:
    iid, name, cc = inst_tuple
    return {
        "id": f"https://openalex.org/{iid}",
        "display_name": name,
        "relevance_score": None,
        "created_date": "2019-01-01",
        "updated_date": "2025-01-01",
        "score": None,
        "cited_by_count": rng.randint(1000, 100000),
        "works_count": rng.randint(100, 50000),
        "works_api_url": f"https://api.openalex.org/works?filter=institutions.id:{iid}",
        "country_code": cc,
        "lineage": [f"https://openalex.org/{iid}"],
        "ror": f"https://ror.org/0{''.join(str(rng.randint(0,9)) for _ in range(6))}01",
        "type": "education",
    }


def _make_authorship(rng: random.Random, idx: int, is_first: bool) -> dict[str, Any]:
    author = _make_author(rng, idx)
    inst_tuple = rng.choice(_INSTITUTIONS)
    inst = _make_institution(rng, inst_tuple)
    return {
        "author": author,
        "raw_author_name": author["display_name"],
        "is_corresponding": is_first,
        "countries": [inst["country_code"]] if rng.random() > 0.2 else [],
        "author_position": "first" if is_first else rng.choice(["middle", "last"]),
        "affiliations": [
            {
                "raw_affiliation_string": f"{inst['display_name']}, {inst['country_code']}",
                "institution_ids": [f"https://openalex.org/{inst_tuple[0]}"],
            }
        ],
        "institutions": [inst],
        "raw_affiliation_strings": [f"{inst['display_name']}, {inst['country_code']}"],
    }


def _make_source(rng: random.Random, journal_tuple: tuple[str, str, str] | None = None) -> dict[str, Any]:
    if journal_tuple is None:
        journal_tuple = rng.choice(_JOURNALS)
    sid, name, stype = journal_tuple
    return {
        "id": f"https://openalex.org/{sid}",
        "display_name": name,
        "relevance_score": None,
        "created_date": "2018-01-01",
        "updated_date": "2025-01-01",
        "score": None,
        "cited_by_count": rng.randint(5000, 200000),
        "works_count": rng.randint(500, 100000),
        "works_api_url": f"https://api.openalex.org/works?filter=primary_location.source.id:{sid}",
        "is_core": rng.random() > 0.3,
        "is_in_doaj": rng.random() > 0.6,
        "is_oa": rng.random() > 0.5,
        "is_indexed_in_scopus": rng.random() > 0.4,
        "type": stype,
        "issn_l": f"{rng.randint(1000,9999)}-{rng.randint(1000,9999)}",
        "issn": [f"{rng.randint(1000,9999)}-{rng.randint(1000,9999)}"],
        "host_organization": f"https://openalex.org/P{rng.randint(1000,9999)}",
        "host_organization_lineage": [],
        "host_organization_name": f"{name} Publisher",
        "host_organization_lineage_names": [f"{name} Publisher"],
        "raw_type": None,
    }


def _make_location(rng: random.Random, with_source: bool = True) -> dict[str, Any]:
    loc: dict[str, Any] = {
        "is_accepted": rng.random() > 0.3,
        "is_oa": rng.random() > 0.5,
        "is_published": rng.random() > 0.3,
        "landing_page_url": f"https://example.org/paper/{rng.randint(1000,99999)}",
        "pdf_url": f"https://example.org/paper/{rng.randint(1000,99999)}.pdf" if rng.random() > 0.4 else None,
        "license": rng.choice(["cc-by", "cc-by-nc", "cc-by-sa", "cc0", None]),
        "license_id": None,
        "source": _make_source(rng) if with_source else None,
        "version": rng.choice(["publishedVersion", "acceptedVersion", "submittedVersion", None]),
        "raw_source_name": None,
        "id": None,
    }
    return loc


def _make_topic(rng: random.Random) -> dict[str, Any]:
    t = rng.choice(_TOPICS)
    return {
        "id": t[0],
        "display_name": t[1],
        "relevance_score": None,
        "created_date": "2023-01-01",
        "updated_date": "2025-01-01",
        "score": None,
        "cited_by_count": rng.randint(1000, 50000),
        "works_count": rng.randint(100, 20000),
        "works_api_url": f"https://api.openalex.org/works?filter=topics.id:{t[0]}",
        "subfield": {"id": f"https://openalex.org/SF{rng.randint(100,999)}", "display_name": t[2]},
        "field": {"id": f"https://openalex.org/F{rng.randint(100,999)}", "display_name": t[3]},
        "domain": {"id": f"https://openalex.org/D{rng.randint(100,999)}", "display_name": t[4]},
    }


def _make_sdg(rng: random.Random) -> dict[str, Any]:
    return {
        "id": f"https://openalex.org/SDG{rng.randint(1, 17)}",
        "display_name": f"SDG {rng.randint(1, 17)}",
        "relevance_score": None,
        "created_date": "2023-01-01",
        "updated_date": "2025-01-01",
        "score": round(rng.uniform(0.5, 1.0), 4),
        "cited_by_count": rng.randint(100, 10000),
        "works_count": rng.randint(50, 5000),
        "works_api_url": None,
    }


def _make_open_access(rng: random.Random) -> dict[str, Any]:
    status = rng.choice(_OA_STATUSES)
    return {
        "is_oa": status != "closed",
        "oa_status": status,
        "oa_url": f"https://example.org/oa/{rng.randint(1000,99999)}" if status != "closed" else None,
        "any_repository_has_fulltext": rng.random() > 0.5,
    }


def _make_base_fields(rng: random.Random, idx: int) -> dict[str, Any]:
    """Fields inherited from BaseOpenAlex."""
    return {
        "id": f"https://openalex.org/W{idx:012d}",
        "display_name": f"Synthetic work {idx}: {_random_title_fragment(rng)}",
        "relevance_score": None,
        "created_date": f"{rng.randint(2020, 2025)}-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}",
        "updated_date": f"2025-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}",
        "score": None,
        "cited_by_count": rng.randint(0, 500),
        "works_count": None,
        "works_api_url": None,
    }


_TITLE_FRAGMENTS = [
    "Advances in", "Analysis of", "Towards", "A novel approach to", "Exploring",
    "Understanding", "Computational", "Empirical study of", "On the nature of",
    "Revisiting", "Optimization of", "Characterization of", "Systematic review of",
    "Meta-analysis of", "Impact of", "Evaluation of", "Design and implementation of",
    "Comparative analysis", "New perspectives on", "Insights into",
]

_TITLE_TOPICS = [
    "quantum computing", "machine learning", "climate change", "nanomaterials",
    "protein folding", "social networks", "renewable energy", "autonomous vehicles",
    "blockchain technology", "gene editing", "dark matter", "neural networks",
    "supply chain optimization", "virtual reality", "cancer immunotherapy",
    "sustainable agriculture", "space exploration", "cybersecurity", "robotics",
    "ocean acidification",
]


def _random_title_fragment(rng: random.Random) -> str:
    return f"{rng.choice(_TITLE_FRAGMENTS)} {rng.choice(_TITLE_TOPICS)}"


def _make_work_dict(
    rng: random.Random,
    idx: int,
    *,
    sparse: bool = False,
) -> dict[str, Any]:
    """Build a single work dict matching the ``Work`` dataclass schema.

    When *sparse* is True, many optional nested fields (locations, topics,
    concepts, keywords, mesh, sustainable_development_goals, best_oa_location,
    primary_location, biblio, apc_list, apc_paid, funders, institutions,
    awards, abstract_inverted_index, grants) are set to empty lists or None.
    This produces rows that, when converted to a Polars DataFrame independently,
    yield a different struct schema from rows that have these fields populated.

    The point is to exercise the schema-alignment code path where the first
    chunk has sparse structs and later chunks add populated nested fields
    (or vice versa).
    """
    work = _make_base_fields(rng, idx)

    # Required scalar fields (always present)
    work["doi"] = f"10.{rng.randint(1000,9999)}/synth.{rng.randint(100000,999999)}"
    work["title"] = work["display_name"]
    year = rng.randint(2018, 2025)
    work["publication_year"] = year
    work["publication_date"] = f"{year}-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}"
    work["type"] = rng.choice(_WORK_TYPES)
    work["type_crossref"] = rng.choice(_WORK_TYPES_CROSSREF)
    work["ids"] = _make_work_ids(rng, idx)
    work["is_paratext"] = False
    work["is_retracted"] = False
    work["has_fulltext"] = rng.random() > 0.3
    work["locations_count"] = rng.randint(0, 5)
    work["cited_by_count"] = rng.randint(0, 500)
    work["countries_distinct_count"] = rng.randint(0, 5)
    work["institutions_distinct_count"] = rng.randint(0, 5)

    work["open_access"] = _make_open_access(rng)

    # Authorships: always present (0-5 authors)
    n_authors = rng.randint(1, 5)
    work["authorships"] = [
        _make_authorship(rng, idx * 100 + a, is_first=(a == 0))
        for a in range(n_authors)
    ]

    if sparse:
        # Sparse mode: minimal nested structures to produce different schema
        work["institution_assertions"] = None
        work["funders"] = None
        work["institutions"] = None
        work["is_xpac"] = None
        work["awards"] = None
        work["abstract_inverted_index"] = None
        work["apc_list"] = None
        work["apc_paid"] = None
        work["best_oa_location"] = None
        work["biblio"] = None
        work["citation_normalized_percentile"] = None
        work["cited_by_api_url"] = None
        work["concepts"] = []
        work["corresponding_author_ids"] = []
        work["corresponding_institution_ids"] = []
        work["counts_by_year"] = []
        work["fulltext_origin"] = None
        work["fwci"] = None
        work["grants"] = []
        work["indexed_in"] = []
        work["keywords"] = []
        work["language"] = None
        work["license"] = None
        work["locations"] = []
        work["mesh"] = []
        work["primary_location"] = None
        work["primary_topic"] = None
        work["referenced_works"] = []
        work["related_works"] = []
        work["sustainable_development_goals"] = []
        work["topics"] = []
        work["has_content"] = None
        work["cited_by_percentile_year"] = None
        work["datasets"] = None
        work["versions"] = None
        work["referenced_works_count"] = None
    else:
        # Full mode: populate everything
        work["institution_assertions"] = None
        n_funders = rng.randint(0, 3)
        inst_choices = _pick(rng, _INSTITUTIONS, n_funders)
        work["funders"] = [
            {
                "id": f"https://openalex.org/F{rng.randint(100000,999999)}",
                "display_name": f" funding body {i}",
                "ror": f"https://ror.org/0{''.join(str(rng.randint(0,9)) for _ in range(6))}",
            }
            for i in range(n_funders)
        ] or None
        work["institutions"] = [
            _make_institution(rng, ic)
            for ic in inst_choices
        ] or None
        work["is_xpac"] = None
        work["awards"] = [
            {
                "id": f"https://openalex.org/G{rng.randint(100000,999999)}",
                "display_name": None,
                "funder_award_id": f"EP/S{rng.randint(10000,99999)}/1",
                "funder_id": f"https://openalex.org/F{rng.randint(100000,999999)}",
                "funder_display_name": "Synthetic Research Council",
            }
            for _ in range(rng.randint(0, 2))
        ] or None
        work["abstract_inverted_index"] = None
        work["apc_list"] = {
            "value": rng.randint(500, 5000) if rng.random() > 0.3 else None,
            "currency": "USD",
            "value_usd": rng.randint(500, 5000) if rng.random() > 0.3 else None,
            "provenance": None,
        } if rng.random() > 0.4 else None
        work["apc_paid"] = None
        work["best_oa_location"] = _make_location(rng, with_source=True) if rng.random() > 0.4 else None
        work["biblio"] = {
            "volume": str(rng.randint(1, 200)),
            "issue": str(rng.randint(1, 12)),
            "first_page": str(rng.randint(1, 500)),
            "last_page": str(rng.randint(1, 500)),
        } if rng.random() > 0.3 else None
        work["citation_normalized_percentile"] = {
            "value": round(rng.uniform(0.0, 1.0), 4),
            "is_in_top_1_percent": rng.random() > 0.95,
            "is_in_top_10_percent": rng.random() > 0.85,
        } if rng.random() > 0.3 else None
        work["cited_by_api_url"] = f"https://api.openalex.org/works?filter=cites:W{idx:012d}"
        work["concepts"] = []
        work["corresponding_author_ids"] = [
            work["authorships"][0]["author"]["id"]
        ] if work["authorships"] else []
        work["corresponding_institution_ids"] = [
            f"https://openalex.org/{_INSTITUTIONS[0][0]}"
        ]
        work["counts_by_year"] = [
            {"year": y, "cited_by_count": rng.randint(0, 100)}
            for y in range(year, year + min(rng.randint(1, 5), 2026 - year + 1))
        ]
        work["fulltext_origin"] = None
        work["fwci"] = round(rng.uniform(0.0, 30.0), 4)
        work["grants"] = []
        work["indexed_in"] = rng.choice([
            ["crossref"],
            ["crossref", "pubmed"],
            ["crossref", "doaj"],
            [],
        ])
        work["keywords"] = [
            {
                "id": f"https://openalex.org/K{rng.randint(100000,999999)}",
                "display_name": f"keyword {i}",
                "relevance_score": None,
                "created_date": "2024-01-01",
                "updated_date": "2025-01-01",
                "score": round(rng.uniform(0.1, 1.0), 4),
                "cited_by_count": rng.randint(0, 1000),
                "works_count": rng.randint(1, 500),
                "works_api_url": None,
            }
            for i in range(rng.randint(0, 3))
        ]
        work["language"] = rng.choice(["en", "de", "fr", "nl", None])
        work["license"] = rng.choice(["cc-by", "cc-by-nc", "cc0", None])
        work["locations"] = [
            _make_location(rng, with_source=True)
            for _ in range(rng.randint(0, 3))
        ]
        work["mesh"] = []
        work["primary_location"] = _make_location(rng, with_source=True) if rng.random() > 0.3 else None
        work["primary_topic"] = {
            **_make_topic(rng),
            "score": round(rng.uniform(0.5, 1.0), 4),
        } if rng.random() > 0.3 else None
        work["referenced_works"] = [
            f"https://openalex.org/W{rng.randint(100000000, 999999999)}"
            for _ in range(rng.randint(0, 10))
        ]
        work["related_works"] = [
            f"https://openalex.org/W{rng.randint(100000000, 999999999)}"
            for _ in range(rng.randint(0, 5))
        ]
        work["sustainable_development_goals"] = [
            _make_sdg(rng) for _ in range(rng.randint(0, 2))
        ]
        work["topics"] = [
            {**_make_topic(rng), "score": round(rng.uniform(0.1, 1.0), 4)}
            for _ in range(rng.randint(0, 3))
        ]
        work["has_content"] = {
            "pdf": rng.random() > 0.5,
            "grobid_xml": rng.random() > 0.8,
        }
        work["cited_by_percentile_year"] = {
            "min": rng.randint(50, 90),
            "max": 100,
        }
        work["datasets"] = None
        work["versions"] = None
        work["referenced_works_count"] = rng.randint(0, 50)

    return work


def generate_work_dicts(
    count: int = 10_000,
    seed: int = 42,
    *,
    sparse_first: int = 0,
) -> list[dict[str, Any]]:
    """Generate *count* work dicts matching the ``Work`` dataclass schema.

    Args:
        count: Number of work items to generate.
        seed: Random seed for reproducibility.
        sparse_first: If > 0, the first *sparse_first* items will be generated
            with ``sparse=True`` (minimal nested fields), and the rest will be
            fully populated.  This is the key mechanism for testing schema
            mismatches across chunks: the first batch of rows will have
            different struct column layouts than later batches.

    Returns:
        List of dicts, each conforming to the OpenAlex Work JSON structure.
    """
    rng = random.Random(seed)
    items: list[dict[str, Any]] = []
    for i in range(count):
        is_sparse = i < sparse_first
        items.append(_make_work_dict(rng, i + 1, sparse=is_sparse))
    return items


def generate_api_response_page(
    items: list[dict[str, Any]],
    total_count: int | None = None,
) -> dict[str, Any]:
    """Wrap work dicts in an OpenAlex API response envelope.

    The returned dict matches the shape::

        {
            "meta": {"count": N, ...},
            "results": [...]
        }

    so it can be fed directly to ``httpx.MockTransport`` handlers.
    """
    if total_count is None:
        total_count = len(items)
    return {
        "meta": {
            "count": total_count,
            "db_response_time_ms": 42,
            "page": 1,
            "per_page": len(items),
            "groups_count": None,
            "next_cursor": None,
        },
        "results": items,
    }
