"""Tests for co-authorship analysis module."""

from __future__ import annotations

import polars as pl
import pytest

from syntheca.analysis.coauthorship import (
    EDGE_SCHEMA,
    LINK_SCHEMA,
    CoauthorshipReport,
    build_author_publication_links,
    build_coauthor_edges,
    build_collaboration_rollups,
    generate_coauthorship_report,
)
from syntheca.models.canonical import (
    CanonicalOrganization,
    CanonicalPerson,
    CanonicalWork,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_works() -> list[CanonicalWork]:
    """Three works with overlapping authorship."""
    return [
        CanonicalWork(
            internal_id="w1",
            doi="10.1234/w1",
            title="Paper Alpha",
            authors=["Alice Smith", "Bob Jones", "Carol White"],
        ),
        CanonicalWork(
            internal_id="w2",
            doi="10.1234/w2",
            title="Paper Beta",
            authors=["Alice Smith", "David Brown"],
        ),
        CanonicalWork(
            internal_id="w3",
            doi=None,
            title="Paper Gamma",
            authors=["Bob Jones", "Eve Green"],
        ),
    ]


@pytest.fixture
def sample_persons() -> list[CanonicalPerson]:
    """Person records with affiliation data."""
    return [
        CanonicalPerson(
            internal_id="p-alice",
            name="Alice Smith",
            orcid="0000-0001-0000-0001",
            affiliations=[{"name": "University of Twente", "country": "NL"}],
        ),
        CanonicalPerson(
            internal_id="p-bob",
            name="Bob Jones",
            orcid="0000-0001-0000-0002",
            affiliations=[{"name": "University of Twente", "country": "NL"}],
        ),
        CanonicalPerson(
            internal_id="p-carol",
            name="Carol White",
            orcid=None,
            affiliations=[{"name": "MIT", "country": "US"}],
        ),
        CanonicalPerson(
            internal_id="p-david",
            name="David Brown",
            orcid="0000-0001-0000-0004",
            affiliations=[{"name": "TechCorp Inc.", "country": "DE"}],
        ),
        CanonicalPerson(
            internal_id="p-eve",
            name="Eve Green",
            orcid=None,
            affiliations=[{"name": "University of Amsterdam", "country": "NL"}],
        ),
    ]


@pytest.fixture
def sample_organizations() -> list[CanonicalOrganization]:
    """Organization records with type info."""
    return [
        CanonicalOrganization(internal_id="org-ut", name="University of Twente", type="university"),
        CanonicalOrganization(internal_id="org-mit", name="MIT", type="university"),
        CanonicalOrganization(internal_id="org-tc", name="TechCorp Inc.", type="company"),
        CanonicalOrganization(
            internal_id="org-uva", name="University of Amsterdam", type="university"
        ),
    ]


# ---------------------------------------------------------------------------
# Author-publication links
# ---------------------------------------------------------------------------


class TestBuildAuthorPublicationLinks:
    """Tests for build_author_publication_links."""

    def test_basic_links(self, sample_works: list[CanonicalWork]):
        links = build_author_publication_links(sample_works)
        # 3 + 2 + 2 = 7 author-work pairs
        assert len(links) == 7
        assert set(links.columns) == set(LINK_SCHEMA.keys())

    def test_author_positions(self, sample_works: list[CanonicalWork]):
        links = build_author_publication_links(sample_works)
        w1_links = links.filter(pl.col("work_id") == "w1")
        positions = w1_links.sort("author_name")["author_position"].to_list()
        # Alice=first, Bob=middle, Carol=last (sorted by name)
        assert positions == ["first", "middle", "last"]

    def test_enrichment_with_persons(
        self, sample_works: list[CanonicalWork], sample_persons: list[CanonicalPerson]
    ):
        links = build_author_publication_links(sample_works, persons=sample_persons)
        alice_rows = links.filter(pl.col("author_name") == "Alice Smith")
        assert alice_rows["author_orcid"][0] == "0000-0001-0000-0001"
        assert alice_rows["author_internal_id"][0] == "p-alice"

    def test_no_persons_leaves_nulls(self, sample_works: list[CanonicalWork]):
        links = build_author_publication_links(sample_works)
        assert links["author_orcid"].null_count() == len(links)
        assert links["author_internal_id"].null_count() == len(links)

    def test_empty_works(self):
        links = build_author_publication_links([])
        assert links.is_empty()
        assert set(links.columns) == set(LINK_SCHEMA.keys())

    def test_work_with_no_authors(self):
        work = CanonicalWork(internal_id="w-empty", title="No Authors")
        links = build_author_publication_links([work])
        assert links.is_empty()

    def test_single_author_position(self):
        work = CanonicalWork(internal_id="w-solo", title="Solo", authors=["Solo Author"])
        links = build_author_publication_links([work])
        assert len(links) == 1
        # Single author is both first and last — we label as "first"
        assert links["author_position"][0] == "first"

    def test_two_author_positions(self):
        work = CanonicalWork(
            internal_id="w-duo", title="Duo", authors=["First Author", "Last Author"]
        )
        links = build_author_publication_links([work])
        positions = links["author_position"].to_list()
        assert positions == ["first", "last"]


# ---------------------------------------------------------------------------
# Co-author edges
# ---------------------------------------------------------------------------


class TestBuildCoauthorEdges:
    """Tests for build_coauthor_edges."""

    def test_basic_edges(self, sample_works: list[CanonicalWork]):
        links = build_author_publication_links(sample_works)
        edges = build_coauthor_edges(links)
        assert not edges.is_empty()
        assert set(edges.columns) == set(EDGE_SCHEMA.keys())

    def test_edge_counts(self, sample_works: list[CanonicalWork]):
        links = build_author_publication_links(sample_works)
        edges = build_coauthor_edges(links)
        # From w1: Alice-Bob, Alice-Carol, Bob-Carol (3 edges)
        # From w2: Alice-David (1 edge)
        # From w3: Bob-Eve (1 edge)
        # Total unique edges: 5
        assert len(edges) == 5

    def test_shared_work_count(self, sample_works: list[CanonicalWork]):
        """Alice and Bob co-author w1, so their shared count should be 1."""
        links = build_author_publication_links(sample_works)
        edges = build_coauthor_edges(links)
        # Find the Alice-Bob edge
        alice_bob = edges.filter(
            ((pl.col("author_a_name") == "Alice Smith") & (pl.col("author_b_name") == "Bob Jones"))
            | (
                (pl.col("author_a_name") == "Bob Jones")
                & (pl.col("author_b_name") == "Alice Smith")
            )
        )
        assert len(alice_bob) == 1
        assert alice_bob["shared_works_count"][0] == 1

    def test_multiple_shared_works(self):
        """Two authors sharing two papers should get count=2."""
        works = [
            CanonicalWork(internal_id="w1", title="P1", authors=["A", "B"]),
            CanonicalWork(internal_id="w2", title="P2", authors=["A", "B"]),
        ]
        links = build_author_publication_links(works)
        edges = build_coauthor_edges(links)
        assert len(edges) == 1
        assert edges["shared_works_count"][0] == 2
        assert sorted(edges["shared_work_ids"][0]) == ["w1", "w2"]

    def test_empty_links(self):
        from syntheca.analysis.coauthorship import _empty_link_df

        edges = build_coauthor_edges(_empty_link_df())
        assert edges.is_empty()
        assert set(edges.columns) == set(EDGE_SCHEMA.keys())

    def test_single_author_no_edges(self):
        """A single-author paper produces no edges."""
        works = [CanonicalWork(internal_id="w1", title="Solo", authors=["Only Me"])]
        links = build_author_publication_links(works)
        edges = build_coauthor_edges(links)
        assert edges.is_empty()

    def test_edge_schema_stability(self, sample_works: list[CanonicalWork]):
        links = build_author_publication_links(sample_works)
        edges = build_coauthor_edges(links)
        for col_name, col_type in EDGE_SCHEMA.items():
            assert col_name in edges.columns
            assert edges[col_name].dtype == col_type


# ---------------------------------------------------------------------------
# Collaboration rollups
# ---------------------------------------------------------------------------


class TestBuildCollaborationRollups:
    """Tests for build_collaboration_rollups."""

    def test_ut_vs_external(
        self,
        sample_works: list[CanonicalWork],
        sample_persons: list[CanonicalPerson],
    ):
        links = build_author_publication_links(sample_works, persons=sample_persons)
        edges = build_coauthor_edges(links)
        rollups = build_collaboration_rollups(edges, persons=sample_persons)

        ut_ext = rollups["ut_vs_external"]
        assert "collab_type" in ut_ext.columns
        assert "edge_count" in ut_ext.columns
        # Alice & Bob are UT; Carol, David, Eve are not
        ut_ut_row = ut_ext.filter(pl.col("collab_type") == "ut_ut")
        assert not ut_ut_row.is_empty()
        # Alice-Bob edge = UT-UT
        assert ut_ut_row["edge_count"][0] >= 1

    def test_empty_edges_returns_stubs(self):
        from syntheca.analysis.coauthorship import _empty_edge_df

        rollups = build_collaboration_rollups(_empty_edge_df())
        for key in ("ut_vs_external", "university_rollup", "company_rollup", "country_rollup"):
            assert key in rollups
            assert isinstance(rollups[key], pl.DataFrame)

    def test_no_persons_returns_empty_rollups(self, sample_works: list[CanonicalWork]):
        links = build_author_publication_links(sample_works)
        edges = build_coauthor_edges(links)
        rollups = build_collaboration_rollups(edges, persons=None)
        assert rollups["ut_vs_external"].is_empty()

    def test_country_rollup(
        self,
        sample_works: list[CanonicalWork],
        sample_persons: list[CanonicalPerson],
    ):
        links = build_author_publication_links(sample_works, persons=sample_persons)
        edges = build_coauthor_edges(links)
        rollups = build_collaboration_rollups(edges, persons=sample_persons)
        country = rollups["country_rollup"]
        assert "country_a" in country.columns
        assert "country_b" in country.columns


# ---------------------------------------------------------------------------
# Full report
# ---------------------------------------------------------------------------


class TestGenerateCoauthorshipReport:
    """Tests for the full pipeline convenience function."""

    def test_report_structure(
        self,
        sample_works: list[CanonicalWork],
        sample_persons: list[CanonicalPerson],
        sample_organizations: list[CanonicalOrganization],
    ):
        report = generate_coauthorship_report(
            sample_works, persons=sample_persons, organizations=sample_organizations
        )
        assert isinstance(report, CoauthorshipReport)
        assert isinstance(report.author_publication_links, pl.DataFrame)
        assert isinstance(report.coauthor_edges, pl.DataFrame)
        assert isinstance(report.ut_vs_external, pl.DataFrame)
        assert isinstance(report.summary, dict)
        assert report.summary["total_works"] == 3
        assert report.summary["total_authors"] > 0
        assert report.summary["total_edges"] > 0

    def test_report_without_persons(self, sample_works: list[CanonicalWork]):
        report = generate_coauthorship_report(sample_works)
        assert isinstance(report, CoauthorshipReport)
        assert report.summary["ut_authors"] == 0
        assert report.summary["external_authors"] == 0
        assert report.summary["total_edges"] > 0

    def test_report_empty_works(self):
        report = generate_coauthorship_report([])
        assert report.summary["total_works"] == 0
        assert report.summary["total_authors"] == 0
        assert report.summary["total_edges"] == 0
        assert report.author_publication_links.is_empty()
        assert report.coauthor_edges.is_empty()

    def test_ut_author_count(
        self,
        sample_works: list[CanonicalWork],
        sample_persons: list[CanonicalPerson],
    ):
        report = generate_coauthorship_report(sample_works, persons=sample_persons)
        # Alice and Bob are UT-affiliated
        assert report.summary["ut_authors"] == 2
        # Carol, David, Eve are external
        assert report.summary["external_authors"] == 3
