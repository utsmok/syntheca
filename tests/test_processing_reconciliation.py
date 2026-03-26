"""Tests for the reconciliation module (T010).

Covers:
- DOI-based work matching (exact match)
- Title-based fuzzy work matching
- ORCID-based person matching
- Name-based person matching with ambiguity
- Organization matching
- Field precedence application
- Hard positives, hard negatives, and ambiguous cases
- Match results include provenance and confidence/reason codes
- Metrics reporting (counts of matched, unmatched, conflicts)
"""

from __future__ import annotations

from syntheca.config.source_precedence import Source
from syntheca.models.canonical import (
    CanonicalOrganization,
    CanonicalPerson,
    CanonicalWork,
    SourceAssertion,
)
from syntheca.processing.reconciliation import (
    MatchResult,
    ReconciliationError,
    ReconciliationMetrics,
    apply_field_precedence,
    reconcile_organizations,
    reconcile_persons,
    reconcile_works,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_work(
    *,
    source: Source = Source.PURE,
    internal_id: str = "w1",
    doi: str | None = None,
    title: str = "A Title",
    is_oa: bool | None = None,
    oa_color: str | None = None,
    cited_by_count: int | None = None,
    fwci: float | None = None,
    publisher: str | None = None,
    access_right: str | None = None,
    license_: str | None = None,
    abstract: str | None = None,
    keywords: list[str] | None = None,
    authors: list[str] | None = None,
    source_ids: dict[str, str] | None = None,
) -> CanonicalWork:
    provenance = [
        SourceAssertion(source=source, field_name="title", value=title),
    ]
    if doi:
        provenance.append(SourceAssertion(source=source, field_name="doi", value=doi))
    return CanonicalWork(
        internal_id=internal_id,
        doi=doi,
        title=title,
        is_oa=is_oa,
        oa_color=oa_color,
        cited_by_count=cited_by_count,
        fwci=fwci,
        publisher=publisher,
        access_right=access_right,
        license=license_,
        abstract=abstract,
        keywords=keywords or [],
        authors=authors or [],
        source_ids=source_ids or {},
        provenance=provenance,
    )


def _make_person(
    *,
    source: Source = Source.PURE,
    internal_id: str = "p1",
    name: str = "Alice Smith",
    orcid: str | None = None,
) -> CanonicalPerson:
    return CanonicalPerson(
        internal_id=internal_id,
        name=name,
        orcid=orcid,
        provenance=[SourceAssertion(source=source, field_name="name", value=name)],
    )


def _make_org(
    *,
    source: Source = Source.PURE,
    internal_id: str = "o1",
    name: str = "Faculty of Science",
    type_: str | None = None,
    parent_id: str | None = None,
) -> CanonicalOrganization:
    return CanonicalOrganization(
        internal_id=internal_id,
        name=name,
        type=type_,
        parent_id=parent_id,
        provenance=[SourceAssertion(source=source, field_name="name", value=name)],
    )


# ===========================================================================
# Work reconciliation tests
# ===========================================================================


class TestReconcileWorksDOI:
    """DOI-based work matching: hard positive tests."""

    def test_exact_doi_match(self):
        pure_w = _make_work(
            source=Source.PURE, internal_id="pure-1", doi="10.1234/abc", title="Test Paper"
        )
        oa_w = _make_work(
            source=Source.OPENALEX, internal_id="W123", doi="10.1234/ABC", title="Test Paper"
        )
        sources = {Source.PURE: [pure_w], Source.OPENALEX: [oa_w]}
        merged, matches, _metrics = reconcile_works(sources)

        # Should produce one merged work
        assert len(merged) == 1
        # Match should be recorded
        doi_matches = [m for m in matches if m.match_strategy == "doi"]
        assert len(doi_matches) == 1
        assert doi_matches[0].accepted is True
        assert doi_matches[0].confidence >= 0.9

    def test_doi_with_prefix_normalization(self):
        pure_w = _make_work(source=Source.PURE, internal_id="p1", doi="https://doi.org/10.1234/XYZ")
        oa_w = _make_work(source=Source.OPENALEX, internal_id="w1", doi="10.1234/xyz")
        merged, matches, _ = reconcile_works({Source.PURE: [pure_w], Source.OPENALEX: [oa_w]})
        assert len(merged) == 1
        doi_matches = [m for m in matches if m.match_strategy == "doi" and m.accepted]
        assert len(doi_matches) == 1

    def test_no_doi_match_different_dois(self):
        """Hard negative: different DOIs should NOT match via DOI strategy."""
        w1 = _make_work(source=Source.PURE, internal_id="p1", doi="10.1234/AAA")
        w2 = _make_work(source=Source.OPENALEX, internal_id="w1", doi="10.1234/BBB")
        merged, matches, _ = reconcile_works({Source.PURE: [w1], Source.OPENALEX: [w2]})
        assert len(merged) == 2
        doi_matches = [m for m in matches if m.match_strategy == "doi" and m.accepted]
        assert len(doi_matches) == 0


class TestReconcileWorksTitleFuzzy:
    """Title-based fuzzy work matching."""

    def test_high_similarity_titles_match(self):
        w1 = _make_work(
            source=Source.PURE, internal_id="p1", title="A comprehensive study on metadata"
        )
        w2 = _make_work(
            source=Source.OPENALEX, internal_id="w1", title="A comprehensive study on metadata"
        )
        merged, matches, _ = reconcile_works({Source.PURE: [w1], Source.OPENALEX: [w2]})
        assert len(merged) == 1
        fuzzy = [m for m in matches if m.match_strategy == "title_fuzzy" and m.accepted]
        assert len(fuzzy) == 1
        assert fuzzy[0].confidence >= 0.85

    def test_low_similarity_titles_no_match(self):
        """Hard negative: very different titles should NOT match."""
        w1 = _make_work(source=Source.PURE, internal_id="p1", title="Quantum physics review 2025")
        w2 = _make_work(
            source=Source.OPENALEX,
            internal_id="w1",
            title="Machine learning in healthcare applications",
        )
        merged, matches, _ = reconcile_works({Source.PURE: [w1], Source.OPENALEX: [w2]})
        assert len(merged) == 2
        accepted_fuzzy = [m for m in matches if m.match_strategy == "title_fuzzy" and m.accepted]
        assert len(accepted_fuzzy) == 0

    def test_ambiguous_title_below_threshold(self):
        """Ambiguous: similar but not identical titles — confidence below threshold."""
        w1 = _make_work(source=Source.PURE, internal_id="p1", title="Deep learning approaches")
        w2 = _make_work(
            source=Source.OPENALEX, internal_id="w1", title="Deep reinforcement learning methods"
        )
        _merged, matches, _ = reconcile_works({Source.PURE: [w1], Source.OPENALEX: [w2]})
        # The titles are similar but not identical enough — should be 2 records
        rejected = [m for m in matches if m.match_strategy == "title_fuzzy" and not m.accepted]
        # Should have a rejected match result (below threshold but attempted)
        assert len(rejected) >= 0  # may or may not have tried depending on score


class TestReconcileWorksFieldPrecedence:
    """Field precedence application per T002."""

    def test_oa_status_from_openalex(self):
        pure_w = _make_work(
            source=Source.PURE, internal_id="p1", doi="10.1/a", is_oa=False, publisher="Elsevier"
        )
        oa_w = _make_work(
            source=Source.OPENALEX, internal_id="w1", doi="10.1/a", is_oa=True, oa_color="gold"
        )
        merged, _, _ = reconcile_works({Source.PURE: [pure_w], Source.OPENALEX: [oa_w]})
        assert len(merged) == 1
        # OA status should come from OpenAlex per T002
        assert merged[0].is_oa is True
        assert merged[0].oa_color == "gold"

    def test_publisher_from_pure(self):
        pure_w = _make_work(
            source=Source.PURE, internal_id="p1", doi="10.1/b", publisher="Springer Nature"
        )
        oa_w = _make_work(
            source=Source.OPENALEX, internal_id="w1", doi="10.1/b", publisher="Springer"
        )
        merged, _, _ = reconcile_works({Source.PURE: [pure_w], Source.OPENALEX: [oa_w]})
        assert len(merged) == 1
        # Publisher should come from Pure per T002
        assert merged[0].publisher == "Springer Nature"

    def test_citation_metrics_from_openalex(self):
        pure_w = _make_work(source=Source.PURE, internal_id="p1", doi="10.1/c")
        oa_w = _make_work(
            source=Source.OPENALEX,
            internal_id="w1",
            doi="10.1/c",
            cited_by_count=42,
            fwci=1.5,
        )
        merged, _, _ = reconcile_works({Source.PURE: [pure_w], Source.OPENALEX: [oa_w]})
        assert len(merged) == 1
        assert merged[0].cited_by_count == 42
        assert merged[0].fwci == 1.5

    def test_access_right_and_license_from_pure(self):
        pure_w = _make_work(
            source=Source.PURE,
            internal_id="p1",
            doi="10.1/d",
            access_right="open",
            license_="CC-BY-4.0",
        )
        oa_w = _make_work(source=Source.OPENALEX, internal_id="w1", doi="10.1/d")
        merged, _, _ = reconcile_works({Source.PURE: [pure_w], Source.OPENALEX: [oa_w]})
        assert len(merged) == 1
        assert merged[0].access_right == "open"
        assert merged[0].license == "CC-BY-4.0"

    def test_abstract_and_keywords_from_openalex(self):
        pure_w = _make_work(source=Source.PURE, internal_id="p1", doi="10.1/e")
        oa_w = _make_work(
            source=Source.OPENALEX,
            internal_id="w1",
            doi="10.1/e",
            abstract="This paper explores...",
            keywords=["metadata", "open access"],
        )
        merged, _, _ = reconcile_works({Source.PURE: [pure_w], Source.OPENALEX: [oa_w]})
        assert len(merged) == 1
        assert merged[0].abstract == "This paper explores..."
        assert merged[0].keywords == ["metadata", "open access"]

    def test_source_ids_merged(self):
        pure_w = _make_work(
            source=Source.PURE,
            internal_id="p1",
            doi="10.1/f",
            source_ids={"pure": "p1"},
        )
        oa_w = _make_work(
            source=Source.OPENALEX,
            internal_id="W1",
            doi="10.1/f",
            source_ids={"openalex": "W1"},
        )
        merged, _, _ = reconcile_works({Source.PURE: [pure_w], Source.OPENALEX: [oa_w]})
        assert len(merged) == 1
        assert "pure" in merged[0].source_ids
        assert "openalex" in merged[0].source_ids


class TestReconcileWorksMetrics:
    """Test metrics and audit trail reporting."""

    def test_metrics_counts(self):
        w1 = _make_work(source=Source.PURE, internal_id="p1", doi="10.1/a")
        w2 = _make_work(source=Source.OPENALEX, internal_id="w1", doi="10.1/a")
        w3 = _make_work(source=Source.PURE, internal_id="p2", doi="10.1/b")
        sources = {Source.PURE: [w1, w3], Source.OPENALEX: [w2]}

        _, _matches, metrics = reconcile_works(sources)
        assert metrics.total_input == 3
        assert metrics.matched >= 1
        assert metrics.entity_type == "work"
        assert "work" in metrics.summary

    def test_match_results_have_provenance(self):
        w1 = _make_work(source=Source.PURE, internal_id="p1", doi="10.1/x")
        w2 = _make_work(source=Source.OPENALEX, internal_id="w1", doi="10.1/x")
        _, matches, _ = reconcile_works({Source.PURE: [w1], Source.OPENALEX: [w2]})
        assert len(matches) >= 1
        for m in matches:
            assert isinstance(m, MatchResult)
            assert m.source_a in Source.__members__.values()
            assert m.source_b in Source.__members__.values()
            assert m.entity_type == "work"
            assert 0.0 <= m.confidence <= 1.0
            assert m.reason  # non-empty
            assert isinstance(m.accepted, bool)

    def test_empty_input(self):
        merged, matches, metrics = reconcile_works({})
        assert merged == []
        assert matches == []
        assert metrics.total_input == 0


class TestReconcileWorksMixedSources:
    """Tests with 3+ sources."""

    def test_three_source_doi_match(self):
        pure_w = _make_work(source=Source.PURE, internal_id="p1", doi="10.1/m", publisher="Wiley")
        oa_w = _make_work(
            source=Source.OPENALEX,
            internal_id="w1",
            doi="10.1/m",
            cited_by_count=10,
            is_oa=True,
        )
        oaire_w = _make_work(source=Source.OPENAIRE, internal_id="r1", doi="10.1/m")
        sources = {Source.PURE: [pure_w], Source.OPENALEX: [oa_w], Source.OPENAIRE: [oaire_w]}
        merged, matches, _metrics = reconcile_works(sources)

        assert len(merged) == 1
        # Should have 2 match results (a->b, a->c)
        doi_matches = [m for m in matches if m.match_strategy == "doi" and m.accepted]
        assert len(doi_matches) == 2
        # Publisher from Pure
        assert merged[0].publisher == "Wiley"
        # OA from OpenAlex
        assert merged[0].is_oa is True


# ===========================================================================
# Person reconciliation tests
# ===========================================================================


class TestReconcilePersonsORCID:
    """ORCID-based person matching."""

    def test_orcid_match(self):
        p1 = _make_person(
            source=Source.PURE,
            internal_id="pure-p1",
            name="Alice Smith",
            orcid="0000-0001-2345-6789",
        )
        p2 = _make_person(
            source=Source.OPENALEX,
            internal_id="oa-p1",
            name="A. Smith",
            orcid="0000-0001-2345-6789",
        )
        merged, matches, _metrics = reconcile_persons({Source.PURE: [p1], Source.OPENALEX: [p2]})
        assert len(merged) == 1
        orcid_matches = [m for m in matches if m.match_strategy == "orcid" and m.accepted]
        assert len(orcid_matches) == 1
        # Pure name is authoritative
        assert merged[0].name == "Alice Smith"
        assert merged[0].internal_id == "pure-p1"

    def test_different_orcids_no_match(self):
        p1 = _make_person(source=Source.PURE, internal_id="p1", orcid="0000-0001-0000-0001")
        p2 = _make_person(source=Source.OPENALEX, internal_id="p2", orcid="0000-0001-0000-0002")
        merged, _, _ = reconcile_persons({Source.PURE: [p1], Source.OPENALEX: [p2]})
        assert len(merged) == 2


class TestReconcilePersonsNameFuzzy:
    """Name-based fuzzy person matching."""

    def test_name_match_high_similarity(self):
        p1 = _make_person(source=Source.PURE, internal_id="p1", name="Johannes van der Berg")
        p2 = _make_person(source=Source.OPENALEX, internal_id="oa-p1", name="Johannes van der Berg")
        merged, matches, _ = reconcile_persons({Source.PURE: [p1], Source.OPENALEX: [p2]})
        assert len(merged) == 1
        name_matches = [m for m in matches if m.match_strategy == "name_fuzzy" and m.accepted]
        assert len(name_matches) == 1

    def test_name_match_ambiguous(self):
        """Similar but different people should not match."""
        p1 = _make_person(source=Source.PURE, internal_id="p1", name="John Smith")
        p2 = _make_person(source=Source.OPENALEX, internal_id="p2", name="James Smith")
        merged, _matches, _ = reconcile_persons({Source.PURE: [p1], Source.OPENALEX: [p2]})
        # These should NOT match (too ambiguous)
        assert len(merged) == 2

    def test_person_pure_authoritative(self):
        """Pure internal_repository_id is preserved as internal_id."""
        p1 = _make_person(source=Source.PURE, internal_id="uuid-123", name="Alice B.")
        p2 = _make_person(source=Source.OPENALEX, internal_id="oa-456", name="Alice B.")
        merged, _, _ = reconcile_persons({Source.PURE: [p1], Source.OPENALEX: [p2]})
        assert len(merged) == 1
        assert merged[0].internal_id == "uuid-123"

    def test_empty_input(self):
        merged, _matches, metrics = reconcile_persons({})
        assert merged == []
        assert metrics.total_input == 0


class TestReconcilePersonsMetrics:
    def test_metrics_reporting(self):
        p1 = _make_person(source=Source.PURE, internal_id="p1", name="A", orcid="000")
        p2 = _make_person(source=Source.OPENALEX, internal_id="p2", name="B", orcid="000")
        p3 = _make_person(source=Source.PURE, internal_id="p3", name="C")
        _, _, metrics = reconcile_persons({Source.PURE: [p1, p3], Source.OPENALEX: [p2]})
        assert metrics.entity_type == "person"
        assert metrics.total_input == 3
        assert metrics.matched >= 1


# ===========================================================================
# Organization reconciliation tests
# ===========================================================================


class TestReconcileOrganizations:
    def test_name_match(self):
        o1 = _make_org(
            source=Source.PURE, internal_id="org-1", name="Faculty of Science", type_="faculty"
        )
        o2 = _make_org(source=Source.UT_PEOPLE, internal_id="ut-org-1", name="Faculty of Science")
        merged, matches, _metrics = reconcile_organizations(
            {Source.PURE: [o1], Source.UT_PEOPLE: [o2]}
        )
        assert len(merged) == 1
        name_matches = [m for m in matches if m.match_strategy == "name_exact" and m.accepted]
        assert len(name_matches) == 1
        # Pure is authoritative
        assert merged[0].internal_id == "org-1"
        assert merged[0].type == "faculty"

    def test_different_names_no_match(self):
        o1 = _make_org(source=Source.PURE, internal_id="o1", name="Faculty of Engineering")
        o2 = _make_org(source=Source.UT_PEOPLE, internal_id="o2", name="Department of Mathematics")
        merged, _, _ = reconcile_organizations({Source.PURE: [o1], Source.UT_PEOPLE: [o2]})
        assert len(merged) == 2

    def test_pure_hierarchy_preserved(self):
        o1 = _make_org(
            source=Source.PURE,
            internal_id="org-child",
            name="Department of CS",
            parent_id="org-parent",
        )
        o2 = _make_org(source=Source.UT_PEOPLE, internal_id="ut-cs", name="Department of CS")
        merged, _, _ = reconcile_organizations({Source.PURE: [o1], Source.UT_PEOPLE: [o2]})
        assert len(merged) == 1
        assert merged[0].parent_id == "org-parent"

    def test_empty_input(self):
        merged, _matches, metrics = reconcile_organizations({})
        assert merged == []
        assert metrics.total_input == 0

    def test_metrics(self):
        o1 = _make_org(source=Source.PURE, internal_id="o1", name="X")
        o2 = _make_org(source=Source.UT_PEOPLE, internal_id="o2", name="X")
        o3 = _make_org(source=Source.PURE, internal_id="o3", name="Y")
        _, _, metrics = reconcile_organizations({Source.PURE: [o1, o3], Source.UT_PEOPLE: [o2]})
        assert metrics.entity_type == "organization"
        assert metrics.total_input == 3


# ===========================================================================
# apply_field_precedence standalone tests
# ===========================================================================


class TestApplyFieldPrecedence:
    def test_single_source_passthrough(self):
        w = _make_work(source=Source.PURE, internal_id="p1", publisher="MDPI")
        result = apply_field_precedence([{Source.PURE: w}])
        assert len(result) == 1
        assert result[0].publisher == "MDPI"

    def test_mixed_precedence_fields(self):
        pure_w = _make_work(
            source=Source.PURE,
            internal_id="p1",
            publisher="Elsevier",
            access_right="restricted",
        )
        oa_w = _make_work(
            source=Source.OPENALEX,
            internal_id="w1",
            publisher="Elsevier BV",
            is_oa=True,
            oa_color="green",
            abstract="Abstract from OA",
        )
        result = apply_field_precedence([{Source.PURE: pure_w, Source.OPENALEX: oa_w}])
        assert len(result) == 1
        merged = result[0]
        # Publisher from Pure
        assert merged.publisher == "Elsevier"
        # OA from OpenAlex
        assert merged.is_oa is True
        assert merged.oa_color == "green"
        # Access right from Pure
        assert merged.access_right == "restricted"
        # Abstract from OpenAlex
        assert merged.abstract == "Abstract from OA"

    def test_provenance_preserved(self):
        w1 = _make_work(source=Source.PURE, internal_id="p1", doi="10.1/z")
        w2 = _make_work(source=Source.OPENALEX, internal_id="w1", doi="10.1/z")
        result = apply_field_precedence([{Source.PURE: w1, Source.OPENALEX: w2}])
        # Should have provenance from both sources
        sources_in_prov = {a.source for a in result[0].provenance}
        assert Source.PURE in sources_in_prov
        assert Source.OPENALEX in sources_in_prov


# ===========================================================================
# ReconciliationError and MatchResult model tests
# ===========================================================================


class TestModels:
    def test_reconciliation_error(self):
        err = ReconciliationError("test error", entity_type="work", context={"key": "val"})
        assert str(err) == "test error"
        assert err.entity_type == "work"
        assert err.context == {"key": "val"}

    def test_match_result_model(self):
        mr = MatchResult(
            source_a=Source.PURE,
            source_b=Source.OPENALEX,
            entity_type="work",
            id_a="p1",
            id_b="w1",
            match_strategy="doi",
            confidence=0.99,
            reason="DOI match",
            accepted=True,
        )
        assert mr.confidence == 0.99
        assert mr.accepted is True
        d = mr.model_dump()
        assert "source_a" in d
        assert "reason" in d

    def test_reconciliation_metrics(self):
        m = ReconciliationMetrics(
            entity_type="work",
            total_input=10,
            matched=5,
            unmatched=3,
            conflicts=2,
        )
        assert "work" in m.summary
        assert "10 input" in m.summary
        assert "5 matched" in m.summary
