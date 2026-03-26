"""Schema contract tests for canonical models and adapters.

Tests cover:
- CanonicalWork creation from minimal data
- Provenance survives serialization round-trips
- Adapters with sample Pure and OpenAlex data
- Canonical records convert to Polars rows
- Person and Organization canonical records
"""

from __future__ import annotations

import polars as pl
import pytest
from pydantic import ValidationError

from syntheca.config.source_precedence import Source
from syntheca.models.adapters import (
    openalex_work_to_canonical,
    pure_orgunit_to_canonical,
    pure_person_to_canonical,
    pure_publication_to_canonical,
)
from syntheca.models.canonical import (
    CanonicalOrganization,
    CanonicalPerson,
    CanonicalWork,
    SourceAssertion,
    canonicals_to_polars,
)

# ---------------------------------------------------------------------------
# CanonicalWork basics
# ---------------------------------------------------------------------------


class TestCanonicalWork:
    """Tests for CanonicalWork model."""

    def test_minimal_creation(self):
        """CanonicalWork can be created with only required fields."""
        work = CanonicalWork(internal_id="test-1", title="Hello World")
        assert work.internal_id == "test-1"
        assert work.title == "Hello World"
        assert work.doi is None
        assert work.publication_year is None
        assert work.authors == []
        assert work.source_ids == {}
        assert work.provenance == []

    def test_full_creation(self):
        """CanonicalWork with all fields populated."""
        assertion = SourceAssertion(source=Source.PURE, field_name="title", value="Full Title")
        work = CanonicalWork(
            internal_id="pure-uuid-1",
            doi="10.1234/test",
            title="Full Title",
            publication_year=2025,
            publication_date="2025-06-15",
            type="article",
            language="en",
            authors=["Alice Smith", "Bob Jones"],
            source_ids={"pure": "pure-uuid-1", "openalex": "W999"},
            is_oa=True,
            oa_color="gold",
            cited_by_count=42,
            fwci=1.5,
            publisher="Elsevier",
            primary_host_name="Journal of Testing",
            ut_is_corresponding=True,
            access_right="open",
            license="cc-by-4.0",
            keywords=["metadata", "testing"],
            abstract="A test abstract.",
            provenance=[assertion],
        )
        assert work.doi == "10.1234/test"
        assert work.publication_year == 2025
        assert len(work.provenance) == 1
        assert work.provenance[0].source == Source.PURE
        assert work.keywords == ["metadata", "testing"]

    def test_provenance_roundtrip(self):
        """Provenance survives model_dump → model_validate cycle."""
        assertion = SourceAssertion(
            source=Source.OPENALEX,
            field_name="cited_by_count",
            value=100,
            confidence=0.95,
        )
        work = CanonicalWork(internal_id="rt-1", title="Roundtrip", provenance=[assertion])
        dumped = work.model_dump(mode="json")
        restored = CanonicalWork.model_validate(dumped)

        assert len(restored.provenance) == 1
        assert restored.provenance[0].source == Source.OPENALEX
        assert restored.provenance[0].field_name == "cited_by_count"
        assert restored.provenance[0].value == 100
        assert restored.provenance[0].confidence == 0.95

    def test_to_flat_dict(self):
        """to_flat_dict returns a dict consumable by Polars."""
        work = CanonicalWork(
            internal_id="flat-1",
            title="Flat test",
            authors=["Someone"],
            provenance=[SourceAssertion(source=Source.PURE, field_name="title", value="Flat test")],
        )
        d = work.to_flat_dict()
        assert isinstance(d, dict)
        assert d["internal_id"] == "flat-1"
        assert isinstance(d["provenance"], list)
        assert isinstance(d["provenance"][0], dict)


# ---------------------------------------------------------------------------
# CanonicalPerson basics
# ---------------------------------------------------------------------------


class TestCanonicalPerson:
    """Tests for CanonicalPerson model."""

    def test_minimal_creation(self):
        person = CanonicalPerson(internal_id="p-1", name="Alice Researcher")
        assert person.internal_id == "p-1"
        assert person.name == "Alice Researcher"
        assert person.orcid is None
        assert person.affiliations == []

    def test_provenance_roundtrip(self):
        assertion = SourceAssertion(source=Source.PURE, field_name="name", value="Alice Researcher")
        person = CanonicalPerson(
            internal_id="p-1",
            name="Alice Researcher",
            orcid="0000-0001-0000-0001",
            provenance=[assertion],
        )
        dumped = person.model_dump(mode="json")
        restored = CanonicalPerson.model_validate(dumped)
        assert restored.orcid == "0000-0001-0000-0001"
        assert len(restored.provenance) == 1


# ---------------------------------------------------------------------------
# CanonicalOrganization basics
# ---------------------------------------------------------------------------


class TestCanonicalOrganization:
    """Tests for CanonicalOrganization model."""

    def test_minimal_creation(self):
        org = CanonicalOrganization(internal_id="org-1", name="Faculty of Science")
        assert org.internal_id == "org-1"
        assert org.type is None
        assert org.parent_id is None

    def test_full_creation(self):
        org = CanonicalOrganization(
            internal_id="org-dept",
            name="Department of CS",
            type="department",
            parent_id="org-fac",
        )
        assert org.type == "department"
        assert org.parent_id == "org-fac"


# ---------------------------------------------------------------------------
# Adapters: Pure publication
# ---------------------------------------------------------------------------


class TestPurePublicationAdapter:
    """Tests for pure_publication_to_canonical."""

    @pytest.fixture
    def sample_pure_pub(self) -> dict:
        return {
            "id": "pure-uuid-001",
            "title": "Advances in Metadata",
            "doi": "10.1234/meta.2025",
            "publication_date": "2025-03-20",
            "type": "article",
            "language": "en",
            "authors": [
                {"first_names": "Alice", "family_names": "Smith", "person_id": "p-100"},
                {"first_names": "Bob", "family_names": "Jones", "person_id": "p-200"},
            ],
            "access_right": "open",
            "license": "cc-by-4.0",
            "publisher_name": "Springer",
            "abstract": "This is an abstract.",
            "keywords": ["metadata", "open access"],
        }

    def test_basic_fields(self, sample_pure_pub):
        canonical = pure_publication_to_canonical(sample_pure_pub)
        assert canonical.internal_id == "pure-uuid-001"
        assert canonical.doi == "10.1234/meta.2025"
        assert canonical.title == "Advances in Metadata"
        assert canonical.publication_year == 2025
        assert canonical.type == "article"
        assert canonical.language == "en"

    def test_authors_extracted(self, sample_pure_pub):
        canonical = pure_publication_to_canonical(sample_pure_pub)
        assert canonical.authors == ["Alice Smith", "Bob Jones"]

    def test_source_ids(self, sample_pure_pub):
        canonical = pure_publication_to_canonical(sample_pure_pub)
        assert canonical.source_ids == {"pure": "pure-uuid-001"}

    def test_provenance_populated(self, sample_pure_pub):
        canonical = pure_publication_to_canonical(sample_pure_pub)
        assert len(canonical.provenance) > 0
        sources = {a.source for a in canonical.provenance}
        assert sources == {Source.PURE}
        fields = {a.field_name for a in canonical.provenance}
        assert "title" in fields
        assert "doi" in fields

    def test_minimal_pure_pub(self):
        """Adapter handles records with minimal data."""
        minimal = {"id": "min-1", "title": "Minimal"}
        canonical = pure_publication_to_canonical(minimal)
        assert canonical.internal_id == "min-1"
        assert canonical.title == "Minimal"
        assert canonical.authors == []


# ---------------------------------------------------------------------------
# Adapters: OpenAlex Work (dict form)
# ---------------------------------------------------------------------------


class TestOpenAlexWorkAdapter:
    """Tests for openalex_work_to_canonical with dict input."""

    @pytest.fixture
    def sample_oa_dict(self) -> dict:
        return {
            "id": "https://openalex.org/W12345",
            "doi": "https://doi.org/10.1234/oa.test",
            "title": "OpenAlex Test Work",
            "publication_year": 2024,
            "publication_date": "2024-11-01",
            "type": "article",
            "language": "en",
            "authorships": [
                {
                    "raw_author_name": "Charlie Engineer",
                    "author": {"display_name": "Charlie Engineer"},
                    "is_corresponding": False,
                    "countries": [],
                    "author_position": "first",
                    "affiliations": [],
                    "institutions": [],
                    "raw_affiliation_strings": [],
                },
            ],
            "open_access": {
                "is_oa": True,
                "oa_status": "gold",
                "oa_url": "https://example.com",
                "any_repository_has_fulltext": False,
            },
            "cited_by_count": 10,
            "fwci": 1.2,
            "primary_location": {
                "source": {
                    "display_name": "Journal of Tests",
                    "host_organization_name": "Test Publisher",
                },
            },
            "keywords": [
                {"display_name": "testing"},
                {"display_name": "metadata"},
            ],
            "corresponding_institution_ids": [],
        }

    def test_basic_fields(self, sample_oa_dict):
        canonical = openalex_work_to_canonical(sample_oa_dict)
        assert canonical.internal_id == "https://openalex.org/W12345"
        assert canonical.doi == "https://doi.org/10.1234/oa.test"
        assert canonical.title == "OpenAlex Test Work"
        assert canonical.publication_year == 2024
        assert canonical.is_oa is True
        assert canonical.oa_color == "gold"
        assert canonical.cited_by_count == 10

    def test_authors_extracted(self, sample_oa_dict):
        canonical = openalex_work_to_canonical(sample_oa_dict)
        assert canonical.authors == ["Charlie Engineer"]

    def test_source_ids(self, sample_oa_dict):
        canonical = openalex_work_to_canonical(sample_oa_dict)
        assert "openalex" in canonical.source_ids
        assert "doi" in canonical.source_ids

    def test_provenance_populated(self, sample_oa_dict):
        canonical = openalex_work_to_canonical(sample_oa_dict)
        assert len(canonical.provenance) > 0
        sources = {a.source for a in canonical.provenance}
        assert sources == {Source.OPENALEX}

    def test_venue_fields(self, sample_oa_dict):
        canonical = openalex_work_to_canonical(sample_oa_dict)
        assert canonical.primary_host_name == "Journal of Tests"
        assert canonical.publisher == "Test Publisher"

    def test_keywords(self, sample_oa_dict):
        canonical = openalex_work_to_canonical(sample_oa_dict)
        assert canonical.keywords == ["testing", "metadata"]


# ---------------------------------------------------------------------------
# Adapters: Pure person
# ---------------------------------------------------------------------------


class TestPurePersonAdapter:
    """Tests for pure_person_to_canonical."""

    def test_basic_person(self):
        record = {
            "id": "person-uuid-1",
            "first_names": "Alice",
            "family_names": "Researcher",
            "orcid": "0000-0001-0000-0001",
            "scopus_author_id": "12345",
            "affiliations": [{"affiliation_id": "org-1", "affiliation_name": "Faculty of Science"}],
        }
        canonical = pure_person_to_canonical(record)
        assert canonical.internal_id == "person-uuid-1"
        assert canonical.name == "Alice Researcher"
        assert canonical.orcid == "0000-0001-0000-0001"
        assert canonical.scopus_author_id == "12345"
        assert len(canonical.affiliations) == 1
        assert len(canonical.provenance) > 0

    def test_minimal_person(self):
        record = {"id": "p-min", "first_names": "Only"}
        canonical = pure_person_to_canonical(record)
        assert canonical.name == "Only"
        assert canonical.orcid is None


# ---------------------------------------------------------------------------
# Adapters: Pure org-unit
# ---------------------------------------------------------------------------


class TestPureOrgUnitAdapter:
    """Tests for pure_orgunit_to_canonical."""

    def test_basic_org(self):
        record = {
            "id": "org-uuid-1",
            "name": "Faculty of Science and Technology",
            "type": "faculty",
            "part_of_org_id": None,
        }
        canonical = pure_orgunit_to_canonical(record)
        assert canonical.internal_id == "org-uuid-1"
        assert canonical.name == "Faculty of Science and Technology"
        assert canonical.type == "faculty"
        assert canonical.parent_id is None

    def test_with_parent(self):
        record = {
            "id": "org-dept-1",
            "name": "Department of CS",
            "type": "department",
            "part_of_org_id": "org-fac-1",
        }
        canonical = pure_orgunit_to_canonical(record)
        assert canonical.parent_id == "org-fac-1"


# ---------------------------------------------------------------------------
# Polars conversion
# ---------------------------------------------------------------------------


class TestPolarsConversion:
    """Tests for canonical records → Polars DataFrame."""

    def test_works_to_polars(self):
        works = [
            CanonicalWork(
                internal_id="w1",
                title="Work One",
                doi="10.1234/w1",
                provenance=[
                    SourceAssertion(source=Source.PURE, field_name="title", value="Work One")
                ],
            ),
            CanonicalWork(internal_id="w2", title="Work Two"),
        ]
        df = canonicals_to_polars(works)
        assert isinstance(df, pl.DataFrame)
        assert df.height == 2
        assert "internal_id" in df.columns
        assert "title" in df.columns
        assert "provenance" in df.columns

    def test_persons_to_polars(self):
        persons = [
            CanonicalPerson(internal_id="p1", name="Alice"),
            CanonicalPerson(internal_id="p2", name="Bob", orcid="0000-0001-0000-0002"),
        ]
        df = canonicals_to_polars(persons)
        assert df.height == 2
        assert "orcid" in df.columns

    def test_organizations_to_polars(self):
        orgs = [
            CanonicalOrganization(internal_id="o1", name="Faculty A"),
            CanonicalOrganization(internal_id="o2", name="Dept B", parent_id="o1"),
        ]
        df = canonicals_to_polars(orgs)
        assert df.height == 2
        assert "parent_id" in df.columns

    def test_empty_list(self):
        df = canonicals_to_polars([])
        assert isinstance(df, pl.DataFrame)
        assert df.height == 0


# ---------------------------------------------------------------------------
# SourceAssertion validation
# ---------------------------------------------------------------------------


class TestSourceAssertion:
    """Tests for SourceAssertion model."""

    def test_defaults(self):
        a = SourceAssertion(source=Source.PURE, field_name="title")
        assert a.confidence == 1.0
        assert a.value is None
        assert a.timestamp is None

    def test_confidence_bounds(self):
        """Confidence must be between 0 and 1."""
        with pytest.raises(ValidationError):
            SourceAssertion(source=Source.PURE, field_name="x", confidence=1.5)
        with pytest.raises(ValidationError):
            SourceAssertion(source=Source.PURE, field_name="x", confidence=-0.1)

    def test_serialization_roundtrip(self):
        a = SourceAssertion(
            source=Source.OPENALEX, field_name="doi", value="10.1234/x", confidence=0.9
        )
        dumped = a.model_dump(mode="json")
        restored = SourceAssertion.model_validate(dumped)
        assert restored.source == Source.OPENALEX
        assert restored.value == "10.1234/x"
