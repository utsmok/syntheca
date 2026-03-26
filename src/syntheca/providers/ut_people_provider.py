"""UT People data provider wrapping UTPeopleClient + canonical conversion."""

from __future__ import annotations

from loguru import logger

from syntheca.clients.ut_people import UTPeopleClient
from syntheca.config.source_precedence import Source
from syntheca.models.canonical import CanonicalPerson, SourceAssertion


class UTPeopleProvider:
    """Provider for UT People person lookups (fallback enrichment source)."""

    def __init__(self, client: UTPeopleClient) -> None:
        """Initialize with a :class:`UTPeopleClient` instance."""
        self._client = client

    @property
    def source(self) -> Source:
        """Return :attr:`Source.UT_PEOPLE`."""
        return Source.UT_PEOPLE

    @property
    def capabilities(self) -> set[str]:
        """Return supported entity types."""
        return {"persons"}

    async def fetch(self, entity: str, **kwargs: object) -> list[CanonicalPerson]:
        """Search UT People for persons by name and return canonical records.

        Args:
            entity: Must be ``"persons"``.
            **kwargs: Expected keyword arguments:

                * ``names`` (``list[str]``): Person names to search for.

        Returns:
            List of :class:`CanonicalPerson` records (best candidate per name).

        Raises:
            ValueError: If *entity* is not ``"persons"``.
        """
        if entity not in self.capabilities:
            raise ValueError(f"UTPeopleProvider does not support entity {entity!r}")

        names: list[str] = list(kwargs.get("names") or [])  # type: ignore[arg-type]
        if not names:
            logger.info("UTPeopleProvider.fetch called with no names; returning empty list")
            return []

        out: list[CanonicalPerson] = []
        for name in names:
            try:
                candidates = await self._client.search_person(name)
                if not candidates:
                    continue
                best = candidates[0]
                out.append(_candidate_to_canonical(best))
            except Exception as exc:
                logger.debug("UT People search failed for '{}': {}", name, exc)
        return out


def _candidate_to_canonical(candidate: dict) -> CanonicalPerson:
    """Convert a UT People search result dict to a :class:`CanonicalPerson`."""
    found_name = candidate.get("found_name") or ""
    profile_url = candidate.get("people_page_url") or ""

    return CanonicalPerson(
        internal_id=profile_url or found_name,
        name=found_name,
        affiliations=[{"name": org} for org in (candidate.get("main_orgs") or [])],
        provenance=[
            SourceAssertion(
                source=Source.UT_PEOPLE,
                field_name="name",
                value=found_name,
            ),
        ],
    )
