# Pure fixtures

## Provenance

This directory keeps both small deterministic smoke fixtures and exact/trimmed live OAI-PMH captures.

| File | Provenance class | Notes |
| --- | --- | --- |
| `publication_page.xml` | Synthetic, repo-authored | Small smoke fixture for parser happy paths. |
| `persons_page.xml` | Synthetic, repo-authored | Smoke fixture for restored person identifiers and affiliations. |
| `orgunits_page.xml` | Synthetic, repo-authored | Smoke fixture for simple hierarchy and scalar identifier cases. |
| `publication_getrecord_live.xml` | Live-captured | Exact `GetRecord` response copied on 2026-03-27 from `docs/syntheca_post_impl_audit/research/live_samples/pure_oai_publication_getrecord_live.xml`. Preserves CERIF 1.2, repeated author affiliations, year-only publication date, URI-valued enums, and live file-location structure. |
| `orgunit_getrecord_live.xml` | Live-captured | Exact `GetRecord` response copied from `docs/syntheca_post_impl_audit/research/live_samples/pure_oai_orgunit_getrecord_live.xml`. Preserves repeated `cerif:Identifier` elements with `type="Scopus affiliation ID"`. |
| `person_getrecord_live.xml` | Live-captured | Exact minimal CERIF 1.2 person sample copied from the audit captures so namespace handling has a real local reference. |

## Usage rules

- Default tests stay offline by loading these XML files directly.
- The `*_live.xml` fixtures are contract fixtures, not golden output baselines.
- Keep exact OAI-PMH structure intact; do not reformat away repeated elements that motivated the audit.
