# OpenAIRE fixtures

## Provenance

This directory separates deterministic smoke fixtures from audited live-shape contract fixtures.

| File | Provenance class | Notes |
| --- | --- | --- |
| `research_product_response.json` | Synthetic / legacy-shaped | Repo-authored smoke fixture that still uses the older `indicators.bipIndicators` layout. Keep it until the compatibility transition is complete. |
| `research_product_live_response.json` | Live-captured, trimmed | Copied on 2026-03-27 from the `body` of `docs/syntheca_post_impl_audit/research/live_samples/openaire_research_products_live.json`. Preserves current `indicators.citationImpact` shape and the live Graph search envelope. |
| `organizations_live_response.json` | Live-captured, trimmed | Copied from the `body` of `docs/syntheca_post_impl_audit/research/live_samples/openaire_organizations_live.json`. Preserves the observed broad `search=` result ordering where a UT query does not rank UT first. |

## Usage rules

- Keep these fixtures offline and deterministic; they replace live smoke calls in default tests.
- `*_live_response.json` files are contract fixtures for schema and query-shape drift, not output baselines.
- Do not silently rewrite `citationImpact` back into `bipIndicators`; preserving the observed live shape is the point.
