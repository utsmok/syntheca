# OpenAlex fixtures

## Provenance

This directory intentionally mixes deterministic smoke fixtures and audited live-shape contract fixtures.

| File | Provenance class | Notes |
| --- | --- | --- |
| `works_response.json` | Synthetic, repo-authored | Deterministic smoke fixture used for happy-path parser tests and `meta.cost_usd` tolerance. It is not a real baseline and it keeps the older `awards: null` / `grants: []` shape on purpose. |
| `works_response_live.json` | Live-captured, trimmed | Copied on 2026-03-27 from `docs/syntheca_post_impl_audit/research/live_samples/openalex_works_list_live.json`. Trimmed only for depth/size markers already present in the audit sample. Preserves the current list envelope, structured `awards`, `funders`, `content_urls`, and the live omission of `grants`. |
| `works_response_live_contract.json` | Live-like, repo-normalized from audited sample | Parseable contract fixture derived from the audited live shape. Keeps structured `awards`, omits `grants`, preserves `meta.cost_usd`, and adds only the null/default fields needed for deterministic offline production-path parsing. |

## Usage rules

- Default tests must stay offline and deterministic; use these files instead of live HTTP calls.
- Treat `works_response_live.json` as a contract fixture for schema drift, not as a golden output baseline.
- Release parity claims still require a real regression baseline in `tests/regression_baseline.json`.
