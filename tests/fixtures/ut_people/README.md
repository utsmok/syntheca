# UT People fixtures

## Provenance

This directory mixes deterministic happy-path fixtures with minimally redacted live captures from the 2026-03-27 audit.

| File | Provenance class | Notes |
| --- | --- | --- |
| `rpc_response.json` | Synthetic, repo-authored | Small happy-path RPC envelope with relative profile URLs. Useful for deterministic parser smoke tests. |
| `profile_page.html` | Synthetic, repo-authored | Happy-path organisation widget where level 1 / level 2 map neatly to faculty / department semantics. |
| `rpc_live_response.json` | Live-captured, trimmed, redacted | Copied from the `body` of `docs/syntheca_post_impl_audit/research/live_samples/ut_people_rpc_live.json`. Emails stay redacted and `resultshtml` remains truncated exactly as in the audit sample. Preserves `totalcount`, paging options, and absolute profile URLs. |
| `profile_live_page.html` | Live-captured, trimmed | Copied from `docs/syntheca_post_impl_audit/research/live_samples/ut_people_profile_live.html`. Preserves the observed hierarchy where a level-1 organisation is `Library, ICT-Services & Archive (LISA)`, proving level 1 is not always a faculty. |

## Usage rules

- Keep UT People fixtures offline; default tests must never depend on public RPC/profile access.
- Treat `*_live_*` files as contract fixtures for envelope and DOM drift, not as identity truth or parity baselines.
- Preserve redactions and avoid adding unnecessary personal data.
