# Merged-output fixtures

## Provenance

| File | Provenance class | Notes |
| --- | --- | --- |
| `enriched_sample.parquet` | Synthetic/local smoke subset | Tiny repo-local parquet fixture used for lightweight shape and export smoke tests. It is intentionally much smaller and thinner than a real regression pack. |
| `final_sample.parquet` | Synthetic/local smoke subset | Tiny repo-local final-output fixture for deterministic smoke coverage only. |

## Usage rules

- These parquet files are **not** release baselines.
- Real parity claims require `tests/regression_baseline.json` to be promoted to `_baseline_status: "real"` with no null metric baselines.
- Keep any future larger regression pack offline, project-local, and provenance-documented before treating it as a real baseline.
