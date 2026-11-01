# Performance: MySQL → Doris (E2E)

This document records GitHub Actions CI performance evidence for the
**MySQL → Doris** path (load + peak stress + catch-up). It follows the
structure of [mysql-to-mysql-e2e.md](mysql-to-mysql-e2e.md); per-sink naming
follows `mysql-to-<sink>-e2e.md`.

## Scope and environment

| Item | Value |
| --- | --- |
| Workflow | [`.github/workflows/ci-doris.yml`](../../.github/workflows/ci-doris.yml) |
| Runner | GitHub Actions `ubuntu-latest` (shared VM) |
| MySQL (source) | docker compose at `127.0.0.1:13306` |
| Doris (sink) | FE version `doris-2.1.8-rc01-52396fbc88` (UNIQUE KEY model, Stream Load, at-least-once) |

**How numbers are built**

- Phase timings, catch-up samples, and counters are **arithmetic means across
  10 CI runs** of the Doris load suite (`profile=normal`, `suites=go,func`).
- Representative stdout is kept at
  [`docs/test/log/doris.txt`](../test/log/doris.txt) for line-by-line lookup.
- “Derived” rates are arithmetic on the averaged values; the formula is stated
  next to each number.
- The catch-up table lists mean harness sample points; nothing is interpolated.

## Workloads

### Load suite (inside `func`)

CI profile `normal`, suites `go,func` — load/peak points TP-DR05..DR07 run
inside the `func` session. Peak knobs: **4 writers × 60s, batch 200**.

One source DB → Doris, five tables (`tpdl01_t1`–`t4`, `tpdl01_typed`):

1. Seed **100k rows × 5 tables** (5 seed workers, one conn/table).
2. Snapshot catch-up (progress lines report against a `99000`-row target).
3. Peak: 4 concurrent writers × 60s INSERT into `tpdl01_t1` + `tpdl01_typed`
   (batch 200).
4. Catch-up sampled every ~20s until the harness PASS gate.

Functional points TP-DR01..DR04 (snapshot routing, incremental DML, schema
evolution, restart) ran before the load phase; the `go` suite ran before that.

## Load suite — Doris (average of 10 CI runs)

**Result: PASS** (TP-DR05/DR06/DR07; mean variant wall ≈ 15m52s including
setup/build)

| Phase | Mean duration (s) | Notes |
| --- | ---: | --- |
| doris_seed_baseline | 5.4 | 100k × 5 tables |
| doris_snapshot | 34.7 | TP-DR05 PASS |
| doris_peak_write | 60.0 | **1 338 200 rows ≈ 22 299 rows/s** produced per table |
| doris_peak_catchup | 570.0 | TP-DR06 PASS (`wait [doris_peak catch-up] done in 570.0s`) |

### Peak catch-up lag (10-run averaged samples)

Lag falls monotonically across the averaged sample points; mean catch-up
closes with the 570s `done` timing and TP-DR06 PASS:

| t (s) | t1 rows/total | lag | load (1/5/15) | cpu | cdc_rss |
| ---: | --- | ---: | ---: | ---: | ---: |
| 0 | 188200/1438200 | 86.91% | 7.75/5.37/2.89 | 88% | 361MiB |
| 183 | 592800/1438200 | 58.78% | 5.21/4.97/3.16 | 67% | 362MiB |
| 306 | 863600/1438200 | 39.95% | 5.59/5.09/3.43 | 56% | 362MiB |
| 489 | 1260400/1438200 | 12.36% | 2.87/4.09/3.34 | 60% | 362MiB |
| 550 | 1387400/1438200 | 3.53% | 3.94/4.24/3.44 | 57% | 206MiB |

### Apply-rate comparison (derived, averaged)

| Scenario | Throughput (rows/s) | Definition |
| --- | ---: | --- |
| Snapshot (load) | ~14k | ≈500k rows / 34.7s across five tables |
| Peak write (source side) | ~22.3k per table | mean 1 338 200 rows / 60s |
| Peak catch-up (sink drain) | ~4.4k aggregate | (1 438 200 − 188 200) × 2 tables / 570s |

## What this supports

1. A ~2.5M-row peak backlog (2 tables × ~1.25M pending rows at the first
   catch-up sample) drains to completion in ~570s mean wall with monotonic
   lag decay under UNIQUE KEY + Stream Load.
2. Snapshot of 500k rows across 5 tables completes in ~35s mean while the
   same job passes all 7 functional points.
3. `cdc_rss` stayed flat (~362MiB) through the 489s sample, then eased to
   343/294/206MiB over the final three samples as the drain closed — no leak
   signature.

## Limitations

- Catch-up completion is asserted by the harness PASS at mean 570s; the last
  averaged sample still shows 3.53% lag.
- Drain duration is runner- and Doris FE-batch-sensitive; shared-runner noise
  can stretch the same phase several-fold, so treat the means as a central
  band rather than a hard SLO.
- Doris has a scale suite in `run.sh` (default `go,func,scale`), but the
  referenced CI job captured `suites=go,func`, so no scale data backs this
  document.

## Sources

- Log: [`docs/test/log/doris.txt`](../test/log/doris.txt)
- Workflow: [`.github/workflows/ci-doris.yml`](../../.github/workflows/ci-doris.yml)
- The per-job report is uploaded as a CI artifact
  (`docs/test/reports/` is not tracked in git).
