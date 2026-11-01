# Performance: MySQL → MySQL (E2E)

This document records GitHub Actions **CI / Integration** performance evidence
for the **MySQL → MySQL** path (load + scale). It expands the summary in
[README.md](../../README.md#performance).

Future sink paths (for example MySQL → Kafka / Doris) should get their own
files under `docs/performance/` using the same `mysql-to-<sink>-e2e.md` naming.

## Scope and environment

| Item | Value |
| --- | --- |
| Samples | Load: **10-run average** each for MySQL 5.7 / 8.0; Scale heavy: **10-run average** (8.0) |
| CI dates | sample window 2026-09-30 · 2026-10-12 |
| Workflow | `CI / Integration` (`.github/workflows/ci.yml`) |
| Runner | GitHub Actions `ubuntu-latest` (shared VM) |
| MySQL (load) | 5.7.42 and 8.0.36 via docker compose |
| MySQL (scale) | 8.0.36 · `SCALE_PROFILE=heavy` only |
| Profiles | `LOAD_PROFILE=full` (`LOAD_ROWS_PER_TABLE=100000`); heavy scale |
| Peak knobs (load) | **Current CI:** 4 workers × 60s, batch 200 |
| Scale knobs | 8 workers; hotspot burst 50k/table × 10; peak 45s |

**How numbers and charts are built**

- Phase timings in tables are **arithmetic means** across the listed runs.
- Lag curves are **run-normalized averages**: each run’s catch-up time is mapped
  to \\([0,1]\\), lag is averaged at each fraction, then the X axis is scaled to
  the **mean** catch-up duration. This keeps shape comparable when runs finish
  at different wall times.
- Chart polylines/markers **collapse plateaus**: points are kept when lag moves
  by ≳1.5–2%, plus endpoints — flat “stuck” stretches are not over-sampled.
- A `SCALE_PROFILE=normal` log (no hotspot phase) is **excluded** from scale
  averages.
- These are shared-runner stress results, **not** vendor-grade benchmarks.

## Workloads

### Load suite (func + types + load)

One source DB → one sink DB, six tables (`t1`–`t4`, `typed`, `large`):

1. Seed **100k rows × 5** normal tables + **50 × 2 MiB** LONGBLOB/LONGTEXT rows.
2. Snapshot catch-up (≤1% row lag).
3. Peak: 4 concurrent writers × 60s INSERT into `tpl01_t1` + `tpl01_typed`.
4. Peak catch-up until both tables are within **≤1%** lag (harness budget 3600s).

Functional (TP-01..13) and types (TP-D01..D04) ran in the same job before load.

> **Evidence note:** Load phase timings, apply rates, and the lag chart below are
> **historical averages from prior CI with peak = 4×90s**. Current knobs are
> **4×60s**; remeasure before treating those numbers as current CI baselines.

The load lag chart has **two lines** because CI runs the same load suite on
**MySQL 5.7 and 8.0**.

### Scale suite (heavy)

Ten source DBs → ten sink DBs, **50 tables**, planned seed **300k** rows:

1. Snapshot + checksum verify (COUNT + SUM(id) + CRC).
2. Concurrent mutations + short catch-up.
3. **Hotspot burst**: 10 tables × 50k rows.
4. **Hotspot peak**: 8 workers × 45s timed inserts across the same 10 tables.
5. Hotspot catch-up to **≤1%** lag, then final checksum on all 50 tables.

Scale is **MySQL 8.0 only** in CI, so it uses a **separate** single-series chart.

## Scale heavy (MySQL 8.0) — average of 10 runs

**Result: SUMMARY PASS** (10-run average)

| Phase | Mean wall time | Notes (min / max across sample) |
| --- | ---: | --- |
| Seed baseline | 7.4s | 6.7 / 8.2 |
| CDC snapshot | 132.6s | 104 / 161 · ~2.3k rows/s on 300k |
| Mutate | 1.0s | 0.9 / 1.1 |
| Mutate catch-up | 14.5s | 5.0 / 23.9 |
| Hotspot burst | 17.0s | 15.7 / 18.3 · 500k inserts |
| Hotspot peak write | 45.1s | mean **1.17M** rows ≈ **26.1k** source rows/s |
| Hotspot catch-up | **1302s** | 577 / 2027 · see curve below |
| Final verify | — | 50/50 COUNT + SUM(id) + CRC · PASS |

Some runs drain nearly linearly (~577s); others show a mid-run apply stall
(up to ~2027s). The 10-run averaged curve still reaches ≤1%, with less visual
weight on flat segments than a single stalled run.

### Scale heavy — hotspot catch-up lag curve

Y-axis: **mean** `delta%` across 10 hotspot tables, then averaged across runs
(normalized time). Markers follow lag change, not every log line.

![Scale hotspot catch-up lag % vs time (MySQL 8.0, avg)](scale-hotspot-catchup-lag.svg)

| Catch-up time (s) | Mean lag % (avg) |
| ---: | ---: |
| 0 | 89.6 |
| 200 | 66.5 |
| 400 | 46.2 |
| 600 | 38.8 |
| 800 | 31.4 |
| 1000 | 24.0 |
| 1302 | ≤1 (PASS) |

## Load suite — MySQL 5.7 (average of 10 runs)

> Historical sample: peak knobs were **4×90s** (not current 4×60s).

**Result: PASS** (10-run average; mean suite total **2269s**)

| Phase | Mean seconds | Notes |
| --- | ---: | --- |
| seed_baseline | 6.7 | 100k×5 + 50 large |
| cdc_snapshot | 130.4 | 46 / 215 · ~500k rows |
| peak_write | 90.0 | **prior 4×90s**; final src ≈ **2.87M**/table (mean) |
| cdc_peak_catchup | **2035** | 1663 / 2408 · ~**2.8k** aggregate apply rows/s |
| suite_total | 2269 | — |

### Peak backlog drain (5.7)

Start lag ~**94%**. Catch-up is near-linear on average to ≤1% in ~34 minutes
(see [lag curve](#load-suite--peak-catch-up-lag-curve)).

## Load suite — peak catch-up lag curve

> Historical sample: peak knobs were **4×90s** (not current 4×60s).

Both versions start near **~94%** lag after the 90s flood. Averaged curves stay
close through mid catch-up; **5.7 finishes earlier on average** (~2035s) than
8.0 (~2280s).

![Load peak catch-up lag % vs time (MySQL 5.7 vs 8.0, avg)](load-peak-catchup-lag.svg)

| Catch-up time (s) | MySQL 5.7 lag % | MySQL 8.0 lag % |
| ---: | ---: | ---: |
| 0 | 93.9 | 94.4 |
| 300 | 85.1 | 81.7 |
| 600 | 70.7 | 68.9 |
| 900 | 55.9 | 56.2 |
| 1200 | 41.3 | 43.4 |
| 1500 | 26.6 | 30.8 |
| 1800 | 11.7 | 19.2 |
| 2035 | ≤1 (PASS) | 10.1 |
| 2280 | — | ≤1 (PASS) |

## Load suite — MySQL 8.0 (average of 10 runs)

> Historical sample: peak knobs were **4×90s** (not current 4×60s).

**Result: PASS** (10-run average; mean suite total **2534s**)

| Phase | Mean seconds | Notes |
| --- | ---: | --- |
| seed_baseline | 6.1 | 100k×5 + 50 large |
| cdc_snapshot | 152.3 | 76 / 228 · ~500k rows |
| peak_write | 90.0 | **prior 4×90s**; final src ≈ **2.30M**/table (mean) |
| cdc_peak_catchup | **2280** | 2235 / 2326 · ~**1.9k** aggregate apply rows/s |
| suite_total | 2534 | — |

### Peak backlog drain (8.0)

Same **prior 4×90s** knobs as 5.7. Start lag ~**94%**; combined apply ~**1.9k**
rows/s on average; near-linear drain to ≤1% (see curve above).

## Apply-rate comparison (derived, averaged)

| Scenario | Approx apply rows/s | Definition |
| --- | ---: | --- |
| Snapshot (5.7 load) | ~3,800 | 500k / mean snapshot wait (high variance) |
| Snapshot (8.0 load) | ~3,300 | same |
| Peak catch-up (5.7 load) | ~2,800 | 2 tables combined, mean of runs |
| Peak catch-up (8.0 load) | ~1,900 | 2 tables combined, mean of runs |
| Snapshot (scale heavy) | ~2,300 | 300k / mean snapshot wait |
| Hotspot catch-up (scale) | varies | 577s vs 2027s runs; see curve |

The tests assert **eventual** ≤1% catch-up, not real-time parity during the
flood. Scale wall time is especially runner-sensitive.

## What this supports

1. Under **prior 4×90s** multi-million-row load peaks, go-cdc drained to ≤1% with
   a near-linear averaged lag curve (~34 min on 5.7, ~38 min on 8.0 across those
   historical samples). Current CI uses **4×60s**; expect smaller backlog /
   shorter catch-up until remeasured.
2. Scale heavy on MySQL 8 reaches ≤1% catch-up and checksum PASS; individual
   runs may include mid-run apply stalls (visible when not averaged away).
3. Correctness suites (func + types) passed in the same CI jobs as the stress
   paths.

## Limitations

- Shared GitHub Actions runners; single-node docker MySQL.
- No comparison to Flink CDC or other tools.
- Averages are over **10** runs per scenario; outliers are smoothed but runner
  noise remains.
- Scale `normal` profile is a different workload and is not mixed into heavy.
- Numbers move with runner noise, MySQL version, and harness knobs.

## Sources

- CI stdout captures under `tests/integration/workdir/` (local copies of job
  logs; not committed): load 5.7 / 8.0 ×10, scale heavy ×10.
- Workflow: [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml)
- Harness: [`tests/integration/sinks/mysql/harness/load_it.py`](../../tests/integration/sinks/mysql/harness/load_it.py),
  [`tests/integration/common/`](../../tests/integration/common/)
