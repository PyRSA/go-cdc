# Performance: MySQL → MySQL (E2E)

This document records GitHub Actions **CI / Integration** performance evidence
for the **MySQL → MySQL** path (load + scale). It expands the summary in
[README.md](../../README.md#performance).

Future sink paths (for example MySQL → Kafka / Doris) should get their own
files under `docs/performance/` using the same `mysql-to-<sink>-e2e.md` naming.

## Scope and environment

| Item | Value |
| --- | --- |
| Date (CI wall clock) | 2026-09-30 ~04:34 UTC |
| Workflow | `CI / Integration` (`.github/workflows/integration.yml`) |
| Runner | GitHub Actions `ubuntu-latest` (shared VM) |
| MySQL (load) | 5.7.42 and 8.0.36 via docker compose |
| MySQL (scale) | 8.0.36 |
| Profiles | `LOAD_PROFILE=full` (`LOAD_ROWS_PER_TABLE=100000`); `SCALE_PROFILE=heavy` |
| Peak knobs (load) | 4 workers × 90s, batch 200 |
| Scale knobs | 8 workers; hotspot burst 50k/table × 10; peak 45s |

**How to read these numbers**

- **Source write throughput** = rows inserted into MySQL by the test harness
  during a timed peak (pressure on the source).
- **CDC apply throughput** = sink rows gained / wall time while catching up
  (how fast go-cdc drains backlog), usually **combined** across stressed tables.
- These are integration-stress results on shared CI hardware, **not** a
  vendor-grade benchmark. Do not treat them as capacity guarantees.

## Workloads

### Load suite (func + types + load)

One source DB → one sink DB, six tables (`t1`–`t4`, `typed`, `large`):

1. Seed **100k rows × 5** normal tables + **50 × 2 MiB** LONGBLOB/LONGTEXT rows.
2. Snapshot catch-up (≤1% row lag).
3. Peak: 4 concurrent writers × 90s INSERT into `tpl01_t1` + `tpl01_typed`.
4. Peak catch-up until both tables are within **≤1%** lag (harness budget 3600s).

Functional (TP-01..13) and types (TP-D01..D04) ran in the same job before load.

### Scale suite (heavy)

Ten source DBs → ten sink DBs, **50 tables**, planned seed **300k** rows:

1. Snapshot + checksum verify (COUNT + SUM(id) + CRC).
2. Concurrent mutations + short catch-up.
3. **Hotspot burst**: 10 tables × 50k rows.
4. **Hotspot peak**: 8 workers × 45s timed inserts across the same 10 tables.
5. Hotspot catch-up to **≤1%** lag, then final checksum on all 50 tables.

## Scale heavy (MySQL 8.0) — primary headline

**Result: SUMMARY PASS**

| Phase | Wall time | Derived / notes |
| --- | ---: | --- |
| Seed baseline | 6.7s | 300k planned rows, workers=8 |
| CDC snapshot | 104.1s | ~2,882 rows/s; 50/50 tables ready |
| Mutate | 0.9s | — |
| Mutate catch-up | 5.0s | checksum match |
| Hotspot burst | 15.7s | 500k inserts ≈ 31.8k source rows/s |
| Hotspot peak write | 45.1s | **1,356,000** rows ≈ **30,082** source rows/s |
| Hotspot catch-up | 577.0s | ≤1% lag; ~**3.1k** aggregate apply rows/s across 10 hotspots (steady interval after ~154s) |
| Final verify | — | 50/50 COUNT + SUM(id) + CRC · PASS |

Aggregate CDC apply (~3.1k rows/s) is the **sum** across ten hotspot tables, not
a single-table rate.

## Load suite — MySQL 5.7 (complete)

**Result: PASS** (load TP-L01..L04; suite total 2294.2s)

| Phase | Seconds | Notes |
| --- | ---: | --- |
| schema_setup | 0.6 | — |
| seed_baseline | 5.6 | 100k×5 + 50 large |
| cdc_snapshot | 168.5 | ~500k rows ≈ **2,968** apply rows/s |
| peak_write | 90.0 | **3,050,400** rows ≈ **33,885** source rows/s |
| cdc_peak_catchup | 2023.1 | ≤1% lag · ~**2.9k** aggregate apply rows/s on 2 tables |
| suite_total | 2294.2 | — |

### Peak backlog drain (5.7)

After the 90s flood, both stressed tables start near **~94%** lag (~2.96M
rows/table backlog). Catch-up is near-linear over ~34 minutes with no stall
(see [lag curve](#load-suite--peak-catch-up-lag-curve) below). Per-table apply
stayed near **~1.45k rows/s** (combined ~**2.9k** rows/s).

## Load suite — peak catch-up lag curve

Both MySQL versions start near ~93% lag after the 90s source flood and drain
toward ≤1% without stalls. 5.7 finishes earlier on this CI run; 8.0 takes
longer but still converges.

![Load peak catch-up lag % vs time (MySQL 5.7 vs 8.0)](docs/performance/load-peak-catchup-lag.svg)

| Catch-up time (s) | MySQL 5.7 lag % | MySQL 8.0 lag % |
| ---: | ---: | ---: |
| 0 | 93.9 | 92.9 |
| 300 | 80.4 | 80.9 |
| 600 | 66.8 | 71.8 |
| 900 | 52.3 | 62.6 |
| 1200 | 38.4 | 53.8 |
| 1500 | 24.3 | 44.6 |
| 1800 | 11.0 | 35.8 |
| 2023 | ≤1 (PASS) | 29.4 |
| 2433 | — | 17.1 |
| 2699 | — | 9.2 |
| 2983.5 | — | ≤1 (PASS) |

## Load suite — MySQL 8.0 (complete)

**Result: PASS** (load TP-L01..L04; suite total 3315.3s)

| Phase | Seconds | Notes |
| --- | ---: | --- |
| schema_setup | 0.5 | — |
| seed_baseline | 7.7 | 100k×5 + 50 large |
| cdc_snapshot | 228.7 | ~500k rows ≈ **2,186** apply rows/s |
| peak_write | 90.0 | **2,241,400** rows ≈ **24,900** source rows/s |
| cdc_peak_catchup | 2983.5 | ≤1% lag · ~**1.5k** aggregate apply rows/s on 2 tables |
| suite_total | 3315.3 | — |

### Peak backlog drain (8.0)

Same load knobs as 5.7. Lag starts near **~93%**; apply is slower than 5.7 on
this CI run (~1.5k vs ~2.9k aggregate rows/s) but still linear to ≤1% (see
[lag curve](#load-suite--peak-catch-up-lag-curve)). Per-table apply ~**0.73k**
rows/s (combined ~**1.5k**).

## Apply-rate comparison (derived)

| Scenario | Approx apply rows/s | Definition |
| --- | ---: | --- |
| Snapshot (5.7 load) | ~2,968 | sink rows / snapshot wait |
| Snapshot (8.0 load) | ~2,186 | includes mega-field tables |
| Peak catch-up (5.7 load) | ~2,900 | 2 tables combined |
| Peak catch-up (8.0 load) | ~1,500 | 2 tables combined |
| Snapshot (scale heavy) | ~2,882 | 50 tables / 300k rows |
| Hotspot catch-up (scale) | ~3,130 | 10 hotspots combined |

Source peak pressure on these runs was roughly **25–34k rows/s** for tens of
seconds. CDC apply on this CI path is roughly **1.5–3k rows/s**. The tests
assert **eventual** ≤1% catch-up, not real-time parity during the flood.

## What this supports

1. Under a multi-million-row backlog (5.7 and 8.0 load peaks), go-cdc applied
   steadily with a linear lag curve to ≤1% (≈34 min on 5.7, ≈50 min on 8.0 in
   this CI run).
2. Scale heavy on MySQL 8: multi-DB snapshot, concurrent hotspot burst/peak,
   ≤1% catch-up in 577s, checksum PASS.
3. Correctness suites (func + types) passed in the same CI jobs as the stress
   paths.

## Limitations

- Shared GitHub Actions runners; single-node docker MySQL; no dedicated
  hardware or tuned InnoDB/binlog settings.
- No comparison to Flink CDC or other tools.
- 5.7 vs 8.0 load apply rates differ on the same day; treat as runner/MySQL
  variance, not a version ranking.
- Numbers move with runner noise, MySQL version, and harness knobs
  (`LOAD_*`, `SCALE_*`).

## Sources

- CI stdout captures: load MySQL 5.7 / 8.0 jobs and scale heavy job
  (`test/integration` harness TIMING / catch-up progress lines).
- Workflow: [`.github/workflows/integration.yml`](../../.github/workflows/integration.yml)
- Harness: [`test/integration/harness/load_it.py`](../../test/integration/harness/load_it.py),
  [`test/integration/scale/`](../../test/integration/scale/)
