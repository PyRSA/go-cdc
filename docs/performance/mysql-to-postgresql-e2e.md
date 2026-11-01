# Performance: MySQL → PostgreSQL (E2E)

This document records GitHub Actions CI performance evidence for the
**MySQL → PostgreSQL** path (load + bench + scale). It follows the structure
of [mysql-to-mysql-e2e.md](mysql-to-mysql-e2e.md); per-sink naming follows
`mysql-to-<sink>-e2e.md`.

## Scope and environment

| Item | Value |
| --- | --- |
| Workflow | [`.github/workflows/ci-postgresql.yml`](../../.github/workflows/ci-postgresql.yml) |
| Runner | GitHub Actions `ubuntu-latest` (shared VM) |
| MySQL (source) | docker compose at `127.0.0.1:13306` |
| PostgreSQL (sink) | **14.24 / 15.19 / 16.4 / 17.11 / 18.6** (Debian pgdg images) at `127.0.0.1:15432/cdc_it` |
| Profile | `heavy` — suites `go,func,load,bench,scale,compat` |

**How numbers are built**

- The `heavy` tier fans the PostgreSQL version matrix out into **five parallel
  CI rows, one per major** (pg14…pg18); every row runs the full suite list
  once. The five matrix rows are therefore **one run per major version** from
  the same run set (2026-11-01), *not* repeated runs of one version — numbers
  are reported per version and are never averaged across majors.
- Phase timings and counters are quoted **verbatim** from the `heavy` CI job
  stdout of each major-version row.
- "Derived" rates are arithmetic on those literals with the formula stated.
- The peak-write counter tracks `tpl01_t1` only
  (`written += len(orders)` in
  [`pg_load_it.py`](../../tests/integration/sinks/postgresql/harness/pg_load_it.py));
  `tpl01_typed` receives the same volume in the same batches, so the rps
  figure is **per peak table** (≈ 2 × that volume lands on the sink).

## Workloads

### Load suite

Peak knobs (`heavy` tier): **8 writers × 60s, batch 200** (the `normal` tier
uses 4 writers — see [`run.sh`](../../tests/integration/scripts/run.sh) tier
table). Sink side: pipeline `cdc_it.tp_pg_load`, `batch 1000/500ms,
parallelism 8`.

One source DB → one PG schema, tables `tpl01_t1..t4` + `tpl01_typed` (shared
load fixture: 100k rows each) plus `tpl01_large` (50 × 2 MiB mega fields);
`tpl01_typed.c_enum` and `c_bit` are dropped after seeding (refused by the
PostgreSQL sink, [`docs/test/POSTGRESQL.md`](../test/POSTGRESQL.md) §4).

1. Seed (6 workers) + schema, then snapshot catch-up (TP-PL01).
2. Peak: 8 concurrent writers × 60s into the MySQL source
   (`tpl01_t1` + `tpl01_typed`), then wait for PG catch-up (TP-PL02).
3. Typed-column reconciliation + spot checks (TP-PL03), mega-field digests
   (TP-PL04).

### Bench suite

Six cells: `insert` / `mixed` DML × target `write-batch-size` 100 / 1000 /
5000; 20 000 source rows per cell, 1 writer, 200 ms probe interval. The
recorded timing is the catch-up after each cell.

### Scale suite (`heavy`)

Scale knobs: **50 tables, 300k total rows, workers 8, `until=all`**; hotspot
phase = 10 tables × 50 000-row burst + 8 writers × 60s peak (batch 200).
Snapshot → concurrent mutations → stop/resume with a 100-row INSERT → row-level
verify at peak / restart / final.

### Func + compat

`func` result is identical in all five rows: **PASS 18 FAIL 0 OTHER 2**
(TP-P17 "PostgreSQL restart while the pipeline is writing" and TP-P18
"Network interruption" are opt-in and SKIPPED). The `go` suite passes in every
row, and each row's version-scoped `compat` suite reports `PGxx: PASS`.

## Load suite — per major version (single heavy run each)

**Result: PASS 4 FAIL 0** in every row. Log window per row (first/last
line, go-cdc build included; docker stack setup ran earlier as its own
workflow step — `IT_SKIP_SETUP=1`) spans ≈ 19m40s (pg17) to 22m07s (pg15).

| Phase | pg14 | pg15 | pg16 | pg17 | pg18 |
| --- | ---: | ---: | ---: | ---: | ---: |
| schema_and_seed (s) | 8.4 | 8.9 | 8.4 | 7.6 | 8.2 |
| snapshot catch-up (s) | 9.2 | 9.3 | 9.2 | 2.5 | 9.2 |
| TP-PL01 total (s) | 23.3 | 25.0 | 23.1 | 10.0 | 23.3 |
| peak_write 60.0s — rows ≈ rps (per peak table) | 1382800 ≈ 23033 | 1313800 ≈ 21879 | 1404600 ≈ 23392 | 2822800 ≈ 47034 | 1405800 ≈ 23416 |
| backlog at catch-up t=0 (t1 rows; derived: src − snk of the `catch-up 0s` sample) | 750600 | 715600 | 762400 | 1647400 | 773000 |
| peak catch-up done (s) | 44.8 | 44.9 | 45.0 | 55.7 | 47.0 |
| TP-PL02 total (s) | 135.1 | 135.3 | 134.5 | 149.4 | 138.5 |
| TP-PL03 (s) | 98.3 | 98.8 | 95.0 | 115.3 | 98.4 |
| TP-PL04 (s) | 1.1 | 1.1 | 1.0 | 0.6 | 0.9 |

pg17 is the visible outlier of the set: roughly double the flood volume, a
2.5s snapshot and a 100 MiB `cdc_rss` snapshot sample against 1197–1238 MiB
in the other four rows. Each row ran on its own runner and each version has
exactly one run, so this is reported as-is (runner-level noise, not a pg17
sink property) and not smoothed into the others.

Derived (arithmetic on the literals above):

- Snapshot apply ≈ **54k rows/s** on pg14/15/16/18 (fixture 500k rows /
  ~9.2–9.3s); pg17 ≈ 200k rows/s (500k / 2.5s).
- Peak-drain apply rate after the flood ≈ **15.9k–16.9k rows/s per table**
  on pg14/15/16/18 (backlog at t=0 / catch-up time, e.g. pg14:
  750600 / 44.8 ≈ 16.8k); pg17 ≈ 29.6k (1647400 / 55.7).
- Producers generated 1.31M–1.41M rows per peak table in 60s on four of the
  five rows (≈ 2.6–2.8M rows across `tpl01_t1` + `tpl01_typed` hitting the
  sink); the exact-count gate and full reconciliation passed after the
  backlog drained in 44.8–55.7s.

## Bench suite — catch-up per cell (single heavy run each)

**Result: PASS 6 FAIL 0** in every row.

| Cell | pg14 | pg15 | pg16 | pg17 | pg18 |
| --- | ---: | ---: | ---: | ---: | ---: |
| insert/batch-100 (s) | 0.6 | 0.6 | 0.6 | 0.5 | 0.6 |
| insert/batch-1000 (s) | 0.6 | 0.6 | 0.0 | 0.5 | 0.6 |
| insert/batch-5000 (s) | 0.6 | 0.6 | 0.6 | 0.5 | 0.6 |
| mixed/batch-100 (s) | 0.2 | 0.2 | 0.2 | 0.1 | 0.2 |
| mixed/batch-1000 (s) | 0.2 | 0.2 | 0.2 | 0.1 | 0.2 |
| mixed/batch-5000 (s) | 0.2 | 0.2 | 0.2 | 0.1 | 0.2 |

Every cell verified `src = sink` row counts and canonical row equality (with
an injected-latency probe passing).

## Scale suite — `heavy` per major version (single run each)

**Result: `SUMMARY PASS`** in every row; `final verify ok (50 tables, row level)`.

| Phase | pg14 | pg15 | pg16 | pg17 | pg18 |
| --- | ---: | ---: | ---: | ---: | ---: |
| seed_baseline (s) | 14.9 | 15.4 | 14.6 | 7.4 | 14.8 |
| cdc_snapshot (s) | 16.8 | 17.1 | 16.9 | 16.2 | 16.9 |
| hotspot burst (s) | 31.9 | 31.3 | 31.4 | 17.6 | 31.3 |
| peak_write 60.1–60.2s — rows ~ rps | 712000 ~ 11841 | 694000 ~ 11532 | 710000 ~ 11806 | 1424200 ~ 23698 | 705800 ~ 11729 |
| peak_catchup (s) | 18.3 | 18.4 | 18.0 | 18.1 | 18.1 |
| row_verify peak/restart/final (s) | 88.3 / 88.0 / 88.0 | 90.3 / 90.1 / 89.9 | 87.1 / 86.8 / 87.0 | 75.4 / 74.4 / 75.7 | 88.1 / 87.2 / 87.5 |

Restart recovery was transparent in every row: 100 rows inserted while the
job was down showed up as `mismatches=1` on the first aggregate probe, then
`mismatches=0` once the checkpoint resumed (wait `[aggregate:restart]`
20.5–21.0s on pg14/15/16/18, 19.1s on pg17). At the `hotspot catch-up 2s`
sample the per-table delta peaked at 4.71% (pg17; 4.60% pg15, 3.55% pg14,
1.52% pg18, 0.91% pg16), and every row showed `delta=0.00%` on all ten
hotspot tables at the 18s sample.

## Resource samples

Load-phase `cdc_rss` (from the periodic catch-up lines): **1197–1238 MiB** on
pg14/15/16/18 vs **100 MiB** on pg17 in the snapshot window (first catch-up
sample after the pipeline starts), and **354–367 MiB** in every row during
the peak-drain window. Reported as observed bands, not a steady-state
footprint.

## What this supports

1. All five supported PostgreSQL majors (14–18) pass the identical suite list
   (`go,func,load,bench,scale,compat`) in one heavy run each: **PASS 18 /
   PASS 4 / PASS 6 / SUMMARY PASS / PGxx: PASS**, zero failures.
2. The PostgreSQL sink absorbs a 1.3M–1.4M-row per-table peak flood (~2×
   volume across the two peak tables) at 8-writer production and reaches
   exact-count parity plus 5000-row-window reconciliation in 44.8–47.0s of
   catch-up (55.7s on the faster-volume pg17 row).
3. 50-table / 300k-row fan-out snapshots complete in ~16–17s on every major;
   row-level verification (peak, restart, final) converges in every row, with
   restart gaps re-converging in ~19–21s.

## Limitations

- **One run per major version** — no within-version spread or mean is
  available; treat each cell as a single-sample observation. Do not read the
  pg14-vs-pg18 differences (e.g. 23033 vs 23416 rps) as version signal; they
  are within single-run noise.
- The pg17 row is a runner-level outlier (≈2× flood, ≈4× faster snapshot,
  2× lower snapshot RSS); it is kept visible rather than averaged away.
- These numbers come from the `heavy` tier (**8** peak writers); the Doris /
  Elasticsearch sibling documents report the `normal` tier (4 writers), so
  flood rps is not directly comparable across those files.
- No periodic lag% lines are emitted, so drain shape is observable only from
  the sampled catch-up lines (t=0, then ≈16s intervals: 0/16/31s on pg14,
  0/16/32/48s on pg17).
- TP-P17 / TP-P18 (target restart, network interruption) are opt-in and were
  not exercised.

## Sources

- Workflow: [`.github/workflows/ci-postgresql.yml`](../../.github/workflows/ci-postgresql.yml)
- Tier / knob table: [`tests/integration/scripts/run.sh`](../../tests/integration/scripts/run.sh)
- Connector: [`docs/connectors/postgresql-sink.md`](../connectors/postgresql-sink.md) ·
  test spec: [`docs/test/POSTGRESQL.md`](../test/POSTGRESQL.md)
- Per-job functional/load/scale reports are uploaded as CI artifacts
  (`docs/test/reports/` is not tracked in git).
