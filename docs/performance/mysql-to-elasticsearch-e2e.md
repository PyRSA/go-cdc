# Performance: MySQL → Elasticsearch (E2E)

This document records GitHub Actions CI performance evidence for the
**MySQL → Elasticsearch** path (load + scale). It follows the structure of
[mysql-to-mysql-e2e.md](mysql-to-mysql-e2e.md); per-sink naming follows
`mysql-to-<sink>-e2e.md`.

## Scope and environment

| Item | Value |
| --- | --- |
| Workflow | [`.github/workflows/ci-elasticsearch.yml`](../../.github/workflows/ci-elasticsearch.yml) |
| Runner | GitHub Actions `ubuntu-latest` (shared VM) |
| MySQL (source) | docker compose at `127.0.0.1:13306` |
| Elasticsearch (sink) | **8.15.3** at `127.0.0.1:19200` (`sink.version=8`) |

**How numbers are built**

- Phase timings and counters are **arithmetic means across 10 CI runs** of
  the ES8 load/scale path (`profile=normal`, `suites=go,func,load,scale`).
  Only the **ES8** variant runs load/scale; ES7/ES9 are `go,func`
  compatibility variants.
- Representative stdout is kept at
  [`docs/test/log/elasticsearch8.txt`](../test/log/elasticsearch8.txt) for
  line-by-line lookup.
- “Derived” rates are arithmetic on the averaged values with the formula
  stated; the snapshot rate assumes the shared load fixture volume
  (100k × 5 tables).
- The load harness emits no periodic lag% samples, so drain is covered with
  mean start/end timings only.

## Workloads

### Load suite

Peak knobs: **4 writers × 60s, batch 200**.

One source DB → ES indices, tables `tpl01_t1..t4` + `tpl01_typed` (shared
load fixture: 100k rows each) plus `tpl01_large` (50 × 2 MiB mega fields);
`tpl01_typed.c_bit` is dropped after seeding, before snapshot verification
(BIT is not representable in the ES sink v1).

1. Seed + schema (6 workers), `write-batch-size` phases at 1000 and 100.
2. Snapshot catch-up (TP-EL01), mixed DML (TP-EL02), checkpoint restart
   (TP-EL03).
3. Peak: 4 concurrent writers × 60s, batch 200, into `tpl01_t1` + `tpl01_typed`
   (TP-EL04).
4. Exact-count gate + streaming checksum/content verification (TP-EL05).

### Scale suite (`normal`)

Scale knobs: 20 tables, planned seed 50 000 rows, workers 4, `until=all`.

20 source tables → 20 indices: snapshot, concurrent mutations, stop/resume
with a 100-row gap INSERT, final verify.

## Load suite — Elasticsearch 8 (average of 10 CI runs)

**Result: PASS** (`PASS 5 FAIL 0`; mean variant wall ≈ 8m04s including
setup/build)

| Phase | Mean duration (s) | Notes |
| --- | ---: | --- |
| schema_and_seed | 7.6 | disk free ~82GiB/144GiB (~43% used) |
| es_snapshot | 15.4 | TP-EL01 PASS |
| mixed_dml_catchup | 0.1 | TP-EL02 PASS (`write-batch-size=1000`) |
| restart_catchup | 5.1 | TP-EL03 PASS (`write-batch-size=100`) |
| peak_write | 60.0 | **2 876 400 rows ≈ 47 930 rows/s** written per peak table (counter tracks `tpl01_t1`; `typed` mirrors the same volume) |
| peak_exact | 5.0 | TP-EL04 PASS (exact-count gate) |
| final consistency | — | TP-EL05 PASS (checksums + document content) |

Derived (arithmetic on means):

- Snapshot apply ≈ **32k rows/s** (fixture 500k rows / 15.4s).
- Producers generated ≈ **48k rows/s per peak table** during the peak; the
  exact-count gate passed after the ~2.88M-row per-table backlog (≈5.75M
  across both peak tables) had fully drained
  (~187s mean wall from the end of the flood).

## Scale suite — ES8 `normal` (average of 10 CI runs)

**Result: `SUMMARY PASS`**

| Phase | Mean duration (s) | Notes |
| --- | ---: | --- |
| seed_baseline | 0.9 | disk free ~80GiB/144GiB (~45% used) |
| es_snapshot | 15.1 | `ready=20/20 missing_index=0 docs=50000/50000` |
| mutate | 0.2 | + catch-up 1.3s, `mismatches=0 ok` |
| restart catch-up | 16.3 | 100-row INSERT while stopped; transient count gap → `mismatches=0` |
| final catch-up + verify | 1.1 | `final verify ok (20 tables)` |

Resource samples (averaged band): `cdc_rss` **27–42 MiB**, CPU 3–56%,
load(1m) 1.27–1.63 — the fan-out workload stays small and memory-flat.

## What this supports

1. ES8 bulk indexing absorbs a ~2.9M-row per-table peak flood (≈5.75M rows
   across the two peak tables, ≈ 48k rows/s produced per table) and still
   reaches exact-count parity plus full content verification in the same job.
2. 20-table fan-out snapshot completes in ~15s mean; a stop/resume gap of
   100 rows is re-converged in ~16.3s mean with transient-count transparency.
3. All gates passed alongside `go` + `func` (`PASS 10 FAIL 0`,
   `PASS 5 FAIL 0`, `SUMMARY PASS`).

## Limitations

- The ES load harness emits no periodic lag% lines, so drain shape (linear
  vs. bursty) is observable only from mean start/end timings.
- The snapshot rate depends on the fixture volume assumption above.
- Lag curves and min/max spreads are not charted here; the MySQL → MySQL
  document covers that presentation format for that path.

## Sources

- Log: [`docs/test/log/elasticsearch8.txt`](../test/log/elasticsearch8.txt)
- Workflow: [`.github/workflows/ci-elasticsearch.yml`](../../.github/workflows/ci-elasticsearch.yml)
- Per-job functional/load/scale reports are uploaded as CI artifacts
  (`docs/test/reports/` is not tracked in git).
