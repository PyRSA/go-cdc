# Performance: MySQL → Kafka (E2E)

This document records GitHub Actions CI performance evidence for the
**MySQL → Kafka** path (load/volume + peak catch-up). It follows the
structure of [mysql-to-mysql-e2e.md](mysql-to-mysql-e2e.md); per-sink naming
follows `mysql-to-<sink>-e2e.md`.

## Scope and environment

| Item | Value |
| --- | --- |
| Workflow | [`.github/workflows/ci-kafka.yml`](../../.github/workflows/ci-kafka.yml) |
| Runner | GitHub Actions `ubuntu-latest` (shared VM) |
| MySQL (source) | docker compose at `127.0.0.1:13306` |
| Kafka (sink) | franz-go producer, CDC JSON envelope, at-least-once; verified by a `kafka-python` consumer harness (`harness/kafka_it.py`) |

**How numbers are built**

- Phase timings, topic counts, and derived rates are **arithmetic means
  across 10 CI runs** of the Kafka load suite (`profile=normal`,
  `suites=func` — load/volume points TP-K05..K08 run inside `func`).
- Representative stdout is kept at
  [`docs/test/log/kafka.txt`](../test/log/kafka.txt) for line-by-line lookup.
- Counts are **consumer-side record observations** on topics
  (`kafka.tpl01_*`), not sink-internal throughput; gates are expressed as
  `need >=` floors (≤1% tolerance) rather than exact parity, leaving room
  for at-least-once duplicates on replays.
- “Derived” rates are arithmetic on the averaged values; the formula is
  stated next to each number.

## Workloads

### Load suite (inside `func`)

Peak knobs: **4 writers × 60s**.

One source DB → Kafka topics. The shared load fixture seeds six tables
(`tpl01_t1`–`t4` + `tpl01_typed`, 100k rows each, plus `tpl01_large` with
50 × 2 097 152 B LONGBLOB/LONGTEXT) with 6 workers (one conn/table); topics
are verified on `tpl01_t1`, `tpl01_typed` and `tpl01_large`:

1. Snapshot catch-up per topic against `>= 99000` / `>= 49` floors.
2. Peak: 4 concurrent writers × 60s into `tpl01_t1` + `tpl01_typed`.
3. Catch-up until each topic clears `need >= 1340064`.

Functional points TP-K01..K04 (routing/topics, incremental c/u/d, DDL emit,
restart) ran before the load phase.

## Load suite — Kafka (average of 10 CI runs)

**Result: PASS** (TP-K05..K08; mean variant wall ≈ 14m32s including
setup/build)

### Snapshot volume (TP-K05)

| Topic | Mean wait (s) | end | baseline | need | Note |
| --- | ---: | ---: | ---: | ---: | --- |
| tpl01_t1 | 0.1 | 100000 | 0 | ≥99000 | already drained when the wait started |
| tpl01_typed | 10.2 | 100000 | 0 | ≥99000 | interim sample at 0s showed 74209 |
| tpl01_large | 0.1 | 50 | 0 | ≥49 | 2 MiB mega-field records |

### Peak catch-up (TP-K06)

Writers ran 60s × 4 workers; averaged consumer samples:

| Sample | Consumer end | baseline | delta vs need ≥1340064 |
| --- | ---: | ---: | --- |
| catch-up 0s | 1163800 | 100000 | delta=1063800 (short) |
| done in 10.2s | 1453600 | 100000 | +1353600 → gate passed |
| typed | 1453600 | 100000 | done in 0.1s |

Mega-field checks (TP-K07 typed spot-check, TP-K08 2 MiB LONGBLOB/LONGTEXT)
passed in the same job window as the peak phase.

### Apply-rate comparison (derived, averaged)

| Scenario | Throughput (records/s) | Definition |
| --- | ---: | --- |
| Peak write window per table | ~22.6k | 1 353 600 delta records over the 60s window |
| Peak catch-up residual drain | ~28k (t1 topic only) | (1 353 600 − 1 063 800) cleared in the measured 10.2s wait; typed drained in 0.1s |

## What this supports

1. The peak backlog cleared within ~10s mean of the writers stopping — the
   fastest drain among the four sink paths in this comparison set (vs ~570s
   Doris, ~3 min wall ES8). This is an async log-append sink versus
   analytical-index comparison, not apples-to-apples.
2. Snapshot of 200k rows + 50 × 2 MiB mega records reaches the ≥99% consumer
   floors; the final topic count (1 453 600 = 100k snapshot + 1 353 600 peak)
   is consistent with the source totals.

## Limitations

- Counts are consumer-side observations; producer acks and sink-internal
  batching are not observable from the harness stdout.
- No scale suite exists for Kafka in the CI variant matrix.

## Sources

- Log: [`docs/test/log/kafka.txt`](../test/log/kafka.txt)
- Workflow: [`.github/workflows/ci-kafka.yml`](../../.github/workflows/ci-kafka.yml)
- The per-job report is uploaded as a CI artifact
  (`docs/test/reports/` is not tracked in git).
