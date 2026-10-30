# MySQL → PostgreSQL E2E / Load / Scale — plan and traceability

PR3 deliverable index for the PostgreSQL sink test suites: what is reused from
`mysql2mysql`, what PostgreSQL changes, and which requirement (`PG-2xx`) each
test point proves.

**Implementation status of every suite in this document: `NOT RUN`.** No
integration result for PostgreSQL exists in this repository yet; nothing here may
be read as a pass. Status is only changed by a real run (see
[§9](#9-status-vocabulary)), and the reports under `docs/test/reports/` are the
evidence. The offline harness tests do run, and they are reported separately
(`OFFLINE PASS`) because they never contact a server.

## 1. Baseline audit of the existing suites

Read before writing new code, as required by the PR3 spec (§3.1). All three
suites are Python harnesses driving the built `go-cdc` binary from outside; none
of them is a Go test.

| Suite | Entry | Orchestrator | Data | Verification |
|---|---|---|---|---|
| E2E functional | `scripts/run-e2e.sh` | `harness/run_it.py` (TP-01…TP-20) | `fixtures/seed.sql`, per-case DML | row/column compare via `pymysql`, `side_by_side()` diff |
| E2E types | same | `harness/types_it.py` (TP-D01…TP-D04) | `fixtures/seed_types.sql` | per-type canonical compare (temporal wall clock, decimal, JSON) |
| E2E load | same | `harness/load_it.py` (TP-L01…TP-L04) | generated `tp01_t1..t4`, `tpl01_typed`, `tpl01_large` | baseline + peak reconcile, per-row diff on failure |
| E2E scale | `scale/run-scale.sh {normal\|heavy}` | `scale/scale_it.py` + `profiles.py` + `schemas.py` | normal: 5 DBs / 20 tables / 50 000 rows / 4 workers; heavy: 10 DBs / 50 tables / 300 000 rows / 8 workers | `checksum_sql()` triples `(count, sum(id), crc32(col))` per table, eventual re-verify, then `verify_update_markers()` |
| ES E2E / load / scale | `scripts/run-es-*.sh` | `harness/es_it.py`, `es_load_it.py`, `scale/es_scale_it.py` | same generators | independent oracle `es_verify.py` + `es_client.py` (no driver from the sink) |

Reusable as-is (no new framework):

- `harness/config.py` — one place for host/port/credentials, suite DBs, server
  ids, checkpoint dirs, pipeline names, `${VAR}` fixture rendering, report path
  rewriting. Extended with the `PG_*` block.
- `harness/logutil.py`, `harness/sysstat.py` — logging and CPU/RSS sampling.
- `harness/load_it.py` — table DDL, baseline seeding, `peak_writer()`,
  `wait_peak_catchup()` (60 s × 4 writers with `LOAD_PEAK_*`).
- `scale/profiles.py`, `scale/schemas.py` — profile topology, row allocation,
  templates, `checksum_sql()`.
- `scripts/_log.sh` helpers: `absolutize_go_cdc`, `absolutize_checkpoint_dirs`,
  `resolve_python_bin`, `KEEP_IT_*`, teardown ownership under CI.

Gaps that PR3 must fill (they do not exist for any non-MySQL target either):

- No snapshot→binlog switchover test that *proves* the DML happened while the
  snapshot was still running (`run_it` writes only after `wait_alive`).
- No sink fault injection (transaction/commit/connection failures).
- No cross-database route E2E: `fixtures/pipeline.yaml.tpl` routes inside one
  database only, so `db1.t` → `db2.t` isolation is untested.

## 2. PostgreSQL suite layout

One adapter layer per suite, mirroring the Elasticsearch/Doris pattern: a thin
client, an independent oracle, and an orchestrator that reuses the MySQL-side
generators.

| File | Role | Reuses |
|---|---|---|
| `harness/pg_client.py` | `psycopg2` connection/DSN, `exec_sql()`, `fetch_all()`, target ping, schema cleanup | `config.PG_*` |
| `harness/pg_verify.py` | canonicalization + primary-key-set/row diff; **stdlib only** so it is unit-testable without a database | — |
| `harness/pg_it.py` | functional E2E, `TP-P01…TP-P20`, report `mysql-postgresql-func.md` | `run_it.start_job/stop_job/kill_job_hard`, `load_it.ensure_schema` |
| `harness/test_pg_verify.py` | offline `unittest` for the oracle | — |
| `harness/pg_load_it.py` | load E2E, `TP-PL01…TP-PL04`, report `mysql-postgresql-load.md` | `load_it` seeding + `peak_writer` |
| `harness/test_pg_load.py` | offline `unittest` for the load adapter | — |
| `harness/pg_compat.py` | PG 14–18 matrix, report `mysql-postgresql-compat.md` + one per-version E2E report | `harness.pg_it`, `scripts/pg-up.sh` |
| `harness/test_pg_compat.py` | offline `unittest` for the matrix planning/statuses | — |
| `harness/pg_bench_it.py` | PG-210 benchmark, report `mysql-postgresql-benchmark.md` | `sysstat.py`, `harness.config` |
| `harness/test_pg_bench.py` | offline `unittest`: matrix, planner, metrics, report honesty | — |
| `scale/pg_schemas.py` | PG-safe copy of `schemas.py` (see §4) | `schemas.py` |
| `scale/test_pg_schemas.py` | offline `unittest` for the PG templates | — |
| `scale/pg_scale_it.py` | scale normal/heavy, report `mysql-postgresql-scale-{profile}.md` | `profiles.py`, `pg_schemas.py` |
| `scale/test_pg_scale_pipeline.py` | offline `unittest` for the scale pipeline rendering | — |
| `scripts/pg-up.sh`, `pg-down.sh`, `wait-for-postgres.sh` | compose lifecycle for the `postgres` service | `_log.sh` |
| `scripts/run-pg-e2e.sh`, `run-pg-load.sh`, `run-pg-scale.sh`, `run-pg-compat.sh`, `run-pg-bench.sh` | entrypoints | `run-e2e.sh` conventions |

MySQL stays the only source; the target is one PostgreSQL database (`cdc_it`)
because `sink.database` is required and single-valued. Isolation is by schema:
`sink.schema` for a whole pipeline, or the route `db` segment per source DB.

| Suite | Checkpoints | `server-id` | Report |
|---|---|---|---|
| E2E | `workdir/ckpts-pg`, `workdir/ckpts-pg-restart` | 5625 / 5626 | `mysql-postgresql-func.md` |
| Load | `workdir/ckpts-pg-load` | 5627 | `mysql-postgresql-load.md` |
| Scale | `workdir/ckpts-pg-scale/<profile>` (profile is part of every target schema too) | 5628 / 5629 (heavy) | `mysql-postgresql-scale-<profile>.md` |
| Compatibility | `workdir/ckpts-pg-compat-<major>` | 5625 / 5626 | `mysql-postgresql-compat.md`, `mysql-postgresql-func-pg<major>.md` |
| Benchmark | `workdir/ckpts-pg-bench` | 5630 | `mysql-postgresql-benchmark.md` |

## 3. Test points

### 3.1 Functional E2E — `harness/pg_it.py`

Each case recreates its source tables and drops the routed target table, then
starts a fresh `go-cdc` on a fresh checkpoint; one pipeline runs at a time.

| ID | Scenario | Key assertions |
|---|---|---|
| TP-P01 | Snapshot, 2 source DBs: same table name in both, an empty table, NULL and default values | PK sets equal per database; every supported column canonically equal; the empty table is created with 0 rows; column list matches the mapped type; target schema/table as routed |
| TP-P02 | Snapshot→binlog switchover: DML issued while the target still holds only part of the snapshot | the overlap is *proven* (`0 < target rows < baseline`) before the DML runs; after catch-up every row canonically equal |
| TP-P03 | Binlog INSERT/UPDATE/DELETE incl. consecutive updates on one PK, delete→insert, insert→delete | one target row per source row; final values equal; no resurrection |
| TP-P04 | Primary-key change (business key) | old key absent, new key present, row count unchanged |
| TP-P05 | Projection + filter: fall-in, fall-out, stay-out | fall-in creates, fall-out deletes (filter miss retracts), stay-out never appears |
| TP-P06 | DELETE carried only by the before image, **composite two-column key** | exactly one row removed; siblings with the same `id` and the other `id` survive |
| TP-P07 | `add.column` / `modify.column` / `rename.column`, then DML immediately | target column list + mapped type; post-DDL rows equal |
| TP-P08 | DDL refused by default: `drop.column` without the gate, `drop.table`, `truncate`, `rename table` | documented failure surface (task/event error) and no silent data loss |
| TP-P09 | `schema.change.drop-truncate.enabled: true` | DDL applied, pipeline continues, later DML equal |
| TP-P10 | Route: source DB → schema, source table → different target table, same-name tables in 2 DBs, mixed-case identifiers | full isolation; quoted/mixed-case identifier preserved |
| TP-P11 | Graceful stop after a checkpoint, resume | no row lost or duplicated; final state equal |
| TP-P12 | `SIGKILL` then resume (at-least-once replay) | final state equal; duplicates allowed only transiently |
| TP-P13 | Resume with an un-replayed DDL between checkpoint and restart (PR2 M3) | recorded limitation reproduced; behaviour reported, not "fixed" implicitly |
| TP-P14 | SQL-layer fault injection: `pg_terminate_backend` (57P01), cancellation of the statement the sink has blocked behind an `ACCESS EXCLUSIVE` lock (57014), a sink role whose right to log in is removed (`ALTER ROLE … NOLOGIN` → 28000) | 57P01 recovers or is retried; 57014 and 28000 stop the job with a diagnosable error; after restoring the role the re-run converges |
| TP-P15 | Replay and ordering: replay a captured window; four writers on overlapping keys | replay reproduces the same final state; ordering claims limited to what binlog order provides |
| TP-P16 | Type round-trip over snapshot and binlog, plus the refused types | canonical value per type on both paths; a refused type fails the table |
| TP-P17 | PostgreSQL restart while the pipeline is writing (`PG_RESTART_ENABLED=1` only) | `SKIPPED` when the switch is off, which is the default |
| TP-P18 | Network interruption between pipeline and target | `SKIPPED` — recorded with the reason that nothing in this repository can cut the network path; a SQL permission error is not a network fault |
| TP-P19 | Permanent target error: `ON CONFLICT` without a unique constraint (`42P10`) | the job exits instead of retrying, and the checkpoint does not advance |
| TP-P20 | Keyless source table | plain `INSERT` (no conflict target); a replay inserts a second row — the documented limitation, reported rather than "fixed" |

### 3.2 Load — `harness/pg_load_it.py`

Same generator, 4 writers × 60 s (`LOAD_PEAK_*`), all writes on the MySQL
source. The writers are **Python threads inserting into MySQL**; `go-cdc` only
reads the binlog, and nothing is ever written to PostgreSQL by the test. The
pipeline's own concurrency is `LOAD_PARALLELISM`, which `run-pg-load.sh`
defaults to `LOAD_PEAK_WORKERS` (4).

| ID | Scenario | Key assertions |
|---|---|---|
| TP-PL01 | Baseline multi-table sync (t1–t4 + typed columns, 50 000 rows each) | planned row count + column canonical values, chunked and diff-locating |
| TP-PL02 | Peak 4×60 s mixed DML | writes all on MySQL; every writer wrote rows, none failed; both tables catch up within budget; job still up; checkpoint holds a resume offset |
| TP-PL03 | Typed columns under load (decimal / float / temporal / JSON / bool) | live PostgreSQL types equal the documented mapping; baseline and peak rows all equal |
| TP-PL04 | Mega-field sync (LONGBLOB + LONGTEXT, 1–5 MiB) | byte/char length + SHA-256 equal on both servers; no truncated payload; JPEG SOI prefix kept |

The catch-up budget is `PG_LOAD_CATCHUP_BUDGET_S` = 3600 s. The PR3 spec's
historical design value was 1800 s; that number is neither enforced nor printed,
so read the reported timings rather than the budget.

### 3.3 Scale — `scale/pg_scale_it.py`

Profiles and topology come from `scale/profiles.py`, unchanged:

| Profile | DBs | Tables | Total rows | Workers | Peak stage |
|---|---|---|---|---|---|
| normal | 5 | 20 | 50 000 | 4 | none (mutations only) |
| heavy | 10 | 50 | 300 000 | 8 | 50 000 rows/hotspot burst, 8×45 s, catch-up budget 3 600 s |

Reconciliation is two-stage, and the first stage is only a gate:

1. a cheap aggregate both engines compute the same way —
   `(COUNT(*), SUM(id), a character-length sum)` per table — decides when to start
   comparing;
2. the acceptance is a chunked **row-level** walk through `harness.pg_verify`:
   primary keys (window `PG_SCALE_VERIFY_CHUNK`, default 20 000 ids), every
   projected column, NULLs, binary values, timestamps, JSON, and duplicate keys on
   the target. A mismatch names the table and the first differing ids.

A count-only or aggregate-only result is never reported as a pass. One budget
(`PEAK_CATCHUP_BUDGET_S = 3600`) spans hotspot convergence plus the aggregate and
row-level verification that follows it, so the phases never re-time
independently; the value lives in `scale/pg_scale_it.py` and mirrors
`scale/scale_it.py`'s.

Isolation is per profile, by construction: source databases are
`db_tp_pg_scale_src_<nn>`, target schemas are `tp_pg_scale_<profile>_<nn>`, and
only schemas matching that pattern are dropped between runs. `public` and the
other profile's schemas are out of reach of the cleanup.

### 3.4 Compatibility and benchmark

**PG-209 — version matrix.** The same functional core (connect, snapshot, DML,
batch/txn, auto-create, supported DDL, type conversion, checkpoint resume,
replay) runs against 14/15/16/17/18 through `scripts/run-pg-compat.sh`. Nothing is
copied per version: the per-version inputs are the image (`postgres:<major>`, or
`PG_COMPAT_IMAGE_<major>` to pin a minor), the `PG_EXPECT_MAJOR` guard, and the
report/checkpoint paths. Execution is serial inside one compose project, because
the matrix owns the `postgres` service and `scripts/pg-up.sh` recreates it when
the image changes (an initialized data directory is version-bound). A version
that could not run is `ENV BLOCKED`; the report is `COMPLETE` only when all five
rows really ran and passed.

**PG-210 — benchmark.** `harness/pg_bench_it.py` sweeps `write-batch-size`
100/1000/5000 for an INSERT/UPSERT-only and an UPDATE/DELETE-mixed workload (6
cells). Each cell: seed + snapshot (not measured), a measured write window all on
the MySQL source, then stop writing, wait for catch-up and compare every row and
column. It records throughput (batch channel), P50/P95 latency plus the maximum on
a separate single-row probe channel (MySQL commit → row visible in PostgreSQL),
host CPU and `go-cdc` RSS from `/proc`, connections from `pg_stat_activity`
excluding the sampler, failure rate, and the reproducibility block from spec
§9.3 (platform, CPU/memory, Go/pgx/Python versions, PostgreSQL image and `SHOW`
settings, dataset, key/index, transaction boundary, concurrency, duration, seed
and command).
Retries are printed as `n/a`: the sink emits no retry event, and a fabricated `0`
would be worse than an honest gap.

The benchmark is a performance report, not an acceptance test. It owns its own
source database, schema, checkpoint directory, pipeline name and server id, so no
number can be mistaken for an E2E/Load/Scale result, and it asserts no throughput
target.

## 4. Deliberate template deviations

The sink refuses `ENUM`, `SET`, `BIT`, `TIME` and the spatial types
(`docs/connectors/postgresql-sink.md`, "Column Type Mapping"). Two existing
generators create exactly those columns, so the PostgreSQL suites derive their
own templates the way `scale/doris_schemas.py` already does:

| Template | Column | Action | Reason |
|---|---|---|---|
| `scale/schemas.py` `wide` | `c_time TIME` | dropped in `scale/pg_schemas.py` | `TIME` is refused by the sink (MySQL time spans negative and over-24-hour values) |
| `harness/load_it.py` `tpl01_typed` | `c_enum ENUM`, `c_bit BIT(8)` | dropped by `harness/pg_load_it.py` after seeding | same |

Rules: the deviation is asserted (seeding fails if the column is still there, and
`create_table_sql` fails when the column it expects to drop is missing), printed
in the run log, and listed in every report's "deviations" section. No column is
dropped silently, and no other template is modified.

## 5. Canonicalization used by the oracle

`pg_verify.py` compares values, never raw driver representations:

| MySQL | PostgreSQL | Rule |
|---|---|---|
| `TINYINT`/`SMALLINT`/`INT` (`INTEGER`/`BIGINT`) | int | integer compare |
| `BIGINT UNSIGNED` → `NUMERIC(20,0)` | `Decimal` | integer compare after `int()` |
| `DECIMAL(p,s)` → `NUMERIC(p,s)` | `Decimal` | numeric compare (`NUMERIC(12,4)` `1.5000` == `1.5`) |
| `FLOAT`/`DOUBLE` | `REAL`/`DOUBLE PRECISION` | relative tolerance `FLOAT_REL_TOL = 1e-12`: `abs(a-b) <= 1e-12 * max(1, |a|, |b|)` |
| `CHAR`/`VARCHAR`/`TEXT` | `CHAR`/`VARCHAR`/`TEXT` | UTF-8 bytes of the string |
| `BINARY`/`BLOB` | `BYTEA` | `bytes` compare (no hex on the MySQL side) |
| `DATE`/`DATETIME` | `DATE`/`TIMESTAMP` | naive wall clock, same values the source sent |
| `TIMESTAMP` | `TIMESTAMP` | sink writes the value the source decoded; compared as wall clock in the pipeline's `server-time-zone` |
| `JSON` | `JSON` | canonical JSON (`sort_keys=True`, separators without spaces) |
| `NULL` | `NULL` | `None`, never `"NULL"` |

Float tolerance is deliberately tight: both sides store IEEE values produced from
the same source text, so `1e-6` would hide a re-rendering defect rather than
absorb float noise.

Primary-key sets are compared as sets, then every differing key's row is
compared field by field; the report prints `src`, `sink` and the column name.

## 6. Fault injection (PG-206)

| Scenario | Mechanism | Default |
|---|---|---|
| transaction begin / mid-transaction / pre-COMMIT failure | `pg_terminate_backend()` on the sink connection → 57P01 on the next statement | on |
| statement cancelled | `LOCK TABLE … ACCESS EXCLUSIVE` on the target table, then `pg_cancel_backend()` on the blocked sink statement → 57014 | on |
| connect refused | `ALTER ROLE … NOLOGIN` for a non-superuser sink role → 28000, restored afterwards. (Not `REVOKE CONNECT`: that yields 42501 during the connection attempt and would test a different failure surface.) | on |
| constraint / permanent error | `ON CONFLICT` without a matching unique constraint → 42P10 | on |
| PostgreSQL service restart | `PG_RESTART_ENABLED=1` + `docker compose restart postgres` (or `PG_RESTART_CMD`) | off |
| network interruption | **not covered**: no injector exists in this repo, and a SQL permission error is not a network fault. Recorded as `SKIPPED` rather than faked | — |
| deadlock (`40P01`) between two sink sessions | **not covered**: the sink writes on one connection (`MaxConns=1`), so two of its own transactions cannot deadlock. The retry classification is proven by the Go unit test (`retryable` table in `sink_test.go`), not by an E2E injector | — |
| connect timeout before a session exists | **partially covered**: TP-P14(c) removes `LOGIN` and the job exits, which exercises the connect-failure path; a timeout that expires during authentication is not injected separately | — |

Every injection targets the disposable integration stack only, is applied by
the harness (never by production code), and is undone in a `finally` block
(the lock is released, the role's `LOGIN` is restored, the next connection is
re-checked) before the case is scored.

Evidence boundary of TP-P14 (recorded, not smoothed over):

- (a) proves that a terminated backend is survivable and that the target
  reconciles afterwards. It does **not** prove the retry budget was consumed,
  because the case passes whether the sink retried or the runner restarted the
  job — the log only has to show that recovery happened.
- (b) asserts `57014` in the log and that the job exits, so "not retried" is
  evidence, not an assumption.
- (c) asserts that the job stops with a diagnosable `ERROR` line. The concrete
  SQLSTATE `28000` is not matched against the log; the mapping from
  `NOLOGIN` to `28000` is documented in the connector guide and is not re-proven
  here.

## 7. Known limits recorded by these suites

1. **PR2 M3 schema recovery** — a resume from an existing checkpoint does not
   re-send captured schemas (`cmd/go-cdc/main.go` re-emits only for Doris), and
   `connector/source/mysql/resume.go` reads the *current* schema. TP-P13 pins the
   observed behaviour; PR3 does not change the recovery architecture.
2. **Ordering** — the source is a single ordered binlog and a checkpoint only
   rewinds it, so "committed DELETE then stale UPDATE" cannot be produced by the
   MySQL source. TP-P15 asserts the achievable property (replay reproduces the
   same final state) and records the rest as unsupported rather than testing a
   fabricated stream.
3. **Exactly-once** — not claimed anywhere. Delivery is at-least-once; targets
   are observed with `INSERT … ON CONFLICT` semantics.
4. **Checkpoint evidence boundary** — TP-P11/TP-P12/TP-P15 prove resume and replay
   by *stopping the process*. The window "COMMIT succeeded, checkpoint not yet
   written" is not isolated by an injector; it is covered only indirectly by the
   replay cases. A green run says nothing about that exact interleaving.
5. **One compose stack per run** — every suite expects its own MySQL and
   PostgreSQL; the PostgreSQL and MySQL suites must not share a stack with a
   concurrent run of another suite, because they would contend for the same
   default ports (13306 / 15432) and the same `workdir`.
6. **Single-version default** — the ordinary E2E run talks to one pinned server
   (16.4). Only the compatibility matrix says anything about PG 14–18, and only
   when its report is `COMPLETE`.
7. **Assertion strength** — a target table with a primary key cannot hold
   duplicate keys, so "`count(*)` equals the number of distinct keys" proves
   nothing on its own. TP-P11/TP-P12 (resume) and TP-P15 (replay) therefore
   compare the target row count with the **source** row count and rely on the
   full-field reconcile for the resurrection check. The weaker form was removed
   from all three; it is not kept as padding.
8. **Column defaults and schema names** — TP-P07's `ADD COLUMN` carries no
   `DEFAULT`, so default conversion is covered by the Go unit tests
   (`connector/sink/postgresql/schema_test.go`), not by E2E. TP-P07/TP-P09 do run
   in `tp_pg_a`, which is not `public`, so DDL outside the default schema is
   covered; a **name collision** between two schemas holding the same table name
   is not constructed.

## 8. Traceability matrix

Every row is currently `NOT RUN` for integration. `ENV BLOCKED` means the local
checkout cannot run it at all (no Docker daemon); it is not a pass. Offline
columns say what has actually been executed.

| Requirement | Suite | Scenario | Source Test | PostgreSQL Test | Command | Profile | Assertion | Status | Evidence |
|---|---|---|---|---|---|---|---|---|---|
| PG-201 | E2E | Multi-DB snapshot, same table names, empty/NULL/default | `run_it` TP-01, `types_it` TP-D01 | TP-P01, TP-P16 | `scripts/run-pg-e2e.sh` | — | PK sets + canonical field values + schema/table mapping; no `COUNT(*)`-only check | NOT RUN | none yet |
| PG-202 | E2E | CRUD, consecutive updates, delete→insert, insert→delete, NULL/default, PK change | `run_it` TP-02/TP-03/TP-06 | TP-P03, TP-P04, TP-P06 (composite key is TP-P06 only) | `scripts/run-pg-e2e.sh` | — | final row-set equality per case | NOT RUN | none yet |
| PG-203 | E2E | DML during snapshot (overlap proven before the writes) | `load_it` TP-L02 (partial) | TP-P02 | `scripts/run-pg-e2e.sh` | — | `0 < target rows < baseline` proven, then full catch-up equality | NOT RUN | none yet |
| PG-204 | E2E | Allowed DDL + immediate DML; default-off DDL; unsupported DDL | `run_it` TP-07/TP-09/TP-15…TP-20 | TP-P07, TP-P08, TP-P09 | `scripts/run-pg-e2e.sh` | — | target DDL state + post-DDL rows; refused DDL surfaces as error, not silent loss | NOT RUN | none yet |
| PG-205 | E2E | Graceful / hard restart, replay, PR2 M3 limitation, scale restart phase | `run_it` TP-13, TP-14 | TP-P11, TP-P12, TP-P13 | `scripts/run-pg-e2e.sh`, `scripts/run-pg-scale.sh` | — | final state equal after resume; M3 behaviour documented; commit-after-checkpoint not isolated (§7) | NOT RUN | none yet |
| PG-206 | E2E | Txn failure, cancelled statement, NOLOGIN 28000, 42P10; optional server restart | none | TP-P14, TP-P17 (`SKIPPED` by default), TP-P18 (`SKIPPED`, no injector), TP-P19 | `scripts/run-pg-e2e.sh` (`PG_RESTART_ENABLED=1` for TP-P17) | — | diagnosable error + recovery to equal state; injections cleaned up; network case recorded `SKIPPED` | NOT RUN | none yet |
| PG-207 | E2E | Replay duplicates, multi-writer same PK, ordering limits | `run_it` TP-03, TP-14 | TP-P15, TP-P05 | `scripts/run-pg-e2e.sh` | — | replayed window reproduces the same final state; unsupported ordering recorded | NOT RUN | none yet |
| PG-208 | E2E | Cross-DB route, schema isolation, renamed target tables, identifier case | `run_it` TP-10/TP-11 (single DB only) | TP-P10, TP-P01 | `scripts/run-pg-e2e.sh` | — | isolation per schema/table; no cross-schema leakage | NOT RUN | none yet |
| PG-209 | Compatibility | Same core on 14/15/16/17/18 | E2E core | TP-P01…TP-P20 per version | `scripts/run-pg-compat.sh` | 5 versions, serial | per-version PASS/FAIL/SKIPPED/ENV BLOCKED + server/Go/pgx versions | NOT RUN (dry-run report only) | `mysql-postgresql-compat.md` = 5 × NOT RUN |
| PG-210 | Benchmark | INSERT/UPSERT and UPDATE/DELETE at batch 100/1000/5000 | — | `harness/pg_bench_it.py` | `scripts/run-pg-bench.sh` | 6 cells | throughput, P50/P95, CPU/RSS, connections, failure rate, `n/a` retries + §9.3 metadata | OFFLINE PASS / NOT RUN | `test_pg_bench.py`, dry-run report |
| PG-211 | Regression | MySQL sink + shared pipeline still green | `run_it`, `types_it`, `load_it`, `scale_it`, Go unit tests | — | `make build && make test`, `scripts/run-e2e.sh`, `scale/run-scale.sh normal` | — | unchanged suites pass | OFFLINE PASS (Go unit tests) / NOT RUN (IT) | CI not re-run here |
| PG-212 | Docs | Full config, defaults, YAML example, security options | — | — | — | — | `docs/connectors/postgresql-sink.md` | IMPLEMENTED | connector doc |
| PG-213 | Docs | Types, DDL matrix, recovery, version compatibility, limits, SQLSTATE | — | — | — | — | connector doc + this file | IMPLEMENTED | connector doc, this file |
| PG-214 | CI | E2E/Load/Scale/Compat/Benchmark wired into CI, layered | `integration.yml` | — | `.github/workflows/postgresql-integration.yml` | normal / heavy / matrix | PR jobs run the light suites; heavy, matrix and benchmark are dispatch- or schedule-only; exit codes propagate | IMPLEMENTED (YAML checked offline) / NOT RUN (jobs) | §12 |

## 9. Status vocabulary

| Status | Meaning |
|---|---|
| `PASS` | The suite ran in this environment and every assertion held; a log or report path is attached |
| `FAIL` | Ran, at least one assertion failed |
| `NOT RUN` | Not executed (yet); never a pass |
| `SKIPPED` | Deliberately not executed for this run (opt-in switch off, profile not selected) |
| `ENV BLOCKED` | Cannot run here (no Docker daemon, missing dependency); reason recorded |

Harness exit codes: `0` = PASS (or `SKIPPED` only), `1` = FAIL, `2` = nothing
usable ran (`ENV BLOCKED` / `NOT RUN`).

## 10. Commands

```bash
# one-time: MySQL + PostgreSQL
bash test/integration/scripts/mysql-up.sh
bash test/integration/scripts/pg-up.sh

# functional E2E (TP-P01..TP-P20); a subset re-runs a single failure
bash test/integration/scripts/run-pg-e2e.sh
bash test/integration/scripts/run-pg-e2e.sh TP-P05 TP-P14
PG_RESTART_ENABLED=1 bash test/integration/scripts/run-pg-e2e.sh TP-P17

# load (4 writers × 60 s, all writes on MySQL)
bash test/integration/scripts/run-pg-load.sh

# scale (`heavy` needs both the argument and PG_SCALE_HEAVY=1)
bash test/integration/scripts/run-pg-scale.sh normal
PG_SCALE_HEAVY=1 bash test/integration/scripts/run-pg-scale.sh heavy

# compatibility matrix / benchmark
bash test/integration/scripts/run-pg-compat.sh              # 14 15 16 17 18
bash test/integration/scripts/run-pg-bench.sh --list        # the 6 cells, nothing runs
bash test/integration/scripts/run-pg-bench.sh --dry-run     # report with 6 × NOT RUN
bash test/integration/scripts/run-pg-bench.sh

# offline (no database, no container)
cd test/integration \
  && python3 -m unittest harness.test_pg_bench harness.test_pg_compat \
       harness.test_pg_load harness.test_pg_verify \
  && python3 -m pytest -q scale/
```

Reports land in `docs/test/reports/mysql-postgresql-*.md` (not tracked; see
`docs/test/reports/.gitignore`).

## 11. Environment of this checkout

`docker` is not available and the Python environment has no server to talk to, so
every integration row above stays `NOT RUN` / `ENV BLOCKED` until the suites are
executed on a machine with the compose stack (or in CI). What *has* run here:

- the pure-Python oracle and template checks (`harness.test_pg_verify`,
  `harness.test_pg_load`, `harness.test_pg_compat`, `harness.test_pg_bench`,
  `scale/test_pg_schemas.py`, `scale/test_pg_scale_pipeline.py`);
- `harness.pg_bench_it --list` / `--dry-run`, which render the matrix and write a
  report with every cell `NOT RUN` and no measured number;
- `harness.pg_compat --list` / `--dry-run`, which likewise writes five `NOT RUN`
  rows.

Those are reported as `OFFLINE PASS`; they are not integration evidence.

## 12. CI

`.github/workflows/postgresql-integration.yml` carries every PostgreSQL job; the
MySQL, Doris and Elasticsearch suites keep their own workflows.

| Job | Trigger | What runs |
|---|---|---|
| `decide` | every event | skips push work when an open PR already covers the head |
| `postgres-sink-dml` | push / PR / dispatch / schedule | Go DML integration suite (`-tags postgres_integration`), one pinned PostgreSQL 16 service |
| `postgresql-offline` | push / PR / dispatch / schedule | harness unit tests, `pg_bench_it --dry-run` honesty check, `pg_compat --list` matrix check — no database |
| `postgresql-e2e` | push / PR / dispatch (not schedule) | `run-pg-e2e.sh`, TP-P01…TP-P20 on PostgreSQL 16.4 |
| `postgresql-load` | `workflow_dispatch` + `run_load` | `run-pg-load.sh` |
| `postgresql-scale` | `workflow_dispatch` + `scale_profile` = `normal`/`heavy` | `run-pg-scale.sh <profile>`; `heavy` also exports `PG_SCALE_HEAVY=1` |
| `postgresql-compat` | weekly `schedule` (Mon 05:00 UTC) or dispatch + `run_compat` | `run-pg-compat.sh`, PG 14–18 |
| `postgresql-benchmark` | `workflow_dispatch` + `run_benchmark` | `run-pg-bench.sh` |

Layering rules the workflow enforces:

- A pull request runs `postgres-sink-dml`, `postgresql-offline` and
  `postgresql-e2e` only. Load, scale, matrix and benchmark are unreachable from
  `pull_request`.
- `heavy` scale needs the dispatch choice **and** `PG_SCALE_HEAVY=1` inside the
  script, so it cannot start by accident.
- Every job owns a compose project (`gocdc-pg-e2e`, `gocdc-pg-load`,
  `gocdc-pg-scale`, `gocdc-pg-compat`, `gocdc-pg-bench`) and therefore its own
  containers, volumes, checkpoint directories and report files.
- Each harness step maps the exit code to an annotation (`0` notice, `2` **ENV
  BLOCKED** error, anything else **FAIL** error) and then exits with the same
  status: an unusable environment is a red job.
- Logs are redacted (`password=…` → `***`) before being printed or archived, and
  the pipeline YAML — which holds the sink password — is never uploaded.
- No job sets `PG_RESTART_ENABLED`, so TP-P17 (server restart) is `SKIPPED` in CI
  and can only be produced by hand: `PG_RESTART_ENABLED=1 bash
  test/integration/scripts/run-pg-e2e.sh TP-P17` on a machine with the compose
  stack. The gap is recorded rather than covered by a green job.
