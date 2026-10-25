# go-cdc integration tests (Docker Compose)

Local runs and GitHub Actions both start a binlog-enabled MySQL via **Docker Compose**, then run:

1. **Functional** TP-01..TP-13 (`harness/run_it.py`)
2. **Types / timezone** TP-D01..TP-D04 (`harness/types_it.py`)
3. **Load / peak** TP-L01..TP-L04 (`harness/load_it.py`, tier A)
4. **Scale** multi-DB/table E2E (`scale/scale_it.py`, profiles `normal` / `heavy`) — run separately via [`scale/run-scale.sh`](scale/run-scale.sh); creates scale DBs at runtime as MySQL root (see [`scale/README.md`](scale/README.md))
5. **Kafka sink** TP-K01..TP-K08 (`harness/kafka_it.py`) — run via [`scripts/run-kafka-e2e.sh`](scripts/run-kafka-e2e.sh) after MySQL + Kafka are up
6. **Doris sink** MySQL→Doris pipeline TP-DR01..DR07 (functional + load parity, `harness/doris_it.py`) plus sink-level Go suite (`doris/`, tag `doris_integration`) — run via [`scripts/run-doris-e2e.sh`](scripts/run-doris-e2e.sh) after MySQL + Doris are up
7. **Doris scale** multi-DB / multi-table stress against Doris (`scale/doris_scale_it.py`, profiles `normal` / `heavy`) — run via [`scripts/run-doris-scale.sh`](scripts/run-doris-scale.sh)

Generated Markdown reports (English) land in `docs/test/reports/`:

| Suite | Report |
|-------|--------|
| Functional | `docs/test/reports/mysql-mysql-func.md` |
| Types | `docs/test/reports/mysql-mysql-types.md` |
| Load | `docs/test/reports/mysql-mysql-load.md` |
| Scale | `docs/test/reports/mysql-mysql-scale-{normal\|heavy}.md` |
| Kafka | `docs/test/reports/mysql-kafka.md` |
| Doris | `docs/test/reports/mysql-doris.md` (functional + load phase; Go suite log in `workdir/doris-integration.log`) |
| Doris scale | `docs/test/reports/mysql-doris-scale-{normal\|heavy}.md` |

Static notes: [`docs/test/TYPES.md`](../../docs/test/TYPES.md) (not a run report).

中文版：[README.zh.md](./README.zh.md)

## Naming (anonymous accounts + test-point tables)

| Kind | Pattern | Examples |
|------|---------|----------|
| Source / sink users | Anonymous fixed tokens | `u_a7k9m2x4` / `u_b3j6c8y1` |
| Databases | `db_tp_{suite}_{src\|snk}` | `db_tp_func_src`, `db_tp_load_snk` |
| Functional tables | `tpNN_*` | `tp01_rows`, `tp07_fail`, `tp10_add` |
| Types tables | `tpdNN_*` | `tpd01_types` |
| Load tables | `tplNN_*` | `tpl01_t1`, `tpl01_typed`, `tpl01_large` |
| Sink tables | source name + `_out` | `tp01_rows_out` |

Default credentials: `mysql/init/01-databases.sql` (overridable via env).

## Layout

```text
test/integration/
├── docker-compose.yml
├── docker-compose.doris.yml
├── doris/                    # sink-level Go suite (doris_integration tag)
├── mysql/init/01-databases.sql
├── fixtures/                 # includes pipeline.doris.yaml.tpl, seed_doris.sql
├── harness/                  # run_it / kafka_it / doris_it / …
├── scale/
├── scripts/
└── workdir/
```

## Quick start

```bash
bash test/integration/scripts/mysql-up.sh
bash test/integration/scripts/run-e2e.sh
ls docs/test/reports/
```

`run-e2e.sh` / `run-scale.sh` remove compose MySQL on exit **only when run locally** (`KEEP_IT_MYSQL=1` to keep it). In CI they do **not** tear down on harness exit — cleanup is solely the workflow **Tear down** step, which is last (`if: always()`), after report upload, so artifacts are never blocked by early `mysql-down`.

## Scale suite

After MySQL is up, run a profile (does not run functional/types/load):

```bash
bash test/integration/scale/run-scale.sh normal
# or
make integration-scale-heavy
```

Scale DBs (`db_tp_scale_src_NN` / `db_tp_scale_snk_NN`) are created at runtime by `scale_it.py` using compose root (`ROOT_USER` / `ROOT_PASSWORD`), then granted to the same anonymous source/sink users as other suites.

### Doris variant

Same profiles, same table/row allocation, Doris as the sink:

```bash
bash test/integration/scripts/mysql-up.sh
bash test/integration/scripts/doris-up.sh
bash test/integration/scripts/run-doris-scale.sh normal
# or: bash test/integration/scripts/run-doris-scale.sh heavy
# or: make integration-doris-scale-normal / integration-doris-scale-heavy
```

It deliberately does **not** share databases with the MySQL→MySQL scale suite
(`db_tp_dscale_src_NN` / `db_tp_dscale_snk_NN`), so both can run in parallel
without wiping each other's tables. Three deviations are forced by Doris v1
semantics, and the report states each of them:

| Deviation | Why |
| --- | --- |
| Tables whose event group is `delete` are remapped to `insert_update` | the Doris sink rejects DELETE and `delete.enabled: true` fails at parse |
| `wide` drops `c_time TIME`; `large` uses `payload LONGTEXT` instead of `binary_data BLOB` | `types.go` has no v1 mapping for TIME or binary types |
| Sink DBs come from `auto-create-table` | the sink issues `CREATE DATABASE IF NOT EXISTS` itself |

The content checksum is probed at startup: MySQL and Doris must return the same
`CRC32` for a fixed string, otherwise the suite degrades to a character-length
sum (`CHAR_LENGTH` on MySQL, `LENGTH` on Doris) and marks the report as degraded
instead of silently comparing unlike aggregates.

## Load suite knobs (tier A)

| Variable | Default | Meaning |
|----------|---------|---------|
| `LOAD_ROWS_PER_TABLE` | `50000` (local default); CI: `30000` normal / `100000` full | Rows per normal table (t1–t4 + typed) |
| `LOAD_LARGE_ROWS` | `50` | Rows in mega-field table `tpl01_large` |
| `LOAD_LARGE_FIELD_BYTES` | `2097152` (2MiB, clamped 1–5MiB) | Per-field size: image `LONGBLOB` + `LONGTEXT` |
| `LOAD_PEAK_SECONDS` | `60` (local + CI MySQL 5.7 / 8.0) | Peak write duration |
| `LOAD_PEAK_WORKERS` | `4` (local + CI both versions) | Peak writers; **seed** uses `workers × 2` (capped at 16 and by table count) |
| `LOAD_PARALLELISM` | same as `LOAD_PEAK_WORKERS` | CDC `pipeline.parallelism` for the load suite |
| `LOAD_PEAK_BATCH` | `200` | Rows per INSERT batch |
| `LOAD_PROFILE` | `local` via `run-e2e.sh`; CI sets `full` / `normal` | Tier label (also printed in the load report) |

Load cases: TP-L01 multi-table snapshot (t1–t4 + typed + large), TP-L02 peak (t1 + typed), TP-L03 typed spot-check, TP-L04 large-field LENGTH check.

## GitHub Actions

[`.github/workflows/integration.yml`](../../.github/workflows/integration.yml) (workflow display name **`CI`**): `push` / `pull_request` when CDC source, modules, or `test/integration/**` change (or this workflow); plus `workflow_dispatch`. Any `*.md` and demo `configs/**` are excluded. Check names look like `CI / Integration / … (push)`.

Repo hygiene: [`repo-checks.yml`](../../.github/workflows/repo-checks.yml) → `CI / Repo / File size`, `CI / Repo / License` on every push/PR.

Jobs:

| Job | When / matrix | What |
| --- | --- | --- |
| `go-check` | always | `gofmt`, `golangci-lint`, `make test`, build, **scale unit tests** (`pytest scale/`) |
| `mysql-mysql-e2e` | MySQL **5.7** + **8.0** | functional + types + load; **8.0 also runs Kafka sink** TP-K01..K08 |
| `mysql-scale-e2e` | MySQL **8.0 only** | scale `normal` or `heavy` (separate compose project / host port `13307`) |
| `doris-e2e` | MySQL **8.0** + Doris **2.1.8** | MySQL→Doris pipeline TP-DR01..DR07 (functional + load) + sink Go suite + Doris scale ([`doris-integration.yml`](../../.github/workflows/doris-integration.yml)) |

**Load tier** (`LOAD_ROWS_PER_TABLE`):

- PR / push `main` / `master` / `release-*` → **`100000`** (`LOAD_PROFILE=full`)
- other branch push → **`30000`** (`LOAD_PROFILE=normal`)
- Peak: both 5.7 and 8.0 → `LOAD_PEAK_SECONDS=60`, workers `4`
- Doris `doris-e2e` uses the same tiers (5 bulk tables), and `workflow_dispatch`
  chooses `normal` / `full` explicitly

**Scale profile**:

- PR / push `main` / `master` / `release-*` → **`heavy`** (10 DB / 50 tables / ~300k rows / workers 8) including hotspot burst + peak
- other branch push → **`normal`** (5 DB / 20 tables / ~50k rows / workers 4)
- `workflow_dispatch` → choose `normal` or `heavy` (default `heavy`)
- `mysql-scale-e2e` job timeout: **120 minutes**

**Doris scale profile** (inside `doris-e2e`):

- PR / branch push → **`normal`** (5 DB / 20 tables / ~50k rows / workers 4)
- `workflow_dispatch` with `load_profile=full` → **`heavy`** (10 DB / 50 tables / ~300k rows / workers 8) including hotspot burst + peak
- `doris-e2e` job timeout: **150 minutes**

Artifacts: `go-cdc-it-report-mysql-{5.7|8.0}` and `go-cdc-scale-report-mysql-8.0` (Markdown under `docs/test/reports/`).
CI uploads **only** on push to `main` / `master` / `release-*` (not PRs or other branches). Before each upload it deletes prior artifacts with the same name so only the latest set remains; GitHub still expires leftovers after the repo default (~90 days).

```bash
MYSQL_IMAGE=mysql:5.7.42 MYSQL_TAG=57 bash test/integration/scripts/mysql-up.sh
bash test/integration/scripts/run-e2e.sh
MYSQL_TAG=57 bash test/integration/scripts/mysql-down.sh
```

## Kafka sink suite

```bash
bash test/integration/scripts/mysql-up.sh
bash test/integration/scripts/kafka-up.sh
bash test/integration/scripts/run-kafka-e2e.sh
# report: docs/test/reports/mysql-kafka.md
```

| TP | Coverage |
| --- | --- |
| K01 | snapshot: route + default topic + projection + `city='hz'` filter + `before=null` |
| K02 | incremental `c`/`u`/`d` + message keys; filtered `city=sh` insert absent |
| K03 | DDL emit off (no `ddl`) and on (`ddl` on default topic) |
| K04 | restart keeping checkpoint; new row + prior row still readable (ALO dups OK) |
| K05 | load snapshot volume (`tpl01_t1` / typed / large) via Kafka end offsets ≥99% src |
| K06 | peak catch-up (same `LOAD_PEAK_*` knobs as mysql→mysql load) |
| K07 | typed column JSON spot-check |
| K08 | mega LONGBLOB/LONGTEXT (JPEG magic + length) |

CI runs this on the **MySQL 8.0** matrix job after mysql→mysql e2e, with the same `LOAD_ROWS_PER_TABLE` / peak knobs.

## Doris sink suite

Requires Linux, Docker Compose, host networking, a mysql client, and kernel
limits for FE/BE:

```bash
sudo sysctl -w fs.inotify.max_user_watches=1048576
sudo sysctl -w fs.inotify.max_user_instances=8192
sudo sysctl -w vm.max_map_count=2000000
```

`doris-up.sh` refuses to start when these are too low (passwordless sudo raises
them automatically in CI). The Go sink suite is skipped unless `DORIS_HOST` is
set, so normal `go test ./...` never starts Doris.

```bash
# make integration-doris-up  # mysql-up + doris-up
bash test/integration/scripts/mysql-up.sh
bash test/integration/scripts/doris-up.sh
bash test/integration/scripts/run-doris-e2e.sh
# report: docs/test/reports/mysql-doris.md
# Go sink log: test/integration/workdir/doris-integration.log
```

| TP | Coverage |
| --- | --- |
| DR01 | snapshot: route + projection + `city='hz'` filter → Doris UNIQUE KEY rows |
| DR02 | incremental insert + update; filtered `city=sh` insert absent (no DELETE; v1 rejects it) |
| DR03 | `tpds01_evolve` (no projection): source `ADD COLUMN` → Doris DESC + row with new col |
| DR04 | restart keeping checkpoint; post-restart upsert converges (ALO) |
| DR05 | bulk multi-table snapshot: `tpdl01_t1`–`t4` + `typed`, `LOAD_ROWS_PER_TABLE` rows each |
| DR06 | peak catch-up: `LOAD_PEAK_*` concurrent INSERT storm; Doris counts within 1% |
| DR07 | typed spot-check under bulk load (int/bigint/decimal/varchar/date/datetime/json/bool) |

The load phase (DR05–DR07) reuses the same `LOAD_*` knobs and CI tiering as the
mysql→mysql / kafka load suites. Doris v1 rejects BLOB/ENUM/BIT/SET/TIME, so the
bulk tables live in `db_tp_load_src.tpdl01_*` with a mappable-only schema; the
mega-field `tpl01_large` case has no Doris equivalent.

The same `run-doris-e2e.sh` then runs the sink-level Go suite (`doris_integration`):
auto-create / UNIQUE KEY, same-batch duplicate keys, empty Schema cache restart,
extra columns, type rejection / round-trips, Label + `SHOW LOAD` (a duplicate
label must never add a second row, filter behaviour, retry with a finished
label; only the row-per-label subtest reports `SKIP` when the build records no
Stream Load job at all, the rest of that probe still runs), `DESC` nullability and
empty-string default parsing, `ADD COLUMN` visibility with a partially
reconciled target and restart re-validation, multi-table first-failure. Forced
`Publish Timeout` / `CANCELLED` / 5xx injection stays with unit/`httptest`
coverage, and so does a DDL that reports an error after taking effect.

The separate [`doris-integration.yml`](../../.github/workflows/doris-integration.yml)
workflow starts MySQL + Doris `2.1.8` FE+BE, raises kernel limits, runs the suite
(functional TP-DR01..DR04, then load TP-DR05..DR07, then the Doris scale profile),
and uploads the Markdown reports plus `cdc-doris*.log` and compose logs.
