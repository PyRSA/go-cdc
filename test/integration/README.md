# go-cdc integration tests (Docker Compose)

Local runs and GitHub Actions both start a binlog-enabled MySQL via **Docker Compose**, then run:

1. **Functional** TP-01..TP-13 (`harness/run_it.py`)
2. **Types / timezone** TP-D01..TP-D04 (`harness/types_it.py`)
3. **Load / peak** TP-L01..TP-L04 (`harness/load_it.py`, tier A)
4. **Scale** multi-DB/table E2E (`scale/scale_it.py`, profiles `normal` / `heavy`) — run separately via [`scale/run-scale.sh`](scale/run-scale.sh); creates scale DBs at runtime as MySQL root (see [`scale/README.md`](scale/README.md))

Generated Markdown reports (English) land in `docs/test/reports/`:

| Suite | Report |
|-------|--------|
| Functional | `docs/test/reports/mysql-mysql-func.md` |
| Types | `docs/test/reports/mysql-mysql-types.md` |
| Load | `docs/test/reports/mysql-mysql-load.md` |
| Scale | `docs/test/reports/mysql-mysql-scale-{normal\|heavy}.md` |

Static notes: [`docs/test/TYPES.md`](../../docs/test/TYPES.md) (not a run report).

中文版：[README.zh.md](./README.zh.md)

## Naming (anonymous accounts + test-point tables)

| Kind | Pattern | Examples |
|------|---------|----------|
| Source / sink users | Anonymous fixed tokens | `u_a7k9m2x4` / `u_b3j6c8y1` |
| Databases | `db_tp_{suite}_{src\|snk}` | `db_tp_func_src`, `db_tp_load_snk` |
| Functional tables | `tpNN_*` | `tp01_rows`, `tp07_fail`, `tp09_add` |
| Types tables | `tpdNN_*` | `tpd01_types` |
| Load tables | `tplNN_*` | `tpl01_t1`, `tpl01_typed`, `tpl01_large` |
| Sink tables | source name + `_out` | `tp01_rows_out` |

Default credentials: `mysql/init/01-databases.sql` (overridable via env).

## Layout

```text
test/integration/
├── docker-compose.yml
├── mysql/init/01-databases.sql
├── fixtures/
├── harness/
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

## Load suite knobs (tier A)

| Variable | Default | Meaning |
|----------|---------|---------|
| `LOAD_ROWS_PER_TABLE` | `50000` (local default); CI: `30000` normal / `100000` full | Rows per normal table (t1–t4 + typed) |
| `LOAD_LARGE_ROWS` | `50` | Rows in mega-field table `tpl01_large` |
| `LOAD_LARGE_FIELD_BYTES` | `2097152` (2MiB, clamped 1–5MiB) | Per-field size: image `LONGBLOB` + `LONGTEXT` |
| `LOAD_PEAK_SECONDS` | `90` (local + CI MySQL 5.7 / 8.0) | Peak write duration |
| `LOAD_PEAK_WORKERS` | `4` (local + CI both versions) | Peak writers; **seed** uses `workers × 2` (capped at 16 and by table count) |
| `LOAD_PARALLELISM` | same as `LOAD_PEAK_WORKERS` | CDC `pipeline.parallelism` for the load suite |
| `LOAD_PEAK_BATCH` | `200` | Rows per INSERT batch |
| `LOAD_PROFILE` | `local` via `run-e2e.sh`; CI sets `full` / `normal` | Tier label (also printed in the load report) |

Load cases: TP-L01 multi-table snapshot (t1–t4 + typed + large), TP-L02 peak (t1 + typed), TP-L03 typed spot-check, TP-L04 large-field LENGTH check.

## GitHub Actions

[`.github/workflows/integration.yml`](../../.github/workflows/integration.yml) (**Go CDC CI**): any-branch `push`, any `pull_request`, and `workflow_dispatch`.

Jobs:

| Job | When / matrix | What |
| --- | --- | --- |
| `go-check` | always | `gofmt`, `golangci-lint`, `make test`, build, **scale unit tests** (`pytest scale/test_*.py`) |
| `mysql-mysql-e2e` | MySQL **5.7** + **8.0** | functional + types + load |
| `mysql-scale-e2e` | MySQL **8.0 only** | scale `normal` or `heavy` (separate compose project / host port `13307`) |

**Load tier** (`LOAD_ROWS_PER_TABLE`):

- PR / push `main` / `master` / `release-*` → **`100000`** (`LOAD_PROFILE=full`)
- other branch push → **`30000`** (`LOAD_PROFILE=normal`)
- Peak: both 5.7 and 8.0 → `LOAD_PEAK_SECONDS=90`, workers `4`

**Scale profile**:

- PR / push `main` / `master` / `release-*` → **`heavy`** (10 DB / 50 tables / ~300k rows / workers 8) including hotspot burst + peak
- other branch push → **`normal`** (5 DB / 20 tables / ~50k rows / workers 4)
- `workflow_dispatch` → choose `normal` or `heavy` (default `heavy`)
- `mysql-scale-e2e` job timeout: **120 minutes**

Artifacts: `go-cdc-it-report-mysql-{5.7|8.0}` and `go-cdc-scale-report-mysql-8.0` (Markdown under `docs/test/reports/`).
CI uploads **only** on push to `main` / `master` / `release-*` (not PRs or other branches). Before each upload it deletes prior artifacts with the same name so only the latest set remains; GitHub still expires leftovers after the repo default (~90 days).

```bash
MYSQL_IMAGE=mysql:5.7.42 MYSQL_TAG=57 bash test/integration/scripts/mysql-up.sh
bash test/integration/scripts/run-e2e.sh
MYSQL_TAG=57 bash test/integration/scripts/mysql-down.sh
```