# Scale integration suite

Parameterized multi-DB / multi-table CDC E2E (`normal` / `heavy`). Reuses the same Docker Compose MySQL as functional, types, and load suites.

## Prerequisites

The scale suite is a suite of the MySQL run, so the one entry point raises the stack itself:

```bash
bash tests/integration/scripts/run.sh mysql heavy      # func,types,load,scale
bash tests/integration/scripts/run.sh mysql normal "" scale   # scale only
```

## Run

From repo root:

```bash
bash tests/integration/scripts/run.sh mysql normal
bash tests/integration/scripts/run.sh mysql heavy
```

Or via Makefile:

```bash
make it-mysql PROFILE=normal
make it-mysql PROFILE=heavy
```

Profile can also be selected with `bash tests/integration/scripts/run.sh mysql heavy`.

## Doris variant

Same profiles, same allocation, Doris as the sink (`sinks/doris/scale/doris_scale_it.py`). Requires Doris FE/BE up in addition to MySQL:

```bash
bash tests/integration/scripts/run.sh doris
bash tests/integration/scripts/run.sh doris normal   # make it-doris PROFILE=normal
```

It uses its own databases (`db_tp_dscale_src_NN` / `db_tp_dscale_snk_NN`) so the two scale suites never touch each other's tables. `delete` event groups are remapped to `insert_update` and the `wide` / `large` templates drop the types Doris v1 rejects; see the [Doris variant notes](../README.md#doris-variant) in the integration README.

## Bootstrap and privileges

Scale databases (`db_tp_scale_src_NN`, `db_tp_scale_snk_NN`) are **not** listed in `sources/mysql/init/01-databases.sql`. The orchestrator (`scale_it.py`) creates them at runtime using the compose **root** account (defaults: `ROOT_USER=root`, `ROOT_PASSWORD=rootpass`), then `GRANT`s the usual anonymous IT users:

- Source: `u_a7k9m2x4` (override with `SRC_USER` / `SRC_PASSWORD`)
- Sink: `u_b3j6c8y1` (override with `SINK_USER` / `SINK_PASSWORD`)

Functional / types / load databases are unchanged.

## Profiles

| Profile | Source DBs | Tables | Default rows | workers |
| --- | ---: | ---: | ---: | ---: |
| `normal` | 5 | 20 | ~50,000 | 4 |
| `heavy` | 10 | 50 | 300,000 | 8 |

Optional overrides (env or CLI flags on `scale_it`):

| Variable | Default | Meaning |
| --- | --- | --- |
| `SCALE_TOTAL_ROWS` | profile default | Override row budget |
| `SCALE_WORKERS` | profile default | Override seed + pipeline parallelism |
| `SCALE_BURST_ROWS_PER_HOT` | `50000` | **heavy only**: fixed INSERT rows per hotspot table after mutate |
| `SCALE_PEAK_SECONDS` | `45` | **heavy only**: timed peak write duration |
| `SCALE_PEAK_WORKERS` | same as `workers` | **heavy only**: peak writer threads |
| `SCALE_PEAK_BATCH` | `200` | **heavy only**: rows per peak INSERT batch |

**Heavy hotspot stress** (after mutate catch-up): one hotspot table per source DB (`min(ordinal)`), then burst → timed peak → hotspot count convergence (src/sink `COUNT(*)` must be exactly equal; the delta shown in logs is trend-only) → eventual-consistency checksum verify (`COUNT + SUM(id) + name_crc`, retried every 15s). Both phases share ONE deadline (`PEAK_CATCHUP_BUDGET_S`, default 3600 s; `run.sh` sets 1800 normal / 5400 heavy, and `SCALE_TOTAL_TIMEOUT` caps the whole suite) created when peak catch-up starts (no stacked timeouts); fetch/SQL errors and a dead go-cdc process fail fast instead of being retried. Normal profile skips this.

Dry topology (no MySQL):

```bash
cd tests/integration && python3 -m common.scale_it --print-plan normal
```

## Report

Markdown report: `docs/test/reports/mysql-mysql-scale-{normal|heavy}.md`

Artifacts under `tests/integration/workdir/`: `pipeline.scale-{profile}.yaml`, `ckpts-scale-{profile}/`, `cdc-scale-{profile}.log`.

## Teardown

`run.sh` removes the compose stack on exit **locally only**. In CI, cleanup is the reusable workflow's final **Teardown** step (after artifact upload). Locally set `IT_KEEP=1` to leave the stack up, then:

```bash
bash tests/integration/scripts/teardown.sh mysql
```
