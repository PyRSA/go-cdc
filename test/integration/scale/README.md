# Scale integration suite

Parameterized multi-DB / multi-table CDC E2E (`normal` / `heavy`). Reuses the same Docker Compose MySQL as functional, types, and load suites.

## Prerequisites

MySQL must already be up (same as other IT suites):

```bash
bash test/integration/scripts/mysql-up.sh
```

## Run

From repo root:

```bash
bash test/integration/scale/run-scale.sh normal
bash test/integration/scale/run-scale.sh heavy
```

Or via Makefile:

```bash
make integration-scale-normal
make integration-scale-heavy
```

Profile can also be selected with `SCALE_PROFILE=heavy bash test/integration/scale/run-scale.sh`.

## Bootstrap and privileges

Scale databases (`db_tp_scale_src_NN`, `db_tp_scale_snk_NN`) are **not** listed in `mysql/init/01-databases.sql`. The orchestrator (`scale_it.py`) creates them at runtime using the compose **root** account (defaults: `ROOT_USER=root`, `ROOT_PASSWORD=rootpass`), then `GRANT`s the usual anonymous IT users:

- Source: `u_a7k9m2x4` (override with `SRC_USER` / `SRC_PASSWORD`)
- Sink: `u_b3j6c8y1` (override with `SINK_USER` / `SINK_PASSWORD`)

Functional / types / load databases are unchanged.

## Profiles

| Profile | Source DBs | Tables | Default rows | workers |
| --- | ---: | ---: | ---: | ---: |
| `normal` | 5 | 20 | ~50,000 | 4 |
| `heavy` | 10 | 50 | 300,000 | 8 |

Optional overrides (env or CLI flags on `scale_it`):

| Variable | Meaning |
| --- | --- |
| `SCALE_TOTAL_ROWS` | Override row budget |
| `SCALE_WORKERS` | Override seed + pipeline parallelism |

Dry topology (no MySQL):

```bash
cd test/integration && python3 -m scale.scale_it --print-plan normal
```

## Report

Markdown report: `docs/test/reports/mysql-mysql-scale-{normal|heavy}.md`

Artifacts under `test/integration/workdir/`: `pipeline.scale-{profile}.yaml`, `ckpts-scale-{profile}/`, `cdc-scale-{profile}.log`.

## Teardown

```bash
bash test/integration/scripts/mysql-down.sh
```
