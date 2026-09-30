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

| Variable | Default | Meaning |
| --- | --- | --- |
| `SCALE_TOTAL_ROWS` | profile default | Override row budget |
| `SCALE_WORKERS` | profile default | Override seed + pipeline parallelism |
| `SCALE_BURST_ROWS_PER_HOT` | `50000` | **heavy only**: fixed INSERT rows per hotspot table after mutate |
| `SCALE_PEAK_SECONDS` | `45` | **heavy only**: timed peak write duration |
| `SCALE_PEAK_WORKERS` | same as `workers` | **heavy only**: peak writer threads |
| `SCALE_PEAK_BATCH` | `200` | **heavy only**: rows per peak INSERT batch |

**Heavy hotspot stress** (after mutate catch-up): one hotspot table per source DB (`min(ordinal)`), then burst → timed peak → hotspot count convergence (≤1%, timeout 3600s) → full checksum verify. Normal profile skips this.

Dry topology (no MySQL):

```bash
cd test/integration && python3 -m scale.scale_it --print-plan normal
```

## Report

Markdown report: `docs/test/reports/mysql-mysql-scale-{normal|heavy}.md`

Artifacts under `test/integration/workdir/`: `pipeline.scale-{profile}.yaml`, `ckpts-scale-{profile}/`, `cdc-scale-{profile}.log`.

## Teardown

`run-scale.sh` removes compose MySQL on exit **locally only**. In CI, cleanup is the workflow's final **Tear down** step (after artifact upload). Locally set `KEEP_IT_MYSQL=1` to leave MySQL up, then:

```bash
bash test/integration/scripts/mysql-down.sh
```
