# go-cdc integration tests (Docker Compose)

Local runs and GitHub Actions both start a binlog-enabled MySQL via **Docker Compose**, then run one or more suites against it. Every sink has exactly one entry point:

```bash
bash tests/integration/scripts/run.sh <sink> [profile] [variant] [suites] [test-point...]
```

| Sink | Suites (every sink runs its full list in **both** tiers; `heavy` only raises the writer count and the timeouts) |
| --- | --- |
| `mysql` | `func` TP-01..TP-13 (`sinks/mysql/harness/run_it.py`) + `types` TP-D01..D04 + `load` TP-L01..L04 + `scale` — all four in both tiers, on 5.7 and 8.0 alike |
| `stdout` | `func` — the shared CDC JSON envelope (`sinks/stdout/harness/stdout_it.py`) |
| `kafka` | `func` TP-K01..TP-K08 (`sinks/kafka/harness/kafka_it.py`); the load/peak points live inside the suite |
| `doris` | `go` sink suite + `func` TP-DR01..DR07 + `scale` — all three in both tiers |
| `elasticsearch` | `go` sink suite + `func` + `load` + `scale` — all four in **both** tiers on es8; es7/es9 stay `go`, `func` (a variant-dimension decision, not a tier cut) |
| `postgresql` | `go` sink suite + `func` TP-P01..P20 + `load` + `bench` + `scale` + `compat` — all six in both tiers; `compat` attests only the matrix row's own PG major, so `heavy` covers pg14..pg18 by widening the variant matrix into five parallel rows while `normal` pins pg16 |

`profile` decides *how much* runs, `variant` decides *what it runs against*
(`57`/`80`, `pg14`..`pg18`, `es7`/`es8`/`es9`, a Doris `2.1.x` tag). Neither is
inferred from the environment. The variant table lives in exactly one place,
`logger.sh: it_apply_variant`, and is applied twice because a child process cannot
export back to its parent: `run.sh` calls it in its own process (the suites read
`ES_URL`, `ES_SINK_VERSION` and `PG_EXPECT_MAJOR` themselves) and `setup.sh`
calls it to pick the images (`MYSQL_IMAGE`, `PG_IMAGE`, `DORIS_VERSION`).

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

Chinese: [README.zh.md](./README.zh.md)

## Naming (anonymous accounts + test-point tables)

| Kind | Pattern | Examples |
|------|---------|----------|
| Source / sink users | Anonymous fixed tokens | `u_a7k9m2x4` / `u_b3j6c8y1` |
| Databases | `db_tp_{suite}_{src\|snk}` | `db_tp_func_src`, `db_tp_load_snk` |
| Functional tables | `tpNN_*` | `tp01_rows`, `tp07_fail`, `tp10_add` |
| Types tables | `tpdNN_*` | `tpd01_types` |
| Load tables | `tplNN_*` | `tpl01_t1`, `tpl01_typed`, `tpl01_large` |
| Sink tables | source name + `_out` | `tp01_rows_out` |

Default credentials: `sources/mysql/init/01-databases.sql` (overridable via env).

## Layout

```text
tests/integration/
├── conftest.py              # puts this directory on sys.path (one pytest tree)
├── sources/mysql/           # the CDC source: compose file, init SQL, conf.d
├── sinks/<sink>/
│   ├── docker-compose.yml
│   ├── fixtures/            # pipeline templates and/or seed SQL (as needed; ES/PG seed inline in Python)
│   ├── harness/             # *_it.py entry points and their helpers
│   ├── scale/               # only where the sink has a scale layer
│   ├── tests/               # pytest unit tests of that harness
│   └── go/                  # only doris: the sink-level Go suite
├── common/                  # config, logging, cluster profiles, shared scale
├── scripts/                 # setup.sh · wait.sh · teardown.sh · run.sh · logger.sh
└── workdir/                 # binary, logs, checkpoints (gitignored)
```

The compose files are stacked (`-f sources/mysql/… -f sinks/<sink>/…`) into one
project per sink (`gocdc-<sink>-mysql`, named after what the project contains),
so `setup.sh <sink>` brings up exactly the source plus the sink under test —
nothing else.

## Quick start

```bash
bash tests/integration/scripts/run.sh mysql             # full suite list, MySQL 8.0
bash tests/integration/scripts/run.sh mysql heavy       # same suites, larger writers + timeouts
bash tests/integration/scripts/run.sh mysql normal 57   # same suites on 5.7
bash tests/integration/scripts/run.sh postgresql normal pg14
ls docs/test/reports/
```

`setup.sh` starts the stack **and** blocks until it accepts work — the probes
live in `wait.sh`, which `setup.sh` sources and which you can also run on its own
(`bash scripts/wait.sh <sink>`) against a stack that is already up. `run.sh` then
builds `go-cdc`, seeds, dispatches the suites and tears the stack down — but
**only locally**. In CI the three are separate steps (`Setup stack` → `Run
suites` with `IT_SKIP_SETUP=1` → `Teardown`), so a stack failure is attributed to
its own step, and the teardown is the last `if: always()` step after the report
upload, so an artifact can never race a `docker compose down`. Escape hatches:
`IT_KEEP=1` keeps the containers, `IT_REUSE=1` reuses the volumes,
`IT_SKIP_SETUP=1` skips the stack entirely.

## Scale suite

`scale` runs in **both** tiers for every sink now - the tiers differ only in the
writer count and the timeouts. Ask for it alone with the 4th positional
(`IT_SUITES` is the env spelling of the same thing):

```bash
bash tests/integration/scripts/run.sh mysql heavy            # func,types,load,scale
bash tests/integration/scripts/run.sh mysql normal "" scale  # scale only
IT_SUITES=scale bash tests/integration/scripts/run.sh mysql normal
```

Scale DBs (`db_tp_scale_src_NN` / `db_tp_scale_snk_NN`) are created at runtime by `scale_it.py` using compose root (`ROOT_USER` / `ROOT_PASSWORD`), then granted to the same anonymous source/sink users as other suites.

### Doris variant

Same profiles, same table/row allocation, Doris as the sink:

```bash
bash tests/integration/scripts/run.sh doris            # go + func + scale
bash tests/integration/scripts/run.sh doris heavy      # same suites, larger writers + timeouts
make it-doris PROFILE=heavy
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
| `LOAD_ROWS_PER_TABLE` | `100000` (both tiers) | Rows per normal table (t1–t4 + typed) |
| `LOAD_LARGE_ROWS` | `50` | Rows in mega-field table `tpl01_large` |
| `LOAD_LARGE_FIELD_BYTES` | `2097152` (2MiB, clamped 1–5MiB) | Per-field size: image `LONGBLOB` + `LONGTEXT` |
| `LOAD_PEAK_SECONDS` | `60` (both tiers) | Peak write duration |
| `LOAD_PEAK_WORKERS` | follows the tier: `4` normal / `8` heavy | Peak writers; **seed** uses `workers × 2` (capped at 16 and by table count) |
| `LOAD_PARALLELISM` | same as `LOAD_PEAK_WORKERS` | CDC `pipeline.parallelism` for the load suite |
| `LOAD_PEAK_BATCH` | `200` | Rows per INSERT batch |
| `LOAD_PROFILE` | follows the run tier (`normal` / `heavy`) | Tier label (also printed in the load report) |

Load cases: TP-L01 multi-table snapshot (t1–t4 + typed + large), TP-L02 peak (t1 + typed), TP-L03 typed spot-check, TP-L04 large-field LENGTH check.

The scale side has its own two-tier timeout: `PEAK_CATCHUP_BUDGET_S`
(`1800` / `5400`) and `SCALE_TOTAL_TIMEOUT` (`2400` / `7200`).

Seed volume, peak duration and the hotspot burst are the same in both tiers on
purpose. They are the shape of the measurement, so a smaller `normal` value would
describe a different experiment, not a cheaper one: only the peak writer count and
the two timeouts follow the profile.

## GitHub Actions

[`ci.yml`](../../.github/workflows/ci.yml) is the only entry point:

```text
decide  ->  plan  ->  go-check  ->  ci-<sink>.yml (one per affected sink)  ->  result
```

| Job | What it does |
| --- | --- |
| `decide` | drops a push that an open PR already covers, and is the **only** place the tier is computed (dispatch input, `ci-heavy` on the PR, default-branch push) |
| `plan` | `tools/ci/plan.py`: git diff -> `{"include": [{"sink": …, "variant": …}]}` (a docs-only change yields `{"include": []}`) |
| `go-check` | `gofmt`, `golangci-lint`, `make test`, build, then `pytest tests/integration tools/ci/test_plan.py` and `bash -n` over every script |
| `sink-<sink>` | a reusable `ci-<sink>.yml`, fed only the rows that sink owns |
| `result` | one required check; `skipped` and an empty matrix both count as success |

Tiers:

| Tier | Trigger | What runs |
| --- | --- | --- |
| `normal` | PR opened / synchronized / reopened, and a push to any other branch | the full suite list of every sink on every supported variant (MySQL 5.7 + 8.0 `func,types,load,scale`; PG 16 `go,func,load,bench,scale,compat`; Doris `go,func,scale`; ES 8 `go,func,load,scale`; ES 7/9 `go,func`; Kafka, stdout `func`) |
| `heavy` | `ci-heavy` already on the PR (push after adding it) or `gh workflow run ci.yml -f profile=heavy`; a push to `master`/`main` | the identical suite list; the peak writer count (4 -> 8) and the scale timeouts (catch-up budget and total timeout) grow, and the PostgreSQL variant matrix widens from pg16 to pg14..pg18 so the compat majors run as five parallel rows |

Every push and every PR is a trigger; `push` has no branch filter. When a push is
already covered by an open PR, `decide` sets `run=false` and the push run does no
work (`result` still reports success) - the pull_request run is the one that tests
that commit. A failed PR lookup fails open: the push runs and prints a
`::warning::`.

`labeled` is not a trigger: a label event shares the PR's concurrency group, so
adding *any* label would cancel the run in flight and report the skipped
replacement as success. Add `ci-heavy` and push, and every run reads it back.

Repo hygiene: [`repo-checks.yml`](../../.github/workflows/repo-checks.yml) → `CI / Repo Checks`, file size and license headers on every push/PR.

Which sinks run is decided by the diff, not by a path filter: a change under
`tests/integration/sinks/kafka/`, `connector/sink/kafka/`, `configs/demo-kafka.yaml`
or `.github/workflows/ci-kafka.yml` runs only Kafka, a change to the CDC core, the
IT plumbing, `ci.yml` or the build files runs everything, and a docs-only change
runs nothing (the `result` job still passes). `release.yml` and `repo-checks.yml`
are standalone workflows and never pull the matrix in.

```bash
python3 tools/ci/plan.py --paths connector/sink/kafka/sink.go   # {"include":[...]}
make it-plan PATHS="runtime/checkpoint/store.go"                 # same, via make
```

Reports are uploaded as `reports-<sink>-<variant>-<profile>` (`docs/test/reports/**`)
on every `heavy` row and on any failure; a sink without variants uses `default`
for the middle segment (`reports-kafka-default-heavy`).

## Kafka sink suite

```bash
bash tests/integration/scripts/run.sh kafka             # func (snapshot + load + peak)
bash tests/integration/scripts/run.sh kafka heavy       # same suite, larger knobs
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

CI runs this in the `sink-kafka` job, with the same `LOAD_ROWS_PER_TABLE` / peak knobs as the mysql→mysql load suite.

## Doris sink suite

Requires Linux, Docker Compose, host networking, a mysql client, and kernel
limits for FE/BE:

```bash
sudo sysctl -w fs.inotify.max_user_watches=1048576
sudo sysctl -w fs.inotify.max_user_instances=8192
sudo sysctl -w vm.max_map_count=2000000
```

`setup.sh doris` refuses to start when these are too low (passwordless sudo
raises them automatically in CI). The Go sink suite is skipped unless
`DORIS_HOST` is set, so a plain `go test ./...` never starts Doris.

```bash
bash tests/integration/scripts/run.sh doris            # go + func + scale
bash tests/integration/scripts/run.sh doris heavy      # same suites, larger writers + timeouts
# report: docs/test/reports/mysql-doris.md
# Go sink log: tests/integration/workdir/doris-integration.log
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

The `go` suite of the same run then runs the sink-level Go tests (`doris_integration`):
auto-create / UNIQUE KEY, same-batch duplicate keys, empty Schema cache restart,
extra columns, type rejection / round-trips, Label + `SHOW LOAD` (a duplicate
label must never add a second row, filter behaviour, retry with a finished
label; only the row-per-label subtest reports `SKIP` when the build records no
Stream Load job at all, the rest of that probe still runs), `DESC` nullability and
empty-string default parsing, `ADD COLUMN` visibility with a partially
reconciled target and restart re-validation, multi-table first-failure. Forced
`Publish Timeout` / `CANCELLED` / 5xx injection stays with unit/`httptest`
coverage, and so does a DDL that reports an error after taking effect.

The `sink-doris` job ([`ci-doris.yml`](../../.github/workflows/ci-doris.yml)) starts
MySQL + Doris `2.1.8` FE+BE, raises the kernel limits, runs the suites
(TP-DR01..DR04, the sink Go suite, then the Doris scale profile - both tiers run
the same three suites) and uploads the Markdown reports plus `cdc-doris*.log`.

## Elasticsearch 8 sink suites

The ES8 sink has the same four layers as the MySQL sink. They share the MySQL
compose service, the source generators (`sinks/mysql/harness/load_it.py`,
`common/scale_it.py`) and the knob names; only the target side is new
(`sinks/elasticsearch/harness/es_client.py` for HTTP, `sinks/elasticsearch/harness/es_verify.py` for the oracle).

```bash
bash tests/integration/scripts/run.sh elasticsearch                   # es8: go + func + load + scale
bash tests/integration/scripts/run.sh elasticsearch heavy             # es8: same suites, larger writers + timeouts
bash tests/integration/scripts/run.sh elasticsearch normal es7        # v7 adapter: go + func
bash tests/integration/scripts/run.sh elasticsearch normal es9        # v9 adapter: go + func
bash tests/integration/scripts/run.sh elasticsearch normal es8 go     # Go IT only
grep -A2 'TP-E' docs/test/reports/mysql-elasticsearch.md
```

Equivalent Make targets: `make it-es`, `make it-es7`, `make it-es9` —
`make it-es PROFILE=heavy` is the stress profile, `SUITES=go,func` narrows a run.

| Layer | Harness | Report |
| --- | --- | --- |
| Integration | `go test -tags esintegration ./connector/sink/elasticsearch/` (real ES8) | `go test` output |
| E2E | `sinks/elasticsearch/harness/es_it.py` (TP-E01..) | `docs/test/reports/mysql-elasticsearch.md` |
| Load | `sinks/elasticsearch/harness/es_load_it.py` (TP-EL01..EL05) | `docs/test/reports/mysql-elasticsearch-load.md` |
| Scale | `sinks/elasticsearch/scale/es_scale_it.py` (normal / heavy) | `docs/test/reports/mysql-elasticsearch-scale-{profile}.md` |

Isolation and knobs:

- Every suite owns an index namespace, used both for naming and cleanup:
  `ES_INDEX_PREFIX` (default `db_tp_es_snk`), `ES_LOAD_INDEX_PREFIX` (default
  `db_tp_es_load_snk`), `ES_SCALE_INDEX_PREFIX` (default `db_tp_scale_snk_*`).
  `action.destructive_requires_name` is on, and cleanup deletes index by index.
- `ES_URL` (default `http://127.0.0.1:19200`), `ES_USERNAME` / `ES_PASSWORD`
  (empty for the throwaway node), `ES_REQUEST_TIMEOUT` (seconds).
- Load batch sweep: `ES_LOAD_BATCH_SIZE` / `ES_LOAD_BATCH_INTERVAL` (default
  `1000` / `500ms`) for TP-EL02 and `ES_LOAD_SMALL_BATCH` /
  `ES_LOAD_SMALL_INTERVAL` (default `100` / `100ms`) for the TP-EL03 restart.
  `LOAD_*` knobs are shared with the MySQL load suite.
- Scale reuses `SCALE_TOTAL_ROWS` / `SCALE_WORKERS` / `SCALE_TOTAL_TIMEOUT`
  (default `1800` / `5400` per tier) and `ES_SCALE_RESTART_ROWS` (default `100`).
- `tpl01_typed.c_bit` is dropped after seeding: sink v1 rejects `BIT` columns.
- `write-batch-size` / `write-batch-interval` are runner-level knobs: one bulk
  per `Sink.Write`, flushed on size, on interval, and on any split / position /
  watermark / captured message (`composer/runner.go`). Snapshot chunks and binlog
  events share that path, so both are exercised by the sweep.

## Elasticsearch 7 sink suites (v7 client adapter)

The ES7 Integration layer is the same test binary as the ES8 Integration layer,
run against a second cluster with `sink.version: 7`. It reuses the ES8 harness
(`es_integration_test.go`) and only adds ES7-specific coverage (adapter wiring,
typeless mapping, schema re-check, close/error paths). It needs no MySQL.

The ES7 E2E layer is the ES8 E2E harness (`sinks/elasticsearch/harness/es_it.py`, TP-E01..TP-E10)
re-run with `sink.version=7` and `ES_EXPECT_MAJOR=7`: real MySQL source,
snapshot + binlog, the real `go-cdc` binary, and a real 7.17 node. It needs
MySQL, so the ES7 node alone is not enough for that layer.

```bash
# Integration + E2E (MySQL + ES7; the ES8 node and Kafka are not started)
bash tests/integration/scripts/run.sh elasticsearch normal es7        # go + func
bash tests/integration/scripts/teardown.sh elasticsearch es7
# report: docs/test/reports/mysql-elasticsearch7.md
```

Make target: `make it-es7` (`SUITES=go` or `SUITES=func` narrows a run).

| Layer | Harness | Report |
| --- | --- | --- |
| Integration | `go test -tags esintegration -run TestElasticsearch7 ...` (real ES7) | `go test` output |
| E2E | `sinks/elasticsearch/harness/es_it.py` with `sink.version=7` (TP-E01..) | `docs/test/reports/mysql-elasticsearch7.md` |

Knobs: `ES7_URL` (default `http://127.0.0.1:19201`), `ES7_IMAGE`, `ES7_HEAP`,
`ES7_HOST_PORT`, plus the ES8 E2E knobs (`ES_SRC_DB`, `ES_SNK_DB`, `ES_URL`) and
`ES_SINK_VERSION` / `ES_EXPECT_MAJOR` for the harness. The ES8 runner filters
`-run 'TestES'`, so the ES7 tests never run against an ES8 node and vice versa;
the E2E harness fails fast when the cluster major version does not match
`ES_EXPECT_MAJOR`, or when a fixture stops pinning `sink.version`. Verified
server: Elasticsearch 7.17.x; 7.0–7.16 are not verified (the Go 7.17 client
requires the `X-Elastic-Product` header, available from 7.14).

## Elasticsearch 9 sink suites (v9 client adapter)

Mirror of the ES7 suites for the v9 adapter. The Integration layer is the same
test binary again, run against a third cluster with `sink.version: 9`; it reuses
`es_integration_test.go` and adds ES9-specific coverage (`es9_integration_test.go`:
adapter wiring, typeless mapping, schema re-check, close/error paths). It needs
no MySQL. The E2E layer is `sinks/elasticsearch/harness/es_it.py` (TP-E01..TP-E10) re-run with
`sink.version=9` and `ES_EXPECT_MAJOR=9`, so it needs MySQL as well.

```bash
# Integration + E2E (MySQL + ES9; the ES8/ES7 nodes and Kafka are not started)
bash tests/integration/scripts/run.sh elasticsearch normal es9        # go + func
bash tests/integration/scripts/teardown.sh elasticsearch es9
# report: docs/test/reports/mysql-elasticsearch9.md
```

Make target: `make it-es9`.

| Layer | Harness | Report |
| --- | --- | --- |
| Integration | `go test -tags esintegration -run TestElasticsearch9 ...` (real ES9) | `go test` output |
| E2E | `sinks/elasticsearch/harness/es_it.py` with `sink.version=9` (TP-E01..) | `docs/test/reports/mysql-elasticsearch9.md` |

Knobs: `ES9_URL` (default `http://127.0.0.1:19202`), `ES9_IMAGE`, `ES9_HEAP`,
`ES9_HOST_PORT`, plus the shared E2E knobs (`ES_SRC_DB`, `ES_SNK_DB`, `ES_URL`)
and `ES_SINK_VERSION` / `ES_EXPECT_MAJOR`. The ES9 node sits behind the `es9`
compose profile, so the default service list and the ES7/ES8 jobs never start it.
Targeted server: Elasticsearch 9.5.x (CI job `sink-elasticsearch`, `es9` row);
the adapter unit tests use `httptest` and need no cluster.
