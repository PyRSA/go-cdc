# Doris Sink Connector

Writes the current state of routed rows into Apache Doris **2.1.x** using
**UNIQUE KEY** tables and **Stream Load JSON Lines**.

**YAML:** top-level `sink` with `type: doris`.

> **Verification status:** unit tests and an `httptest` mock FE pass. A real
> Doris 2.1.x environment has **not** been run for this implementation. All
> wire-level items in [Not verified](#not-verified) are **pending**, not PASS.
> Do not treat this connector as production-verified until they are checked
> against the target cluster.

## Version & Dependencies

| Item | Support |
| --- | --- |
| Target Doris | **2.1.x** (exact patch and FE/BE topology not verified) |
| FE HTTP endpoint | `hostname` + `port` (default `8030`), **plain HTTP only**; the host must not contain a scheme or path |
| FE SQL endpoint | `hostname` + `sql-port` (default `9030`, MySQL protocol) |
| SQL driver | `go-sql-driver/mysql` (already linked into `go-cdc`) |
| HTTP client | Go standard library |
| Packaging | Shipped inside the `go-cdc` binary |
| Table model | `UNIQUE KEY`; merge-on-write is left at the cluster default |

## Delivery semantics (at-least-once)

- `Write` sends one Stream Load per routed table and returns only after every
  table's **business result** is confirmed. `Flush` is therefore a no-op.
- Runner calls `Flush` before saving a checkpoint. A failed Stream Load fails
  the job and **does not** advance the checkpoint.
- Multi-table batches are grouped by routed table and sent in stable
  (lexicographic) order. The first failure stops later tables. The batch is
  **not atomic**: earlier tables may already be committed.
- After a crash where Doris committed a batch but the checkpoint was not saved,
  recovery re-sends the events with **new labels**. `UNIQUE KEY` makes replayed
  rows converge by key, but this is **at-least-once**, not exactly-once.
- Repeated primary keys inside one batch are coalesced to the **last** event
  before loading, because Stream Load does not guarantee intra-batch ordering.

## Example

```yaml
sink:
  type: doris
  hostname: 127.0.0.1
  port: 8030            # FE HTTP (Stream Load)
  sql-port: 9030        # FE MySQL protocol (DDL / metadata)
  username: root
  password: secret
  auto-create-table: true
  replication-num: 1    # omit to use the cluster default
  # buckets: auto       # default; or a positive integer
  # database: demo_sink
  # request-timeout: 30s
  # schema.change.enabled: false

route:
  - source-table: demo_db\.orders
    sink-table: demo_sink.orders
```

Full job: [`configs/demo-doris.yaml`](../../configs/demo-doris.yaml).

## Options

| Option | Required | Default | Type | Description |
| --- | --- | --- | --- | --- |
| `type` | yes | — | String | Must be `doris` |
| `hostname` | yes | — | String | FE host for both endpoints. Must not contain `://`, `/`, `?` or `#` |
| `port` | no | `8030` | Integer | FE HTTP port used by Stream Load (`1..65535`) |
| `sql-port` | no | `9030` | Integer | FE MySQL-protocol port used for `CREATE`, `DESC`, `ALTER`, `SHOW LOAD` (`1..65535`) |
| `username` | yes | — | String | Doris user |
| `password` | no | empty | String | Doris password; never written to logs or errors |
| `database` | no | empty | String | Fallback database when an event/table arrives without one. Routes normally provide the database |
| `auto-create-table` | no | `true` | Boolean | Create a missing database/table from the projected, routed schema, then always validate the live table |
| `write-batch-size` | no | `1000` | Integer | Shared runner option: flush the DML batch when it reaches this many events (`>= 1`) |
| `write-batch-interval` | no | `1s` | Duration | Shared runner option: time-based flush (`0` disables the timer) |
| `max-retries` | no | `3` | Integer | Extra attempts after the first (`>= 0`), so at most `max-retries + 1` Stream Load attempts per table. Backoff is 100ms × attempt, capped at 2s |
| `request-timeout` | no | `30s` | Duration | Doris-side Stream Load `timeout` (seconds) and SQL connect/read/write timeout (`> 0`). The HTTP client deadline is this plus 30s so Doris reports its own verdict first. Sub-second values round up in the `timeout` header, because a truncated `0` is rejected by Doris |
| `buckets` | no | `auto` | String | `auto` or a positive integer, emitted as `BUCKETS ...` |
| `replication-num` | no | `0` | Integer | `0` omits `PROPERTIES`; a positive value emits `"replication_num"` |
| `schema.change.enabled` | no | `false` | Boolean | Enables Doris `ADD COLUMN` handling; see [Schema evolution](#schema-evolution) |
| `schema.change.include` | no | `["add.column"]` | List | v1 accepts only `add.column`; any other entry is a config error |
| `schema.change.wait-timeout` | no | `5s` | Duration | How long an `ADD COLUMN` waits for the new column to appear in `DESC` (`> 0`). `DESC` is re-read every 500ms until the budget expires |
| `partial-update.enabled` | no | `false` | Boolean | `true` is rejected at parse time (not implemented in v1) |
| `delete.enabled` | no | `false` | Boolean | `true` is rejected at parse time (not implemented in v1) |
| `schema.change.drop-truncate.enabled` | no | `false` | Boolean | `true` is rejected for Doris in v1 |

`kafka`-only options (`brokers`, `topic`, …) and MySQL's `3306` default do not
apply here: the parser keeps the two Doris ports separate.

## Target table and auto-create

When `auto-create-table` is `true`, the sink:

1. Runs `CREATE DATABASE IF NOT EXISTS <db>`.
2. Runs `CREATE TABLE IF NOT EXISTS <db>.<table>` with:

```sql
CREATE TABLE IF NOT EXISTS `db`.`table` (
  `id` BIGINT NOT NULL,
  ...
) UNIQUE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS auto
```

- Key columns are moved to the front in source primary-key order; other columns
  keep their source order. Doris requires key columns to lead the definition.
- Key columns are always `NOT NULL`. Source nullable columns are defined
  `NULL`; non-nullable columns are `NOT NULL`.
- Source column comments are copied. Literal defaults from
  `information_schema` are copied (numeric, string, `DATE`/`DATETIME`); SQL
  expression defaults (`DEFAULT_GENERATED`, for example `CURRENT_TIMESTAMP`)
  are **omitted rather than guessed**. CDC writes every projected column.
- No partitions, indexes, engines, or merge-on-write properties are generated.
  `replication_num` is emitted only when configured.
- `BUCKETS auto` and the cluster default merge-on-write mode are **not
  verified** against a real cluster.

When `auto-create-table` is `false`, a missing database or table fails the job.
Whether the table was just created or already existed, the sink reads
`SHOW CREATE TABLE` plus `DESC` and validates the target before caching it as
writable:

- The target is a `UNIQUE KEY` table. `DESC` marks `UNIQUE`, `DUPLICATE` and
  `AGGREGATE` key columns with the same `Key=YES`, so the model is read from
  `SHOW CREATE TABLE`; a `DUPLICATE KEY` or `AGGREGATE KEY` target is rejected.
- The target key columns match the source primary key in membership **and
  order**.
- Every source column exists and has a **compatible** type. A source column the
  target does not have yet is accepted with a warning instead of failing: the
  `ADD COLUMN` that created it is still ahead in the binlog and is replayed
  before the first row that carries the column. `Write` still rejects any event
  that references a column missing from the target.
- A nullable source column is not written into a `NOT NULL` target column.
- Every source primary-key column exists and is a Doris key column.
- The target key set equals the source primary-key set (no extra or missing
  key columns).
- Extra target columns must be nullable, have a default, or be generated;
  otherwise the table is incompatible.
- A source table without a primary key is rejected.

If validation fails for any other reason, the table is not cached and the Runner
does not mark it captured. Auto-create is independent of schema evolution: it
never alters an existing table.

## Type mapping

MySQL `column_type` is mapped centrally; unknown or lossy types are rejected.

| MySQL | Doris |
| --- | --- |
| `tinyint` | `TINYINT` |
| `tinyint unsigned` | `SMALLINT` |
| `smallint` | `SMALLINT` |
| `smallint unsigned` | `INT` |
| `mediumint` (signed/unsigned) | `INT` |
| `int` / `integer` | `INT` |
| `int unsigned` | `BIGINT` |
| `bigint` | `BIGINT` |
| `bigint unsigned` | `LARGEINT` |
| `float` | `FLOAT` |
| `double` / `real` | `DOUBLE` |
| `decimal(p,s)` / `numeric(p,s)` | `DECIMAL(p,s)` (`1 <= p <= 38`, `0 <= s <= p`) |
| `char(n)` | `CHAR(n*3)`; above 255 bytes becomes `VARCHAR(n*3)` |
| `varchar(n)` | `VARCHAR(n*3)`; above 65533 bytes becomes `STRING` |
| `tinytext` / `text` / `mediumtext` / `longtext` | `STRING` |
| `date` | `DATE` |
| `datetime(p)` / `timestamp(p)` | `DATETIME(p)` (`0 <= p <= 6`) |
| `year` | `SMALLINT` |
| `json` | `JSONB` |

Rejected: `binary` / `varbinary` / all `*blob`, `enum`, `set`, `bit`, `time`,
spatial types, `decimal` without `(p,s)`, `p > 38`, and any unknown type. v1
never truncates or converts to an unrelated type.

MySQL counts `CHAR`/`VARCHAR` lengths in characters, Doris counts them in bytes.
Auto-created columns reserve three bytes per character (`n*3`) — the same
convention the Flink CDC Doris sink uses — so CJK text cannot fail a load on a
column that fits the source. The widen is capped by the Doris per-type limit:
`char(n)` above 255 bytes becomes `VARCHAR(n*3)`, and `varchar(n)` above 65533
bytes becomes `STRING`. Characters outside the Basic Multilingual Plane (emoji)
need four bytes and are rejected by the load rather than truncated.

For an existing target, widening is accepted; narrowing is not:

- Integer widening (`TINYINT` → `SMALLINT` → `INT` → `BIGINT` → `LARGEINT`).
- `FLOAT` → `DOUBLE`.
- `CHAR(n)` → `VARCHAR(m)` with `m >= 3n`; `CHAR`/`VARCHAR` → `STRING`. All
  lengths are Doris byte capacities, so a pre-created `VARCHAR(64)` target does
  **not** satisfy a `varchar(64)` source — that source needs `VARCHAR(192)`.
- `DECIMAL(p,s)` → `DECIMAL(p',s')` only when both fractional digits
  (`s' >= s`) and integer digits (`p'-s' >= p-s`) fit.
- `DATETIME(p)` → `DATETIME(p')` with `p' >= p`.
- `DATE` accepts `DATE` or `DATEV2`; `json` accepts `JSON` or `JSONB`
  (Doris 2.1 may report `JSON` as an alias of `JSONB`).

`STRING`, `JSONB`, `FLOAT`, and `DOUBLE` are value-only and cannot be key
columns.

## DML scope

| Event | v1 behavior |
| --- | --- |
| Snapshot / Read (`r`) | Full row written |
| INSERT (`c`) | Full row written |
| UPDATE (`u`) | `After` written only when it contains **every** declared column; otherwise the batch fails |
| Partial-column UPDATE | Rejected (the missing column is not treated as “unchanged”) |
| DELETE (`d`) | Rejected with an error |
| Unknown op | Rejected with an error |

`nil` is a real JSON `null`, not “column absent”. A source `DELETE` — including
a delete produced by transform filter retraction — fails the job; v1 has no
Doris delete path.

Projection interacts with the load body: only `event.Columns` are sent. If the
target table has additional non-projected columns, Doris fills them with their
defaults on each load (expected behavior, **pending real-environment
confirmation**). Keep the target schema and projection aligned.

## Stream Load and labels

Each table's sub-batch is one request:

```
PUT http://<hostname>:<port>/api/<database>/<table>/_stream_load
```

Headers: Basic auth, `label`, `format: json`, `read_json_by_line: true`,
`strict_mode: true`, `max_filter_ratio: 0`, `Expect: 100-continue`, and a
`timeout` in seconds. The HTTP client deadline is `request-timeout` plus 30s, so
Doris always reports its own verdict before the request is cancelled; a client
deadline equal to the Doris timeout would turn every timeout into an
unconfirmed result plus a full body retry. The body is JSON Lines — one JSON
object per row.

A `2xx` response is **not** enough. The sink checks the Doris business status:

| Observation | Action |
| --- | --- |
| `Success` | Success when `NumberFilteredRows == 0`, `NumberTotalRows == rows sent`, and `NumberLoadedRows == NumberTotalRows`; if the response `Label` is non-empty it must match the request label |
| `Publish Timeout` | Treated as success; the same logical batch is **not** retried with a new label |
| `Label Already Exists` + `FINISHED` | Treated as success |
| `Label Already Exists` + `RUNNING` | Retry with the **same label**; if the status never becomes confirmed, the batch fails after the bounded attempts |
| `Label Already Exists` + `CANCELLED` | Fatal |
| `Label Already Exists` + other / missing `ExistingJobStatus` | Other non-empty status is fatal; missing status is treated as unknown and queries `SHOW LOAD` |
| `Fail` | Fatal; data-quality failures are not blindly retried |
| Unknown status, invalid body, transport error, timeout | Query `SHOW LOAD` for the label first (see below) |

For `SHOW LOAD`, the sink first runs
`SHOW LOAD FROM <db> WHERE Label = '<label>'`. If that returns no row (observed
on some Doris 2.1.x Stream Load paths), it falls back to
`SHOW LOAD FROM <db>` and matches the label case-insensitively among recent
rows.

State mapping:

| `SHOW LOAD` state | Outcome |
| --- | --- |
| `FINISHED` | Success |
| `CANCELLED` | Fatal |
| `RUNNING` / `PENDING` / `ETL` / `LOADING` | Retryable with the same label |
| empty / `UNAVAILABLE` | Unknown (reconcile / retry path) |
| any other non-empty state | Treated as still in flight; retry with the same label |

If the filtered and unfiltered queries both find no row, the sink falls back to
the HTTP error class: `5xx` and `429` are retryable, other `4xx` are fatal,
transport errors are retryable. If `SHOW LOAD` itself errors (not “not found”),
the batch is **fatal** — an unconfirmed load is never reported as success.

Labels have the form `go-cdc_<fnv64hex>_<unixnano>_<seq>`. They are unique
within a process, reused across retries of one sub-batch, and **not** reproduced
after a restart. Label retention depends on Doris configuration.

FE redirects (for example `307` to a BE) are followed at most twice, and only
to `http`/`https` locations. Credentials are re-attached only when the redirect
host is allowlisted; otherwise the **redirect is refused** and the Stream Load
fails (credentials are never forwarded to a disallowed host):

- the same hostname as the configured FE, or
- a loopback / private / link-local **IP** (for example `127.0.0.1`, `10.x`,
  `192.168.x`)

Hostname-only BE addresses (for example `doris-be-0`) are **not** allowlisted.
Prefer FE configurations that advertise BE IPs, or run with host networking so
redirects stay on loopback. Only a plain-HTTP FE endpoint is supported; HTTPS
URLs are not configurable.

## Schema evolution

Schema evolution is **off by default**. When disabled, an `ADD COLUMN` (and
other column changes) affecting a projected column **fails the job explicitly**
instead of being silently applied or dropped. DDL for unprojected columns is
ignored. Primary-key changes, `RENAME TABLE`, and unrecognised `ALTER TABLE`
statements fail the job; secondary-index DDL is ignored because the Doris
target never carries secondary indexes.

When `schema.change.enabled: true` and `schema.change.include` contains
`add.column`:

1. The event must carry structured metadata: `AddColumns` with exactly one
   name and a `ColumnDef` whose name matches it.
2. Multi-column `ADD` is rejected as a whole; v1 applies one column per
   `ALTER TABLE ADD COLUMN`.
3. The sink reads `DESC`. If the column exists and is compatible (type and
   nullability), the repeat is an idempotent success. If it exists with a
   different definition, the job fails.
4. Otherwise it executes `ALTER TABLE <db>.<table> ADD COLUMN ...`, then polls
   `DESC` every 500ms until the column is visible and compatible or
   `schema.change.wait-timeout` expires (default `5s`). A
   `NOT NULL` addition needs a usable literal default (Doris rejects a new
   `NOT NULL` value column without one); otherwise the sink fails before
   touching the FE. Expression defaults are never translated.
5. Failure at any step stops writes to the affected table.

Not supported: drop column, modify column, rename column, key/model/partition/
bucket changes, `RENAME TABLE`, and `DROP TABLE` / `TRUNCATE`. These fail the
job explicitly (`DROP TABLE` / `TRUNCATE` fail when `drop.truncate` is
enabled; otherwise that switch is an explicit opt-out). MySQL DDL text is
never executed against Doris.

## Logging

The sink writes structured `slog` records through the process logger installed
by `cmd/go-cdc` (text handler on stderr, `INFO` by default). Passwords and
Stream Load bodies are never logged.

| Event | Level | Message and fields |
| --- | --- | --- |
| Target validated after create or restart | `INFO` | `doris: target table validated` — `database`, `table`, `columns`, `key_columns`, `auto_create` |
| Target accepted with source columns still pending | `WARN` | `doris: target table accepted with pending source columns; a replayed ADD COLUMN must create them before the first row` — `database`, `table`, `columns` |
| Target rejected (model, key order, types) | `WARN` | `doris: target table rejected` — `database`, `table`, `error` |
| Target loaded without a catalogue (cache miss) | `DEBUG` | `doris: target table loaded from DESC` — `database`, `table`, `columns`, `key_columns` |
| `ADD COLUMN` statement issued | `INFO` | `doris: sending schema change` — `database`, `table`, `action`, `column`, `statement` |
| `ADD COLUMN` confirmed | `INFO` | `doris: schema change applied` — `database`, `table`, `action`, `column`, `elapsed` |
| `ADD COLUMN` Exec failed | `ERROR` | `doris: schema change failed` — `database`, `table`, `action`, `column`, `error` |
| `ADD COLUMN` visibility timed out | `ERROR` | `doris: schema change not confirmed` — `database`, `table`, `column`, `wait_timeout`, `error` |
| Stream Load committed | `INFO` | `doris: stream load succeeded` — `database`, `table`, `label`, `rows`, `attempt`, `elapsed` |
| Load still unconfirmed, retrying | `WARN` | `doris: stream load not confirmed; retrying` — plus `outcome` (`retryable`, `unknown`, `label-exists`) |
| Fatal load after confirmation | `ERROR` | `doris: stream load failed` — plus `outcome` (`fatal`) |
| Last attempt still unconfirmed | `ERROR` | `doris: stream load was never confirmed` — plus `outcome` (`retryable`, `unknown`, or `label-exists`) |
| `SHOW LOAD` reconciliation | `DEBUG` | `doris: stream load state resolved from SHOW LOAD` — `database`, `label`, `state`, `outcome` |
| Generated DDL and idempotent repeats | `DEBUG` | `doris: create table statement`, `doris: schema change already present` |

Every failure is still returned to the Runner and fails the job; the records are
for operators, not control flow. `DEBUG` records are only emitted when the
handler level permits — the shipped binary runs at `INFO`.

## Checkpoint and recovery

- Doris `CreateTable` / compatibility validation, `ApplySchema`, and `Write`
  errors propagate to the Runner and fail the job.
- A table becomes captured only after its `CREATE` was applied and validated
  through the catalog message; a failed create does not enter the captured set.
- Because `Write` confirms every Stream Load synchronously, a successful
  `Flush` means the batch is visible (subject to Doris publish semantics). A
  failure means the checkpoint is not saved.
- Partial multi-table success is possible. Recovery replays the whole batch
  with new labels; `UNIQUE KEY` converges rows by primary key.
- At-least-once only. Checkpoint state and Doris commits are not one atomic
  transaction.

After a restart, a Doris pipeline opts the MySQL source into
`ReemitCapturedSchemas`: the source re-reads every captured table and sends a
schema message before binlog replay, so the sink re-runs the full compatibility
check (model, key order, column types) before the first write. This applies to
every startup mode that resumes from a checkpoint (`initial`, `latest`,
`earliest`, `timestamp`, `specific-offset`); a first start has no captured
tables and skips the read. MySQL / Kafka / stdout pipelines leave that flag off.
If a captured table no longer exists at the source, that read is skipped with a
warning; the sink then falls back to the lazy target `DESC` check (a `UNIQUE KEY`
target is required and the event key set must equal the target key set). Any
other catalogue error fails the job rather than silently skipping the check.

A source column added while the job was down is still pending in the binlog.
The resume check reports it as a warning and the table stays writable; the
replayed `ADD COLUMN` creates the column before the first row that carries it.
If the DDL never arrives (for example `schema.change.enabled: false`), the first
event that carries the column fails the job explicitly.

## Tests

### Unit and Mock tests

```bash
# Doris package: type mapping, DDL, JSON Lines, label state machine,
# httptest Stream Load, compatibility and restart paths
go test ./connector/sink/doris/ -count=1 -v

# Runner, config, schema/event, source DDL metadata
go test ./composer/ ./composer/definition/ ./common/schema/ ./common/event/ ./connector/source/mysql/ -count=1

# Repository gates
make fmt-check
make lint
make test
```

These tests use a fake SQL layer and `httptest` servers; they do **not**
contact a real Doris FE/BE.

### Real Doris integration suite

The suite in [`test/integration/doris`](../../test/integration/doris) uses the
public Sink API and a real Doris FE/BE pair. It is excluded from normal
`go test ./...` runs by the `doris_integration` build tag. It drives the Sink
directly; it does not start a MySQL source or the Runner, so Runner checkpoint
replay remains covered by unit tests.

The same `run-doris-e2e.sh` first runs the pipeline harness
(`harness/doris_it.py`): functional TP-DR01..DR04 plus load parity TP-DR05..DR07
(bulk multi-table snapshot, concurrent peak writers + catch-up, typed
spot-check). The load phase reuses the `LOAD_*` knobs and CI tiers of the
MySQL/Kafka load suites and covers only Doris-mappable types (no
BLOB/ENUM/BIT/SET/TIME).

```bash
# Linux + Docker Compose + mysql client; host networking is required.
# Kernel limits (doris-up.sh tries `sudo -n sysctl -w` and fails early otherwise):
#   vm.max_map_count >= 2000000                    (BE)
#   fs.inotify.max_user_watches >= 1048576         (FE / BerkeleyDB JE)
#   fs.inotify.max_user_instances >= 8192
bash test/integration/scripts/doris-up.sh
bash test/integration/scripts/run-doris-e2e.sh
bash test/integration/scripts/doris-down.sh

# Against an existing cluster:
DORIS_HOST=127.0.0.1 DORIS_HTTP_PORT=8030 DORIS_SQL_PORT=9030 \
  go test -tags doris_integration ./test/integration/doris/ -count=1 -v
```

The inotify limit is a hard requirement, not a tuning knob: FE exits with
`User limit of inotify watches reached` when its BerkeleyDB JE meta directory
cannot be watched, and the BE then loops on "FE is not started". On a desktop
where an IDE already holds thousands of watches, the distro default of 8192 is
not enough even though RAM and disk are fine.

| Variable | Default | Meaning |
|---|---|---|
| `DORIS_HOST` | required by tests; `doris-up.sh` uses `127.0.0.1` | FE host |
| `DORIS_HTTP_PORT` | `8030` | FE HTTP / Stream Load port |
| `DORIS_SQL_PORT` | `9030` | FE MySQL protocol port |
| `DORIS_USER` | `root` | Basic Auth / SQL user |
| `DORIS_PASSWORD` | empty | password |
| `DORIS_REPLICATION_NUM` | `1` | table replication property for the single-BE IT |
| `DORIS_BUCKETS` | `auto` | table bucket count |
| `DORIS_VERSION` | `2.1.8` | FE/BE image version used by compose |

The suite covers:

- auto-create + `UNIQUE KEY` DDL and snapshot / INSERT / full UPDATE upsert;
- duplicate primary keys inside one batch (last event wins);
- restart with an empty Schema cache (lazy `DESC` + key-set validation);
- extra target columns with defaults, `ADD COLUMN`, idempotent replay,
  incompatible-column rejection, and narrow live column rejection;
- DATETIME, DECIMAL, JSON, integer (including `BIGINT UNSIGNED`), CHAR/VARCHAR,
  TEXT, DATE and YEAR round-trips, plus binary rejection before DDL;
- duplicate Label and `SHOW LOAD` state lookup through a real Stream Load;
- multi-table partial success and stop-at-first-failure ordering.

CI: [`.github/workflows/doris-integration.yml`](../../.github/workflows/doris-integration.yml)
runs the suite on Doris `2.1.8` for Doris-specific changes and via
`workflow_dispatch`. It starts FE+BE with
[`docker-compose.doris.yml`](../../test/integration/docker-compose.doris.yml),
raises `vm.max_map_count` and the inotify limits, runs the pipeline harness
(functional + load) and the Go suite, and uploads the report, `cdc-doris*.log`,
and compose logs.

**Execution status:** the suite and CI job are present, but a real Doris run
has **not** been performed in this workspace. Do not change the pending items
in [Not verified](#not-verified) to PASS until the CI job (or a recorded local
run) succeeds on the target 2.1.x image.

Not forced by the real suite because Doris does not expose a deterministic
hook: `Publish Timeout`, `CANCELLED` jobs, injected 5xx/429/network loss, and
Runner checkpoint replay. Those remain covered by unit/`httptest` tests and
must be checked in a failure-injection environment before being claimed as
validated.

## Not verified

The following are implemented to the 2.1.x documentation but have **not** been
exercised against a real cluster. The integration suite is intended to cover
several of them, but they stay pending until a real run is recorded as green:

- `BUCKETS auto`, the cluster's default merge-on-write mode, and generated DDL.
- `DESC` rendering of defaults, `NULL`, key columns, `DATEV2`, and `JSON`.
- `ADD COLUMN` asynchronous visibility and the default `5s` wait window.
- `Publish Timeout`, `SHOW LOAD` state strings, and label retention.
- FE `307` redirects, credential re-attachment, and BE authentication.
- Snapshot / INSERT / full UPDATE upsert semantics and duplicate-key
  convergence.
- `DATETIME`, `DECIMAL`, `JSON`, binary rejection, and extra-column defaults.
- Multi-table partial success and recovery after a failed checkpoint.

## Not implemented in v1

- Partial-column updates and DELETE.
- `DROP TABLE` / `TRUNCATE`, drop/modify/rename column, multi-column ADD.
- Exactly-once delivery, transactions, or stable labels across restarts.
- HTTPS FE endpoints, multiple FE addresses, or custom Stream Load headers.
- Automatic partitions, merge-on-write selection, or index generation.
- Translating SQL expression defaults (they are omitted, never guessed).
- Deterministic failure injection for `Publish Timeout`, `CANCELLED`, 5xx/429,
  and network loss in the real-Doris suite.

## Related Docs

- [Pipeline](../pipeline.md)
- [MySQL Source](mysql-source.md)
- [MySQL Sink](mysql-sink.md)
- [Design spec](../superpowers/specs/go-cdc-doris-sink-design.md)
