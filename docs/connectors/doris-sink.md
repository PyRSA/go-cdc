# Doris Sink Connector

Writes the current state of routed rows into Apache Doris **2.1.x** using
**UNIQUE KEY** tables and **Stream Load JSON Lines**.

**YAML:** top-level `sink` with `type: doris`.

## Version & Dependencies

| Item | Support |
| --- | --- |
| Target Doris | **2.1.x** |
| FE HTTP endpoint | `hostname` + `port` (default `8030`), **plain HTTP only**; the host must not contain a scheme or path |
| FE SQL endpoint | `hostname` + `sql-port` (default `9030`, MySQL protocol) |
| Table model | `UNIQUE KEY`; merge-on-write is left at the cluster default |
| Packaging | Shipped inside the `go-cdc` binary |

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
| `replication-num` | no | `0` | Integer | `0` omits `PROPERTIES`; a positive value emits `"replication_num"`. A single-BE cluster needs `1` — Doris' own default of 3 is rejected there |
| `schema.change.enabled` | no | `false` | Boolean | Enables Doris `ADD COLUMN` handling; see [Schema evolution](#schema-evolution) |
| `schema.change.include` | no | `["add.column"]` | List | v1 accepts only `add.column`; any other entry is a config error |
| `schema.change.wait-timeout` | no | `5s` | Duration | How long an `ADD COLUMN` waits for the new column to appear in `DESC` (`> 0`). `DESC` is re-read every 500ms until the budget expires |
| `partial-update.enabled` | no | `false` | Boolean | `true` is rejected at parse time (not implemented) |
| `delete.enabled` | no | `false` | Boolean | `true` is rejected at parse time (not implemented) |
| `schema.change.drop-truncate.enabled` | no | `false` | Boolean | `true` is rejected for Doris in v1 |

`kafka`-only options (`brokers`, `topic`, …) and MySQL's `3306` default do not
apply here: the parser keeps the two Doris ports separate.

## Delivery semantics (at-least-once)

- Each Stream Load is confirmed through the Doris **business status**, not just a
  `2xx`, before `Write` returns. A failed load fails the job and does **not**
  advance the checkpoint.
- A failed or unrecognised response is first resolved by the same label in
  `SHOW LOAD`: `FINISHED` counts as success, `CANCELLED` fails the job, and an
  in-flight state is retried. When Doris returns no row for that label at all, a
  transport error, `5xx`, or `429` is retried with the same label (Doris
  deduplicates it), while other `4xx` responses fail the job. A label matching
  more than one `SHOW LOAD` row is ambiguous and fails the job rather than
  guessing a state.
- Recovery after a crash re-sends the events with **new labels**. The target is a
  `UNIQUE KEY` table, so replayed rows converge by primary key — at-least-once,
  not exactly-once.
- A multi-table batch is **not atomic**: tables are written in lexicographic order
  and the first failure stops the rest, so earlier tables may already be committed.
- Repeated primary keys inside one batch keep the **last** event.
- `DELETE` is not supported; see [DML scope](#dml-scope).

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

- Key columns lead the definition in source primary-key order; other columns keep
  their source order. Key columns are always `NOT NULL`, nullable source columns
  become `NULL`, non-nullable ones `NOT NULL`.
- Source column comments and literal defaults are copied. SQL expression defaults
  (`DEFAULT_GENERATED`, for example `CURRENT_TIMESTAMP`) are **omitted rather
  than guessed**; CDC writes every projected column anyway.
- No partitions, indexes, engines, or merge-on-write properties are generated.
  `replication_num` is emitted only when configured.

When `auto-create-table` is `false`, a missing database or table fails the job.

Whether the table was just created or already existed, the sink reads
`SHOW CREATE TABLE` plus `DESC` and validates the target before caching it as
writable. A table is rejected when:

- It is not a `UNIQUE KEY` table. `DESC` marks `UNIQUE`, `DUPLICATE` and
  `AGGREGATE` key columns with the same `Key=YES`, so the model is read from
  `SHOW CREATE TABLE`; `DUPLICATE KEY` and `AGGREGATE KEY` targets are rejected.
- Its key columns differ from the source primary key in membership **or** order, a
  source primary-key column is missing or is not a Doris key column, or the target
  carries extra key columns.
- A source column has a type that is incompatible with the target's (see
  [Type mapping](#type-mapping)).
- A nullable source column maps onto a `NOT NULL` target column.
- An extra target column is neither nullable, defaulted, nor generated.

A source column that the target does not have is handled by
[Schema evolution](#schema-evolution), and only there:

- With `schema.change.enabled: true` and `add.column` in `schema.change.include`,
  the sink runs `ALTER TABLE ... ADD COLUMN` while validating the target, then
  re-reads `DESC` and re-validates the whole table before it is cached. A
  `NOT NULL` column without a usable literal default cannot be added this way and
  fails the job; Doris requires a default for a new value column.
- Otherwise the job stops at startup and names every missing column. Waiting for a
  replayed `ADD COLUMN` is not an option: a column that reached the source before
  the checkpoint has no DDL left to replay, and the snapshot emits rows carrying
  it before any binlog DDL. `Write` also rejects any event that references a
  column the target does not have.

A source table without a primary key is rejected. `auto-create-table` only creates
missing tables: it never alters an existing one, and it does not grant permission
to do so — that stays with [Schema evolution](#schema-evolution).

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

MySQL counts `CHAR`/`VARCHAR` lengths in characters, Doris counts them in bytes,
so auto-created columns reserve three bytes per character (`n*3`) — the same
convention the Flink CDC Doris sink uses. Characters outside the Basic
Multilingual Plane (emoji) need four bytes and are rejected by the load rather
than truncated.

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

`nil` is a real JSON `null`, not “column absent”. A source `DELETE` — including a
delete produced by transform filter retraction — fails the job; v1 has no Doris
delete path.

Only `event.Columns` are sent. If the target table has additional non-projected
columns, Doris fills them with their defaults on each load. Keep the target
schema and projection aligned.

## Schema evolution

Schema evolution is **off by default**. When disabled, an `ADD COLUMN` (and other
column changes) affecting a projected column **fails the job explicitly** instead
of being silently applied or dropped. DDL for unprojected columns is ignored.
Primary-key changes, `RENAME TABLE`, and unrecognised `ALTER TABLE` statements
fail the job; secondary-index DDL is ignored because the Doris target never
carries secondary indexes.

When `schema.change.enabled: true` and `schema.change.include` contains
`add.column`, the grant covers both replayed DDL and target validation:

- The event must carry one `ADD COLUMN` with a matching `ColumnDef`; multi-column
  `ADD` is rejected as a whole (v1 applies one column per statement).
- If `DESC` already shows the column with the same type and nullability, the
  repeat is an idempotent success; a different definition fails the job.
- Otherwise the sink runs `ALTER TABLE ... ADD COLUMN` and polls `DESC` every
  500ms until the column is visible and compatible or
  `schema.change.wait-timeout` expires. A `NOT NULL` addition needs a usable
  literal default, because Doris rejects a new `NOT NULL` value column without
  one. Expression defaults are never translated.
- Failure at any step stops writes to the affected table.

Operational notes:

- Validation runs at startup, so a restart can issue `ALTER TABLE ... ADD COLUMN`
  against an existing target before the first row is written.
- Doris DDL has no transactional rollback. When several columns are missing, a
  failure part-way leaves the columns already added in place. The error names the
  failing column and the job stops; the target is never cached as validated.
- The sink account needs `ALTER` on the target tables, plus `CREATE` when
  `auto-create-table` is on. Adding a column to a large table can be slow; the
  visibility wait is `schema.change.wait-timeout` (default 5s).
- After a restart the live structure is read again, so a column that already
  exists with a compatible definition is recognised instead of being added twice.
  A column whose definition does not match the source fails the job and needs a
  manual fix.

Not supported: drop column, modify column, rename column, key/model/partition/
bucket changes, `RENAME TABLE`, and `DROP TABLE` / `TRUNCATE`. These fail the job
explicitly (`DROP TABLE` / `TRUNCATE` fail when `drop.truncate` is enabled;
otherwise that switch is an explicit opt-out). MySQL DDL text is never executed
against Doris.

## Limitations

Not implemented in v1:

- Partial-column updates and DELETE.
- Primary-key changes on the source: the new key cannot be written as an upsert
  without leaving the old row behind, so such an `UPDATE` fails the job.
- `DROP TABLE` / `TRUNCATE`, drop/modify/rename column, multi-column ADD.
- Exactly-once delivery, transactions, or stable labels across restarts.
- HTTPS FE endpoints, multiple FE addresses, or custom Stream Load headers.
- Automatic partitions, merge-on-write selection, or index generation.
- Translating SQL expression defaults (they are omitted, never guessed).

Networking: the sink follows at most two FE redirects (plain `http`/`https`
locations) and re-attaches credentials only when the redirect host is the
configured FE hostname or a loopback / private / link-local IP. Hostname-only BE
addresses (for example `doris-be-0`) are **not** allowlisted, so prefer an FE
that advertises BE IPs, or run with host networking so redirects stay on
loopback.

Not yet verified against a real cluster: generated DDL (`BUCKETS auto`,
merge-on-write default), `DESC` rendering of defaults / `DATEV2` / `JSON`,
`ADD COLUMN` visibility timing, `Publish Timeout` and `SHOW LOAD` state strings,
FE `307` redirects with BE authentication, and multi-table partial recovery.
To check them on the target cluster, run the integration suite — see
[`test/integration/README.md`](../../test/integration/README.md).

## Related Docs

- [Pipeline](../pipeline.md)
- [MySQL Source](mysql-source.md)
- [MySQL Sink](mysql-sink.md)
- [Design spec](../superpowers/specs/go-cdc-doris-sink-design.md)
