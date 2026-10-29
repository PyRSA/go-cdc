# PostgreSQL Sink Connector

Applies pipeline changes to a PostgreSQL table as current-state writes (`INSERT ... ON CONFLICT` / `DELETE` by key). The target can be created from the source definition and kept in step with column-level DDL, but both are opt-in: with the defaults the sink writes only into a table you created yourself and stops the job on any schema change.

**YAML:** top-level `sink` with `type: postgresql`.

## Version & Dependencies

| Item | Support |
| --- | --- |
| Target PostgreSQL | **16** (validated; other versions are untested in this release) |
| SQL driver | `github.com/jackc/pgx/v5` (linked into `go-cdc`) |
| Packaging | Shipped inside the `go-cdc` binary |
| Time zone | **No sink-side time-zone option**; the connection session `TimeZone` follows `source.server-time-zone` |
| Tables | Created on request (`auto-create-table`) or pre-created; in both cases the live structure is validated before the first batch |

## Example

```yaml
sink:
  type: postgresql
  hostname: 127.0.0.1
  port: 5432
  username: cdc
  password: secret
  database: target_db
  schema: public
  sslmode: prefer
  connect-timeout: 30s
  write-batch-size: 1000
  write-batch-interval: 1s
  max-retries: 3
  auto-create-table: true
  schema.change.enabled: true
  schema.change.include: [add.column]
  schema.change.drop-truncate.enabled: false
```

A PostgreSQL connection is bound to one database, so `database` is a required option. The target schema comes from [`route`](../pipeline.md#route) or, when no route moved the table, from `schema`.

## Options

| Option | Required | Default | Type | Description |
| --- | --- | --- | --- | --- |
| `type` | yes | — | String | Must be `postgresql` |
| `hostname` | yes | — | String | Target host |
| `port` | no | `5432` | Integer | Port (1..65535) |
| `username` | yes | — | String | User |
| `password` | yes | — | String | Password |
| `database` | yes | — | String | Target database; PostgreSQL always connects to exactly one database |
| `schema` | no | `public` | String | Target schema used for events that no route moved; a single name without dots |
| `sslmode` | no | `prefer` | String | `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full` |
| `connect-timeout` | no | `30s` | Duration | Connection timeout (≥ 250ms) |
| `write-batch-size` | no | `1000` | Integer | Flush when the batch reaches this many events (≥ 1) |
| `write-batch-interval` | no | `1s` | Duration | Time-based flush; `0` disables the timer |
| `max-retries` | no | `3` | Integer | Extra write attempts after a **retryable** failure (≥ 0) |
| `auto-create-table` | no | `false` | Boolean | Create a missing target from the source definition, including its primary key and unique constraints. Off by default: creating a table changes the target, so it stays an explicit authorisation |
| `schema.change.enabled` | no | `false` | Boolean | Apply column-level DDL to the target. Off by default; it never blocks `auto-create-table`, so a pipeline with the switch off still starts |
| `schema.change.include` | no | unset | List | Restrict which changes are applied. Accepted entries: `add.column`, `drop.column`, `rename.column`, `modify.column`, `drop.table`, `truncate`. An empty list applies every kind the other switches allow |
| `schema.change.drop-truncate.enabled` | no | `false` | Boolean | Allow `DROP TABLE` and `TRUNCATE TABLE`. A destructive kind listed in `include` is still refused while this switch is off |

Options that belong to other sinks are rejected: `brokers`, `topic`, `source-name`, `client-id`, `schema.change.emit.enabled`, `allow.auto.create.topics`, `producer-batch-max-bytes`, `flush-timeout` (Kafka), and `sql-port`, `request-timeout`, `buckets`, `replication-num`, `partial-update.enabled`, `delete.enabled`, `schema.change.wait-timeout` (Doris). `schema.change.wait-timeout` has no PostgreSQL equivalent because a PostgreSQL statement is finished when it returns.

## Target Table Resolution

| Situation | Target |
| --- | --- |
| No `route` matched | `sink.schema` + event table |
| `route` matched, database segment differs from the source database (e.g. `sink-table: sales.users`) | `sales.users` |
| `route` matched, database segment equals the source database (identity route or table-only rename) | the database segment as schema + routed table |

The database segment of a matched `route` is the only thing that can name a schema. Without a match the configured `schema` is used, so an unrouted source database name never becomes a schema by accident. A `sink-table` must be exactly `schema.table`; a third segment is rejected at write time.

### Multiple Source Databases

The sink connects to one PostgreSQL database and separates tables by schema only. Every event that no route matches falls back to the same `sink.schema`, so `db1.users` and `db2.users` both resolve to `<schema>.users`:

- their rows share one target table, and an upsert with the same business key **overwrites the other database's row**;
- a `DELETE` from one database **removes the row the other database owns**, because both sides use the same business key.

Configure one route per source database to keep them apart, together with the matching schemas:

```yaml
route:
  - source-table: "db1\\..*"
    sink-table: db1.<>
    replace-symbol: "<>"
  - source-table: "db2\\..*"
    sink-table: db2.<>
    replace-symbol: "<>"
```

Tables that no route matches still fall back to `sink.schema`, so cover every captured database. There is no start-up check for overlapping targets in this release.

## Table Creation

With `auto-create-table: false` (the default) the target must already exist. The sink reads it from `pg_catalog` and validates the live structure against the source definition, so a missing column, a different type, or a key the conflict target cannot use surfaces before a row is written instead of halfway through a write. A table that fails validation is an error naming the columns at fault.

The comparison is by mapped type, not by width: `text` where the source maps to `VARCHAR(64)`, or `VARCHAR(128)` where it maps to `VARCHAR(64)`, is a mismatch even though the wider column would store every value. A target column matches only when it is exactly the type the mapping below produces, and a nullable source column only when the target is not `NOT NULL`.

`auto-create-table: false` says the sink may not create a table; it does not turn that check off. An existing table is validated the same way with the switch on or off.

The whole table is checked when the source announces it with its definition: on the first scan of a new table, on a snapshot, and on a `CREATE TABLE`. The other changes — `ADD`, `DROP`, `RENAME`, `CHANGE`, `MODIFY` — carry no table definition, so each verifies only the column it touches (see [Schema Evolution](#schema-evolution)) and the columns it leaves alone are not re-checked. A restart announces only the tables whose snapshot is still pending, so a target that changed while the job was stopped is not re-validated at start-up: a row it cannot accept fails the batch and stops the job with the server's `SQLSTATE`, and a target that is merely wider keeps accepting rows. Check the target after changing it by hand, or start the job once against a new target that `auto-create-table` creates and validates from the source definition.

With `auto-create-table: true` a missing target is created from the source definition — never from the first row it happens to carry:

- every projected column is created with the mapped type, `NOT NULL` where the source column is not nullable, and a `PRIMARY KEY` on the source key;
- a unique index the sink can express (BTREE or HASH, no prefix length) becomes a `UNIQUE` constraint, because the `ON CONFLICT` target of every upsert has to exist; a plain secondary index, a `FULLTEXT` or `SPATIAL` index, and a prefix index are left out, since PostgreSQL builds its own access path;
- a missing target schema is created with `CREATE SCHEMA IF NOT EXISTS`; the database itself never is, because one connection serves exactly one database;
- a default is copied when it is portable (`NULL`, `CURRENT_TIMESTAMP` with optional precision, a numeric literal for a numeric or floating point column, and a quoted string literal for every other type). The source default is text for every column type, so the mapped type decides the spelling: `'0'` stays the one-character string in a `CHAR(1)` column and becomes `0` in an `INTEGER` column, and a value such as `1e5` or `-1` is quoted wherever the column is not numeric. A generated expression, a parenthesised expression, and the MySQL zero date are left out rather than guessed: the sink writes an explicit value for every projected column, so an absent default never changes what a captured row stores;
- `AUTO_INCREMENT` is not reproduced as `IDENTITY`/`SERIAL`. The sink always writes the value the source sent, and a generator on the target could hand out a value the source never had. The skipped attribute is logged as a warning;
- a type listed as unsupported below refuses the whole table, because half a table would be a target that silently stores the wrong thing.

An existing table is validated the same way whether or not it was created by the sink, and the comparison is exact: a column you widened by hand (`text` where `varchar(64)` maps to `VARCHAR(64)`) is reported as a mismatch, because a target structure the sink did not choose is a target whose future writes it cannot reason about. To move past a mismatch, either bring the column to the mapped type (`ALTER TABLE ... TYPE VARCHAR(64)`), or `transform.projection` the column out so the source definition no longer requires it — a projected-out column still has to accept a row, so a `NOT NULL` column without a default refuses the projection. A column the target holds but the source definition does not project is accepted as long as it is nullable or has a default.

Upgrading a pipeline that ran before schema evolution: nothing about the mapping changes, but a target that was created by hand (or widened by hand while PR1 was running) is now held to that comparison. The comparison is not guaranteed to run on the first start after the upgrade: a pipeline that resumes from an existing checkpoint does not re-announce the tables the checkpoint already captured (see [Limits in This Release](#limits-in-this-release)). Until the source announces the table again — on a new snapshot or a `CREATE TABLE` — a mismatch goes undetected at start-up, and the job surfaces it the way PR1 always did: at the first write the target refuses. Compare the target against the [Column Type Mapping](#column-type-mapping) before switching over.

After a `CREATE TABLE` the sink re-reads `pg_catalog` and validates the result, so the catalogue is the evidence and not the statement that ran. A `CREATE` that lost a race with another process (SQLSTATE `42P07`) is handled the same way.

## Schema Evolution

A DDL statement is applied to the target only when the switches above allow it, and every applied change follows the same contract:

- the runner flushes the rows it still holds before the change, so an `INSERT` never lands after the `ALTER` that made room for it; a flush that fails stops the job before any DDL runs;
- the statements of one change run in a single transaction, so a change that renames and retypes a column applies whole or not at all;
- after the transaction commits the target is re-read from `pg_catalog` and validated, and the cached statement templates of that one target are dropped. A change that fails is reported with its statement text and the server error, leaves the caches alone, and stops the job: the checkpoint does not advance past a change the target does not have.
- a failed `COMMIT` is treated the same way, even though its result is unknown. The caches keep the last structure the catalogue proved, nothing is retried inside the sink, and the job stops. After a restart the statement is replayed — `ADD COLUMN IF NOT EXISTS`, `DROP COLUMN IF EXISTS`, and the rename/retype paths all converge — and it is the read-back that follows the replay, not the lost acknowledgement, that rewrites the caches. A replayed `ADD COLUMN` whose column already exists is held to one rule: the target column has to be able to store every value the source sends. A column a later change widened (`ADD c VARCHAR(32)`, then `MODIFY c VARCHAR(64)`, with the checkpoint still behind both) is accepted and reported as a warning, because every `VARCHAR(32)` value fits; a column that is narrower, of another family, or `NOT NULL` where the source is nullable still stops the job. The rollback of a failed change is bounded exactly like the one on the DML path, so a server that stopped answering cannot hold the shutdown open.

| Change | Config name | Behaviour |
| --- | --- | --- |
| `ADD COLUMN` (one or several columns) | `add.column` | Added with the source type and nullability. `NOT NULL` without a default is refused: MySQL fills the existing rows with an implicit value and PostgreSQL has none. A multi-column statement is applied in one transaction from the source catalogue, because the statement text carries no definition. A replayed statement whose column already exists is accepted when that column can store every source value, so a later widening it did not undo does not stop the replay |
| `DROP COLUMN` | `drop.column` | Dropped after the target is read back. Refused when the column carries a primary key, a unique constraint, a foreign key, or a unique index, because that takes the conflict target with it. A plain index, a check constraint, and an exclusion constraint leave with the column. A replayed statement whose column is already gone is a no-op |
| `RENAME COLUMN` | `rename.column` | Renamed only. The source catalogue is not consulted, because the statement restates nothing |
| `CHANGE COLUMN` | `rename.column` | Renamed **and** restated: the new definition is read from the source catalogue and the type and nullability are adjusted in the same transaction. `CHANGE` that keeps the name is applied as a type change |
| `MODIFY COLUMN` | `modify.column` | Type and nullability restated, name untouched |
| `DROP TABLE` | `drop.table` | `DROP TABLE IF EXISTS`, and needs `schema.change.drop-truncate.enabled: true` |
| `TRUNCATE TABLE` | `truncate` | `TRUNCATE TABLE`, and needs `schema.change.drop-truncate.enabled: true`. A missing target is an error, because `TRUNCATE` carries no `IF EXISTS` |

A type change is applied only when it is a verified widening: integer to a wider integer, integer or numeric to a numeric that still holds every digit, a longer `char`/`varchar` limit or `text`, `real` to `double precision`, `date` to `timestamp`, and a larger fractional-seconds precision. Anything else — a narrowing, `varchar` to `char`, a cast between unrelated families — stops the job instead of being attempted, because PostgreSQL would either reject it or reinterpret the stored values.

`CHANGE` and `MODIFY` are interpreted against the definition the source catalogue holds **now**, because that is the only structured description of a column: the statement text is not a reliable source for a type, a nullability, or a default. A pipeline that restarts behind a historical DDL therefore moves the target to what the source currently has, never to what the statement said when it ran. Two consequences are deliberate. A replayed change whose column the source has since renamed again or dropped is refused before a single statement runs (`a change the source no longer shows cannot be replayed safely`), so the target is never renamed on a guess; and a change that ends on a definition the sink cannot verify is refused by the read-back. Neither is skipped: the job stops, the checkpoint stays, and the operator decides.

The dependency read that guards `DROP COLUMN` inspects the constraints and the indexes the target table owns. Objects that live elsewhere — a view or materialized view that reads the column, a generated column derived from it, an expression index over it — are not in that list, and are not needed: the statement carries no `CASCADE`, so PostgreSQL refuses the drop (`SQLSTATE 2BP01`), the one transaction rolls back, and the job stops with the server's error. A foreign key that another table points at the column is covered by the guard already, because a referenced column must be a primary key or unique column on this table, and those are exactly what the guard refuses to drop. In every one of these cases the column, the dependent object, and the cached structure all survive unchanged; the sink cannot half-drop a column.

Index statements are split by what they touch. A plain non-unique secondary index only changes how PostgreSQL plans a query, so it is skipped and logged as a warning. A statement naming `PRIMARY`, `UNIQUE`, `FOREIGN`, `CHECK`, `CONSTRAINT`, or `EXCLUDE`, and any index statement the sink cannot classify, stops the job: reproducing a key wrongly would silently change which rows the target considers the same.

`RENAME TABLE` and any statement that cannot be classified are refused outright. The target table is part of the checkpoint's table identity, so moving it without a verified rewrite would leave the two disagreeing with nothing to show for it.

With `schema.change.enabled: false` a column-level change is an explicit error naming the switch — and it is still not in the way of `auto-create-table`, so a pipeline that only captures new tables starts normally with the switch off.

Applying `CHANGE`/`MODIFY` and multi-column `ADD COLUMN` needs the source catalogue, which the runner supplies from the MySQL source; the sink only borrows that handle and never closes it.

A `transform.projection` is matched against the column names a statement carries, so a rename and a projection interact: with a projection that lists the old name, `RENAME COLUMN old TO new` is applied and the rows that follow name `new`, which the projection no longer admits — the renamed column keeps its last value and stops receiving new ones. With a projection that lists only the new name, the rename itself is skipped and the following row that names `new` fails with `SQLSTATE 42703`. Keep the projection in step with a rename, or drop the projection for the table.

## Behavior Notes

- One `Write` call is one transaction: every statement of the batch commits or none does. A failing statement rolls the whole batch back, so a retry cannot apply half a batch.
- `INSERT ... ON CONFLICT (keys) DO UPDATE SET ...` mirrors the MySQL sink's current-state semantics. When every written column belongs to the key, the statement uses `DO NOTHING`.
- The conflict target is the business key. `ON CONFLICT` needs a `PRIMARY KEY`, `UNIQUE` constraint, or unique index on exactly those columns; without one PostgreSQL rejects the statement with SQLSTATE `42P10` (no unique or exclusion constraint matching the `ON CONFLICT` specification). `42P10` is permanent: it is not retried and the job exits. A table without a business key is unaffected because it gets a plain `INSERT`.
- An `UPDATE` that changes a business key becomes `DELETE` (old key) followed by the upsert of the new row, matching the MySQL sink.
- A table without a business key gets a plain `INSERT`. An `UPDATE` therefore inserts another row, and so does every replayed event; configure `transform.primary-keys` when the source table has no primary key.
- `INSERT` and `UPDATE` on a table with a business key require every key column to be present in the after image with a non-NULL value. A missing or NULL key cannot conflict with an existing row, so the statement would insert a row that later upserts cannot reach; such an event fails instead. Composite keys are checked column by column.
- `DELETE` requires a business key, and every key column must be present and non-NULL in the before image.
- `transform.projection` must keep every key column. A projection that drops a key column makes `DELETE` fail on the missing before-image key, and makes `INSERT`/`UPDATE` (including an `UPDATE` that a key change turns into delete plus upsert) fail on the missing after-image key, so the event fails instead of writing the wrong row.
- Retryable SQLSTATEs: `40001`, `40P01`, `57P01`, `57P03`, and any `08xxx`. Backoff starts at 100ms and doubles up to 2s. A failed `COMMIT` is never retried: the job exits and replays from the last checkpoint. Other errors, including client-side encode failures and socket errors without a SQLSTATE, also exit the job.
- The error message names the operation, `schema.table`, and the SQLSTATE (when the server supplied one).
- Each transaction has a 30s operation timeout. `SIGINT`/`SIGTERM` cancels the first connection attempt and interrupts the retry backoff. A signal that lands while a batch is in flight aborts that transaction: it is rolled back, never retried, never reported as committed, and the job exits without advancing the checkpoint, so the batch is replayed from the last saved position. A signal that lands between batches lets the runner drain the batch it still holds: that single write does not inherit the cancellation, so a clean stop commits it and advances the checkpoint, still bounded by the 30s operation timeout.
- Connection pool defaults (code, not YAML): `MaxConns=1`, `MinConns=0`, `MaxConnLifetime=30m`, `MaxConnIdleTime=10m`.

## Type Handling

The source normalizes every value to `nil`, `int64`, `uint64`, `string`, or `[]byte`, and the two capture paths agree everywhere this sink accepts a type. The snapshot path reads through `database/sql` in raw form: integer columns become `int64` (or `uint64` when `unsigned`), the binary and blob columns become `[]byte`, and every other column — `DECIMAL`, the date and time types, `YEAR`, `JSON`, an `ENUM`/`SET` label, a `BIT` field — arrives as the text MySQL's text protocol returned. The binlog path decodes the row event: the same integers, `[]byte` for the blob types, text for `DECIMAL`, the temporal types and the decoded `JSON` document, and `int64` for the `ENUM` member index, the `SET` bitmask, `BIT`, and `YEAR`. The three types whose paths disagree — `ENUM`, `SET`, `BIT` — are refused at table creation, so no column is ever written with a value whose meaning depends on which path produced it. The sink passes values through unchanged:

| Source value | Argument | Result |
| --- | --- | --- |
| `nil` | SQL `NULL` | Any nullable column |
| `int64` | `int64` | Smallint/integer/bigint/numeric |
| `uint64` ≤ `MaxInt64` | `int64` | Smallint/integer/bigint/numeric |
| `uint64` > `MaxInt64` | exact decimal text | Exact in `NUMERIC`; a `BIGINT` column fails with `22003` |
| `string` | text parameter | Parsed by the server for the target column (numeric, date, timestamp, text, json) |
| `[]byte` | byte slice | `BYTEA`; sent verbatim as the document text to a `json` column; a `text` column only accepts bytes that are valid in the database encoding |

In practice only the binary and blob columns reach the sink as `[]byte`, and a `json` document as text, because those are the shapes both paths produce.

No value is rewritten on the way in: the sink passes what the source sent and lets the target column parse it, so a `DECIMAL` never travels through a float and a large `uint64` keeps its exact decimal text. A value the column cannot parse fails the batch with the server's SQLSTATE (for example `22P02` invalid text representation, `22003` numeric out of range, `42804` datatype mismatch), which is permanent: the job stops and the checkpoint stays where it is.

### Column Type Mapping

`CREATE TABLE` and `ADD COLUMN`/`CHANGE`/`MODIFY` map the MySQL `COLUMN_TYPE` onto PostgreSQL. An unmapped type is an error, never a fallback to `text`, because `text` would accept the write and lose the source type.

| MySQL | PostgreSQL |
| --- | --- |
| `tinyint(1)`, `tinyint(4)`, `year` | `SMALLINT` (no boolean is implied; the source option that would make `tinyint(1)` a boolean is rejected for every sink) |
| `smallint` | `INTEGER` |
| `mediumint`, `int`, `integer` | `INTEGER`, or `BIGINT` when `unsigned` |
| `bigint` | `BIGINT`, or `NUMERIC(20,0)` when `unsigned` |
| `decimal(p,s)`, `numeric`, `dec`, `fixed` | `NUMERIC(p,s)`; `zerofill` is dropped, an out-of-range precision or scale is refused |
| `float` | `REAL` |
| `double`, `real` | `DOUBLE PRECISION` |
| `char(n)` | `CHAR(n)`, or `TEXT` above 10000 |
| `varchar(n)` | `VARCHAR(n)`, or `TEXT` above 10485760 |
| `tinytext`, `text`, `mediumtext`, `longtext` | `TEXT` |
| `binary`, `varbinary`, `tinyblob`, `blob`, `mediumblob`, `longblob` | `BYTEA` |
| `date` | `DATE` |
| `datetime(p)`, `timestamp(p)` | `TIMESTAMP[(p)] WITHOUT TIME ZONE` (the session `TimeZone` follows `source.server-time-zone`) |
| `json` | `JSON` — not `JSONB`, which would reorder keys and re-render numbers. Both paths deliver the document as text and pgx sends it verbatim, so the stored document is the one the source produced |

Refused, at table creation and at every column change: `enum`, `set`, and `bit` — the value they reach the sink with is not verified in this release, because the snapshot path and the binlog path render them differently (an `enum` arrives as the member label on the snapshot path and as the member index on the binlog path, a `set` as its label list and as its bitmask, and `bit(N)` as the packed bytes and as an integer of N bits). Build those targets by hand and choose the representation yourself; the sink will not guess on the strength of one path. `time` is refused because MySQL spans negative and over-24-hour values that PostgreSQL `time` cannot hold, and the spatial types are refused because they need a PostGIS extension this release does not require.

The MySQL zero date (`0000-00-00`, `0000-00-00 00:00:00`) has no PostgreSQL value either. A zero-date **default** is left out of a created column, and a zero-date **value** is passed through unchanged so the server rejects the row: the sink does not turn it into `NULL` and does not retype the column to `TEXT`, because either would store something the source never had. The failing batch stops the job with the server's SQLSTATE and the table name, and the checkpoint does not advance past it.

## Limits in This Release

- No table is created and no column is changed until `auto-create-table` or `schema.change.enabled` says so; the vocabulary of applied changes is the table in [Schema Evolution](#schema-evolution) and nothing else. `CREATE INDEX CONCURRENTLY` is never emitted, because a transaction is how the sink applies a change and `CONCURRENTLY` cannot run inside one.
- A created table carries the columns, the key, the expressible unique constraints, and the portable defaults of the source definition. Character set, collation, comment, partitioning, `AUTO_INCREMENT` generation, and plain secondary indexes are not reproduced.
- A target table is validated as a whole only when the source announces its definition — on a first scan, on a snapshot, and on a `CREATE TABLE` — not once per restart and not on every statement. `ADD`, `DROP`, `RENAME`, `CHANGE`, and `MODIFY` carry no table definition, so each verifies only the column it changes, and a target column they do not touch can stay out of step with the source until a write refuses it. A restart replays from the checkpoint and announces only the tables still being snapshotted, so a target that changed while the job was stopped is caught by the first write it refuses, and a target that is only wider keeps accepting rows. Re-validating at start-up would have to compare the target with the source definition as of the checkpoint, which the checkpoint does not store: the current source definition can be ahead of the target by a change that has not been replayed yet, and refusing it would stop a job that is about to repair it.
- `ENUM`, `SET`, `BIT`, `TIME`, and the spatial types are refused rather than mapped (see [Column Type Mapping](#column-type-mapping)), and the MySQL zero date fails the row that carries it.
- A restated column change (`CHANGE`, `MODIFY`, multi-column `ADD COLUMN`) is read from the source catalogue as it stands when the change is applied, not as it stood when the statement ran. A replayed change for a column the source no longer shows stops the job instead of being guessed, and only a verified widening of an existing column is ever applied.
- The `DROP COLUMN` guard inspects the constraints and indexes of the target table. A view, a materialized view, a generated column, or an expression index that depends on the column is left to PostgreSQL's default `RESTRICT`, which refuses the drop inside the sink's own transaction; nothing is dropped with `CASCADE`.
- A `transform.projection` names columns, not their history: after a source rename the renamed column falls out of a projection that lists the old name, and a projection that lists only the new name makes the following row fail. See [Schema Evolution](#schema-evolution).
- No automatic retraction of rows that a filter or projection drops.
- Unsigned `BIGINT` values above `MaxInt64` still arrive as decimal text and are written into `NUMERIC(20,0)`; a hand-made `BIGINT` target column rejects them with `22003`.

## Integration Test

The Go integration test is build-tagged and skips when no server is configured:

```bash
PG_DSN='postgres://cdc:cdc@127.0.0.1:5432/cdc?sslmode=disable' \
  go test -tags postgres_integration ./connector/sink/postgresql/ -v
```

Without `PG_DSN` the test reports `SKIP`, which means nothing was verified against a server. The suite covers the DML path, the value shapes this document describes (unsigned `BIGINT`, `DECIMAL`, the temporal types, `BYTEA`, `JSON`, `NULL`, and the rejected zero date), and the DDL path (create, add, refuse, change, modify, drop, key-column guard, view dependency, index skip, truncate, drop table) against a real PostgreSQL 16 instance.

## Related Docs

- [QuickStart](../QuickStart.md)
- [Pipeline](../pipeline.md)
- Source connectors
  - [MySQL Source](mysql-source.md)
- Sink connectors
  - [Stdout Sink](stdout-sink.md)
  - [MySQL Sink](mysql-sink.md)
  - [Kafka Sink](kafka-sink.md)
  - [Doris Sink](doris-sink.md)
