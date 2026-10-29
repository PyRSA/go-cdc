# PostgreSQL Sink Connector

Applies pipeline changes to a pre-created PostgreSQL table as current-state writes (`INSERT ... ON CONFLICT` / `DELETE` by key). This release does not create tables, does not map source types, and stops the job on schema changes.

**YAML:** top-level `sink` with `type: postgresql`.

## Version & Dependencies

| Item | Support |
| --- | --- |
| Target PostgreSQL | **16** (validated; other versions are untested in this release) |
| SQL driver | `github.com/jackc/pgx/v5` (linked into `go-cdc`) |
| Packaging | Shipped inside the `go-cdc` binary |
| Time zone | **No sink-side time-zone option**; the connection session `TimeZone` follows `source.server-time-zone` |
| Tables | Must exist before the job starts; target columns must accept the written values |

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

Options that belong to other sinks are rejected: `auto-create-table`, `schema.change.drop-truncate.enabled` (MySQL) and `brokers`, `topic`, `source-name`, `client-id`, `schema.change.emit.enabled`, `allow.auto.create.topics`, `producer-batch-max-bytes`, `flush-timeout` (Kafka), and `sql-port`, `request-timeout`, `buckets`, `replication-num`, `schema.change.enabled`, `schema.change.include`, `schema.change.wait-timeout`, `partial-update.enabled`, `delete.enabled` (Doris). Setting `auto-create-table: true` fails startup; `false` is accepted and changes nothing.

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

The source normalizes every value to `nil`, `int64`, `uint64`, `string`, or `[]byte`; float, `DECIMAL`, date/time, `ENUM`/`SET`, and `YEAR` values arrive as strings. The sink passes values through unchanged:

| Source value | Argument | Result |
| --- | --- | --- |
| `nil` | SQL `NULL` | Any nullable column |
| `int64` | `int64` | Smallint/integer/bigint/numeric |
| `uint64` ≤ `MaxInt64` | `int64` | Smallint/integer/bigint/numeric |
| `uint64` > `MaxInt64` | exact decimal text | Exact in `NUMERIC`; a `BIGINT` column fails with `22003` |
| `string` | text parameter | Parsed by the server for the target column (numeric, date, timestamp, enum, text) |
| `[]byte` | byte slice | `BYTEA`; a text column only accepts bytes that are valid in the database encoding |

No source type mapping is performed. A value the target column cannot parse fails the batch with the server's SQLSTATE (for example `22P02` invalid text representation, `22003` numeric out of range, `42804` datatype mismatch).

## Limits in This Release

- No table creation, no `CREATE TABLE`/`ALTER TABLE` handling, no type mapping: tables must exist and schema changes stop the job (see [Schema Evolution](../pipeline.md#schema-evolution)).
- No automatic retraction of rows that a filter or projection drops.
- MySQL-only column types such as unsigned `BIGINT` above `MaxInt64` and `BIT` are written as text/numeric rather than as native PostgreSQL types.

## Integration Test

The Go integration test is build-tagged and skips when no server is configured:

```bash
PG_DSN='postgres://cdc:cdc@127.0.0.1:5432/cdc?sslmode=disable' \
  go test -tags postgres_integration ./connector/sink/postgresql/ -v
```

Without `PG_DSN` the test reports `SKIP`, which means nothing was verified against a server.

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
