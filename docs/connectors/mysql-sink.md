# MySQL Sink Connector

Applies pipeline changes to a target MySQL as current-state writes (upsert / delete by key), with optional DDL handling.

**YAML:** top-level `sink` with `type: mysql`.

## Version & Dependencies

| Item | Support |
| --- | --- |
| Target MySQL | **5.7**, **8.0** and **8.4** (CI matrix: 5.7.42 / 8.0.36 / 8.4.11) |
| MySQL 9.x | Experimental: the DML and DDL this sink emits are still accepted, but 9.x is not verified and `VECTOR` columns are not supported |
| SQL driver | `go-sql-driver/mysql` v1.7.1 (linked into `go-cdc`) |
| Packaging | Shipped inside the `go-cdc` binary |
| Time zone | **No sink-side time-zone option**; uses `source.server-time-zone` |

## Example

```yaml
sink:
  type: mysql
  hostname: 127.0.0.1
  port: 3306
  username: root
  password: secret
  auto-create-table: true
  write-batch-size: 1000
  write-batch-interval: 1s
  max-retries: 3
  schema.change.drop-truncate.enabled: false
```

See [`configs/demo-mysql.yaml`](../../configs/demo-mysql.yaml). Target table names usually come from [`route`](../pipeline.md#route).

## Options

| Option | Required | Default | Type | Description |
| --- | --- | --- | --- | --- |
| `type` | yes | — | String | Must be `mysql` |
| `hostname` | yes | — | String | Target host |
| `port` | no | `3306` | Integer | Port |
| `username` | yes | — | String | User |
| `password` | yes | — | String | Password |
| `auto-create-table` | no | `true` | Boolean | Create database/table from inferred schema when missing (columns after projection/route, plus comments, charset/collation, defaults, secondary/unique indexes; no FK/ENGINE/partition; `AUTO_INCREMENT` starts at 1) |
| `write-batch-size` | no | `1000` | Integer | Flush when the batch reaches this many events (≥ 1) |
| `write-batch-interval` | no | `1s` | Duration | Time-based flush; `0` disables the timer |
| `max-retries` | no | `3` | Integer | Extra write attempts after failure (≥ 0) |
| `schema.change.drop-truncate.enabled` | no | `false` | Boolean | Whether DROP / TRUNCATE DDL is applied to the target; see [Schema Evolution](../pipeline.md#schema-evolution) |

## Behavior Notes

- Without `route`, the sink table name stays as the event’s `database.table`.
- With `route`, writes go to the mapped table.
- Batching is applied by the job runner before calling the sink.
- JDBC pool defaults (code, not YAML): `MaxIdleConns=1`, `ConnMaxLifetime=30m`, `ConnMaxIdleTime=10m`. Write retries stay on `max-retries` only.
- Upserts keep `INSERT ... ON DUPLICATE KEY UPDATE col=VALUES(col)`. The alias form is deprecated since MySQL 8.0.20 but still accepted on 8.4 and 9.x; it is kept because the alias-free form does not run on 5.7.
- `auto-create-table` copies each column's `CHARSET` / `COLLATE` and the table collation from the source verbatim, so the target must support them. Replicating an 8.4 source (`utf8mb4_0900_ai_ci`) into a 5.7 target fails with `ER_UNKNOWN_COLLATION` (1273); pre-create the table or align the collation on the source side.
- A `VECTOR` column (MySQL 9.x) is rejected wherever its schema could be introduced. Catalogue discovery fails the job for a captured table that has one, naming the database, table and column, and so does the `DescribeTable` the source runs when a `CREATE TABLE` arrives in the binlog: neither path falls back to the binlog-derived schema, because the snapshot and binlog paths would disagree on the column's value type. One gap remains — the default resume path does not re-read the catalogue, so a table that already carries a `VECTOR` column is not re-validated when a job resumes without newly added tables.

## Related Docs

- [QuickStart](../QuickStart.md)
- [Pipeline](../pipeline.md)
- Source connectors
  - [MySQL Source](mysql-source.md)
- Sink connectors
  - [Stdout Sink](stdout-sink.md)
