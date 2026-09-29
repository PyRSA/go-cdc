# MySQL Sink Connector

Applies pipeline changes to a target MySQL as current-state writes (upsert / delete by key), with optional DDL handling.

**YAML:** top-level `sink` with `type: mysql`.

## Version & Dependencies

| Item | Support |
| --- | --- |
| Target MySQL | **5.7** and **8.0** (tested: 5.7.42 / 8.0.36) |
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
| `auto-create-table` | no | `true` | Boolean | Create database/table from inferred schema when missing |
| `write-batch-size` | no | `1000` | Integer | Flush when the batch reaches this many events (≥ 1) |
| `write-batch-interval` | no | `1s` | Duration | Time-based flush; `0` disables the timer |
| `max-retries` | no | `3` | Integer | Extra write attempts after failure (≥ 0) |
| `schema.change.drop-truncate.enabled` | no | `false` | Boolean | Whether DROP / TRUNCATE DDL is applied to the target; see [Schema Evolution](../pipeline.md#schema-evolution) |

## Behavior Notes

- Without `route`, the sink table name stays as the event’s `database.table`.
- With `route`, writes go to the mapped table.
- Batching is applied by the job runner before calling the sink.

## Related Docs

- [QuickStart](../QuickStart.md)
- [Pipeline](../pipeline.md)
- Source connectors
  - [MySQL Source](mysql-source.md)
- Sink connectors
  - [Stdout Sink](stdout-sink.md)
