# MySQL Source Connector

Reads an initial snapshot and binlog changes from MySQL, and emits a unified change stream for the pipeline.

**YAML:** top-level `source` with `type: mysql`.

## Version & Dependencies

| Item | Support |
| --- | --- |
| Source MySQL | **5.7** and **8.0** (tested: 5.7.42 / 8.0.36) |
| Binlog | `binlog_format=ROW`, `binlog_row_image=FULL` |
| Client libraries | `go-mysql` v1.16.0, `go-sql-driver/mysql` v1.7.1 (linked into `go-cdc`) |
| Packaging | Shipped inside the `go-cdc` binary; no separate connector install |

## Example

```yaml
source:
  type: mysql
  hostname: 127.0.0.1
  port: 3306
  username: root
  password: secret
  tables: demo_db\..*, other_db\.orders
  tables.exclude: demo_db\.skip_.*
  server-id: 5401
  server-time-zone: Asia/Shanghai
  scan.startup.mode: initial
  scan.incremental.snapshot.chunk.size: 8096
  scan.snapshot.fetch.size: 1024
  schema-change.enabled: true
  connect.timeout: 30s
  heartbeat.interval: 30s
  jdbc.properties.useSSL: "false"
```

## Options

| Option | Required | Default | Type | Description |
| --- | --- | --- | --- | --- |
| `type` | recommended | — | String | Must be `mysql` |
| `hostname` | yes | — | String | Source host |
| `port` | no | `3306` | Integer | Port |
| `username` | yes | — | String | User |
| `password` | yes | — | String | Password |
| `tables` | yes | — | String | Comma-separated regexes matching full `database.table`; escape `.` as `\.` in regex |
| `tables.exclude` | no | — | String | Same syntax as `tables` |
| `server-id` | no | auto | Integer | Positive unique ID; if omitted, auto-picked in 5400–6400 |
| `server-time-zone` | no | `UTC` | String | Source session time zone; **also used by the MySQL sink connection** |
| `scan.startup.mode` | no | `initial` | String | See below |
| `scan.startup.specific-offset.file` | conditional | — | String | Required for `specific-offset` unless GTID is set |
| `scan.startup.specific-offset.pos` | conditional | — | Integer | Binlog position (≥ 0) |
| `scan.startup.specific-offset.gtid-set` | no | — | String | GTID set; if set, file/pos are optional |
| `scan.startup.specific-offset.skip-events` | no | `0` | Integer | Skip first N binlog events after connect |
| `scan.startup.specific-offset.skip-rows` | no | `0` | Integer | Skip first N row events |
| `scan.startup.timestamp-millis` | conditional | — | Long | Required for `timestamp` mode (milliseconds) |
| `scan.incremental.snapshot.chunk.size` | no | `8096` | Integer | Snapshot chunk size in rows (≥ 1) |
| `scan.incremental.snapshot.chunk.key-column` | no | first PK | String | Chunk key column |
| `scan.snapshot.fetch.size` | no | `1024` | Integer | Rows fetched per page inside a chunk (≥ 1) |
| `schema-change.enabled` | no | `true` | Boolean | Whether to capture DDL for schema evolution |
| `connect.timeout` | no | `30s` | Duration | Connect timeout (≥ `250ms`) |
| `heartbeat.interval` | no | `30s` | Duration | Heartbeat interval |
| `jdbc.properties.*` | no | — | String | Extra JDBC properties, e.g. `jdbc.properties.useSSL: "false"` |

### `scan.startup.mode`

| Value | Meaning |
| --- | --- |
| `initial` | Snapshot then binlog (default); may resume from checkpoint |
| `snapshot` | Snapshot only, then exit |
| `earliest` | Skip snapshot; read from earliest accessible binlog |
| `latest` | Skip snapshot; read from current position |
| `specific-offset` | Start from file/pos or GTID |
| `timestamp` | Start from `scan.startup.timestamp-millis` |

## Related Docs

- [QuickStart](../QuickStart.md)
- [Pipeline](../pipeline.md)
- Sink connectors
  - [MySQL Sink](mysql-sink.md)
  - [Stdout Sink](stdout-sink.md)
