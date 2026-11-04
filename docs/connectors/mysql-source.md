# MySQL Source Connector

Reads an initial snapshot and binlog changes from MySQL, and emits a unified change stream for the pipeline.

**YAML:** top-level `source` with `type: mysql`.

## Version & Dependencies

| Item | Support |
| --- | --- |
| Source MySQL | **5.7**, **8.0** and **8.4** (CI matrix: 5.7.42 / 8.0.36 / 8.4.11) |
| MySQL 9.x | Experimental: no CI row; the connector avoids the statements 8.4 removed, but 9.x is not verified |
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
| `scan.newly-added-table.enabled` | no | `false` | Boolean | On resume with `acked_offset` and `initial` mode: whether config-diff tables are included (stop job, edit `tables`, restart from the same checkpoint) |
| `scan.newly-added-table.snapshot.enabled` | no | `true` | Boolean | When parent is `true`: snapshot+spill for diff tables (`true`) or binlog-only after a warn (`false`); ignored when parent is `false` |
| `scan.binlog.newly-added-table.enabled` | no | `false` | Boolean | While reading binlog: accept matching runtime `CREATE TABLE` and following rows without backfill |
| `schema-change.enabled` | no | `true` | Boolean | Whether to capture DDL for schema evolution |
| `connect.timeout` | no | `30s` | Duration | Connect timeout (≥ `250ms`) |
| `heartbeat.interval` | no | `30s` | Duration | Master heartbeat period for the binlog dump connection |
| `jdbc.properties.*` | no | — | String | Extra JDBC properties, e.g. `jdbc.properties.useSSL: "false"` |

### Connection defaults (code, not YAML)

- Snapshot JDBC pool: `MaxIdleConns=1`, `ConnMaxLifetime=30m`, `ConnMaxIdleTime=10m`.
- Binlog client `ReadTimeout` defaults to `2 × heartbeat.interval` (or `60s` if heartbeat is unset). In go-mysql v1.16.0 the deadline refreshes after each packet; this does **not** stop the server from closing the dump under `net_write_timeout` when the consumer is slow to read.
- Runtime dump disconnects use canal reconnect and its in-memory position; job restart uses the checkpoint file. Neither jumps to the latest binlog.

### `scan.startup.mode`

| Value | Meaning |
| --- | --- |
| `initial` | Snapshot then binlog (default); may resume from checkpoint |
| `snapshot` | Snapshot only, then exit |
| `earliest` | Skip snapshot; read from earliest accessible binlog |
| `latest` | Skip snapshot; read from current position |
| `specific-offset` | Start from file/pos or GTID |
| `timestamp` | Start from `scan.startup.timestamp-millis` |

## Version Compatibility

The server release is read once per connection (`SELECT VERSION()`) and decides which replication statements the source sends:

| Purpose | MySQL 5.7 / 8.0 | MySQL 8.4+ |
| --- | --- | --- |
| Position for `latest`, `timestamp` and the snapshot low watermark | `SHOW MASTER STATUS` | `SHOW BINARY LOG STATUS` |
| Replica ids used by automatic `server-id` | `SHOW SLAVE HOSTS` | `SHOW REPLICAS` |

- A release string that cannot be parsed (connection proxies, forks) tries the 8.4 spelling first and retries the legacy one once after `ER_PARSE_ERROR` (1064). A parsed release never falls back: a rejected statement is a real failure and is reported with the statement name and the server version.
- MariaDB is recognised by name and always uses the legacy spelling; its `10.x` numbers are never compared against MySQL thresholds.
- An explicit `server-id` skips the replica query entirely.
- `caching_sha2_password` — the 8.4 default — works without TLS: both linked drivers request the server RSA public key for the full-authentication handshake. Keep `caching_sha2_password_auto_generate_rsa_keys` enabled, or pass `jdbc.properties.tls: "true"`.
- `binlog_format` is deprecated (8.0.34+) but still reported by 5.7, 8.0, 8.4 and 9.x. Both `binlog_format=ROW` and `binlog_row_image=FULL` are required. A variable the server does not report, or a failed `SHOW VARIABLES`, fails startup instead of being assumed to be ROW: a STATEMENT or MIXED source would otherwise stream events this capture cannot replay.
- `VECTOR(n)` columns (MySQL 9.x) are not captured. The snapshot and binlog paths would emit different Go types for the same column, so the source rejects the table and names the database, table and column whenever catalogue metadata is read for a table in the capture set (current `source.tables` match **or** checkpoint `captured_tables`, so a narrowed selector cannot leave a previously captured VECTOR table unchecked):
  - catalogue discovery (`ListTables` on snapshot / newly-added resume);
  - runtime `CREATE TABLE` (`DescribeTable`; a VECTOR column stops the job instead of falling back to the binlog-derived schema);
  - default resume with `acked_offset` (`DescribeTable` on each already-captured table before binlog replay). Non-Doris sinks only validate here and do not emit an extra Schema batch; Doris still re-emits Schema so the sink can re-check the target.

## Related Docs

- [QuickStart](../QuickStart.md)
- [Pipeline](../pipeline.md)
- Sink connectors
  - [MySQL Sink](mysql-sink.md)
  - [Stdout Sink](stdout-sink.md)
