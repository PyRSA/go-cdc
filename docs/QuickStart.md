# Welcome to go-cdc

go-cdc describes a sync job in a single YAML file: capture full and incremental changes from a database, apply optional transform and route, then write to a sink. One process, one config, one entrypoint: `go-cdc`.

What you get today:

- End-to-end YAML jobs (source → transform / route → sink)
- MySQL source: snapshot + binlog
- Sinks: stdout (JSON lines) and MySQL
- Column projection, row filter, table route, schema-change policy, file checkpoints

## Requirements

| Item | Requirement |
| --- | --- |
| OS | Linux / macOS / Windows |
| Go (build only) | **1.25+** (see `go.mod`) |
| Runtime | The `go-cdc` binary only |
| Databases | **MySQL 5.7** and **MySQL 8.0** (CI covers 5.7.42 / 8.0.36) |
| Source binlog | `binlog_format=ROW`, `binlog_row_image=FULL` |
| Source privileges | At least `SELECT` on captured tables plus privileges to read the binlog |

Libraries linked into the binary (no separate connector packages to install):

| Component | Dependency |
| --- | --- |
| MySQL protocol / binlog | `github.com/go-mysql-org/go-mysql` **v1.16.0** |
| MySQL SQL driver | `github.com/go-sql-driver/mysql` **v1.7.1** |

> `server-id` must be unique in the MySQL topology. If omitted, go-cdc picks one in the 5400–6400 range.

## Supported Connectors

| Connector | Role | Doc |
| --- | --- | --- |
| MySQL | Source | [connectors/mysql-source.md](connectors/mysql-source.md) |
| MySQL | Sink | [connectors/mysql-sink.md](connectors/mysql-sink.md) |
| Stdout | Sink | [connectors/stdout-sink.md](connectors/stdout-sink.md) |

Pipeline concepts (transform, projection, route, schema evolution): [pipeline.md](pipeline.md).

## How to Use

1. Prepare a MySQL instance that meets the requirements above.
2. Write a YAML file (use the sample below or `configs/demo-*.yaml`).
3. Build and start:

```bash
make build
./bin/go-cdc <your-pipeline.yaml>
```

The process parses YAML, loads a checkpoint if present, then runs snapshot and/or binlog until it receives a stop signal (or finishes in `snapshot` mode).

## Write Your First Pipeline (Stdout)

Print MySQL changes to stdout to verify capture.

### 1. Prepare a source table (example)

```sql
CREATE DATABASE IF NOT EXISTS demo_db;
USE demo_db;
CREATE TABLE orders (
  id BIGINT PRIMARY KEY,
  city VARCHAR(32),
  amt  DECIMAL(10,2)
);
INSERT INTO orders VALUES (1, 'hz', 9.50);
```

### 2. Write the YAML

Save as `pipeline-stdout.yaml` (or edit [`configs/demo-stdout.yaml`](../configs/demo-stdout.yaml)):

```yaml
source:
  type: mysql
  hostname: 127.0.0.1
  port: 3306
  username: root
  password: secret
  tables: demo_db\..*
  server-id: 5401
  server-time-zone: Asia/Shanghai
  scan.startup.mode: initial

sink:
  type: stdout

pipeline:
  name: demo-stdout

checkpoint:
  storage: file
  interval: 10s
  file-path: ./checkpoints
```

Replace `hostname` / `username` / `password` / `tables` for your environment.

### 3. Start the job from YAML

```bash
make build
./bin/go-cdc pipeline-stdout.yaml
```

Stdout prints one JSON object per line (`"op":"r"` for snapshot rows, `"op":"c"` for inserts, …). See [Stdout Sink](connectors/stdout-sink.md).

In another session, `INSERT` / `UPDATE` / `DELETE` on the source table; the terminal should keep printing events.

Stop with `Ctrl+C` (SIGINT) or SIGTERM; go-cdc flushes and saves the checkpoint when possible.

### 4. Next steps

| Goal | Doc |
| --- | --- |
| Sync to another MySQL | [MySQL Sink](connectors/mysql-sink.md) and [`configs/demo-mysql.yaml`](../configs/demo-mysql.yaml) |
| Source options (startup mode, chunks, …) | [MySQL Source](connectors/mysql-source.md) |
| Projection / filter / route / schema evolution | [Pipeline](pipeline.md) |

## Related Docs

- [Pipeline](pipeline.md)
- Source connectors
  - [MySQL Source](connectors/mysql-source.md)
- Sink connectors
  - [MySQL Sink](connectors/mysql-sink.md)
  - [Stdout Sink](connectors/stdout-sink.md)
