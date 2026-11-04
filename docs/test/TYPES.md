# Types / timezone test notes

Harness: `tests/integration/sinks/mysql/harness/types_it.py`  
Seed: `tests/integration/sinks/mysql/fixtures/seed_types.sql`  
Pipeline: `tests/integration/sinks/mysql/fixtures/pipeline.types.yaml.tpl`

Chinese: [TYPES.zh.md](./TYPES.zh.md)

## Why a separate suite

Functional cases (TP-01..12) cover sync semantics (filter, keys, add-table, …) and barely cover column types. CDC usually breaks on:

1. **TIMESTAMP vs DATETIME** (zone-aware vs naive wall clock)
2. MySQL `default-time-zone=UTC` with CDC `server-time-zone=Asia/Shanghai` — snapshot JDBC and binlog decode must keep the same wall clock
3. **Decimal / unsigned / JSON / BIT / BLOB** precision through auto-create and upsert

This suite targets those cases. Reports compare column-by-column; a ±8h shift fails the case.

## Environment

| Item | Value |
|------|-------|
| MySQL `default-time-zone` | `+00:00` (UTC; see `sources/mysql/conf.d/my.cnf`) |
| CDC `source.server-time-zone` | `Asia/Shanghai` |
| Seed / assert session | `SET time_zone='+08:00'` |

## Coverage matrix

| Category | Columns |
|----------|---------|
| Integer | `TINYINT` / `TINYINT UNSIGNED` / `SMALLINT` / `INT` / `BIGINT` / `BIGINT UNSIGNED` |
| Decimal | `DECIMAL(10,4)` / `NUMERIC(18,6)` / `FLOAT` / `DOUBLE` |
| String | `CHAR(8)` / `VARCHAR(64)` / `TEXT` |
| Temporal | `DATE` / `TIME(3)` / `DATETIME(6)` / `TIMESTAMP(6)` |
| Other | `JSON` / `ENUM` / `SET` / `BIT(8)` / `TINYINT(1)` / `BLOB` / `BINARY(4)` / `NULL` |

## Not covered

`VECTOR(n)` (MySQL 9.x) is not classified: the snapshot path would return it as text while the binlog path returns raw bytes. The source therefore rejects a captured table that has a vector column whenever it reads catalogue metadata for that table (discovery, runtime `CREATE TABLE`, and default checkpoint resume), naming the database, table and column, instead of streaming values whose type differs between the two paths. The suite does not seed one.

## Test points

| ID | Assertion |
|----|-----------|
| TP-D01 | Snapshot round-trip for all types; temporal wall clock matches under +08:00 |
| TP-D02 | Binlog UPDATE of temporal columns: sink has no ±8h drift |
| TP-D03 | DATETIME is timezone-**naive** (same wall clock in +08 and UTC sessions); TIMESTAMP is absolute (UTC session shows −8h → 00:00) |
| TP-D04 | Auto-create keeps `decimal(10,4)` / `datetime(6)` / `timestamp(6)` / `time(3)` / `json` / `enum` / `bigint unsigned` |

## How to run

```bash
bash tests/integration/scripts/run.sh mysql          # func + types + load
# Report: docs/test/reports/mysql-mysql-types.md
```

Types suite only (stack already raised by `setup.sh`):

```bash
bash tests/integration/scripts/run.sh mysql normal "" types
```
