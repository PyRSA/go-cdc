# 数据类型 / 时区测试说明

配套 harness：`tests/integration/sinks/mysql/harness/types_it.py`  
造数：`tests/integration/sinks/mysql/fixtures/seed_types.sql`  
作业：`tests/integration/sinks/mysql/fixtures/pipeline.types.yaml.tpl`

English: [TYPES.md](./TYPES.md)

## 为什么单独一套

功能样例（TP-01..12）盖的是同步语义（过滤、主键、加表等），**几乎不覆盖列类型**。  
CDC 最容易翻车的是：

1. **TIMESTAMP vs DATETIME**（一个随时区变，一个不变）
2. **server default-time-zone=UTC** 而 **CDC `server-time-zone=Asia/Shanghai`** 时，快照 JDBC 与 binlog 解码是否墙钟一致
3. **小数精度 / unsigned / JSON / BIT / BLOB** 在自动建表与 upsert 时是否丢精度

本套样例专门打这些点；报告按列对照，出现 ±8 小时会直接标失败。

## 环境约定

| 项 | 值 |
|----|----|
| MySQL `default-time-zone` | `+00:00`（UTC，见 `sources/mysql/conf.d/my.cnf`） |
| CDC `source.server-time-zone` | `Asia/Shanghai` |
| 造数 / 断言会话 | `SET time_zone='+08:00'` |

## 覆盖矩阵

| 类别 | 列 |
|------|----|
| 整数 | `TINYINT` / `TINYINT UNSIGNED` / `SMALLINT` / `INT` / `BIGINT` / `BIGINT UNSIGNED` |
| 小数 | `DECIMAL(10,4)` / `NUMERIC(18,6)` / `FLOAT` / `DOUBLE` |
| 字符 | `CHAR(8)` / `VARCHAR(64)` / `TEXT` |
| 时间 | `DATE` / `TIME(3)` / `DATETIME(6)` / `TIMESTAMP(6)` |
| 其它 | `JSON` / `ENUM` / `SET` / `BIT(8)` / `TINYINT(1)` / `BLOB` / `BINARY(4)` / `NULL` |

## 测试点

| ID | 断言 |
|----|------|
| TP-D01 | 全量快照：全类型 round-trip，时间字段在 +08:00 下墙钟一致 |
| TP-D02 | binlog UPDATE 时间字段：目标无 ±8h 漂移 |
| TP-D03 | DATETIME **无时区**（+08 / UTC 会话墙钟相同）；TIMESTAMP 为绝对时间（UTC 会话显示 −8h → 00:00） |
| TP-D04 | 自动建表保留 `decimal(10,4)` / `datetime(6)` / `timestamp(6)` / `time(3)` / `json` / `enum` / `bigint unsigned` |

## 怎么跑

```bash
bash tests/integration/scripts/run.sh mysql          # func + types + load
# 报告：docs/test/reports/mysql-mysql-types.md
```

只跑类型套件（stack 已由 `setup.sh` 拉起时）：

```bash
bash tests/integration/scripts/run.sh mysql normal "" types
```
