#!/usr/bin/env python3
"""Type + timezone integration suite (TP-D01..).

Asserts MySQL→MySQL round-trip for common column types, with special focus on
DATE / TIME / DATETIME / TIMESTAMP under session Asia/Shanghai while the
server default-time-zone is UTC (see mysql/conf.d/my.cnf).
"""
from __future__ import annotations

import json
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pymysql

from . import config as cfg

SRC = cfg.conn_kwargs(cfg.SRC_USER, cfg.SRC_PASSWORD, cfg.TYPES_SRC_DB)
SNK = cfg.conn_kwargs(cfg.SINK_USER, cfg.SINK_PASSWORD, cfg.TYPES_SNK_DB)
SRC_DB = cfg.TYPES_SRC_DB
SINK_DB = cfg.TYPES_SNK_DB
WORKDIR = cfg.WORKDIR
BINARY = cfg.BINARY
ASSERT_TZ = cfg.ASSERT_TIME_ZONE
CK = Path(cfg.CHECKPOINT_TYPES_PATH)
YAML = WORKDIR / "pipeline.types.yaml"
LOG = WORKDIR / "cdc-types.log"
REPORT_DIR = cfg.REPORT_DIR
TABLE = "tpd01_types"
OUT = "tpd01_types_out"

# Columns compared as formatted strings (session time_zone = ASSERT_TZ).
COMPARE_COLS = [
    "id",
    "c_tinyint", "c_tinyint_u", "c_smallint", "c_int", "c_bigint", "c_bigint_u",
    "c_decimal", "c_numeric", "c_float", "c_double",
    "c_char", "c_varchar", "c_text",
    "c_date", "c_time", "c_datetime", "c_timestamp",
    "c_json", "c_enum", "c_set", "c_bit", "c_bool", "c_blob", "c_binary", "c_null_int",
]

SELECT_FMT = """
SELECT
  id,
  CAST(c_tinyint AS CHAR) AS c_tinyint,
  CAST(c_tinyint_u AS CHAR) AS c_tinyint_u,
  CAST(c_smallint AS CHAR) AS c_smallint,
  CAST(c_int AS CHAR) AS c_int,
  CAST(c_bigint AS CHAR) AS c_bigint,
  CAST(c_bigint_u AS CHAR) AS c_bigint_u,
  CAST(c_decimal AS CHAR) AS c_decimal,
  CAST(c_numeric AS CHAR) AS c_numeric,
  CAST(c_float AS CHAR) AS c_float,
  CAST(c_double AS CHAR) AS c_double,
  RTRIM(CAST(c_char AS CHAR)) AS c_char,
  CAST(c_varchar AS CHAR) AS c_varchar,
  CAST(c_text AS CHAR) AS c_text,
  DATE_FORMAT(c_date, '%Y-%m-%d') AS c_date,
  TIME_FORMAT(c_time, '%H:%i:%s.%f') AS c_time,
  DATE_FORMAT(c_datetime, '%Y-%m-%d %H:%i:%s.%f') AS c_datetime,
  IFNULL(DATE_FORMAT(c_timestamp, '%Y-%m-%d %H:%i:%s.%f'), 'NULL') AS c_timestamp,
  IFNULL(CAST(c_json AS CHAR), 'NULL') AS c_json,
  CAST(c_enum AS CHAR) AS c_enum,
  CAST(c_set AS CHAR) AS c_set,
  LPAD(HEX(c_bit), 2, '0') AS c_bit,
  CAST(c_bool AS CHAR) AS c_bool,
  IFNULL(HEX(c_blob), 'NULL') AS c_blob,
  HEX(c_binary) AS c_binary,
  IFNULL(CAST(c_null_int AS CHAR), 'NULL') AS c_null_int
FROM `{{db}}`.`{{table}}`
ORDER BY id
"""

results: list["TPResult"] = []
proc: subprocess.Popen | None = None


@dataclass
class TPResult:
    id: str
    title: str
    action: str
    expected_desc: str
    expected_rows: list[dict]
    actual_rows: list[dict]
    columns: list[str]
    extra_md: str = ""
    passed: bool = False
    compare_md: str = ""


def conn(dbcfg, timezone: str | None = None):
    c = pymysql.connect(**dbcfg, cursorclass=pymysql.cursors.DictCursor, autocommit=True)
    if timezone:
        with c.cursor() as cur:
            cur.execute(f"SET time_zone = '{timezone}'")
    return c


def q(dbcfg, sql: str, args=None, timezone: str | None = ASSERT_TZ) -> list[dict]:
    with conn(dbcfg, timezone=timezone) as c:
        with c.cursor() as cur:
            if args is None:
                cur.execute(sql)
            else:
                cur.execute(sql, args)
            return list(cur.fetchall()) if cur.description else []


def exec_sql(dbcfg, sql: str, args=None, timezone: str | None = ASSERT_TZ):
    with conn(dbcfg, timezone=timezone) as c:
        with c.cursor() as cur:
            if args is None:
                cur.execute(sql)
            else:
                cur.execute(sql, args)


def table_exists(dbcfg, database: str, table: str) -> bool:
    rows = q(
        {**dbcfg, "database": database},
        "SELECT 1 AS ok FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
        (database, table),
        timezone=None,
    )
    return bool(rows)


def dump_fmt(dbcfg, database: str, table: str) -> list[dict]:
    if not table_exists(dbcfg, database, table):
        return []
    sql = SELECT_FMT.replace("{{db}}", database).replace("{{table}}", table)
    rows = q(dbcfg, sql)
    # normalize pymysql values to str
    out = []
    for r in rows:
        out.append({c: ("NULL" if r.get(c) is None else str(r.get(c))) for c in COMPARE_COLS})
    return out


def md_table(rows: list[dict], columns: list[str] | None = None) -> str:
    if not rows:
        return "_(empty table / table missing)_"
    cols = columns or list(rows[0].keys())
    # keep wide tables readable: if many cols, show transposed for single-row
    if len(rows) <= 2 and len(cols) > 8:
        lines = ["| Column | " + " | ".join(f"row{i+1}" for i in range(len(rows))) + " |",
                 "| --- | " + " | ".join(["---"] * len(rows)) + " |"]
        for c in cols:
            cells = [str(rows[i].get(c, "")) for i in range(len(rows))]
            lines.append("| " + c + " | " + " | ".join(cells) + " |")
        return "\n".join(lines)
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for r in rows:
        lines.append("| " + " | ".join(str(r.get(c, "")) for c in cols) + " |")
    return "\n".join(lines)


def soft_json_equal(ev: str, av: str) -> bool:
    if ev == av:
        return True
    if ev == "NULL" or av == "NULL":
        return ev == av
    try:
        return json.loads(ev) == json.loads(av)
    except Exception:
        return False


def col_compare(expected: dict, actual: dict, columns: list[str]) -> tuple[bool, str]:
    lines = ["| Column | Expected | Actual | Verdict |", "| --- | --- | --- | --- |"]
    ok = True
    for c in columns:
        ev = expected.get(c, "")
        av = actual.get(c, "")
        if c == "c_json":
            hit = soft_json_equal(ev, av)
        elif c in ("c_float", "c_double") and ev not in ("", "NULL") and av not in ("", "NULL"):
            try:
                hit = abs(float(ev) - float(av)) < 1e-4
            except ValueError:
                hit = ev == av
        elif c == "c_time" and ev and av:
            hit = ev.rstrip("0") == av.rstrip("0") or ev == av
        else:
            hit = ev == av
        if not hit:
            ok = False
        lines.append(f"| `{c}` | `{ev}` | `{av}` | {'✅' if hit else '❌'} |")
    lines.append(f"| Summary | | | **{'✅ PASS' if ok else '❌ FAIL'}** |")
    return ok, "\n".join(lines)


def record(tp: TPResult, override: bool | None = None, compare_md: str | None = None):
    if compare_md is not None:
        tp.compare_md = compare_md
        tp.passed = bool(override)
    elif tp.expected_rows and tp.actual_rows and len(tp.expected_rows) == 1 and len(tp.actual_rows) == 1:
        ok, cmp_md = col_compare(tp.expected_rows[0], tp.actual_rows[0], tp.columns)
        tp.passed = ok if override is None else override
        tp.compare_md = cmp_md
    else:
        # multi-row: pairwise by id
        exp_by = {str(r["id"]): r for r in tp.expected_rows}
        act_by = {str(r["id"]): r for r in tp.actual_rows}
        keys = sorted(set(exp_by) | set(act_by), key=lambda x: int(x) if x.isdigit() else x)
        ok_all = set(exp_by) == set(act_by)
        blocks = []
        for k in keys:
            if k not in exp_by:
                ok_all = False
                blocks.append(f"#### id={k}\nexpected missing, actual has row\n")
                continue
            if k not in act_by:
                ok_all = False
                blocks.append(f"#### id={k}\nactual missing\n")
                continue
            ok, cmp_md = col_compare(exp_by[k], act_by[k], tp.columns)
            ok_all = ok_all and ok
            blocks.append(f"#### id={k}\n{cmp_md}\n")
        tp.passed = ok_all if override is None else override
        tp.compare_md = "\n".join(blocks)
    results.append(tp)
    print(f"[{tp.id}] {'PASS' if tp.passed else 'FAIL'} {tp.title}", flush=True)


def start_job():
    global proc
    LOG.write_text("")
    proc = subprocess.Popen([BINARY, str(YAML)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    def pump():
        assert proc and proc.stdout
        with LOG.open("a") as f:
            for line in proc.stdout:
                f.write(line)

    threading.Thread(target=pump, daemon=True).start()


def stop_job():
    global proc
    if proc and proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    proc = None


def wait_until(predicate, timeout=120, interval=1.0, desc="condition"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            if predicate():
                return True
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(interval)
    raise TimeoutError(f"timeout waiting for {desc}: {last}")


def expected_row1() -> dict:
    """Wall-clock expectations under session +08:00 after seed."""
    return {
        "id": "1",
        "c_tinyint": "-12",
        "c_tinyint_u": "200",
        "c_smallint": "-30000",
        "c_int": "123456789",
        "c_bigint": "-9007199254740991",
        "c_bigint_u": "18446744073709551615",
        "c_decimal": "1234.5678",
        "c_numeric": "9876543210.123456",
        "c_float": "3.1415",
        "c_double": "2.718281828459",
        "c_char": "abcdefgh",
        "c_varchar": "hello-world",
        "c_text": "text-line-1\ntext-line-2",
        "c_date": "2024-03-15",
        "c_time": "13:45:30.123000",
        "c_datetime": "2024-03-15 13:45:30.123456",
        "c_timestamp": "2024-03-15 13:45:30.123456",
        "c_json": '{"k": "v", "n": 1}',  # MySQL may reorder keys — handled in compare
        "c_enum": "green",
        "c_set": "x,z",
        "c_bit": "AA",  # 10101010 => 0xAA
        "c_bool": "1",
        "c_blob": "DEADBEEF",
        "c_binary": "01020304",
        "c_null_int": "NULL",
    }


def expected_row2() -> dict:
    return {
        "id": "2",
        "c_tinyint": "0",
        "c_tinyint_u": "0",
        "c_smallint": "0",
        "c_int": "0",
        "c_bigint": "0",
        "c_bigint_u": "0",
        "c_decimal": "0.0000",
        "c_numeric": "0.000000",
        "c_float": "0",
        "c_double": "0",
        "c_char": "",
        "c_varchar": "",
        "c_text": "",
        "c_date": "1970-01-01",
        "c_time": "00:00:00.000000",
        "c_datetime": "1970-01-01 00:00:00.000000",
        "c_timestamp": "NULL",
        "c_json": "NULL",
        "c_enum": "red",
        "c_set": "y",
        "c_bit": "00",
        "c_bool": "0",
        "c_blob": "NULL",
        "c_binary": "00000000",
        "c_null_int": "NULL",
    }


def main() -> int:
    WORKDIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    if not Path(BINARY).exists():
        raise SystemExit(f"go-cdc not found: {BINARY}")

    cfg.render_template("pipeline.types.yaml.tpl", YAML)
    try:
        exec_sql(SNK, f"DROP TABLE IF EXISTS {OUT}", timezone=None)
    except Exception:
        pass
    if CK.exists():
        CK.unlink()

    # --- TP-D01 snapshot all types ---
    start_job()
    wait_until(
        lambda: table_exists(SNK, SINK_DB, OUT) and len(dump_fmt(SNK, SINK_DB, OUT)) >= 2,
        desc="types snapshot",
    )
    time.sleep(2)
    actual = dump_fmt(SNK, SINK_DB, OUT)
    expect = [expected_row1(), expected_row2()]
    record(
        TPResult(
            id="TP-D01",
            title="Initial snapshot: full type round-trip (incl. temporal)",
            action=f"initial snapshot tpd01_types; assert session time_zone={ASSERT_TZ}",
            expected_desc="Source/sink wall-clock match at +08:00 for DATE/TIME/DATETIME/TIMESTAMP; numeric/string/JSON/BIT/BLOB match",
            expected_rows=expect,
            actual_rows=actual,
            columns=COMPARE_COLS,
            extra_md=(
                f"- Source default-time-zone expected UTC; CDC `server-time-zone=Asia/Shanghai`\n"
                f"- Assert query: `SET time_zone='{ASSERT_TZ}'` + DATE_FORMAT/HEX/CAST\n"
            ),
        )
    )

    # --- TP-D02 binlog UPDATE temporal fields ---
    exec_sql(
        SRC,
        "UPDATE tpd01_types SET "
        "c_datetime='2024-12-31 23:59:59.999999', "
        "c_timestamp='2024-12-31 23:59:59.999999', "
        "c_time='23:59:59.999', "
        "c_date='2025-01-01', "
        "c_varchar='after-binlog' "
        "WHERE id=1",
    )
    wait_until(
        lambda: any(
            r.get("c_varchar") == "after-binlog" for r in dump_fmt(SNK, SINK_DB, OUT)
        ),
        desc="binlog temporal update",
    )
    time.sleep(2)
    actual1 = next(r for r in dump_fmt(SNK, SINK_DB, OUT) if r["id"] == "1")
    expect1 = expected_row1()
    expect1.update(
        {
            "c_datetime": "2024-12-31 23:59:59.999999",
            "c_timestamp": "2024-12-31 23:59:59.999999",
            "c_time": "23:59:59.999000",
            "c_date": "2025-01-01",
            "c_varchar": "after-binlog",
        }
    )
    temporal_cols = ["id", "c_date", "c_time", "c_datetime", "c_timestamp", "c_varchar"]
    ok2, cmp2 = col_compare(expect1, actual1, temporal_cols)
    # also flag classic UTC+8 hour shift on timestamp
    ts = actual1.get("c_timestamp", "")
    shifted = ts.startswith("2025-01-01 07:") or ts.startswith("2024-12-31 15:")
    extra2 = ""
    if shifted:
        ok2 = False
        extra2 = (
            f"\n**Possible timezone skew**: actual TIMESTAMP=`{ts}`."
            "Under assert session +08:00 should remain `2024-12-31 23:59:59.999999`."
            "If ±8h appears, binlog/normalization wrote TIMESTAMP as UTC wall clock.\n"
        )
    record(
        TPResult(
            id="TP-D02",
            title="Incremental UPDATE: temporal fields stable via binlog",
            action="UPDATE id=1 date/time/datetime/timestamp (session +08:00)",
            expected_desc="Sink wall-clock matches source at +08:00; no ±8h drift",
            expected_rows=[{k: expect1[k] for k in temporal_cols}],
            actual_rows=[{k: actual1.get(k, "") for k in temporal_cols}],
            columns=temporal_cols,
            extra_md=extra2,
        ),
        override=ok2,
        compare_md=cmp2 + extra2,
    )

    # --- TP-D03 DATETIME vs TIMESTAMP timezone semantics ---
    # Insert under +08:00, then read sink under +08:00 and under UTC.
    exec_sql(
        SRC,
        "INSERT INTO tpd01_types ("
        "id, c_tinyint, c_tinyint_u, c_smallint, c_int, c_bigint, c_bigint_u,"
        "c_decimal, c_numeric, c_float, c_double, c_char, c_varchar, c_text,"
        "c_date, c_time, c_datetime, c_timestamp, c_json, c_enum, c_set, c_bit, c_bool, c_blob, c_binary, c_null_int"
        ") VALUES ("
        "3, 1,1,1,1,1,1, 1.0000,1.000000,1,1, 'tztest01','tz','tz',"
        "'2024-06-01','08:00:00.000','2024-06-01 08:00:00.000000','2024-06-01 08:00:00.000000',"
        "NULL,'blue','x', b'00000001', 1, NULL, X'FFFFFFFF', NULL)",
    )
    wait_until(
        lambda: any(r.get("id") == "3" for r in dump_fmt(SNK, SINK_DB, OUT)),
        desc="id=3 inserted",
    )
    time.sleep(2)

    def read_temporal(timezone: str) -> dict:
        sql = (
            f"SELECT id, "
            f"DATE_FORMAT(c_datetime, '%Y-%m-%d %H:%i:%s') AS c_datetime, "
            f"IFNULL(DATE_FORMAT(c_timestamp, '%Y-%m-%d %H:%i:%s'), 'NULL') AS c_timestamp "
            f"FROM `{SINK_DB}`.`{OUT}` WHERE id=3"
        )
        rows = q(SNK, sql, timezone=timezone)
        return rows[0] if rows else {}

    under_p8 = read_temporal("+08:00")
    under_utc = read_temporal("+00:00")
    # Correct MySQL semantics:
    # DATETIME is timezone-naive → same wall clock in any session TZ
    # TIMESTAMP is absolute → +08 shows 08:00, UTC shows 00:00
    expect_p8_dt, expect_p8_ts = "2024-06-01 08:00:00", "2024-06-01 08:00:00"
    expect_utc_dt, expect_utc_ts = "2024-06-01 08:00:00", "2024-06-01 00:00:00"
    ok3 = (
        str(under_p8.get("c_datetime")) == expect_p8_dt
        and str(under_p8.get("c_timestamp")) == expect_p8_ts
        and str(under_utc.get("c_datetime")) == expect_utc_dt
        and str(under_utc.get("c_timestamp")) == expect_utc_ts
    )
    cmp3 = (
        "| Read session time_zone | Field | Expected | Actual | Verdict |\n"
        "| --- | --- | --- | --- | --- |\n"
        f"| +08:00 | DATETIME | `{expect_p8_dt}` | `{under_p8.get('c_datetime')}` | "
        f"{'✅' if str(under_p8.get('c_datetime')) == expect_p8_dt else '❌'} |\n"
        f"| +08:00 | TIMESTAMP | `{expect_p8_ts}` | `{under_p8.get('c_timestamp')}` | "
        f"{'✅' if str(under_p8.get('c_timestamp')) == expect_p8_ts else '❌'} |\n"
        f"| +00:00 | DATETIME | `{expect_utc_dt}` | `{under_utc.get('c_datetime')}` | "
        f"{'✅' if str(under_utc.get('c_datetime')) == expect_utc_dt else '❌'} |\n"
        f"| +00:00 | TIMESTAMP | `{expect_utc_ts}` | `{under_utc.get('c_timestamp')}` | "
        f"{'✅' if str(under_utc.get('c_timestamp')) == expect_utc_ts else '❌'} |\n"
        f"| Summary | | | | **{'✅ PASS' if ok3 else '❌ FAIL'}** |\n"
    )
    record(
        TPResult(
            id="TP-D03",
            title="Timezone semantics: DATETIME naive / TIMESTAMP session-dependent",
            action="INSERT id=3 wall clock 2024-06-01 08:00:00 (+08:00); read sink at +08 and UTC",
            expected_desc=(
                "DATETIME is TZ-naive → 08:00 in both +08 and UTC sessions; "
                "TIMESTAMP is absolute → 08:00 at +08, 00:00 at UTC"
            ),
            expected_rows=[
                {"session": "+08:00", "datetime": expect_p8_dt, "timestamp": expect_p8_ts},
                {"session": "+00:00", "datetime": expect_utc_dt, "timestamp": expect_utc_ts},
            ],
            actual_rows=[
                {
                    "session": "+08:00",
                    "datetime": str(under_p8.get("c_datetime")),
                    "timestamp": str(under_p8.get("c_timestamp")),
                },
                {
                    "session": "+00:00",
                    "datetime": str(under_utc.get("c_datetime")),
                    "timestamp": str(under_utc.get("c_timestamp")),
                },
            ],
            columns=["session", "datetime", "timestamp"],
            extra_md=(
                "```mermaid\n"
                "flowchart TD\n"
                "  seed[Seed_under_plus08] --> cdc[CDC_server_time_zone_AsiaShanghai]\n"
                "  cdc --> sink[Sink_table]\n"
                "  sink --> r1[Read_plus08]\n"
                "  sink --> r2[Read_UTC]\n"
                "  r1 --> dt1[DATETIME_0800]\n"
                "  r1 --> ts1[TIMESTAMP_0800]\n"
                "  r2 --> dt2[DATETIME_0800_naive]\n"
                "  r2 --> ts2[TIMESTAMP_0000]\n"
                "```\n"
            ),
        ),
        override=ok3,
        compare_md=cmp3,
    )

    # --- TP-D04 schema types on sink (CREATE TABLE column types) ---
    sink_cols = q(
        SNK,
        "SELECT COLUMN_NAME AS name, COLUMN_TYPE AS type, IS_NULLABLE AS nullable "
        "FROM information_schema.columns "
        "WHERE table_schema=%s AND table_name=%s ORDER BY ORDINAL_POSITION",
        (SINK_DB, OUT),
        timezone=None,
    )
    by_name = {r["name"]: r for r in sink_cols}
    checks = [
        ("c_decimal", "decimal(10,4)"),
        ("c_datetime", "datetime(6)"),
        ("c_timestamp", "timestamp(6)"),
        ("c_time", "time(3)"),
        ("c_json", "json"),
        ("c_enum", "enum('red','green','blue')"),
        ("c_bigint_u", "bigint unsigned"),
    ]

    def col_type_ok(got: str, want: str) -> bool:
        """Match COLUMN_TYPE across MySQL 5.7 (display widths) and 8.0."""
        g = got.lower().strip()
        w = want.lower().strip()
        if g == w:
            return True
        # 5.7: bigint(20) unsigned / int(11) etc.
        if w == "bigint unsigned" and g.startswith("bigint") and "unsigned" in g:
            return True
        return False

    ok4 = True
    lines = ["| Column | Expected type | Actual type | Verdict |", "| --- | --- | --- | --- |"]
    for name, want in checks:
        got = str(by_name.get(name, {}).get("type", "")).lower()
        hit = col_type_ok(got, want)
        if not hit:
            ok4 = False
        lines.append(f"| {name} | `{want}` | `{got}` | {'✅' if hit else '❌'} |")
    lines.append(f"| Summary | | | **{'✅ PASS' if ok4 else '❌ FAIL'}** |")
    record(
        TPResult(
            id="TP-D04",
            title="Auto DDL: key type precision preserved",
            action="Inspect sink information_schema column types",
            expected_desc="decimal(10,4) / datetime(6) / timestamp(6) / time(3) / json / enum / bigint unsigned",
            expected_rows=[{"k": n, "t": w} for n, w in checks],
            actual_rows=[{"k": n, "t": str(by_name.get(n, {}).get("type", "")).lower()} for n, _ in checks],
            columns=["k", "t"],
            extra_md=md_table(sink_cols, ["name", "type", "nullable"]) + "\n",
        ),
        override=ok4,
        compare_md="\n".join(lines),
    )

    stop_job()
    report = write_report()
    failed = [r for r in results if not r.passed]
    print("REPORT", report)
    print("PASS", sum(1 for r in results if r.passed), "FAIL", len(failed))
    return 0 if not failed else 1


def write_report() -> Path:
    passed = sum(1 for r in results if r.passed)
    failed = len(results) - passed
    path = cfg.REPORT_TYPES
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        "# go-cdc types / timezone integration test report\n",
        "## 0. Overview\n",
        f"- Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- MySQL：`{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}`（default-time-zone=UTC）",
        f"- CDC `server-time-zone`: Asia/Shanghai; assert session: `{ASSERT_TZ}`",
        f"- Source: `{SRC_DB}.{TABLE}` → sink: `{SINK_DB}.{OUT}`",
        f"- **Summary: pass {passed} / fail {failed} / total {len(results)}** → "
        f"{'**all passed**' if failed == 0 else '**failures present**'}\n",
        "| TP | Name | Result |",
        "| --- | --- | --- |",
    ]
    for r in results:
        lines.append(f"| {r.id} | {r.title} | {'✅ PASS' if r.passed else '❌ FAIL'} |")
    lines += [
        "",
        "## Type coverage matrix\n",
        "| Category | Columns |",
        "| --- | --- |",
        "| Integer | tinyint / tinyint unsigned / smallint / int / bigint / bigint unsigned |",
        "| Decimal | decimal(10,4) / numeric(18,6) / float / double |",
        "| String | char / varchar / text |",
        "| Temporal | date / time(3) / datetime(6) / timestamp(6) |",
        "| Other | json / enum / set / bit / bool / blob / binary / NULL |",
        "",
    ]
    for r in results:
        lines += [
            f"## {r.id} {r.title}\n",
            f"- **Result: {'✅ PASS' if r.passed else '❌ FAIL'}**",
            f"- **Action:** {r.action}",
            f"- **Expected:** {r.expected_desc}\n",
            "### Expected\n",
            md_table(r.expected_rows),
            "\n### Actual\n",
            md_table(r.actual_rows),
            "\n### Compare\n",
            r.compare_md,
        ]
        if r.extra_md:
            lines += ["\n### Notes\n", r.extra_md]
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        stop_job()
