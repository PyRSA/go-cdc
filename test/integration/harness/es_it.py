#!/usr/bin/env python3
"""MySQL -> go-cdc -> Elasticsearch 7/8/9 end-to-end suite (TP-E01..TP-E10).

Drives the real binary (no fake client) through the compose MySQL service and
one Elasticsearch node - ES8 by default, ES7 when `ES_SINK_VERSION=7` /
`ES_EXPECT_MAJOR=7` are set (see `scripts/run-es7-e2e.sh`), ES9 when both are
`9` (see `scripts/run-es9-e2e.sh`) - and verifies Elasticsearch over plain HTTP:

  * `harness.es_client`   - scroll-based reads, index/mapping/detail checks
  * `harness.es_verify`   - independent re-implementation of the `_id` contract,
                            canonical values and the MySQL/ES field comparison

The pipeline under test is `fixtures/pipeline.es.yaml.tpl` (snapshot + binlog),
plus `fixtures/pipeline.es.binlog.yaml.tpl` for binlog-only startup and DDL.
Run it with `scripts/run-es-e2e.sh` (ES8), `scripts/run-es7-e2e.sh` (ES7) or
`scripts/run-es9-e2e.sh` (ES9); the report lands in
`docs/test/reports/mysql-elasticsearch.md` for ES8, `mysql-elasticsearch7.md`
for ES7 and `mysql-elasticsearch9.md` for ES9.
"""
from __future__ import annotations

import json
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import pymysql

from . import config as cfg
from . import es_verify as verify
from .es_client import ESClient
from .logutil import log

SRC = cfg.conn_kwargs(cfg.SRC_USER, cfg.SRC_PASSWORD, cfg.ES_SRC_DB)
SRC_DB = cfg.ES_SRC_DB
SNK_DB = cfg.ES_SNK_DB
WORKDIR = cfg.WORKDIR
BINARY = cfg.BINARY
CK = Path(cfg.CHECKPOINT_ES_PATH)
CK_BINLOG = Path(cfg.CHECKPOINT_ES_RESTART_PATH)
YAML = WORKDIR / "pipeline.es.yaml"
YAML_BINLOG = WORKDIR / "pipeline.es.binlog.yaml"
LOG = WORKDIR / "cdc-es.log"
LOG_BINLOG = WORKDIR / "cdc-es-binlog.log"
REPORT = cfg.REPORT_ES

ES = ESClient(
    cfg.ES_URL,
    timeout=cfg.ES_REQUEST_TIMEOUT,
    username=cfg.ES_USERNAME,
    password=cfg.ES_PASSWORD,
)

# Projected columns of each final table, with the MySQL type the sink saw.
USERS_COLS = {
    "id": "bigint",
    "name": "varchar(64)",
    "city": "varchar(16)",
    "balance": "decimal(12,2)",
}
ORDER_COLS = {
    "id": "bigint",
    "user_id": "bigint",
    "amount": "decimal(12,2)",
    "status": "varchar(16)",
}
MERGE_COLS = {
    "id": "bigint",
    "tenant_id": "varchar(16)",
    "amount": "decimal(12,2)",
    "note": "varchar(64)",
}
COMPOSITE_COLS = {
    "tenant_id": "varchar(16)",
    "seq": "int",
    "v": "varchar(32)",
}
FILTERED_COLS = {
    "id": "bigint",
    "city": "varchar(16)",
    "note": "varchar(64)",
}
TYPES_COLS = {
    "id": "bigint unsigned",
    "c_tinyint": "tinyint(4)",
    "c_small_u": "smallint unsigned",
    "c_int_u": "int unsigned",
    "c_amount": "decimal(18,6)",
    "c_ratio": "float",
    "c_score": "double",
    "c_name": "varchar(64)",
    "c_long": "varchar(600)",
    "c_note": "text",
    "c_meta": "json",
    "c_blob": "varbinary(32)",
    "c_day": "date",
    "c_seen": "datetime(6)",
    "c_clock": "time(6)",
    "c_status": "enum('new','done')",
    "c_tags": "set('a','b','c')",
    "c_year": "year",
}

DDL = {
    "tpe01_users": (
        "(id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(64) NOT NULL, "
        "city VARCHAR(16) NOT NULL, balance DECIMAL(12,2) NULL, "
        "secret VARCHAR(32) NOT NULL)"
    ),
    "tpe01_orders": (
        "(id BIGINT NOT NULL PRIMARY KEY, user_id BIGINT NOT NULL, "
        "amount DECIMAL(12,2) NOT NULL, status VARCHAR(16) NOT NULL)"
    ),
    "tpe01_merge_a": (
        "(id BIGINT NOT NULL PRIMARY KEY, tenant_id VARCHAR(16) NOT NULL, "
        "amount DECIMAL(12,2) NOT NULL, note VARCHAR(64) NOT NULL)"
    ),
    "tpe01_merge_b": (
        "(id BIGINT NOT NULL PRIMARY KEY, tenant_id VARCHAR(16) NOT NULL, "
        "amount DECIMAL(12,2) NOT NULL, note VARCHAR(64) NOT NULL)"
    ),
    "tpe01_composite": (
        "(tenant_id VARCHAR(16) NOT NULL, seq INT NOT NULL, v VARCHAR(32) NOT NULL, "
        "PRIMARY KEY (tenant_id, seq))"
    ),
    "tpe01_types": (
        "(id BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
        "c_tinyint TINYINT NOT NULL, "
        "c_small_u SMALLINT UNSIGNED NOT NULL, "
        "c_int_u INT UNSIGNED NOT NULL, "
        "c_amount DECIMAL(18,6) NOT NULL, "
        "c_ratio FLOAT NOT NULL, "
        "c_score DOUBLE NOT NULL, "
        "c_name VARCHAR(64) NOT NULL, "
        "c_long VARCHAR(600) NOT NULL, "
        "c_note TEXT NOT NULL, "
        "c_meta JSON NULL, "
        "c_blob VARBINARY(32) NOT NULL, "
        "c_day DATE NOT NULL, "
        "c_seen DATETIME(6) NOT NULL, "
        "c_clock TIME(6) NOT NULL, "
        "c_status ENUM('new','done') NOT NULL, "
        "c_tags SET('a','b','c') NOT NULL, "
        "c_year YEAR NOT NULL)"
    ),
    "tpe01_filtered": (
        "(id BIGINT NOT NULL PRIMARY KEY, city VARCHAR(16) NOT NULL, "
        "note VARCHAR(64) NOT NULL)"
    ),
}

results: list["TPResult"] = []
proc: subprocess.Popen | None = None
_log_handle = None


@dataclass
class TPResult:
    id: str
    title: str
    action: str
    expected_desc: str
    passed: bool = False
    detail_md: str = ""


# ------------------------------------------------------------------ MySQL
def _connect() -> pymysql.connections.Connection:
    conn = pymysql.connect(**SRC, autocommit=True)
    with conn.cursor() as cur:
        cur.execute("SET time_zone = '+08:00'")
    return conn


def exec_sql(sql: str, args: tuple | None = None) -> None:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args or ())
    finally:
        conn.close()


def q(sql: str, args: tuple | None = None) -> list[dict]:
    conn = pymysql.connect(**SRC, autocommit=True, cursorclass=pymysql.cursors.DictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute("SET time_zone = '+08:00'")
            cur.execute(sql, args or ())
            return list(cur.fetchall())
    finally:
        conn.close()


def table_rows(table: str, where: str = "") -> list[dict]:
    sql = f"SELECT * FROM `{SRC_DB}`.`{table}`"
    if where:
        sql += f" WHERE {where}"
    return q(sql + " ORDER BY 1")


def ensure_schema() -> None:
    """Drop and recreate the E2E source tables (idempotent, no leftover state)."""
    for name, cols in DDL.items():
        exec_sql(f"DROP TABLE IF EXISTS `{SRC_DB}`.`{name}`")
        exec_sql(f"CREATE TABLE `{SRC_DB}`.`{name}` {cols} ENGINE=InnoDB")


def seed() -> None:
    rows = [
        ("INSERT INTO `%s`.tpe01_users(id,name,city,balance,secret) VALUES (1,'alice','hz',10.50,'s1')" % SRC_DB, ()),
        ("INSERT INTO `%s`.tpe01_users(id,name,city,balance,secret) VALUES (2,'bob','sh',NULL,'s2')" % SRC_DB, ()),
        ("INSERT INTO `%s`.tpe01_orders(id,user_id,amount,status) VALUES (11,1,99.00,'paid')" % SRC_DB, ()),
        ("INSERT INTO `%s`.tpe01_orders(id,user_id,amount,status) VALUES (12,2,5.25,'new')" % SRC_DB, ()),
        ("INSERT INTO `%s`.tpe01_merge_a(id,tenant_id,amount,note) VALUES (1,'t1',1.00,'a1')" % SRC_DB, ()),
        ("INSERT INTO `%s`.tpe01_merge_a(id,tenant_id,amount,note) VALUES (2,'t1',2.00,'a2')" % SRC_DB, ()),
        ("INSERT INTO `%s`.tpe01_merge_b(id,tenant_id,amount,note) VALUES (1,'t2',1.50,'b1')" % SRC_DB, ()),
        ("INSERT INTO `%s`.tpe01_merge_b(id,tenant_id,amount,note) VALUES (2,'t2',2.50,'b2')" % SRC_DB, ()),
        ("INSERT INTO `%s`.tpe01_composite(tenant_id,seq,v) VALUES ('t1',1,'c11')" % SRC_DB, ()),
        ("INSERT INTO `%s`.tpe01_composite(tenant_id,seq,v) VALUES ('t1',2,'c12')" % SRC_DB, ()),
        ("INSERT INTO `%s`.tpe01_composite(tenant_id,seq,v) VALUES ('t2',1,'c21')" % SRC_DB, ()),
        ("INSERT INTO `%s`.tpe01_filtered(id,city,note) VALUES (1,'hz','keep')" % SRC_DB, ()),
        ("INSERT INTO `%s`.tpe01_filtered(id,city,note) VALUES (2,'sh','drop')" % SRC_DB, ()),
    ]
    for sql, args in rows:
        exec_sql(sql, args)
    exec_sql(
        f"INSERT INTO `{SRC_DB}`.tpe01_types("
        "id,c_tinyint,c_small_u,c_int_u,c_amount,c_ratio,c_score,c_name,c_long,"
        "c_note,c_meta,c_blob,c_day,c_seen,c_clock,c_status,c_tags,c_year"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            18446744073709551615,
            -7,
            65535,
            4294967295,
            "1234567890.123456",
            1.5,
            2.25,
            "widget",
            "L" * 600,
            "a text value",
            '{"b": 2, "a": 1}',
            bytes([0x00, 0xFF, 0x10]),
            "2024-01-02",
            "2024-01-02 03:04:05.123456",
            "12:34:56.123456",
            "done",
            "a,c",
            2024,
        ),
    )
    exec_sql(
        f"INSERT INTO `{SRC_DB}`.tpe01_types("
        "id,c_tinyint,c_small_u,c_int_u,c_amount,c_ratio,c_score,c_name,c_long,"
        "c_note,c_meta,c_blob,c_day,c_seen,c_clock,c_status,c_tags,c_year"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            1,
            7,
            1,
            2,
            "-1.500000",
            -0.5,
            -2.5,
            "gadget",
            "short",
            "another",
            None,
            b"",
            "1999-12-31",
            "1999-12-31 23:59:59.000000",
            "-01:02:03.000001",
            "new",
            "",
            2000,
        ),
    )


# --------------------------------------------------------------- process
def start_job(
    yaml_path: Path,
    log_path: Path,
    *,
    reset_ckpt: Path | None,
    expect_exit: bool = False,
) -> None:
    global proc, _log_handle
    stop_job()
    if reset_ckpt is not None and reset_ckpt.exists():
        reset_ckpt.unlink()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    _log_handle = log_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        [BINARY, str(yaml_path)],
        stdout=_log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if not expect_exit:
        wait_alive(log_path, desc=yaml_path.name)


def wait_alive(log_path: Path, *, timeout_s: float = 60.0, ready_after_s: float = 4.0, desc: str = "go-cdc") -> None:
    """Fail fast when the job dies at startup; otherwise wait until it is stable."""
    deadline = time.time() + timeout_s
    ready_at = time.time() + ready_after_s
    while time.time() < deadline:
        if proc is None:
            raise RuntimeError(f"{desc}: process not started")
        code = proc.poll()
        if code is not None:
            raise RuntimeError(f"{desc} exited early ({code}):\n{_tail(log_path)}")
        if time.time() >= ready_at:
            return
        time.sleep(0.25)
    raise TimeoutError(f"timeout waiting for {desc} to stay alive:\n{_tail(log_path)}")


def wait_exit(*, timeout_s: float = 120.0, desc: str = "go-cdc", log_path: Path | None = None) -> int:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if proc is None:
            raise RuntimeError(f"{desc}: process not started")
        code = proc.poll()
        if code is not None:
            return code
        time.sleep(0.25)
    detail = f": {_tail_on_one_line(log_path)}" if log_path is not None else ""
    raise TimeoutError(f"timeout waiting for {desc} to exit{detail}")


def stop_job() -> None:
    global proc, _log_handle
    if proc is not None:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        proc = None
    if _log_handle is not None:
        _log_handle.close()
        _log_handle = None


def _tail(path: Path, limit: int = 3000) -> str:
    if not path.exists():
        return "(no log)"
    return path.read_text(encoding="utf-8", errors="replace")[-limit:]


def _tail_on_one_line(path: Path, count: int = 3, limit: int = 600) -> str:
    """The last non-empty log lines joined on one line, for exception text.

    A multi-line tail would be cut by record() (it prints only the lines that
    carry a verdict) and would break the markdown table in the report.
    """
    if not path.exists():
        return "(no log)"
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return (" ; ".join(lines[-count:]) or "(empty log)")[-limit:]


def log_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""


# ----------------------------------------------------------- Elasticsearch
def assert_target(version: str | None = None) -> None:
    """Fail fast when the cluster and the fixtures disagree with the run's intent.

    The suite is version-parameterised, so three silent mismatches are possible:
    an ES7 launch pointed at an ES8 node, an ES7 run whose pipeline still renders
    the default ``sink.version: 8``, and a fixture that lost its ``version:``
    line altogether. Each one would keep passing while testing the wrong adapter,
    so all three are errors here instead of a green report.

    ``version`` is the cluster version to check; it is fetched when omitted so
    the rules can also be exercised without a cluster (see test_es_target.py).
    """
    if version is None:
        version = ES.version()
    expected = cfg.ES_EXPECT_MAJOR.rstrip(".")
    if expected:
        if version.split(".")[0] != expected:
            raise RuntimeError(
                f"expected Elasticsearch {expected}.x at {cfg.ES_URL}, got {version or 'unknown'}; "
                "check ES_URL / ES7_URL / ES9_URL / ES7_HOST_PORT / ES9_HOST_PORT"
            )
        if cfg.ES_SINK_VERSION != expected:
            raise RuntimeError(
                f"ES_SINK_VERSION={cfg.ES_SINK_VERSION} but ES_EXPECT_MAJOR={expected}; "
                f"the pipeline would not run the {expected}.x adapter"
            )
    for name in ("pipeline.es.yaml.tpl", "pipeline.es.binlog.yaml.tpl"):
        text = (cfg.FIXTURES / name).read_text(encoding="utf-8")
        if "version: ${ES_SINK_VERSION}" not in text:
            raise RuntimeError(f"{name} does not pin sink.version; it would use the sink default")


def index_for(table: str) -> str:
    return verify.index_name_for("", SNK_DB, table)


def es_docs(index: str) -> dict[str, dict]:
    ES.refresh(index)
    return ES.scan_all(index)


def wait_count(index: str, want: int, *, timeout: float = 240.0) -> None:
    def done() -> bool:
        ES.refresh(index)
        return ES.count(index) == want

    verify.wait_for(done, timeout=timeout, interval=1.0, desc=f"{index} reaches {want} document(s)")


def wait_doc(index: str, doc_id: str, predicate, *, timeout: float = 120.0, desc: str = "") -> dict:
    found: dict = {}

    def done() -> bool:
        ES.refresh(index)
        doc = ES.get_doc(index, doc_id)
        if doc is None:
            return False
        found.clear()
        found.update(doc)
        return bool(predicate(doc))

    verify.wait_for(done, timeout=timeout, interval=0.5, desc=desc or f"{index}/{doc_id[:12]}")
    return found


def expected_id(final_table: str, pks: dict[str, str], row: dict) -> str:
    return verify.expected_doc_id(SNK_DB, final_table, pks, row)


def clean_indices(prefix: str) -> list[str]:
    removed = ES.delete_indices(prefix + "*")
    if removed:
        log(f"    removed {len(removed)} index(es): {removed}")
    return removed


# --------------------------------------------------------------- checks
def check_table(table: str, final_table: str, columns: dict[str, str], pks: dict[str, str], where: str = "") -> tuple[list[str], int]:
    """Full comparison of one source table against its final index."""
    index = index_for(final_table)
    docs = es_docs(index)
    rows = table_rows(table, where)
    expected = {expected_id(final_table, pks, row): row for row in rows}
    problems: list[str] = []
    if set(docs) != set(expected):
        missing = sorted(set(expected) - set(docs))[:3]
        extra = sorted(set(docs) - set(expected))[:3]
        problems.append(f"document set differs: missing={missing} extra={extra}")
    for doc_id, row in expected.items():
        doc = docs.get(doc_id)
        if doc is None:
            continue
        problems.extend(f"{doc_id[:12]}: {item}" for item in verify.compare_row(columns, row, doc))
    return problems, len(expected)


def verdict(ok: bool) -> str:
    return "✅" if ok else "❌"


def record(tp: TPResult) -> None:
    results.append(tp)
    log(tp.id, "PASS" if tp.passed else "FAIL", tp.title)
    if not tp.passed:
        # CI on feature branches does not upload the markdown report, so mirror
        # every failing row to stdout; without this a FAIL is just a headline
        # with no way to tell which assertion tripped.
        for line in tp.detail_md.splitlines():
            if "❌" in line:
                log(f"    {tp.id} | {line.strip()}")


# ------------------------------------------------------------------ TPs
def tp_e01_snapshot() -> None:
    """Snapshot -> route -> projection -> filter -> deterministic _id."""
    wait_count(index_for("tpe01_users"), 2)
    wait_count(index_for("tpe01_orders"), 2)
    wait_count(index_for("tpe01_merged"), 2)
    wait_count(index_for("tpe01_composite"), 3)
    wait_count(index_for("tpe01_types"), 2)
    wait_count(index_for("tpe01_filtered"), 1)

    users_problems, users_expected = check_table("tpe01_users", "tpe01_users", USERS_COLS, {"id": "bigint"})
    orders_problems, orders_expected = check_table("tpe01_orders", "tpe01_orders", ORDER_COLS, {"id": "bigint"})
    filtered_problems, filtered_expected = check_table(
        "tpe01_filtered", "tpe01_filtered", FILTERED_COLS, {"id": "bigint"}, where="city = 'hz'"
    )
    merged_rows = table_rows("tpe01_merge_a") + table_rows("tpe01_merge_b")
    merged_ids = {expected_id("tpe01_merged", {"id": "bigint"}, row) for row in merged_rows}
    merged_docs = es_docs(index_for("tpe01_merged"))

    # Two source tables, same target PK -> one document each (scope-deduped).
    merge_ok = len(merged_rows) == 4 and set(merged_docs) == merged_ids and len(merged_docs) == 2
    users_doc = es_docs(index_for("tpe01_users"))
    secret_leak = [doc_id for doc_id, doc in users_doc.items() if "secret" in doc]
    filtered_ids = es_docs(index_for("tpe01_filtered"))
    sh_leak = [doc_id for doc_id, doc in filtered_ids.items() if doc.get("city") != "hz"]
    ok = (
        not users_problems
        and not orders_problems
        and not filtered_problems
        and merge_ok
        and not secret_leak
        and not sh_leak
    )
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| users docs / expected | {len(users_doc)} / {users_expected} | {verdict(not users_problems and len(users_doc) == users_expected)} |\n"
        f"| users _source + _id | {users_problems[:2] or 'all match'} | {verdict(not users_problems)} |\n"
        f"| orders _source + _id | {orders_problems[:2] or 'all match'} | {verdict(not orders_problems)} |\n"
        f"| projection drops `secret` | leaks={secret_leak} | {verdict(not secret_leak)} |\n"
        f"| filter city='hz' only | leaks={sh_leak} | {verdict(not sh_leak)} |\n"
        f"| 2 source tables -> 1 target table | 4 rows -> {len(merged_docs)} docs | {verdict(merge_ok)} |\n"
        f"| composite key docs | {len(es_docs(index_for('tpe01_composite')))} | {verdict(len(es_docs(index_for('tpe01_composite'))) == 3)} |\n"
    )
    record(
        TPResult(
            id="TP-E01",
            title="snapshot: route + projection + filter + deterministic _id",
            action="seed 7 source tables; wait for snapshot convergence",
            expected_desc="final index per route; projected fields only; hz-only filter; _id matches the independent hash",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_e02_incremental_crud() -> None:
    """Binlog insert/update/delete reach Elasticsearch with a stable _id."""
    users_index = index_for("tpe01_users")
    orders_index = index_for("tpe01_orders")
    filtered_index = index_for("tpe01_filtered")

    before = es_docs(users_index)
    old_id = expected_id("tpe01_users", {"id": "bigint"}, {"id": 1})

    exec_sql(f"INSERT INTO `{SRC_DB}`.tpe01_users(id,name,city,balance,secret) VALUES (3,'cara','hz',30.00,'s3')")
    exec_sql(f"UPDATE `{SRC_DB}`.tpe01_users SET name='alice2', balance=11.00 WHERE id=1")
    exec_sql(f"DELETE FROM `{SRC_DB}`.tpe01_orders WHERE id=12")
    # Filtered-out insert must never create a document.
    exec_sql(f"INSERT INTO `{SRC_DB}`.tpe01_filtered(id,city,note) VALUES (3,'sh','drop3')")

    wait_count(users_index, 3)
    wait_count(orders_index, 1)
    updated = wait_doc(
        users_index,
        old_id,
        lambda doc: doc.get("name") == "alice2",
        desc="updated user doc",
    )
    wait_count(filtered_index, 1)

    users_problems, _ = check_table("tpe01_users", "tpe01_users", USERS_COLS, {"id": "bigint"})
    orders_problems, _ = check_table("tpe01_orders", "tpe01_orders", ORDER_COLS, {"id": "bigint"})
    filtered_problems, _ = check_table(
        "tpe01_filtered", "tpe01_filtered", FILTERED_COLS, {"id": "bigint"}, where="city = 'hz'"
    )
    deleted_gone = not ES.index_exists(orders_index) or ES.get_doc(
        orders_index, expected_id("tpe01_orders", {"id": "bigint"}, {"id": 12})
    ) is None
    id_stable = old_id in before and old_id in es_docs(users_index)
    ok = (
        not users_problems
        and not orders_problems
        and not filtered_problems
        and deleted_gone
        and id_stable
        and updated.get("balance") == "11.00"
    )
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| insert -> 3 users | {len(es_docs(users_index))} | {verdict(len(es_docs(users_index)) == 3)} |\n"
        f"| update overwrote old value | name={updated.get('name')} balance={updated.get('balance')} | {verdict(ok and updated.get('balance') == '11.00')} |\n"
        f"| update kept the snapshot _id | {id_stable} | {verdict(id_stable)} |\n"
        f"| delete removed the document | {deleted_gone} | {verdict(deleted_gone)} |\n"
        f"| filtered insert (city=sh) absent | {len(es_docs(filtered_index))} doc(s) | {verdict(len(es_docs(filtered_index)) == 1)} |\n"
        f"| users content | {users_problems[:2] or 'all match'} | {verdict(not users_problems)} |\n"
        f"| orders content | {orders_problems[:2] or 'all match'} | {verdict(not orders_problems)} |\n"
    )
    record(
        TPResult(
            id="TP-E02",
            title="incremental insert/update/delete + filter",
            action="INSERT user 3; UPDATE user 1; DELETE order 12; INSERT filtered city=sh",
            expected_desc="documents converge; update overwrites; delete removes; filtered insert absent; _id stable",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_e03_merge_same_target_pk() -> None:
    """Two source tables on one final table: same target PK == same document."""
    index = index_for("tpe01_merged")
    doc_id = expected_id("tpe01_merged", {"id": "bigint"}, {"id": 50})

    exec_sql(f"INSERT INTO `{SRC_DB}`.tpe01_merge_a(id,tenant_id,amount,note) VALUES (50,'ta',1.00,'a50')")
    first = wait_doc(index, doc_id, lambda doc: doc.get("note") == "a50", desc="merge_a document")
    count_after_a = len(es_docs(index))

    exec_sql(f"INSERT INTO `{SRC_DB}`.tpe01_merge_b(id,tenant_id,amount,note) VALUES (50,'tb',2.00,'b50')")
    second = wait_doc(index, doc_id, lambda doc: doc.get("note") == "b50", desc="merge_b overwrite")
    count_after_b = len(es_docs(index))

    ok = (
        first.get("note") == "a50"
        and second.get("note") == "b50"
        and count_after_a == count_after_b == 3
    )
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| merge_a id=50 lands as its own doc | note={first.get('note')} | {verdict(first.get('note') == 'a50')} |\n"
        f"| merge_b id=50 targets the same _id | note={second.get('note')} | {verdict(second.get('note') == 'b50')} |\n"
        f"| document count (3 seeded + 1 merged) | {count_after_a} -> {count_after_b} | {verdict(count_after_a == count_after_b == 3)} |\n"
    )
    record(
        TPResult(
            id="TP-E03",
            title="two source tables -> one final table (same target PK = same doc)",
            action="INSERT merge_a id=50, wait, INSERT merge_b id=50",
            expected_desc="second source overwrites the first document; no duplicate document",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_e04_composite_and_types() -> None:
    """Composite primary key hashing plus the full type round-trip."""
    composite_index = index_for("tpe01_composite")
    pks = {"tenant_id": "varchar(16)", "seq": "int"}
    doc_id = expected_id("tpe01_composite", pks, {"tenant_id": "t9", "seq": 5})

    exec_sql(f"INSERT INTO `{SRC_DB}`.tpe01_composite(tenant_id,seq,v) VALUES ('t9',5,'x')")
    wait_count(composite_index, 4)
    exec_sql(f"UPDATE `{SRC_DB}`.tpe01_composite SET v='y' WHERE tenant_id='t9' AND seq=5")
    updated = wait_doc(composite_index, doc_id, lambda doc: doc.get("v") == "y", desc="composite update")

    composite_problems, composite_expected = check_table("tpe01_composite", "tpe01_composite", COMPOSITE_COLS, pks)
    types_problems, types_expected = check_table(
        "tpe01_types", "tpe01_types", TYPES_COLS, {"id": "bigint unsigned"}
    )
    types_docs = es_docs(index_for("tpe01_types"))
    decimal_doc = types_docs.get(expected_id("tpe01_types", {"id": "bigint unsigned"}, {"id": 18446744073709551615}), {})
    mapping = ES.get_mapping(index_for("tpe01_types"))
    amount_mapping = verify.field_mapping(mapping, "c_amount")
    blob_mapping = verify.field_mapping(mapping, "c_blob")
    ok = (
        not composite_problems
        and not types_problems
        and updated.get("v") == "y"
        and len(types_docs) == types_expected
        and amount_mapping.get("type") == "keyword"
        and blob_mapping.get("type") == "binary"
    )
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| composite key update keeps _id | v={updated.get('v')} | {verdict(updated.get('v') == 'y')} |\n"
        f"| composite content | {composite_problems[:2] or f'{composite_expected} rows match'} | {verdict(not composite_problems)} |\n"
        f"| DECIMAL kept as keyword | mapping={amount_mapping} | {verdict(amount_mapping.get('type') == 'keyword')} |\n"
        f"| DECIMAL value (18,6) preserved | c_amount={decimal_doc.get('c_amount')} | {verdict(decimal_doc.get('c_amount') in ('1234567890.123456',))} |\n"
        f"| BINARY as binary + base64 | mapping={blob_mapping} blob={decimal_doc.get('c_blob')} | {verdict(blob_mapping.get('type') == 'binary' and decimal_doc.get('c_blob') == 'AP8Q')} |\n"
        f"| JSON object round-trip | c_meta={decimal_doc.get('c_meta')} | {verdict(decimal_doc.get('c_meta') == {'a': 1, 'b': 2})} |\n"
        f"| DATE/DATETIME/TIME | {decimal_doc.get('c_day')} / {decimal_doc.get('c_seen')} / {decimal_doc.get('c_clock')} | "
        f"{verdict(decimal_doc.get('c_seen') == '2024-01-02 03:04:05.123456')} |\n"
        f"| ENUM/SET labels | {decimal_doc.get('c_status')} / {decimal_doc.get('c_tags')} | {verdict(decimal_doc.get('c_status') == 'done' and decimal_doc.get('c_tags') == 'a,c')} |\n"
        f"| full types content ({len(TYPES_COLS)} columns) | {types_problems[:2] or 'all match'} | {verdict(not types_problems)} |\n"
    )
    record(
        TPResult(
            id="TP-E04",
            title="composite primary key + full type round-trip",
            action="INSERT/UPDATE composite (t9,5); compare every typed column",
            expected_desc="_id from both keys in schema order; decimal/json/binary/date/time/enum/set preserved",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_e05_checkpoint() -> None:
    """The real checkpoint file records the binlog position and captured tables."""
    state: dict = {}

    def loaded() -> bool:
        if not CK.exists() or CK.stat().st_size == 0:
            return False
        try:
            state.update(json.loads(CK.read_text(encoding="utf-8")))
        except ValueError:
            return False
        return bool(state.get("acked_offset"))

    verify.wait_for(loaded, timeout=90, interval=0.5, desc="checkpoint acked_offset")
    acked = state.get("acked_offset") or {}
    captured = state.get("captured_tables") or []
    splits = state.get("finished_splits") or {}
    want_tables = {f"{SRC_DB}.tpe01_users", f"{SRC_DB}.tpe01_orders", f"{SRC_DB}.tpe01_types"}
    ok = (
        bool(acked.get("file"))
        and int(acked.get("pos", 0)) > 4
        and want_tables <= set(captured)
        and any(splits.values())
    )
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| acked_offset file/pos | {acked.get('file')}:{acked.get('pos')} | {verdict(bool(acked.get('file')) and int(acked.get('pos', 0)) > 4)} |\n"
        f"| captured_tables | {sorted(captured)} | {verdict(want_tables <= set(captured))} |\n"
        f"| finished_splits recorded | {sorted(splits)[:3]} | {verdict(bool(splits))} |\n"
    )
    record(
        TPResult(
            id="TP-E05",
            title="checkpoint state after snapshot + binlog",
            action="read the pipeline checkpoint JSON",
            expected_desc="acked_offset advanced past the snapshot, captured tables and finished splits recorded",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_e06_consistency() -> None:
    """Every final table matches MySQL: count, _id set, field values."""
    specs = [
        ("tpe01_users", "tpe01_users", USERS_COLS, {"id": "bigint"}, ""),
        ("tpe01_orders", "tpe01_orders", ORDER_COLS, {"id": "bigint"}, ""),
        ("tpe01_composite", "tpe01_composite", COMPOSITE_COLS, {"tenant_id": "varchar(16)", "seq": "int"}, ""),
        ("tpe01_types", "tpe01_types", TYPES_COLS, {"id": "bigint unsigned"}, ""),
        ("tpe01_filtered", "tpe01_filtered", FILTERED_COLS, {"id": "bigint"}, "city = 'hz'"),
    ]
    detail = "| Table | MySQL rows | ES docs | Content | Verdict |\n| --- | ---: | ---: | --- | --- |\n"
    ok = True
    for table, final, columns, pks, where in specs:
        problems, expected = check_table(table, final, columns, pks, where)
        docs = len(es_docs(index_for(final)))
        hit = not problems and docs == expected
        ok = ok and hit
        detail += f"| {final} | {expected} | {docs} | {problems[:1] or 'all fields match'} | {verdict(hit)} |\n"
    detail += f"| summary | | | | **{verdict(ok)}** |\n"
    record(
        TPResult(
            id="TP-E06",
            title="MySQL/Elasticsearch consistency sweep",
            action="compare every final index with its MySQL source (scroll, not _count)",
            expected_desc="same document set, same _id derivation, same canonical field values",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_e07_restart() -> None:
    """Restart resumes from the checkpoint without re-snapshotting or duplicating."""
    users_index = index_for("tpe01_users")
    before = set(es_docs(users_index))
    state_before = json.loads(CK.read_text(encoding="utf-8"))
    pos_before = (state_before.get("acked_offset") or {}).get("pos", 0)

    stop_job()
    cfg.render_template("pipeline.es.yaml.tpl", YAML)
    start_job(YAML, LOG, reset_ckpt=None)

    exec_sql(f"INSERT INTO `{SRC_DB}`.tpe01_users(id,name,city,balance,secret) VALUES (4,'dave','bj',40.00,'s4')")
    new_id = expected_id("tpe01_users", {"id": "bigint"}, {"id": 4})
    wait_doc(users_index, new_id, lambda doc: doc.get("name") == "dave", desc="post-restart insert")

    after = set(es_docs(users_index))
    state_after: dict = {}

    def advanced() -> bool:
        try:
            state_after.update(json.loads(CK.read_text(encoding="utf-8")))
        except (OSError, ValueError):
            return False
        return int((state_after.get("acked_offset") or {}).get("pos", 0)) >= int(pos_before)

    verify.wait_for(advanced, timeout=60, interval=0.5, desc="checkpoint advance after restart")
    pos_after = (state_after.get("acked_offset") or {}).get("pos", 0)
    no_duplicates = len(after) == len(es_docs(users_index)) == 4
    ok = before < after and new_id in after and no_duplicates and int(pos_after) >= int(pos_before)
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| resume kept existing docs | {len(before)} -> {len(after)} | {verdict(before < after)} |\n"
        f"| new binlog row written | {new_id[:12]} present={new_id in after} | {verdict(new_id in after)} |\n"
        f"| no duplicated documents | {len(after)} distinct docs | {verdict(no_duplicates)} |\n"
        f"| checkpoint advanced | {pos_before} -> {pos_after} | {verdict(int(pos_after) >= int(pos_before))} |\n"
    )
    record(
        TPResult(
            id="TP-E07",
            title="restart resumes from the checkpoint",
            action="stop, start with the same checkpoint, INSERT user 4",
            expected_desc="no re-snapshot, no duplicates, _id stable, checkpoint advances",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_e08_binlog_catalog() -> None:
    """Binlog-only start: the catalog pre-registers indices before any event.

    Scope of this test point is the Elasticsearch-side catalog: every final
    index must exist and be usable before any event lands. ``captured_tables``
    in the checkpoint is populated by the snapshot phase and by runtime
    ``CREATE TABLE`` events; a ``scan.startup.mode: latest`` run that observes
    no DDL is expected to leave it empty, so the assertion below records the
    checkpoint contents as informational rather than gating on them.
    """
    stop_job()
    clean_indices(cfg.ES_INDEX_PREFIX)
    if CK_BINLOG.exists():
        CK_BINLOG.unlink()

    cfg.render_template("pipeline.es.binlog.yaml.tpl", YAML_BINLOG, extra={"ES_STARTUP_MODE": "latest"})
    start_job(YAML_BINLOG, LOG_BINLOG, reset_ckpt=None)

    indices = [index_for(t) for t in ("tpe01_users", "tpe01_orders", "tpe01_merged", "tpe01_composite", "tpe01_types", "tpe01_filtered")]

    def catalog_ready() -> bool:
        return all(ES.index_exists(name) for name in indices)

    elapsed = verify.wait_for(catalog_ready, timeout=120, interval=0.5, desc="catalog pre-registered indices")

    empty_users = ES.count(index_for("tpe01_users"))
    exec_sql(f"INSERT INTO `{SRC_DB}`.tpe01_users(id,name,city,balance,secret) VALUES (5,'eve','hz',50.00,'s5')")
    new_id = expected_id("tpe01_users", {"id": "bigint"}, {"id": 5})
    doc = wait_doc(index_for("tpe01_users"), new_id, lambda d: d.get("name") == "eve", desc="binlog-only insert")

    state: dict = {}
    if CK_BINLOG.exists():
        try:
            state.update(json.loads(CK_BINLOG.read_text(encoding="utf-8")))
        except (OSError, ValueError):
            pass
    captured = sorted(state.get("captured_tables") or [])

    ok = elapsed >= 0 and empty_users == 0 and doc.get("city") == "hz" and len(es_docs(index_for("tpe01_users"))) == 1
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| indices created before any event | {len(indices)} indices in {elapsed:.1f}s | {verdict(all(ES.index_exists(n) for n in indices))} |\n"
        f"| checkpoint captured_tables (informational: empty on binlog-only) | {captured} | |\n"
        f"| no snapshot rows (startup=latest) | {empty_users} | {verdict(empty_users == 0)} |\n"
        f"| binlog row written with expected _id | city={doc.get('city')} | {verdict(doc.get('city') == 'hz' and len(es_docs(index_for('tpe01_users'))) == 1)} |\n"
    )
    record(
        TPResult(
            id="TP-E08",
            title="binlog-only startup pre-registers the catalog",
            action="wipe indices + checkpoint, start with scan.startup.mode=latest",
            expected_desc="all final indices exist before the first event; no snapshot rows; binlog write reaches the index",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_e09_ddl_fails() -> None:
    """A schema change on a projected column must fail the job, not diverge.

    The DDL has to touch a column the projection keeps: ``schemaevol.Decide``
    deliberately ignores column DDL outside the projection (policy_test.go:
    "excluded column should not be executed"), so an Elasticsearch sink never
    sees it and the job keeps running. ``name`` is in the ``tpe01_users``
    projection, so this reaches ``Sink.ApplySchema`` and must stop the job.
    """
    users_index = index_for("tpe01_users")
    mapping_before = ES.get_mapping(users_index)
    exec_sql(f"ALTER TABLE `{SRC_DB}`.tpe01_users MODIFY COLUMN name VARCHAR(128) NOT NULL")
    code = wait_exit(timeout_s=120, desc="go-cdc after DDL", log_path=LOG_BINLOG)
    text = log_text(LOG_BINLOG)
    mapping_after = ES.get_mapping(users_index)
    name_field = verify.field_mapping(mapping_after, "name")
    mentions = "schema change" in text and "not supported" in text
    ok = code != 0 and mentions and mapping_before == mapping_after
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| job exit code | {code} | {verdict(code != 0)} |\n"
        f"| log explains the rejection | {mentions} | {verdict(mentions)} |\n"
        f"| index mapping unchanged | name={name_field} | {verdict(mapping_before == mapping_after)} |\n"
        f"| index still exists | {ES.index_exists(users_index)} | {verdict(ES.index_exists(users_index))} |\n"
        f"| log tail | `{_tail(LOG_BINLOG, 400).splitlines()[-1] if _tail(LOG_BINLOG, 400) else ''}` | |\n"
    )
    record(
        TPResult(
            id="TP-E09",
            title="unsupported DDL fails the pipeline",
            action="ALTER TABLE tpe01_users MODIFY COLUMN name VARCHAR(128)",
            expected_desc="non-zero exit, schema-change rejection in the log, mapping untouched",
            passed=ok,
            detail_md=detail,
        )
    )
    exec_sql(f"ALTER TABLE `{SRC_DB}`.tpe01_users MODIFY COLUMN name VARCHAR(64) NOT NULL")


def tp_e10_mapping_conflict() -> None:
    """Startup fails loudly when an existing index cannot accept the schema."""
    stop_job()
    orders_index = index_for("tpe01_orders")
    ES.delete_index(orders_index)
    ES.create_index(
        orders_index,
        {"mappings": {"properties": {"id": {"type": "keyword"}, "user_id": {"type": "long"}}}},
    )
    cfg.render_template("pipeline.es.yaml.tpl", YAML)
    start_job(YAML, LOG, reset_ckpt=CK, expect_exit=True)
    code = wait_exit(timeout_s=120, desc="go-cdc with an incompatible mapping", log_path=LOG)
    text = log_text(LOG)
    mentions = "cannot be written to mapping type" in text and orders_index in text
    ok = code != 0 and mentions
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| job exit code | {code} | {verdict(code != 0)} |\n"
        f"| log names the index and the conflict | {mentions} | {verdict(mentions)} |\n"
        f"| log tail | `{(text.strip().splitlines() or [''])[-1][:300]}` | |\n"
    )
    record(
        TPResult(
            id="TP-E10",
            title="incompatible existing mapping fails startup",
            action="pre-create tpe01_orders with id:keyword, start the pipeline",
            expected_desc="non-zero exit; error names the index and the incompatible column",
            passed=ok,
            detail_md=detail,
        )
    )


# ------------------------------------------------------------------ report
def write_report() -> Path:
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    failed = sum(1 for r in results if not r.passed)
    version = ES.version()
    lines = [
        f"# MySQL → Elasticsearch {version.split('.')[0]} integration report",
        "",
        f"- Elasticsearch: `{cfg.ES_URL}` (version {version})",
        f"- sink.version: `{cfg.ES_SINK_VERSION}` (client adapter under test)",
        f"- Source DB: `{SRC_DB}` · final-table namespace: `{SNK_DB}`",
        f"- Functional pipeline: `{cfg.PIPELINE_ES_NAME}`",
        f"- Binlog-only pipeline: `{cfg.PIPELINE_ES_RESTART_NAME}`",
        f"- Checkpoints: `{cfg.project_display_path(CK)}`, `{cfg.project_display_path(CK_BINLOG)}`",
        "- Verification: `harness/es_verify.py` re-derives index names, `_id` hashes and canonical",
        "  values independently of the Go sink; reads use scroll (`size=1000`), never `from`/`size`",
        "  past the 10k window and never `_count` alone.",
        "",
    ]
    for r in results:
        mark = "✅ PASS" if r.passed else "❌ FAIL"
        lines += [
            f"## {r.id} — {r.title} ({mark})",
            "",
            f"- Action: {r.action}",
            f"- Expected: {r.expected_desc}",
            "",
            r.detail_md,
            "",
        ]
    lines.append(f"**Summary:** {len(results) - failed}/{len(results)} passed")
    text = cfg.sanitize_report_text("\n".join(lines) + "\n")
    REPORT.write_text(text, encoding="utf-8")
    return REPORT


def _run_tp(tp_id: str, title: str, fn) -> None:
    """Run one TP; a raised exception becomes a FAIL row instead of aborting.

    Without this wrapper a single timeout (e.g. wait_doc waiting on an ES write
    that never lands) short-circuits the whole suite and skips write_report(),
    leaving CI with no artifact to inspect. Recording the exception keeps the
    remaining TPs independent and lets the report describe the failure.
    """
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 - each TP is independent; the report captures the error
        record(
            TPResult(
                id=tp_id,
                title=title,
                action=f"{fn.__name__}() raised {type(exc).__name__}",
                expected_desc="test point completes without raising",
                passed=False,
                detail_md=(
                    "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
                    f"| uncaught exception | {type(exc).__name__}: {exc} | ❌ |\n"
                ),
            )
        )


def main() -> int:
    WORKDIR.mkdir(parents=True, exist_ok=True)
    if not ES.ping():
        raise RuntimeError(
            f"Elasticsearch is not reachable at {cfg.ES_URL}; "
            "run scripts/es-up.sh (ES8), scripts/es7-up.sh (ES7) or scripts/es9-up.sh (ES9)"
        )
    log(f"==> Elasticsearch {ES.version()} at {cfg.ES_URL} (sink.version={cfg.ES_SINK_VERSION})")
    assert_target()
    log("==> schema + seed")
    ensure_schema()
    seed()
    clean_indices(cfg.ES_INDEX_PREFIX)
    for path in (CK, CK_BINLOG):
        if path.exists():
            path.unlink()
    cfg.render_template("pipeline.es.yaml.tpl", YAML)

    try:
        start_job(YAML, LOG, reset_ckpt=None)
        _run_tp("TP-E01", "snapshot: route + projection + filter + deterministic _id", tp_e01_snapshot)
        _run_tp("TP-E02", "incremental insert/update/delete + filter", tp_e02_incremental_crud)
        _run_tp("TP-E03", "two source tables -> one final table (same target PK = same doc)", tp_e03_merge_same_target_pk)
        _run_tp("TP-E04", "composite primary key + full type round-trip", tp_e04_composite_and_types)
        _run_tp("TP-E05", "checkpoint state after snapshot + binlog", tp_e05_checkpoint)
        _run_tp("TP-E06", "MySQL/Elasticsearch consistency sweep", tp_e06_consistency)
        _run_tp("TP-E07", "restart resumes from the checkpoint", tp_e07_restart)
        _run_tp("TP-E08", "binlog-only startup pre-registers the catalog", tp_e08_binlog_catalog)
        _run_tp("TP-E09", "unsupported DDL fails the pipeline", tp_e09_ddl_fails)
        _run_tp("TP-E10", "incompatible existing mapping fails startup", tp_e10_mapping_conflict)
    finally:
        stop_job()
        # Always emit the report, even after start_job itself failed, so CI
        # logs and artifact uploads (on the branches that gate them) can point
        # to a concrete state instead of an aborted process.
        try:
            path = write_report()
            log("REPORT", cfg.project_display_path(path))
        except Exception as exc:  # noqa: BLE001 - never mask the original failure
            log("REPORT write failed:", type(exc).__name__, exc)
    failed = [r for r in results if not r.passed]
    log("PASS", len(results) - len(failed), "FAIL", len(failed))
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
