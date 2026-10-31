#!/usr/bin/env python3
"""MySQL → Doris pipeline integration suite (TP-DR01..TP-DR07).

Mirrors kafka_it style: seed MySQL → start go-cdc YAML job → assert sink state.
Functional (DR01–DR04): snapshot / route / projection / filter, incremental
insert+update, ADD COLUMN, and restart at-least-once (UNIQUE KEY converge).
Load parity (DR05–DR07): multi-table bulk snapshot, typed spot-check, and
concurrent peak writers + catch-up. The load phase reuses the LOAD_* knobs of
the MySQL/Kafka load suites and restricts itself to types Doris v1 can map
(no BLOB/ENUM/BIT).

Doris v1 rejects DELETE and filter-retraction deletes, so no test issues one.

IDs use TP-DR* so they do not collide with types suite TP-D01..D04.

Requires MySQL (mysql-up) + Doris FE/BE (doris-up).
"""
from __future__ import annotations

import json
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pymysql

from common import config as cfg
from common.logutil import log
from common.sysstat import format_cpu_mem, format_host_res

SRC = cfg.conn_kwargs(cfg.SRC_USER, cfg.SRC_PASSWORD, cfg.FUNC_SRC_DB)
WORKDIR = cfg.WORKDIR
BINARY = cfg.BINARY
CK = Path(cfg.CHECKPOINT_DORIS_PATH)
YAML = WORKDIR / "pipeline.doris.yaml"
LOG = WORKDIR / "cdc-doris.log"
REPORT = cfg.REPORT_DORIS
SNK_DB = cfg.DORIS_SNK_DB

CK_LOAD = Path(cfg.CHECKPOINT_DORIS_LOAD_PATH)
YAML_LOAD = WORKDIR / "pipeline.doris.load.yaml"
LOG_LOAD = WORKDIR / "cdc-doris-load.log"

# Load phase volumes: same MySQL knobs as the mysql→mysql / kafka load suites.
LOAD_SRC_DB = cfg.LOAD_SRC_DB
LOAD_SRC = cfg.conn_kwargs(cfg.SRC_USER, cfg.SRC_PASSWORD, LOAD_SRC_DB)
LOAD_ROWS = cfg.LOAD_ROWS_PER_TABLE
PEAK_SEC = cfg.LOAD_PEAK_SECONDS
WORKERS = cfg.LOAD_PEAK_WORKERS
PEAK_BATCH = cfg.LOAD_PEAK_BATCH
LOAD_BIZ = ("tpdl01_t1", "tpdl01_t2", "tpdl01_t3", "tpdl01_t4")
LOAD_TYPED = "tpdl01_typed"
LOAD_TABLES = LOAD_BIZ + (LOAD_TYPED,)

results: list["TPResult"] = []
proc: subprocess.Popen | None = None
# Set by start_cdc / start_load_cdc so liveness errors quote the active phase log.
current_log = LOG
load_timings: dict[str, float] = {}


@dataclass
class TPResult:
    id: str
    title: str
    action: str
    expected_desc: str
    passed: bool = False
    detail_md: str = ""


def exec_sql(sql: str, args: tuple | None = None) -> None:
    c = pymysql.connect(**SRC, autocommit=True)
    try:
        with c.cursor() as cur:
            cur.execute(sql, args or ())
    finally:
        c.close()


def doris_conn(**kwargs):
    return pymysql.connect(
        host=cfg.DORIS_HOST,
        port=cfg.DORIS_SQL_PORT,
        user=cfg.DORIS_USER,
        password=cfg.DORIS_PASSWORD,
        charset="utf8mb4",
        autocommit=True,
        **kwargs,
    )


def wait_cdc_alive(
    log_path: Path,
    *,
    timeout_s: float = 20.0,
    ready_after_s: float = 4.0,
    interval: float = 0.25,
    desc: str = "go-cdc",
) -> None:
    deadline = time.time() + timeout_s
    ready_at = time.time() + ready_after_s
    while time.time() < deadline:
        if proc is None:
            raise RuntimeError(f"{desc}: process not started")
        code = proc.poll()
        if code is not None:
            text = log_path.read_text(errors="replace")[-2000:] if log_path.exists() else ""
            raise RuntimeError(f"{desc} exited early ({code}):\n{text}")
        if time.time() >= ready_at:
            return
        time.sleep(interval)
    if proc is not None and proc.poll() is None:
        return
    text = log_path.read_text(errors="replace")[-2000:] if log_path.exists() else ""
    raise TimeoutError(f"timeout waiting for {desc} to stay alive:\n{text}")


def start_cdc(*, reset_ckpt: bool = True) -> None:
    global proc, current_log
    stop_cdc()
    if reset_ckpt and CK.exists():
        CK.unlink()
    CK.parent.mkdir(parents=True, exist_ok=True)
    cfg.render_template("pipeline.doris.yaml.tpl", YAML)
    current_log = LOG
    log_path = LOG.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        [BINARY, str(YAML)],
        stdout=log_path,
        stderr=subprocess.STDOUT,
        text=True,
    )
    wait_cdc_alive(LOG, desc="go-cdc doris")


def stop_cdc() -> None:
    global proc
    if proc is None:
        return
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    proc = None


def ensure_alive(desc: str = "go-cdc doris") -> None:
    if proc is not None and proc.poll() is not None:
        text = current_log.read_text(errors="replace")[-2000:] if current_log.exists() else ""
        raise RuntimeError(f"{desc} exited ({proc.returncode}):\n{text}")


def start_load_cdc() -> None:
    """Start the bulk/peak pipeline (db_tp_load_src.tpdl01_* → Doris)."""
    global proc, current_log
    stop_cdc()
    if CK_LOAD.exists():
        CK_LOAD.unlink()
    CK_LOAD.parent.mkdir(parents=True, exist_ok=True)
    cfg.render_template("pipeline.doris.load.yaml.tpl", YAML_LOAD)
    current_log = LOG_LOAD
    log_path = LOG_LOAD.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        [BINARY, str(YAML_LOAD)],
        stdout=log_path,
        stderr=subprocess.STDOUT,
        text=True,
    )
    wait_cdc_alive(LOG_LOAD, desc="go-cdc doris load")


def wait_ckpt(path: Path, timeout_s: float = 60.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        ensure_alive()
        if path.exists() and path.stat().st_size > 0:
            return True
        time.sleep(0.5)
    return path.exists() and path.stat().st_size > 0


def query_rows(table: str, *, where: str = "1=1") -> list[dict]:
    """Return Doris rows as dicts (column name → str|None)."""
    sql = f"SELECT * FROM `{SNK_DB}`.`{table}` WHERE {where}"
    c = doris_conn()
    try:
        with c.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            out: list[dict] = []
            for r in rows:
                out.append({k: (None if v is None else str(v)) for k, v in r.items()})
            return out
    finally:
        c.close()


def wait_rows(
    table: str,
    *,
    predicate,
    timeout_s: float = 120.0,
    interval: float = 1.0,
    desc: str = "doris rows",
) -> list[dict]:
    """Poll Doris until predicate(rows) is true or timeout."""
    deadline = time.time() + timeout_s
    last: list[dict] = []
    while time.time() < deadline:
        ensure_alive(desc)
        try:
            last = query_rows(table)
        except pymysql.err.ProgrammingError:
            # Table may not exist yet (auto-create still in flight).
            last = []
        if predicate(last):
            return last
        time.sleep(interval)
    raise TimeoutError(f"timeout waiting for {desc} on {SNK_DB}.{table}: last={last}")


def drop_sink_tables() -> None:
    """Best-effort wipe of suite sink tables between runs."""
    c = doris_conn()
    try:
        with c.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{SNK_DB}`")
            for t in (
                "tpds01_users",
                "tpds01_orders",
                "tpds01_misc",
                "tpds01_evolve",
                *LOAD_TABLES,
            ):
                try:
                    cur.execute(f"DROP TABLE IF EXISTS `{SNK_DB}`.`{t}`")
                except pymysql.Error:
                    pass
    finally:
        c.close()


def doris_exec(sql: str) -> None:
    c = doris_conn()
    try:
        with c.cursor() as cur:
            cur.execute(sql)
    finally:
        c.close()


def doris_count(table: str) -> int:
    c = doris_conn()
    try:
        with c.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{SNK_DB}`.`{table}`")
            row = cur.fetchone()
            return int(row[0]) if row else 0
    finally:
        c.close()


def doris_query(sql: str) -> list[dict]:
    c = doris_conn(cursorclass=pymysql.cursors.DictCursor)
    try:
        with c.cursor() as cur:
            cur.execute(sql)
            return [{k: (None if v is None else v) for k, v in r.items()} for r in cur.fetchall()]
    finally:
        c.close()


def load_exec_sql(sql: str, args: tuple | None = None) -> None:
    c = pymysql.connect(**LOAD_SRC, autocommit=True)
    try:
        with c.cursor() as cur:
            cur.execute(sql, args or ())
    finally:
        c.close()


def load_count_rows(table: str) -> int:
    c = pymysql.connect(**LOAD_SRC)
    try:
        with c.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{LOAD_SRC_DB}`.`{table}`")
            row = cur.fetchone()
            return int(row[0]) if row else 0
    finally:
        c.close()


def load_query(sql: str) -> list[dict]:
    c = pymysql.connect(**LOAD_SRC, cursorclass=pymysql.cursors.DictCursor)
    try:
        with c.cursor() as cur:
            cur.execute(sql)
            return [{k: (None if v is None else v) for k, v in r.items()} for r in cur.fetchall()]
    finally:
        c.close()


def desc_columns(table: str) -> list[str]:
    """Return Doris column names from DESC (lowercased)."""
    c = doris_conn()
    try:
        with c.cursor() as cur:
            cur.execute(f"DESC `{SNK_DB}`.`{table}`")
            rows = cur.fetchall()
            # DESC: Field, Type, Null, Key, Default, Extra, …
            return [str(r[0]).lower() for r in rows if r and r[0] is not None]
    finally:
        c.close()


def record(tp: TPResult) -> None:
    results.append(tp)
    mark = "PASS" if tp.passed else "FAIL"
    log(tp.id, mark, tp.title)


def tp_d01_snapshot_route_proj_filter() -> None:
    """Snapshot: routed tables, projection, filter city='hz'."""
    start_cdc(reset_ckpt=True)
    users = wait_rows(
        "tpds01_users",
        predicate=lambda rows: any(r.get("id") == "1" for r in rows),
        desc="d01 users snapshot",
    )
    orders = wait_rows(
        "tpds01_orders",
        predicate=lambda rows: len(rows) >= 2,
        desc="d01 orders snapshot",
    )
    misc = wait_rows(
        "tpds01_misc",
        predicate=lambda rows: any(r.get("id") == "100" for r in rows),
        desc="d01 misc snapshot",
    )

    user_ids = {r.get("id") for r in users}
    filter_ok = "1" in user_ids and "2" not in user_ids
    alice = next((r for r in users if r.get("id") == "1"), {})
    proj_ok = (
        "secret" not in alice
        and alice.get("name") == "alice"
        and alice.get("city") == "hz"
    )
    orders_ok = {r.get("id") for r in orders} >= {"10", "11"}
    misc_ok = any(r.get("id") == "100" and r.get("note") == "default-route" for r in misc)
    ok = filter_ok and proj_ok and orders_ok and misc_ok
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| users ids (hz only) | {sorted(user_ids)} | {'✅' if filter_ok else '❌'} |\n"
        f"| bob id=2 filtered | {'2' not in user_ids} | "
        f"{'✅' if '2' not in user_ids else '❌'} |\n"
        f"| projection (no secret) | {sorted(alice.keys())} | "
        f"{'✅' if proj_ok else '❌'} |\n"
        f"| orders ids | {sorted(r.get('id') for r in orders)} | "
        f"{'✅' if orders_ok else '❌'} |\n"
        f"| misc id=100 | {misc_ok} | {'✅' if misc_ok else '❌'} |\n"
    )
    record(
        TPResult(
            id="TP-DR01",
            title="snapshot: route + projection + filter",
            action="seed users/orders/misc; wait Doris UNIQUE KEY rows",
            expected_desc="hz-only users; orders+misc present; no secret column",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_d02_incremental_insert_update() -> None:
    """Incremental insert + update; filtered insert must not appear. No DELETE (v1)."""
    exec_sql(
        "INSERT INTO tpds01_users (id, name, city, secret) VALUES (%s, %s, %s, %s)",
        (3, "cara", "hz", "s3"),
    )
    exec_sql(
        "INSERT INTO tpds01_users (id, name, city, secret) VALUES (%s, %s, %s, %s)",
        (4, "dan", "sh", "s4"),
    )
    exec_sql("UPDATE tpds01_users SET name=%s WHERE id=%s", ("alice2", 1))
    exec_sql("UPDATE tpds01_orders SET amt=%s WHERE id=%s", ("12.00", 10))

    users = wait_rows(
        "tpds01_users",
        predicate=lambda rows: (
            any(r.get("id") == "3" for r in rows)
            and any(r.get("id") == "1" and r.get("name") == "alice2" for r in rows)
        ),
        desc="d02 users incr",
    )
    orders = wait_rows(
        "tpds01_orders",
        predicate=lambda rows: any(
            r.get("id") == "10" and r.get("amt") is not None and r.get("amt").startswith("12")
            for r in rows
        ),
        desc="d02 orders update",
    )

    ids = {r.get("id") for r in users}
    c_ok = "3" in ids
    filt_ok = "4" not in ids
    u_ok = any(r.get("id") == "1" and r.get("name") == "alice2" for r in users)
    amt_ok = any(
        r.get("id") == "10" and r.get("amt") is not None and r.get("amt").startswith("12")
        for r in orders
    )
    ok = c_ok and filt_ok and u_ok and amt_ok
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| insert id=3 (hz) | {c_ok} | {'✅' if c_ok else '❌'} |\n"
        f"| insert id=4 (sh) absent | {filt_ok} | {'✅' if filt_ok else '❌'} |\n"
        f"| update id=1 name=alice2 | {u_ok} | {'✅' if u_ok else '❌'} |\n"
        f"| update order 10 amt≈12 | {amt_ok} | {'✅' if amt_ok else '❌'} |\n"
    )
    record(
        TPResult(
            id="TP-DR02",
            title="incremental insert + update + filter",
            action="INSERT hz/sh; UPDATE user name + order amt",
            expected_desc="hz insert+updates land; sh insert filtered; no DELETE",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_d03_add_column() -> None:
    """Source ADD COLUMN → Doris ADD COLUMN on a table without projection.

    Users/orders use a fixed projection list, so a new column would never be
    emitted there. tpds01_evolve has primary-keys only (all columns pass).
    """
    # Ensure snapshot row exists first (auto-create + baseline).
    wait_rows(
        "tpds01_evolve",
        predicate=lambda rows: any(r.get("id") == "1" for r in rows),
        desc="d03 evolve snapshot",
    )
    exec_sql("ALTER TABLE tpds01_evolve ADD COLUMN tag VARCHAR(16) NULL")
    exec_sql(
        "INSERT INTO tpds01_evolve (id, name, tag) VALUES (%s, %s, %s)",
        (2, "erin", "vip"),
    )

    def ready(rows: list[dict]) -> bool:
        try:
            cols = desc_columns("tpds01_evolve")
        except pymysql.Error:
            return False
        if "tag" not in cols:
            return False
        return any(r.get("id") == "2" and r.get("tag") == "vip" for r in rows)

    rows = wait_rows(
        "tpds01_evolve",
        predicate=ready,
        timeout_s=180.0,
        desc="d03 add column",
    )
    cols = desc_columns("tpds01_evolve")
    row = next((r for r in rows if r.get("id") == "2"), {})
    col_ok = "tag" in cols
    row_ok = row.get("tag") == "vip" and row.get("name") == "erin"
    ok = col_ok and row_ok
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| DESC has tag | {cols} | {'✅' if col_ok else '❌'} |\n"
        f"| id=2 tag=vip | {row} | {'✅' if row_ok else '❌'} |\n"
    )
    record(
        TPResult(
            id="TP-DR03",
            title="schema evolution ADD COLUMN",
            action="ALTER tpds01_evolve ADD tag; INSERT id=2 tag=vip",
            expected_desc="Doris DESC gains tag; row shows vip",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_d04_restart_at_least_once() -> None:
    """Stop/start go-cdc; UNIQUE KEY rows still converge (ALO, not EO)."""
    if not wait_ckpt(CK, timeout_s=60):
        record(
            TPResult(
                id="TP-DR04",
                title="restart at-least-once",
                action="wait checkpoint then restart",
                expected_desc="checkpoint exists; post-restart upsert converges",
                passed=False,
                detail_md="| Check | Actual | Verdict |\n| --- | --- | --- |\n"
                "| checkpoint before restart | missing | ❌ |\n",
            )
        )
        return

    stop_cdc()
    exec_sql("UPDATE tpds01_orders SET amt=%s WHERE id=%s", ("20.00", 10))
    start_cdc(reset_ckpt=False)
    orders = wait_rows(
        "tpds01_orders",
        predicate=lambda rows: any(
            r.get("id") == "10" and r.get("amt") is not None and r.get("amt").startswith("20")
            for r in rows
        ),
        desc="d04 post-restart",
    )
    amt_ok = any(
        r.get("id") == "10" and r.get("amt") is not None and r.get("amt").startswith("20")
        for r in orders
    )
    # Snapshot of users still present after restart (no wipe).
    users = query_rows("tpds01_users")
    users_ok = any(r.get("id") == "1" for r in users)
    ok = amt_ok and users_ok
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| order 10 amt≈20 after restart | {amt_ok} | {'✅' if amt_ok else '❌'} |\n"
        f"| users still present | {users_ok} | {'✅' if users_ok else '❌'} |\n"
        "| semantics | ALO UNIQUE KEY converge | ✅ |\n"
    )
    record(
        TPResult(
            id="TP-DR04",
            title="restart at-least-once",
            action="SIGTERM; UPDATE order; restart without wiping ckpt",
            expected_desc="post-restart upsert visible; prior rows intact",
            passed=ok,
            detail_md=detail,
        )
    )


# Load phase tables. Doris v1 rejects BLOB/ENUM/BIT/SET/TIME, so the typed table
# carries only mappable columns; everything else mirrors the mysql→mysql load
# suite (same MySQL source knobs, no projection).
LOAD_DDL = {
    "tpdl01_t1": (
        "(id BIGINT PRIMARY KEY, c_ref BIGINT NOT NULL, "
        "c_amt DECIMAL(12,2) NOT NULL, c_flag VARCHAR(16) NOT NULL)"
    ),
    "tpdl01_t2": (
        "(id BIGINT PRIMARY KEY, c_ref BIGINT NOT NULL, "
        "c_code VARCHAR(32) NOT NULL, c_qty INT NOT NULL)"
    ),
    "tpdl01_t3": "(id BIGINT PRIMARY KEY, c_name VARCHAR(64) NOT NULL, c_city VARCHAR(32) NOT NULL)",
    "tpdl01_t4": (
        "(id BIGINT PRIMARY KEY, c_ref BIGINT NOT NULL, "
        "c_method VARCHAR(16) NOT NULL, c_paid DECIMAL(12,2) NOT NULL)"
    ),
    LOAD_TYPED: (
        "(id BIGINT PRIMARY KEY, "
        "c_tinyint TINYINT NOT NULL, "
        "c_int INT NOT NULL, "
        "c_bigint BIGINT NOT NULL, "
        "c_decimal DECIMAL(10,4) NOT NULL, "
        "c_float FLOAT NOT NULL, "
        "c_double DOUBLE NOT NULL, "
        "c_varchar VARCHAR(64) NOT NULL, "
        "c_date DATE NOT NULL, "
        "c_datetime DATETIME(6) NOT NULL, "
        "c_timestamp TIMESTAMP(6) NULL, "
        "c_json JSON NULL, "
        "c_bool TINYINT(1) NOT NULL)"
    ),
}

_INSERT_T1 = (
    f"INSERT INTO `{LOAD_SRC_DB}`.tpdl01_t1(id,c_ref,c_amt,c_flag) VALUES (%s,%s,%s,%s)"
)
_INSERT_T2 = (
    f"INSERT INTO `{LOAD_SRC_DB}`.tpdl01_t2(id,c_ref,c_code,c_qty) VALUES (%s,%s,%s,%s)"
)
_INSERT_T3 = (
    f"INSERT INTO `{LOAD_SRC_DB}`.tpdl01_t3(id,c_name,c_city) VALUES (%s,%s,%s)"
)
_INSERT_T4 = (
    f"INSERT INTO `{LOAD_SRC_DB}`.tpdl01_t4(id,c_ref,c_method,c_paid) VALUES (%s,%s,%s,%s)"
)
_INSERT_TYPED = (
    f"INSERT INTO `{LOAD_SRC_DB}`.tpdl01_typed("
    "id,c_tinyint,c_int,c_bigint,c_decimal,c_float,c_double,c_varchar,"
    "c_date,c_datetime,c_timestamp,c_json,c_bool"
    ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
)


def _seed_load_range(sql: str, row_fn, id_end: int) -> None:
    c = pymysql.connect(**LOAD_SRC, autocommit=True)
    try:
        with c.cursor() as cur:
            cur.execute("SET time_zone = '+08:00'")
            for start in range(1, id_end + 1, PEAK_BATCH):
                end = min(start + PEAK_BATCH - 1, id_end)
                cur.executemany(sql, [row_fn(i) for i in range(start, end + 1)])
    finally:
        c.close()


def _load_typed_row(i: int) -> tuple:
    return (
        i,
        (i % 100) - 50,
        i,
        i * 1000,
        float(i % 1000) + 0.1234,
        float(i % 50) + 0.5,
        float(i) + 0.25,
        f"row-{i}",
        f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
        f"2024-06-15 {(i % 24):02d}:30:00.123456",
        f"2024-06-15 {(i % 24):02d}:30:00.123456",
        json.dumps({"id": i, "k": "v"}),
        i % 2,
    )


def ensure_load_schema() -> None:
    """Recreate source tables and wipe the matching Doris targets."""
    for name, cols in LOAD_DDL.items():
        load_exec_sql(f"DROP TABLE IF EXISTS `{LOAD_SRC_DB}`.`{name}`")
        load_exec_sql(f"CREATE TABLE `{LOAD_SRC_DB}`.`{name}` {cols} ENGINE=InnoDB")
    for name in LOAD_TABLES:
        try:
            doris_exec(f"DROP TABLE IF EXISTS `{SNK_DB}`.`{name}`")
        except pymysql.Error:
            pass


def seed_load_baseline() -> None:
    """Seed every load table in parallel (one connection per table, batch inserts)."""
    cities = ("hz", "sh", "bj", "sz")
    specs = (
        ("tpdl01_t1", _INSERT_T1, lambda i: (i, i % 1000 + 1, float(i % 100) + 0.5, "ok")),
        ("tpdl01_t2", _INSERT_T2, lambda i: (i, i, f"C{i % 500:04d}", (i % 5) + 1)),
        ("tpdl01_t3", _INSERT_T3, lambda i: (i, f"n{i}", cities[i % 4])),
        (
            "tpdl01_t4",
            _INSERT_T4,
            lambda i: (i, i, "m1" if i % 2 == 0 else "m2", float(i % 50) + 1.0),
        ),
        (LOAD_TYPED, _INSERT_TYPED, _load_typed_row),
    )
    seed_workers = max(1, min(WORKERS * 2, 8, len(specs)))
    log(
        f"    seed workers={seed_workers} "
        f"(LOAD_PEAK_WORKERS={WORKERS} × 2, cap=8, one conn/table)"
    )
    with ThreadPoolExecutor(max_workers=seed_workers) as pool:
        futs = {
            pool.submit(_seed_load_range, sql, fn, LOAD_ROWS): name
            for name, sql, fn in specs
        }
        for fut in as_completed(futs):
            name = futs[fut]
            try:
                fut.result()
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(f"{LOAD_SRC_DB}.{name}: seed failed: {exc}") from exc
            log(f"    seed done {name}")


def peak_writer(worker_id: int, stop_at: float, counter: list[int], lock: threading.Lock) -> int:
    """Insert t1+typed batches until stop_at; ids stay above the snapshot range."""
    written = 0
    base = LOAD_ROWS + 1 + worker_id * 10_000_000
    seq = 0
    c = pymysql.connect(**LOAD_SRC, autocommit=True)
    try:
        with c.cursor() as cur:
            cur.execute("SET time_zone = '+08:00'")
            while time.time() < stop_at:
                orders = []
                typed = []
                for _ in range(PEAK_BATCH):
                    seq += 1
                    i = base + seq
                    orders.append((i, i % 1000 + 1, float(i % 100) + 0.5, "peak"))
                    typed.append(_load_typed_row(i))
                cur.executemany(_INSERT_T1, orders)
                cur.executemany(_INSERT_TYPED, typed)
                written += len(orders)
                with lock:
                    counter[0] += len(orders)
    finally:
        c.close()
    return written


def wait_doris_count(
    table: str,
    minimum: int,
    *,
    deadline: float,
    interval: float = 5.0,
    desc: str = "doris count",
) -> int:
    """Poll Doris COUNT(*) until the table is visible and reaches minimum."""
    last = 0
    last_log = 0.0
    while time.time() < deadline:
        ensure_alive(desc)
        try:
            last = doris_count(table)
        except pymysql.Error:
            last = 0
        if last >= minimum:
            return last
        now = time.time()
        if now - last_log >= 15:
            log(f"    {desc}: {last}/{minimum} rows")
            last_log = now
        time.sleep(interval)
    raise TimeoutError(
        f"timeout waiting for {desc}: {SNK_DB}.{table} has {last} rows, need {minimum}"
    )


def wait_load_catchup(
    expect_t1: int,
    expect_typed: int,
    *,
    timeout: float = 3600,
    interval: float = 10.0,
    log_every: float = 15.0,
) -> float:
    """Wait until t1 and typed are within 1% of the post-storm source counts."""
    t0 = time.time()
    deadline = t0 + timeout
    last_log = 0.0
    format_cpu_mem()  # baseline /proc/stat so the first periodic line has cpu=%
    while time.time() < deadline:
        ensure_alive("go-cdc doris load")
        try:
            snk_t1 = doris_count("tpdl01_t1")
            snk_ty = doris_count(LOAD_TYPED)
        except pymysql.Error:
            snk_t1 = snk_ty = 0
        lag_t1 = abs(expect_t1 - snk_t1) / max(expect_t1, 1)
        lag_ty = abs(expect_typed - snk_ty) / max(expect_typed, 1)
        if lag_t1 <= 0.01 and lag_ty <= 0.01:
            elapsed = time.time() - t0
            log(f"TIMING  wait [doris_peak catch-up] done in {elapsed:.1f}s")
            return elapsed
        now = time.time()
        if now - last_log >= log_every:
            cdc_pid = proc.pid if proc is not None else None
            log(
                f"    doris peak catch-up {now - t0:.0f}s: "
                f"t1 {snk_t1}/{expect_t1} lag={lag_t1:.2%} | "
                f"typed {snk_ty}/{expect_typed} lag={lag_ty:.2%} | {format_host_res(cdc_pid)}"
            )
            last_log = now
        time.sleep(interval)
    raise TimeoutError(
        f"timeout waiting for doris_peak catch-up after {time.time() - t0:.1f}s "
        f"(t1 {lag_t1:.2%}, typed {lag_ty:.2%})"
    )


def _norm_typed(rows: list[dict]) -> dict[int, dict]:
    """Normalise the spot-check columns so MySQL and Doris values compare equal."""
    out: dict[int, dict] = {}
    for row in rows:
        rid = int(row["id"])
        doc = row["c_json"]
        if isinstance(doc, (bytes, bytearray)):
            doc = doc.decode("utf-8", "replace")
        try:
            doc = json.loads(doc) if doc is not None else None
        except (TypeError, ValueError):
            pass
        try:
            dec = str(Decimal(str(row["c_decimal"])).normalize())
        except (InvalidOperation, TypeError, ValueError):
            dec = str(row["c_decimal"])
        out[rid] = {
            "c_int": int(row["c_int"]),
            "c_bigint": int(row["c_bigint"]),
            "c_decimal": dec,
            "c_varchar": str(row["c_varchar"]),
            "c_date": str(row["c_date"]),
            "c_datetime": str(row["c_datetime"]),
            "c_json": doc,
            "c_bool": int(row["c_bool"]),
        }
    return out


def tp_d05_bulk_multi_table_snapshot() -> None:
    """Bulk snapshot across five routed tables (DR05)."""
    stop_cdc()
    ensure_load_schema()
    log(f"==> doris load seed {LOAD_ROWS} rows x {len(LOAD_TABLES)} tables")
    t0 = time.time()
    seed_load_baseline()
    load_timings["doris_seed_baseline"] = time.time() - t0
    log(f"TIMING  doris_seed_baseline {load_timings['doris_seed_baseline']:.1f}s")

    t0 = time.time()
    start_load_cdc()
    threshold = max(1, int(LOAD_ROWS * 0.99))
    deadline = time.time() + 3600
    for table in LOAD_TABLES:
        wait_doris_count(
            table,
            threshold,
            deadline=deadline,
            desc=f"d05 {table} snapshot",
        )
    load_timings["doris_snapshot"] = time.time() - t0
    log(f"TIMING  doris_snapshot {load_timings['doris_snapshot']:.1f}s")

    rows = []
    ok = True
    for table in LOAD_TABLES:
        src = load_count_rows(table)
        snk = doris_count(table)
        lag = abs(src - snk) / max(src, 1)
        hit = src == LOAD_ROWS and lag <= 0.01
        ok = ok and hit
        rows.append(
            f"| {table} | {LOAD_ROWS} | {src} | {snk} | {lag:.2%} | "
            f"{'✅' if hit else '❌'} |"
        )
    detail = (
        "| Table | Expected | Source | Doris | Lag | Verdict |\n"
        "| --- | ---: | ---: | ---: | ---: | --- |\n"
        + "\n".join(rows)
        + "\n"
        + f"\nseed={load_timings['doris_seed_baseline']:.1f}s, "
        + f"snapshot={load_timings['doris_snapshot']:.1f}s, "
        + f"tables={len(LOAD_TABLES)}\n"
        + f"\n| Summary | | | | | **{'✅ PASS' if ok else '❌ FAIL'}** |"
    )
    record(
        TPResult(
            id="TP-DR05",
            title="bulk multi-table snapshot (t1–t4 + typed)",
            action=(
                f"seed {LOAD_ROWS} rows x {len(LOAD_TABLES)} MySQL tables; "
                "wait Doris COUNT(*) ≥99%"
            ),
            expected_desc="Every table keeps src/sink delta <=1% after snapshot",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_d07_typed_spotcheck() -> None:
    """Spot-check mappable typed columns after the bulk snapshot (DR07)."""
    ids = sorted({1, max(1, LOAD_ROWS // 2), max(1, LOAD_ROWS)})
    id_list = ", ".join(str(i) for i in ids)
    columns = (
        "id, c_int, c_bigint, c_decimal, c_varchar, "
        "DATE_FORMAT(c_date, '%Y-%m-%d') AS c_date, "
        "DATE_FORMAT(c_datetime, '%Y-%m-%d %H:%i:%s') AS c_datetime, "
        "c_json, c_bool"
    )
    src_rows = load_query(
        f"SELECT {columns} FROM `{LOAD_SRC_DB}`.`{LOAD_TYPED}` "
        f"WHERE id IN ({id_list}) ORDER BY id"
    )
    snk_rows = doris_query(
        f"SELECT {columns} FROM `{SNK_DB}`.`{LOAD_TYPED}` "
        f"WHERE id IN ({id_list}) ORDER BY id"
    )
    src = _norm_typed(src_rows)
    snk = _norm_typed(snk_rows)
    missing = [i for i in ids if i not in src or i not in snk]
    mismatched = [i for i in ids if i in src and i in snk and src[i] != snk[i]]
    ok = not missing and not mismatched
    lines = []
    for i in ids:
        if i in missing:
            verdict = "missing"
        elif i in mismatched:
            verdict = "mismatch: " + ", ".join(
                key for key in src[i] if src[i][key] != snk[i][key]
            )
        else:
            verdict = "match"
        lines.append(f"| {i} | {verdict} | {'✅' if verdict == 'match' else '❌'} |")
    detail = (
        "| Sample id | Verdict | Result |\n| --- | --- | --- |\n"
        + "\n".join(lines)
        + "\n"
        + f"\nchecked: int/bigint/decimal/varchar/date/datetime/json/bool; "
        + f"rows src={len(src)} sink={len(snk)}\n"
        + f"\n| Summary | | **{'✅ PASS' if ok else '❌ FAIL'}** |"
    )
    record(
        TPResult(
            id="TP-DR07",
            title="typed columns under bulk load (spot-check)",
            action=f"compare ids {ids} source vs Doris for mappable types",
            expected_desc="int/decimal/varchar/date/datetime/json/bool match",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_d06_peak_catchup() -> None:
    """Concurrent INSERT storm on t1 + typed; Doris must catch up within 1% (DR06)."""
    ensure_alive("go-cdc doris load")
    src_before = load_count_rows("tpdl01_t1")
    typed_before = load_count_rows(LOAD_TYPED)

    counter = [0]
    lock = threading.Lock()
    stop_at = time.time() + PEAK_SEC
    log(f"==> doris peak writers {WORKERS}x{PEAK_SEC}s (batch {PEAK_BATCH})")
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futs = [
            pool.submit(peak_writer, i, stop_at, counter, lock)
            for i in range(WORKERS)
        ]
        written_each = [f.result() for f in as_completed(futs)]
    elapsed = max(time.time() - t0, 0.001)
    written = sum(written_each)
    load_timings["doris_peak_write"] = elapsed
    log(
        f"TIMING  doris_peak_write {elapsed:.1f}s "
        f"({written} rows ≈ {written / elapsed:.0f} rps)"
    )

    src_after = load_count_rows("tpdl01_t1")
    typed_after = load_count_rows(LOAD_TYPED)
    load_timings["doris_peak_catchup"] = wait_load_catchup(src_after, typed_after)
    log(f"TIMING  doris_peak_catchup {load_timings['doris_peak_catchup']:.1f}s")

    ck_ok = wait_ckpt(CK_LOAD, timeout_s=60)
    ck = json.loads(CK_LOAD.read_text()) if ck_ok and CK_LOAD.exists() else {}
    has_offset = bool(ck.get("acked_offset"))
    snk_t1 = doris_count("tpdl01_t1")
    snk_ty = doris_count(LOAD_TYPED)
    lag_t1 = abs(src_after - snk_t1) / max(src_after, 1)
    lag_ty = abs(typed_after - snk_ty) / max(typed_after, 1)
    grew = src_after > src_before and typed_after > typed_before
    job_alive = proc is not None and proc.poll() is None
    ok = written > 0 and grew and lag_t1 <= 0.01 and lag_ty <= 0.01 and job_alive and has_offset
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| peak writes | {written} rows / {elapsed:.1f}s ≈ {written / elapsed:.0f} rps | "
        f"{'✅' if written > 0 else '❌'} |\n"
        f"| t1 source grew | {src_before}→{src_after} | {'✅' if src_after > src_before else '❌'} |\n"
        f"| t1 src/sink | {src_after}/{snk_t1} lag={lag_t1:.2%} | "
        f"{'✅' if lag_t1 <= 0.01 else '❌'} |\n"
        f"| typed src/sink | {typed_after}/{snk_ty} lag={lag_ty:.2%} | "
        f"{'✅' if lag_ty <= 0.01 else '❌'} |\n"
        f"| catch-up time | {load_timings['doris_peak_catchup']:.1f}s | |\n"
        f"| job still running | {job_alive} | {'✅' if job_alive else '❌'} |\n"
        f"| resume offset recorded | {'yes' if has_offset else 'no'} | "
        f"{'✅' if has_offset else '❌'} |\n"
        f"| Summary | | **{'✅ PASS' if ok else '❌ FAIL'}** |"
    )
    record(
        TPResult(
            id="TP-DR06",
            title="peak incremental stress + catch-up",
            action=(
                f"{WORKERS} workers x {PEAK_SEC}s concurrent INSERT "
                f"t1+typed; wait Doris counts"
            ),
            expected_desc="After catch-up delta <=1%; job up; resume offset saved",
            passed=ok,
            detail_md=detail,
        )
    )


def write_report() -> Path:
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# MySQL → Doris integration report",
        "",
        f"- Doris FE: `{cfg.DORIS_HOST}:{cfg.DORIS_HTTP_PORT}` (HTTP) / "
        f"`{cfg.DORIS_SQL_PORT}` (SQL)",
        f"- Sink database: `{SNK_DB}`",
        f"- Pipeline: `{cfg.PIPELINE_DORIS_NAME}`",
        f"- MySQL source: `{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.FUNC_SRC_DB}`",
        "",
        "Note: Doris sink v1 rejects DELETE / filter-retraction deletes; "
        "this suite does not cover delete paths.",
        "",
    ]
    if load_timings:
        lines += [
            "## Load phase plan",
            "",
            f"- Load source: `{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{LOAD_SRC_DB}`",
            f"- Load tables: {', '.join(f'`{t}`' for t in LOAD_TABLES)}",
            "",
            "| Knob | Value | Meaning |",
            "| --- | --- | --- |",
            f"| `LOAD_PROFILE` | {cfg.LOAD_PROFILE or '(unset)'} | CI/local tier label |",
            f"| `LOAD_ROWS_PER_TABLE` | {LOAD_ROWS} | rows in each of {len(LOAD_TABLES)} load tables |",
            f"| `LOAD_PEAK_SECONDS` | {PEAK_SEC} | peak writer duration |",
            f"| `LOAD_PEAK_WORKERS` | {WORKERS} | concurrent peak writers (seed uses ×2, cap 8) |",
            f"| `LOAD_PEAK_BATCH` | {PEAK_BATCH} | rows per INSERT batch |",
            f"| `LOAD_PARALLELISM` | {cfg.LOAD_PARALLELISM} | snapshot table readers |",
            "",
            "Note: the load phase uses Doris-mappable types only "
            "(no BLOB/ENUM/BIT/SET/TIME), so it cannot reuse `tpl01_typed` / "
            "`tpl01_large` from the mysql→mysql suite.",
            "",
            "## Load phase timings",
            "",
            "| Phase | Seconds |",
            "| --- | ---: |",
        ]
        lines += [f"| {name} | {sec:.1f} |" for name, sec in load_timings.items()]
        lines.append("")
    failed = 0
    for r in results:
        mark = "✅ PASS" if r.passed else "❌ FAIL"
        if not r.passed:
            failed += 1
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
    """Run one TP; convert unexpected errors into a FAIL row for the CI report."""
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 — harness must still emit a report
        log(tp_id, "ERROR", title, exc)
        record(
            TPResult(
                id=tp_id,
                title=title,
                action="(aborted)",
                expected_desc="no unhandled exception",
                passed=False,
                detail_md=(
                    "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
                    f"| exception | `{type(exc).__name__}: {exc}` | ❌ |\n"
                ),
            )
        )


def main() -> int:
    WORKDIR.mkdir(parents=True, exist_ok=True)
    # Probe Doris before starting the CDC binary.
    try:
        c = doris_conn()
        c.close()
    except Exception as exc:  # noqa: BLE001 — surface connection errors as harness fail
        log("Doris SQL not reachable:", exc)
        return 1
    drop_sink_tables()
    try:
        _run_tp(
            "TP-DR01",
            "snapshot: route + projection + filter",
            tp_d01_snapshot_route_proj_filter,
        )
        _run_tp(
            "TP-DR02",
            "incremental insert + update + filter",
            tp_d02_incremental_insert_update,
        )
        _run_tp(
            "TP-DR03",
            "schema evolution ADD COLUMN",
            tp_d03_add_column,
        )
        _run_tp(
            "TP-DR04",
            "restart at-least-once",
            tp_d04_restart_at_least_once,
        )
        _run_tp(
            "TP-DR05",
            "bulk multi-table snapshot (t1–t4 + typed)",
            tp_d05_bulk_multi_table_snapshot,
        )
        _run_tp(
            "TP-DR07",
            "typed columns under bulk load (spot-check)",
            tp_d07_typed_spotcheck,
        )
        _run_tp(
            "TP-DR06",
            "peak incremental stress + catch-up",
            tp_d06_peak_catchup,
        )
    finally:
        stop_cdc()
        path = write_report()
        log("REPORT", cfg.project_display_path(path))
    failed = [r for r in results if not r.passed]
    log("PASS", sum(1 for r in results if r.passed), "FAIL", len(failed))
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
