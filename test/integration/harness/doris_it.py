#!/usr/bin/env python3
"""MySQL → Doris pipeline integration suite (TP-DR01..DR04).

Mirrors kafka_it style: seed MySQL → start go-cdc YAML job → assert sink state.
Doris v1 rejects DELETE and filter-retraction deletes; this suite covers
snapshot / route / projection / filter, incremental insert+update, ADD COLUMN,
and restart at-least-once (UNIQUE KEY converge).

IDs use TP-DR* so they do not collide with types suite TP-D01..D04.

Requires MySQL (mysql-up) + Doris FE/BE (doris-up).
"""
from __future__ import annotations

import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import pymysql

from . import config as cfg
from .logutil import log

SRC = cfg.conn_kwargs(cfg.SRC_USER, cfg.SRC_PASSWORD, cfg.FUNC_SRC_DB)
WORKDIR = cfg.WORKDIR
BINARY = cfg.BINARY
CK = Path(cfg.CHECKPOINT_DORIS_PATH)
YAML = WORKDIR / "pipeline.doris.yaml"
LOG = WORKDIR / "cdc-doris.log"
REPORT = cfg.REPORT_DORIS
SNK_DB = cfg.DORIS_SNK_DB

results: list["TPResult"] = []
proc: subprocess.Popen | None = None


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
    global proc
    stop_cdc()
    if reset_ckpt and CK.exists():
        CK.unlink()
    CK.parent.mkdir(parents=True, exist_ok=True)
    cfg.render_template("pipeline.doris.yaml.tpl", YAML)
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
        text = LOG.read_text(errors="replace")[-2000:] if LOG.exists() else ""
        raise RuntimeError(f"{desc} exited ({proc.returncode}):\n{text}")


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
            ):
                try:
                    cur.execute(f"DROP TABLE IF EXISTS `{SNK_DB}`.`{t}`")
                except pymysql.Error:
                    pass
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
    finally:
        stop_cdc()
        path = write_report()
        log("REPORT", cfg.project_display_path(path))
    failed = [r for r in results if not r.passed]
    log("PASS", sum(1 for r in results if r.passed), "FAIL", len(failed))
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
