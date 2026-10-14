#!/usr/bin/env python3
"""go-cdc integration harness: Expected vs Actual tables per TP (TP-01..TP-20)."""
from __future__ import annotations

import json
import re
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
from .logutil import log

SRC = cfg.conn_kwargs(cfg.SRC_USER, cfg.SRC_PASSWORD, cfg.FUNC_SRC_DB)
SNK = cfg.conn_kwargs(cfg.SINK_USER, cfg.SINK_PASSWORD, cfg.FUNC_SNK_DB)
SRC_DB = cfg.FUNC_SRC_DB
SINK_DB = cfg.FUNC_SNK_DB
WORKDIR = cfg.WORKDIR
BINARY = cfg.BINARY
CK = Path(cfg.CHECKPOINT_PATH)
LOG = WORKDIR / "cdc.log"
YAML = WORKDIR / "pipeline.yaml"
REPORT_DIR = cfg.REPORT_DIR

results: list["TPResult"] = []
proc: subprocess.Popen | None = None


def conn(dbcfg):
    return pymysql.connect(**dbcfg, cursorclass=pymysql.cursors.DictCursor, autocommit=True)


def q(dbcfg, sql: str, args=None) -> list[dict]:
    with conn(dbcfg) as c:
        with c.cursor() as cur:
            cur.execute(sql, args or ())
            return list(cur.fetchall()) if cur.description else []


def exec_sql(dbcfg, sql: str, args=None):
    with conn(dbcfg) as c:
        with c.cursor() as cur:
            cur.execute(sql, args or ())


def table_exists(dbcfg, database: str, table: str) -> bool:
    rows = q(
        {**dbcfg, "database": database},
        "SELECT 1 AS ok FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
        (database, table),
    )
    return bool(rows)


def dump_table(dbcfg, database: str, table: str, order: str) -> list[dict]:
    if not table_exists(dbcfg, database, table):
        return []
    return q({**dbcfg, "database": database}, f"SELECT * FROM `{database}`.`{table}` ORDER BY {order}")


def md_table(rows: list[dict], columns: list[str] | None = None) -> str:
    if not rows:
        return "_(empty table / table missing)_"
    cols = columns or list(rows[0].keys())
    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join(["---"] * len(cols)) + " |",
    ]
    for r in rows:
        cells = [cfg.md_cell(r.get(c)) for c in cols]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def normalize(rows: list[dict], columns: list[str]) -> list[tuple]:
    return sorted(
        tuple(None if r.get(c) is None else str(r.get(c)) for c in columns) for r in rows
    )


def side_by_side(expected: list[dict], actual: list[dict], columns: list[str]) -> tuple[bool, str]:
    e = normalize(expected, columns)
    a = normalize(actual, columns)
    ok = e == a
    n_cols = 1 + len(columns) * 2 + 1
    header = "| " + " | ".join(["Key/#"] + [f"exp.{c}" for c in columns] + [f"act.{c}" for c in columns] + ["Verdict"]) + " |"
    sep = "| " + " | ".join(["---"] * n_cols) + " |"
    lines = [header, sep]
    n = max(len(e), len(a), 1)
    for i in range(n):
        er = e[i] if i < len(e) else tuple(["_(missing)_"] * len(columns))
        ar = a[i] if i < len(a) else tuple(["_(extra)_"] * len(columns))
        hit = i < len(e) and i < len(a) and e[i] == a[i]
        key = er[0] if i < len(e) else (ar[0] if i < len(a) else i + 1)
        row = (
            [cfg.md_cell(key)]
            + [cfg.md_cell(x) for x in er]
            + [cfg.md_cell(x) for x in ar]
            + ["✅" if hit else "❌"]
        )
        lines.append("| " + " | ".join(row) + " |")
    if len(e) != len(a):
        ok = False
        cells = [""] * n_cols
        cells[0] = "Row count"
        cells[1] = str(len(e))
        cells[1 + len(columns)] = str(len(a))
        cells[-1] = "❌"
        lines.append("| " + " | ".join(cells) + " |")
    summary = ["Summary"] + [""] * (n_cols - 2) + [f"**{'✅ PASS' if ok else '❌ FAIL'}**"]
    lines.append("| " + " | ".join(summary) + " |")
    return ok, "\n".join(lines)


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


def start_job(yaml_path: Path = YAML):
    global proc
    LOG.write_text("")
    proc = subprocess.Popen(
        [BINARY, str(yaml_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    def pump():
        assert proc and proc.stdout
        with LOG.open("a") as f:
            for line in proc.stdout:
                f.write(line)
                f.flush()

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


def kill_job_hard() -> int | None:
    """SIGKILL without graceful checkpoint flush on exit. Returns process return code."""
    global proc
    code = None
    if proc is not None:
        if proc.poll() is None:
            proc.send_signal(signal.SIGKILL)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
        code = proc.returncode
    proc = None
    return code


def wait_until(predicate, timeout=90, interval=1.0, desc="condition"):
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            if predicate():
                return True
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(interval)
    raise TimeoutError(f"timeout waiting for {desc}: {last_err}")


def record(tp: TPResult, override: bool | None = None, compare_md: str | None = None):
    ok, cmp_md = side_by_side(tp.expected_rows, tp.actual_rows, tp.columns)
    tp.passed = ok if override is None else override
    tp.compare_md = compare_md if compare_md is not None else cmp_md
    results.append(tp)
    log(f"[{tp.id}] {'PASS' if tp.passed else 'FAIL'} {tp.title}", flush=True)


def set_tables_pattern(pattern: str):
    text = YAML.read_text(encoding="utf-8")
    text = re.sub(r"tables:.*", f"tables: '{pattern}'", text, count=1)
    YAML.write_text(text, encoding="utf-8")


def set_source_flag(key: str, value: bool):
    """Upsert a boolean source.* key in the rendered pipeline YAML."""
    text = YAML.read_text(encoding="utf-8")
    line = f"  {key}: {'true' if value else 'false'}"
    pattern = re.compile(rf"^  {re.escape(key)}:.*$", re.M)
    if pattern.search(text):
        text = pattern.sub(line, text)
    else:
        text = re.sub(
            r"(  scan\.snapshot\.fetch\.size:.*\n)",
            rf"\1{line}\n",
            text,
            count=1,
        )
    YAML.write_text(text, encoding="utf-8")


def log_contains(needle: str) -> bool:
    if not LOG.exists():
        return False
    return needle in LOG.read_text(encoding="utf-8", errors="replace")


def _captured_has(ck: dict, table: str) -> bool:
    return any(table in str(x) for x in (ck.get("captured_tables") or []))


def run_newly_added_combo(
    *,
    tp_id: str,
    title: str,
    newly_added: bool,
    snapshot: bool,
    binlog_newly: bool,
    diff_table: str,
    seed_id: int,
    crt_table: str,
    crt_id: int,
    tables_pattern: str,
    live_id: int | None = None,
) -> None:
    """One start with fixed (A,B,C): assert config-diff and runtime CREATE together."""
    stop_job()
    time.sleep(1)
    set_source_flag("scan.newly-added-table.enabled", newly_added)
    set_source_flag("scan.newly-added-table.snapshot.enabled", snapshot)
    set_source_flag("scan.binlog.newly-added-table.enabled", binlog_newly)
    set_tables_pattern(tables_pattern)

    diff_out = f"{diff_table}_out"
    crt_out = f"{crt_table}_out"
    try:
        exec_sql(SNK, f"DROP TABLE IF EXISTS {diff_out}")
        exec_sql(SNK, f"DROP TABLE IF EXISTS {crt_out}")
    except Exception:
        pass
    exec_sql(SRC, f"DROP TABLE IF EXISTS {crt_table}")

    # snapshot=false: user prepared sink (empty — no seed history).
    if newly_added and not snapshot:
        exec_sql(
            SNK,
            f"CREATE TABLE {diff_out} ("
            "id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL"
            ") ENGINE=InnoDB",
        )

    start_job()
    time.sleep(3)

    if newly_added and not snapshot:
        wait_until(
            lambda: log_contains("scan.newly-added-table.snapshot.enabled=false"),
            desc=f"{tp_id} snapshot-disabled warn",
            timeout=30,
        )
        assert live_id is not None
        exec_sql(SRC, f"INSERT INTO {diff_table}(id,name) VALUES ({live_id},'live-{tp_id}')")
        wait_until(
            lambda: any(
                str(r.get("id")) == str(live_id)
                for r in dump_table(SNK, SINK_DB, diff_out, "id")
            ),
            desc=f"{tp_id} diff live row",
        )
    elif newly_added and snapshot:
        wait_until(
            lambda: table_exists(SNK, SINK_DB, diff_out)
            and any(
                str(r.get("id")) == str(seed_id)
                for r in dump_table(SNK, SINK_DB, diff_out, "id")
            ),
            desc=f"{tp_id} diff snapshot seed",
        )
    else:
        # A=false: config-diff must stay ignored even when C=true (CREATE-only).
        probe_id = seed_id + 1000
        exec_sql(SRC, f"INSERT INTO {diff_table}(id,name) VALUES ({probe_id},'probe-{tp_id}')")
        time.sleep(8)
        if table_exists(SNK, SINK_DB, diff_out) and any(
            str(r.get("id")) == str(probe_id) for r in dump_table(SNK, SINK_DB, diff_out, "id")
        ):
            raise AssertionError(
                f"{tp_id}: A=false must ignore live INSERT on {diff_table} "
                f"(saw id={probe_id} on sink)"
            )

    # Runtime CREATE axis (C).
    exec_sql(
        SRC,
        f"CREATE TABLE {crt_table} ("
        "id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL"
        ") ENGINE=InnoDB",
    )
    exec_sql(SRC, f"INSERT INTO {crt_table}(id,name) VALUES ({crt_id},'crt-{tp_id}')")
    if binlog_newly:
        wait_until(
            lambda: table_exists(SNK, SINK_DB, crt_out)
            and any(
                str(r.get("id")) == str(crt_id)
                for r in dump_table(SNK, SINK_DB, crt_out, "id")
            ),
            desc=f"{tp_id} runtime CREATE row",
        )
    else:
        time.sleep(8)

    time.sleep(2)
    ck = json.loads(CK.read_text()) if CK.exists() else {}
    diff_rows = dump_table(SNK, SINK_DB, diff_out, "id")
    crt_rows = dump_table(SNK, SINK_DB, crt_out, "id")
    has_seed = any(str(r.get("id")) == str(seed_id) for r in diff_rows)
    has_live = (
        any(str(r.get("id")) == str(live_id) for r in diff_rows) if live_id is not None else False
    )
    has_crt = any(str(r.get("id")) == str(crt_id) for r in crt_rows)
    diff_cap = _captured_has(ck, diff_table)
    crt_cap = _captured_has(ck, crt_table)
    warn_ok = (not newly_added) or snapshot or log_contains(
        "scan.newly-added-table.snapshot.enabled=false"
    )
    job_alive = proc is not None and proc.poll() is None

    if not newly_added:
        diff_ok = (not table_exists(SNK, SINK_DB, diff_out)) and (not diff_cap) and (not has_seed)
    elif snapshot:
        diff_ok = has_seed and diff_cap
    else:
        diff_ok = (not has_seed) and has_live and diff_cap and warn_ok

    if binlog_newly:
        crt_ok = has_crt and crt_cap
    else:
        crt_ok = (not table_exists(SNK, SINK_DB, crt_out)) and (not crt_cap) and (not has_crt)

    ok = diff_ok and crt_ok and job_alive
    flags = f"A={str(newly_added).lower()} B={str(snapshot).lower()} C={str(binlog_newly).lower()}"
    cmp_md = (
        "| Check | Expected | Actual | Verdict |\n| --- | --- | --- | --- |\n"
        f"| flags | {flags} | {flags} | ✅ |\n"
        f"| config-diff ok | yes | {'yes' if diff_ok else 'no'} | {'✅' if diff_ok else '❌'} |\n"
        f"| runtime CREATE ok | yes | {'yes' if crt_ok else 'no'} | {'✅' if crt_ok else '❌'} |\n"
        f"| diff seed id={seed_id} | "
        f"{'present' if newly_added and snapshot else 'absent'} | "
        f"{'yes' if has_seed else 'no'} | "
        f"{'✅' if (has_seed if (newly_added and snapshot) else not has_seed) else '❌'} |\n"
        f"| crt id={crt_id} | {'present' if binlog_newly else 'absent'} | "
        f"{'yes' if has_crt else 'no'} | "
        f"{'✅' if (has_crt if binlog_newly else not has_crt) else '❌'} |\n"
        f"| job running | yes | {'yes' if job_alive else 'no'} | {'✅' if job_alive else '❌'} |\n"
        f"| Summary | | | **{'✅ PASS' if ok else '❌ FAIL'}** |"
    )
    record(
        TPResult(
            id=tp_id,
            title=title,
            action=(
                f"{flags}; add {diff_table} to tables; restart; "
                f"runtime CREATE {crt_table}+INSERT({crt_id})"
            ),
            expected_desc=(
                f"diff={'snapshot' if newly_added and snapshot else ('binlog-only' if newly_added else 'ignore')}; "
                f"CREATE={'follow' if binlog_newly else 'drop'}"
            ),
            expected_rows=[{"ok": "yes"}],
            actual_rows=[{"ok": "yes" if ok else "no"}],
            columns=["ok"],
            extra_md=(
                f"#### {diff_out}\n"
                + md_table(diff_rows, ["id", "name"])
                + f"\n\n#### {crt_out}\n"
                + md_table(crt_rows, ["id", "name"])
                + "\n"
            ),
        ),
        override=ok,
        compare_md=cmp_md,
    )


def _checkpoint_shape_ok(ck: dict) -> bool:
    """True when acked_offset has file+pos and/or a non-empty gtid (no values leaked)."""
    ao = ck.get("acked_offset")
    if not isinstance(ao, dict) or not ao:
        return False
    gtid = ao.get("gtid")
    if isinstance(gtid, str) and gtid.strip():
        return True
    file_ok = isinstance(ao.get("file"), str) and bool(ao.get("file"))
    pos = ao.get("pos")
    pos_ok = isinstance(pos, int) and pos > 0
    return file_ok and pos_ok


def _checkpoint_shape_label(ck: dict) -> str:
    ao = ck.get("acked_offset")
    if not isinstance(ao, dict) or not ao:
        return "missing"
    parts = []
    if isinstance(ao.get("file"), str) and ao.get("file"):
        parts.append("file")
    if isinstance(ao.get("pos"), int) and ao.get("pos", 0) > 0:
        parts.append("pos")
    if isinstance(ao.get("gtid"), str) and ao.get("gtid").strip():
        parts.append("gtid")
    return "+".join(parts) if parts else "empty"


def main() -> int:
    global proc
    WORKDIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    if not Path(BINARY).exists():
        raise SystemExit(f"go-cdc not found: {BINARY} (run scripts/run-e2e.sh to build)")

    cfg.render_template("pipeline.yaml.tpl", YAML)
    for t in [
        "tp01_rows_out",
        "tp02_keys_out",
        "tp10_add_out",
        "tp11_ddl_out",
        "tp16_diff_out",
        "tp16_crt_out",
        "tp17_diff_out",
        "tp17_crt_out",
        "tp18_diff_out",
        "tp18_crt_out",
        "tp19_diff_out",
        "tp19_crt_out",
        "tp20_diff_out",
        "tp20_crt_out",
        "tp20_diff_out",
        "tp20_crt_out",
        "tp07_fail_out",
    ]:
        try:
            exec_sql(SNK, f"DROP TABLE IF EXISTS {t}")
        except Exception:
            pass
    if CK.exists():
        CK.unlink()

    # TP-01
    start_job()
    wait_until(
        lambda: table_exists(SNK, SINK_DB, "tp01_rows_out")
        and len(dump_table(SNK, SINK_DB, "tp01_rows_out", "id")) >= 2,
        desc="initial tp01_rows snapshot",
    )
    time.sleep(2)
    actual_users = dump_table(SNK, SINK_DB, "tp01_rows_out", "id")
    expect_users = [
        {"id": 1, "name": "alice", "city": "hz"},
        {"id": 3, "name": "ann", "city": "hz"},
    ]
    cols_ok = set(actual_users[0].keys()) == {"id", "name", "city"} if actual_users else False
    bob = [r for r in actual_users if str(r.get("id")) == "2"]
    extra = (
        f"- Sink columns: `{sorted(actual_users[0].keys()) if actual_users else []}` → "
        f"{'✅' if cols_ok else '❌'}\n"
        f"- Filtered-out id=2: {'✅ absent' if not bob else '❌ present'}\n"
    )
    record(
        TPResult(
            id="TP-01",
            title="Initial snapshot + projection/filter (city=hz)",
            action="First start: project id,name,city; filter city='hz'",
            expected_desc="Sink has id=1,3 only; no amount; no id=2",
            expected_rows=expect_users,
            actual_rows=actual_users,
            columns=["id", "name", "city"],
            extra_md=extra,
        ),
        override=bool(cols_ok and not bob and normalize(actual_users, ["id", "name", "city"]) == normalize(expect_users, ["id", "name", "city"])),
    )

    wait_until(lambda: table_exists(SNK, SINK_DB, "tp02_keys_out"), desc="tp02_keys out")

    # TP-02
    exec_sql(SRC, "UPDATE tp02_keys SET docid=%s, title=%s WHERE id=1", ("B", "title-b"))
    wait_until(
        lambda: any(str(r.get("docid")) == "B" for r in dump_table(SNK, SINK_DB, "tp02_keys_out", "docid")),
        desc="tp02_keys docid=B",
    )
    time.sleep(2)
    actual_ls = dump_table(SNK, SINK_DB, "tp02_keys_out", "docid")
    record(
        TPResult(
            id="TP-02",
            title="Business key docid: old key gone, new key kept",
            action="UPDATE tp02_keys SET docid='B', title='title-b'",
            expected_desc="Sink only (B, title-b)",
            expected_rows=[{"docid": "B", "title": "title-b"}],
            actual_rows=actual_ls,
            columns=["docid", "title"],
            extra_md=f"- No id column: {'✅' if actual_ls and 'id' not in actual_ls[0] else '❌'}\n",
        )
    )

    # TP-03
    exec_sql(SRC, "UPDATE tp01_rows SET name=%s WHERE id=1", ("alice2",))
    wait_until(
        lambda: any(str(r.get("id")) == "1" and str(r.get("name")) == "alice2" for r in dump_table(SNK, SINK_DB, "tp01_rows_out", "id")),
        desc="alice2",
    )
    time.sleep(1)
    actual_users = dump_table(SNK, SINK_DB, "tp01_rows_out", "id")
    record(
        TPResult(
            id="TP-03",
            title="Same-key update overwrites (no extra row)",
            action="UPDATE tp01_rows SET name='alice2' WHERE id=1",
            expected_desc="Still two rows; id=1 name=alice2",
            expected_rows=[
                {"id": 1, "name": "alice2", "city": "hz"},
                {"id": 3, "name": "ann", "city": "hz"},
            ],
            actual_rows=actual_users,
            columns=["id", "name", "city"],
        )
    )

    # TP-04
    exec_sql(SRC, "UPDATE tp01_rows SET city=%s WHERE id=3", ("bj",))
    wait_until(
        lambda: all(str(r.get("id")) != "3" for r in dump_table(SNK, SINK_DB, "tp01_rows_out", "id")),
        desc="filter fall-out deletes id=3",
    )
    time.sleep(1)
    actual_users = dump_table(SNK, SINK_DB, "tp01_rows_out", "id")
    record(
        TPResult(
            id="TP-04",
            title="Filter miss retract (sink delete)",
            action="UPDATE tp01_rows SET city='bj' WHERE id=3 (source no longer matches city='hz')",
            expected_desc="Sink deletes id=3; only rows still matching filter remain",
            expected_rows=[
                {"id": 1, "name": "alice2", "city": "hz"},
            ],
            actual_rows=actual_users,
            columns=["id", "name", "city"],
            extra_md="- Semantics: filter slide-out → retract delete on sink\n",
        )
    )

    # TP-05
    exec_sql(SRC, "INSERT INTO tp01_rows(id,name,city,amount) VALUES (4,'cara','hz',1.00),(5,'dave','sh',2.00)")
    wait_until(
        lambda: any(str(r.get("id")) == "4" for r in dump_table(SNK, SINK_DB, "tp01_rows_out", "id")),
        desc="cara",
    )
    time.sleep(2)
    actual_users = dump_table(SNK, SINK_DB, "tp01_rows_out", "id")
    record(
        TPResult(
            id="TP-05",
            title="Filter: match enters / miss excluded",
            action="INSERT (4,cara,hz), (5,dave,sh)",
            expected_desc="Has cara; no dave; no retracted id=3",
            expected_rows=[
                {"id": 1, "name": "alice2", "city": "hz"},
                {"id": 4, "name": "cara", "city": "hz"},
            ],
            actual_rows=actual_users,
            columns=["id", "name", "city"],
        )
    )

    # TP-06
    exec_sql(SRC, "DELETE FROM tp01_rows WHERE id=1")
    wait_until(
        lambda: all(str(r.get("id")) != "1" for r in dump_table(SNK, SINK_DB, "tp01_rows_out", "id")),
        desc="delete id=1",
    )
    time.sleep(1)
    actual_users = dump_table(SNK, SINK_DB, "tp01_rows_out", "id")
    record(
        TPResult(
            id="TP-06",
            title="DELETE retracts by before-image key",
            action="DELETE FROM tp01_rows WHERE id=1",
            expected_desc="No id=1; id=4 remains",
            expected_rows=[
                {"id": 4, "name": "cara", "city": "hz"},
            ],
            actual_rows=actual_users,
            columns=["id", "name", "city"],
        )
    )

    # TP-07
    time.sleep(3)
    ck = json.loads(CK.read_text()) if CK.exists() else {}
    excluded = ck.get("excluded_tables") or []
    bad_excluded = any("tp07_fail" in str(x) for x in excluded)
    bad_exists = table_exists(SNK, SINK_DB, "tp07_fail_out")
    users_n = len(dump_table(SNK, SINK_DB, "tp01_rows_out", "id"))
    job_alive = proc is not None and proc.poll() is None
    ck_ok = _checkpoint_shape_ok(ck)
    ok7 = (not bad_excluded) and (not bad_exists) and users_n >= 1 and job_alive
    cmp7 = (
        "| Check | Expected | Actual | Verdict |\n| --- | --- | --- | --- |\n"
        f"| tp07_fail not in excluded_tables | yes | {'yes' if not bad_excluded else 'no'} | "
        f"{'✅' if not bad_excluded else '❌'} |\n"
        f"| {SINK_DB}.tp07_fail_out absent | yes | {'yes' if not bad_exists else 'no'} | "
        f"{'✅' if not bad_exists else '❌'} |\n"
        f"| tp01_rows still syncing | >=1 rows | {users_n} rows | {'✅' if users_n >= 1 else '❌'} |\n"
        f"| job still running | yes | {'yes' if job_alive else 'no'} | {'✅' if job_alive else '❌'} |\n"
        f"| resume offset shape | file+pos or gtid | {_checkpoint_shape_label(ck)} | "
        f"{'✅' if ck_ok else '❌'} |\n"
        f"| Summary | | | **{'✅ PASS' if ok7 else '❌ FAIL'}** |"
    )
    record(
        TPResult(
            id="TP-07",
            title="Create-table failure does not exclude table; job continues",
            action="tp07_fail routed to no_such_db (empty source); create-table fails",
            expected_desc="Not in excluded; no tp07_fail sink table; tp01_rows continue; job does not exit",
            expected_rows=[{"ok": "yes"}],
            actual_rows=[{"ok": "yes" if ok7 else "no"}],
            columns=["ok"],
        ),
        override=ok7,
        compare_md=cmp7,
    )

    # TP-09
    exec_sql(SRC, "ALTER TABLE tp01_rows ADD COLUMN remark VARCHAR(32) NULL")
    exec_sql(SRC, "ALTER TABLE tp01_rows ADD COLUMN tag VARCHAR(32) NULL")
    exec_sql(SRC, "ALTER TABLE tp01_rows MODIFY name VARCHAR(128) NOT NULL")
    time.sleep(8)
    cols = q(
        SNK,
        "SELECT COLUMN_NAME AS name, COLUMN_TYPE AS type FROM information_schema.columns "
        f"WHERE table_schema=%s AND table_name='tp01_rows_out' ORDER BY ORDINAL_POSITION",
        (SINK_DB,),
    )
    names = [r["name"] for r in cols]
    name_type = next((r["type"] for r in cols if r["name"] == "name"), "")
    ok8 = names == ["id", "name", "city"] and "varchar(128)" in name_type.lower()
    cmp8 = (
        "| Check | Expected | Actual | Verdict |\n| --- | --- | --- | --- |\n"
        f"| Column set | id,name,city | {names} | {'✅' if names == ['id','name','city'] else '❌'} |\n"
        f"| name type | varchar(128) | {name_type} | {'✅' if 'varchar(128)' in name_type.lower() else '❌'} |\n"
        f"| Summary | | | **{'✅ PASS' if ok8 else '❌ FAIL'}** |"
    )
    record(
        TPResult(
            id="TP-09",
            title="Extra projected columns not applied; in-projection type change applied",
            action="ADD remark/tag；MODIFY name VARCHAR(128)",
            expected_desc="Columns still id,name,city; name=varchar(128)",
            expected_rows=[{"columns": "id,name,city", "name_type": "varchar(128)"}],
            actual_rows=[{"columns": ",".join(names), "name_type": name_type}],
            columns=["columns", "name_type"],
            extra_md=md_table(cols, ["name", "type"]) + "\n",
        ),
        override=ok8,
        compare_md=cmp8,
    )

    # TP-10 — newly-added=true + snapshot=true (original scenario; flags explicit)
    stop_job()
    time.sleep(2)
    set_source_flag("scan.newly-added-table.enabled", True)
    set_source_flag("scan.newly-added-table.snapshot.enabled", True)
    set_source_flag("scan.binlog.newly-added-table.enabled", False)
    set_tables_pattern(rf"{SRC_DB}\\.tp(01_rows|02_keys|07_fail|10_add)")
    users_before = dump_table(SNK, SINK_DB, "tp01_rows_out", "id")
    start_job()
    wait_until(
        lambda: table_exists(SNK, SINK_DB, "tp10_add_out")
        and len(dump_table(SNK, SINK_DB, "tp10_add_out", "id")) >= 1,
        desc="tp10_add snapshot",
    )
    time.sleep(2)
    actual_kept = dump_table(SNK, SINK_DB, "tp10_add_out", "id")
    users_after = dump_table(SNK, SINK_DB, "tp01_rows_out", "id")
    bob_in = any(str(r.get("id")) == "2" for r in users_after)
    ok9 = (
        normalize(actual_kept, ["id", "name"]) == normalize([{"id": 7, "name": "add-row"}], ["id", "name"])
        and not bob_in
        and normalize(users_before, ["id", "name", "city"]) == normalize(users_after, ["id", "name", "city"])
    )
    record(
        TPResult(
            id="TP-10",
            title="After stop, add existing table: backfill that table only",
            action=(
                "scan.newly-added-table.enabled=true + snapshot.enabled=true; "
                "add tp10_add; restart from saved resume state"
            ),
            expected_desc="tp10_add=(7,add-row); tp01_rows unchanged; no id=2",
            expected_rows=[{"id": 7, "name": "add-row"}],
            actual_rows=actual_kept,
            columns=["id", "name"],
            extra_md=(
                "#### tp01_rows before restart\n"
                + md_table(users_before, ["id", "name", "city"])
                + "\n\n#### tp01_rows after restart\n"
                + md_table(users_after, ["id", "name", "city"])
                + f"\n\n- id=2 backfilled: {'❌ yes' if bob_in else '✅ no'}\n"
            ),
        ),
        override=ok9,
    )

    # TP-11 — binlog.newly-added=true (original scenario; flag explicit)
    stop_job()
    time.sleep(1)
    set_source_flag("scan.newly-added-table.enabled", True)
    set_source_flag("scan.newly-added-table.snapshot.enabled", True)
    set_source_flag("scan.binlog.newly-added-table.enabled", True)
    set_tables_pattern(rf"{SRC_DB}\\.tp(01_rows|02_keys|07_fail|10_add|11_ddl)")
    start_job()
    time.sleep(5)
    exec_sql(SRC, "DROP TABLE IF EXISTS tp11_ddl")
    try:
        exec_sql(SNK, "DROP TABLE IF EXISTS tp11_ddl_out")
    except Exception:
        pass
    time.sleep(2)
    exec_sql(SRC, "CREATE TABLE tp11_ddl (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL) ENGINE=InnoDB")
    exec_sql(SRC, "INSERT INTO tp11_ddl(id,name) VALUES (9,'ddl-row')")
    wait_until(
        lambda: table_exists(SNK, SINK_DB, "tp11_ddl_out")
        and any(str(r.get("id")) == "9" for r in dump_table(SNK, SINK_DB, "tp11_ddl_out", "id")),
        desc="ddl-row",
    )
    time.sleep(3)
    actual_live = dump_table(SNK, SINK_DB, "tp11_ddl_out", "id")
    ck = json.loads(CK.read_text()) if CK.exists() else {}
    captured = ck.get("captured_tables") or []
    live_captured = any("tp11_ddl" in str(x) for x in captured)
    ok10 = normalize(actual_live, ["id", "name"]) == normalize([{"id": 9, "name": "ddl-row"}], ["id", "name"]) and live_captured
    live_row_ok = normalize(actual_live, ["id", "name"]) == normalize(
        [{"id": 9, "name": "ddl-row"}], ["id", "name"]
    )
    cmp10 = (
        "| Check | Expected | Actual | Verdict |\n| --- | --- | --- | --- |\n"
        f"| binlog.newly-added | true | true | ✅ |\n"
        f"| sink row (9,ddl-row) | present | {'yes' if live_row_ok else 'no'} | "
        f"{'✅' if live_row_ok else '❌'} |\n"
        f"| captured_tables contains tp11_ddl | yes | {'yes' if live_captured else 'no'} | "
        f"{'✅' if live_captured else '❌'} |\n"
        f"| Summary | | | **{'✅ PASS' if ok10 else '❌ FAIL'}** |"
    )
    record(
        TPResult(
            id="TP-11",
            title="Runtime CREATE TABLE: follows incremental and updates captured",
            action="scan.binlog.newly-added-table.enabled=true; runtime CREATE tp11_ddl + INSERT (9,ddl-row)",
            expected_desc="Sink (9,ddl-row); captured includes tp11_ddl",
            expected_rows=[{"id": 9, "name": "ddl-row"}],
            actual_rows=actual_live,
            columns=["id", "name"],
        ),
        override=ok10,
        compare_md=cmp10,
    )

    # TP-15..TP-20 — full (A,B,C) combos (renumbered from former TP-18..22).
    _combo_base = rf"{SRC_DB}\\.tp(01_rows|02_keys|07_fail|10_add|11_ddl"
    run_newly_added_combo(
        tp_id="TP-15",
        title="Combo#1 A=false C=false: ignore diff + drop CREATE",
        newly_added=False,
        snapshot=True,
        binlog_newly=False,
        diff_table="tp15_diff",
        seed_id=15,
        crt_table="tp15_crt",
        crt_id=150,
        tables_pattern=_combo_base + r"|15_diff|15_crt)",
    )
    run_newly_added_combo(
        tp_id="TP-16",
        title="Combo#2 A=false C=true: ignore diff + follow CREATE",
        newly_added=False,
        snapshot=True,
        binlog_newly=True,
        diff_table="tp16_diff",
        seed_id=16,
        crt_table="tp16_crt",
        crt_id=160,
        tables_pattern=_combo_base + r"|15_diff|15_crt|16_diff|16_crt)",
    )
    run_newly_added_combo(
        tp_id="TP-17",
        title="Combo#3 A=true B=true C=false: snapshot diff + drop CREATE",
        newly_added=True,
        snapshot=True,
        binlog_newly=False,
        diff_table="tp17_diff",
        seed_id=17,
        crt_table="tp17_crt",
        crt_id=170,
        tables_pattern=_combo_base + r"|15_diff|15_crt|16_diff|16_crt|17_diff|17_crt)",
    )
    run_newly_added_combo(
        tp_id="TP-18",
        title="Combo#4 A=true B=true C=true: snapshot diff + follow CREATE",
        newly_added=True,
        snapshot=True,
        binlog_newly=True,
        diff_table="tp18_diff",
        seed_id=18,
        crt_table="tp18_crt",
        crt_id=180,
        tables_pattern=_combo_base
        + r"|15_diff|15_crt|16_diff|16_crt|17_diff|17_crt|18_diff|18_crt)",
    )
    run_newly_added_combo(
        tp_id="TP-19",
        title="Combo#5 A=true B=false C=false: binlog-only diff + drop CREATE",
        newly_added=True,
        snapshot=False,
        binlog_newly=False,
        diff_table="tp19_diff",
        seed_id=19,
        live_id=191,
        crt_table="tp19_crt",
        crt_id=190,
        tables_pattern=_combo_base
        + r"|15_diff|15_crt|16_diff|16_crt|17_diff|17_crt|18_diff|18_crt|19_diff|19_crt)",
    )
    run_newly_added_combo(
        tp_id="TP-20",
        title="Combo#6 A=true B=false C=true: binlog-only diff + follow CREATE",
        newly_added=True,
        snapshot=False,
        binlog_newly=True,
        diff_table="tp20_diff",
        seed_id=20,
        live_id=201,
        crt_table="tp20_crt",
        crt_id=200,
        tables_pattern=_combo_base
        + r"|15_diff|15_crt|16_diff|16_crt|17_diff|17_crt|18_diff|18_crt|19_diff|19_crt|20_diff|20_crt)",
    )

    # TP-13 — prove resume-state layout by shape only (no file contents in the report)
    ck = json.loads(CK.read_text()) if CK.exists() else {}
    only_one = CK.exists() and CK.is_file()
    has_offset = isinstance(ck.get("acked_offset"), dict) and bool(ck.get("acked_offset"))
    has_splits = "finished_splits" in ck
    shape_ok = _checkpoint_shape_ok(ck)
    ok12 = only_one and has_offset and shape_ok
    cmp12 = (
        "| Check | Expected | Actual | Verdict |\n| --- | --- | --- | --- |\n"
        f"| single resume-state file | yes | {'yes' if only_one else 'no'} | {'✅' if only_one else '❌'} |\n"
        f"| resume offset recorded | yes | {'yes' if has_offset else 'no'} | {'✅' if has_offset else '❌'} |\n"
        f"| offset shape | file+pos or gtid | {_checkpoint_shape_label(ck)} | "
        f"{'✅' if shape_ok else '❌'} |\n"
        f"| split progress recorded | optional | {'yes' if has_splits else 'no'} | "
        f"{'✅' if has_splits else '⚠️'} |\n"
        f"| Summary | | | **{'✅ PASS' if ok12 else '❌ FAIL'}** |"
    )
    record(
        TPResult(
            id="TP-13",
            title="Resume state: splits and offset in one file",
            action="snapshot + incremental, interval=2s",
            expected_desc="One resume-state file with a well-formed offset (file+pos or gtid)",
            expected_rows=[{"file": "1", "acked": "yes"}],
            actual_rows=[{"file": "1" if only_one else "0", "acked": "yes" if has_offset else "no"}],
            columns=["file", "acked"],
        ),
        override=ok12,
        compare_md=cmp12,
    )

    # TP-14 — abnormal stop (SIGKILL) then restart from on-disk checkpoint
    wait_until(
        lambda: CK.exists() and _checkpoint_shape_ok(json.loads(CK.read_text())),
        desc="checkpoint flushed before kill",
        timeout=30,
    )
    time.sleep(3)  # past checkpoint.interval=2s so latest flush is on disk
    ck_before = json.loads(CK.read_text())
    users_before = dump_table(SNK, SINK_DB, "tp01_rows_out", "id")
    add_before = dump_table(SNK, SINK_DB, "tp10_add_out", "id")
    kill_code = kill_job_hard()
    killed_ok = kill_code is not None and kill_code != 0
    ck_after_kill = json.loads(CK.read_text()) if CK.exists() else {}
    ck_survived = _checkpoint_shape_ok(ck_after_kill)

    # Events while job is dead must be picked up after resume from acked_offset.
    exec_sql(
        SRC,
        "INSERT INTO tp01_rows(id,name,city,amount) VALUES (13,'crash-resume','hz',0.13)",
    )

    start_job()
    wait_until(
        lambda: any(
            str(r.get("id")) == "13" and str(r.get("name")) == "crash-resume"
            for r in dump_table(SNK, SINK_DB, "tp01_rows_out", "id")
        ),
        desc="id=13 after SIGKILL resume",
        timeout=120,
    )
    time.sleep(2)
    users_after = dump_table(SNK, SINK_DB, "tp01_rows_out", "id")
    add_after = dump_table(SNK, SINK_DB, "tp10_add_out", "id")
    job_alive = proc is not None and proc.poll() is None
    old_kept = normalize(users_before, ["id", "name", "city"]) == normalize(
        [r for r in users_after if str(r.get("id")) != "13"],
        ["id", "name", "city"],
    )
    new_ok = any(str(r.get("id")) == "13" for r in users_after)
    bob_back = any(str(r.get("id")) == "2" for r in users_after)
    add_unchanged = normalize(add_before, ["id", "name"]) == normalize(add_after, ["id", "name"])
    ok13 = (
        killed_ok
        and ck_survived
        and job_alive
        and old_kept
        and new_ok
        and not bob_back
        and add_unchanged
    )
    cmp13 = (
        "| Check | Expected | Actual | Verdict |\n| --- | --- | --- | --- |\n"
        f"| SIGKILL exit non-zero | yes | code={kill_code} | {'✅' if killed_ok else '❌'} |\n"
        f"| checkpoint survived kill | yes | {_checkpoint_shape_label(ck_after_kill)} | "
        f"{'✅' if ck_survived else '❌'} |\n"
        f"| pre-kill sink rows kept | unchanged | "
        f"{'yes' if old_kept else 'no'} | {'✅' if old_kept else '❌'} |\n"
        f"| downtime INSERT id=13 synced | yes | {'yes' if new_ok else 'no'} | "
        f"{'✅' if new_ok else '❌'} |\n"
        f"| filtered id=2 still absent | yes | {'yes' if not bob_back else 'no'} | "
        f"{'✅' if not bob_back else '❌'} |\n"
        f"| tp10_add unchanged | yes | {'yes' if add_unchanged else 'no'} | "
        f"{'✅' if add_unchanged else '❌'} |\n"
        f"| job running after restart | yes | {job_alive} | {'✅' if job_alive else '❌'} |\n"
        f"| Summary | | | **{'✅ PASS' if ok13 else '❌ FAIL'}** |"
    )
    record(
        TPResult(
            id="TP-14",
            title="Abnormal stop (SIGKILL) then resume from checkpoint",
            action=(
                "Wait for flushed acked_offset → SIGKILL (no graceful flush) → "
                "INSERT id=13 while down → restart same YAML"
            ),
            expected_desc=(
                "Checkpoint on disk still valid; old sink rows kept; downtime INSERT caught up; "
                "no filtered-row resurrection"
            ),
            expected_rows=[{"ok": "yes"}],
            actual_rows=[{"ok": "yes" if ok13 else "no"}],
            columns=["ok"],
            extra_md=(
                "#### tp01_rows_out before kill\n"
                + md_table(users_before, ["id", "name", "city"])
                + "\n\n#### tp01_rows_out after resume\n"
                + md_table(users_after, ["id", "name", "city"])
                + f"\n\n- offset before kill: `{_checkpoint_shape_label(ck_before)}`\n"
            ),
        ),
        override=ok13,
        compare_md=cmp13,
    )

    # TP-08 (was TP-07b): writing to a sink table that never got created fails the job
    log_before = LOG.read_text(errors="replace") if LOG.exists() else ""
    exec_sql(SRC, "INSERT INTO tp07_fail(id, name) VALUES (1, 'fail-row')")
    wait_until(
        lambda: proc is not None and proc.poll() is not None,
        desc="job exits after write to missing sink table",
        timeout=60,
    )
    exit_code = proc.returncode if proc else None
    log_after = LOG.read_text(errors="replace") if LOG.exists() else ""
    log_delta = log_after[len(log_before) :]
    mentions_missing = (
        "does not exist" in log_delta.lower()
        or "doesn't exist" in log_delta.lower()
        or "1146" in log_delta
        or "1142" in log_delta
        or "1049" in log_delta
    )
    ok7b = exit_code not in (None, 0) and mentions_missing
    cmp7b = (
        "| Check | Expected | Actual | Verdict |\n| --- | --- | --- | --- |\n"
        f"| job exit code non-zero | yes | {exit_code} | {'✅' if exit_code not in (None, 0) else '❌'} |\n"
        f"| log mentions missing table | yes | {mentions_missing} | {'✅' if mentions_missing else '❌'} |\n"
        f"| Summary | | | **{'✅ PASS' if ok7b else '❌ FAIL'}** |"
    )
    record(
        TPResult(
            id="TP-08",
            title="Write after create failure: missing table fails job",
            action="INSERT INTO tp07_fail (routed to no_such_db)",
            expected_desc="Write fails on missing table; job fails; no silent drop from create failure",
            expected_rows=[{"ok": "yes"}],
            actual_rows=[{"ok": "yes" if ok7b else "no"}],
            columns=["ok"],
            extra_md="```\n" + log_delta[-2000:] + "\n```\n",
        ),
        override=ok7b,
        compare_md=cmp7b,
    )
    proc = None

    # TP-12 stdout (was TP-11)
    stop_job()
    stdout_yaml = WORKDIR / "pipeline.stdout.yaml"
    cfg.render_template("pipeline.stdout.yaml.tpl", stdout_yaml)
    out_log = WORKDIR / "stdout.out"
    ck_stdout = Path(cfg.CHECKPOINT_STDOUT_PATH)
    if ck_stdout.exists():
        ck_stdout.unlink()
    p = subprocess.Popen([BINARY, str(stdout_yaml)], stdout=out_log.open("w"), stderr=subprocess.STDOUT, text=True)
    time.sleep(5)
    exec_sql(SRC, "UPDATE tp01_rows SET name=%s WHERE id=4", ("cara2",))
    time.sleep(5)
    exec_sql(SRC, "ALTER TABLE tp01_rows ADD COLUMN tag2 VARCHAR(16) NULL")
    time.sleep(8)
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(timeout=15)
    except subprocess.TimeoutExpired:
        p.kill()
    out_text = out_log.read_text(errors="replace")
    has_u = '"op":"u"' in out_text or '"op": "u"' in out_text
    has_before_after = False
    for line in out_text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("op") == "u" and obj.get("before") and obj.get("after"):
            has_before_after = True
            break
    has_ddl = "ddl" in out_text.lower() and "tag2" in out_text
    ok11 = has_u and has_before_after and has_ddl
    cmp11 = (
        "| Check | Expected | Actual | Verdict |\n| --- | --- | --- | --- |\n"
        f"| op=u | present | {has_u} | {'✅' if has_u else '❌'} |\n"
        f"| u has before+after | present | {has_before_after} | {'✅' if has_before_after else '❌'} |\n"
        f"| ddl mentions tag2 | present | {has_ddl} | {'✅' if has_ddl else '❌'} |\n"
        f"| Summary | | | **{'✅ PASS' if ok11 else '❌ FAIL'}** |"
    )
    record(
        TPResult(
            id="TP-12",
            title="stdout CRUD: u with before/after + ddl",
            action="stdout sink；UPDATE cara→cara2；ADD tag2",
            expected_desc="Single-line u(before+after) and ddl(tag2) appear",
            expected_rows=[{"ops": "u,ddl"}],
            actual_rows=[{"ops": f"u={has_u},u_ba={has_before_after},ddl={has_ddl}"}],
            columns=["ops"],
            extra_md="```\n" + "\n".join(out_text.splitlines()[-30:]) + "\n```\n",
        ),
        override=ok11,
        compare_md=cmp11,
    )

    report_path = write_report()
    failed = [r for r in results if not r.passed]
    log("REPORT", cfg.project_display_path(report_path))
    log("PASS", sum(1 for r in results if r.passed), "FAIL", len(failed))
    return 0 if not failed else 1


def _tp_sort_key(tp_id: str) -> tuple[int, str]:
    """Sort TP-07 before TP-08; numeric id then optional letter suffix."""
    m = re.fullmatch(r"TP-(\d+)([a-z]*)", tp_id)
    if not m:
        return (9999, tp_id)
    return (int(m.group(1)), m.group(2))


def _tp_anchor(tp_id: str) -> str:
    """Stable fragment id for overview → detail links (e.g. TP-08 → tp-08)."""
    return tp_id.lower()


def write_report() -> Path:
    passed = sum(1 for r in results if r.passed)
    failed = len(results) - passed
    path = cfg.REPORT_FUNC
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ordered = sorted(results, key=lambda item: _tp_sort_key(item.id))
    lines = [
        "# go-cdc integration test report (expected vs actual)\n",
        '<a id="overview"></a>\n',
        "## 0. Overview\n",
        f"- Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Source: `{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}` / `{cfg.SRC_USER}` / `{SRC_DB}`",
        f"- Sink: `{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}` / `{cfg.SINK_USER}` / `{SINK_DB}`",
        f"- Entry point: `{cfg.project_display_path(BINARY)} <pipeline.yaml>`",
        f"- **Summary: pass {passed} / fail {failed} / total {len(results)}** → "
        f"{'**all passed**' if failed == 0 else '**failures present**'}\n",
        "| TP | Name | Result |",
        "| --- | --- | --- |",
    ]
    for r in ordered:
        lines.append(
            f"| [{r.id}](#{_tp_anchor(r.id)}) | {r.title} | "
            f"{'✅ PASS' if r.passed else '❌ FAIL'} |"
        )
    lines += [
        "",
        "## 1. Data flow\n",
        "```mermaid",
        "flowchart LR",
        f"  src[{SRC_DB}] --> job[cdc_cli]",
        f"  job --> snk[{SINK_DB}]",
        "```\n",
        "## 2. Test points\n",
    ]
    for r in ordered:
        anchor = _tp_anchor(r.id)
        lines += [
            f'<a id="{anchor}"></a>\n',
            f"## {r.id} {r.title}\n",
            f"- [↑ Overview](#overview)",
            f"- **Result: {'✅ PASS' if r.passed else '❌ FAIL'}**",
            f"- **Action:** {r.action}",
            f"- **Expected:** {r.expected_desc}\n",
            "### Expected vs actual\n",
            r.compare_md,
        ]
        if r.extra_md:
            lines += ["\n### Extra evidence\n", r.extra_md]
        lines.append("")

    lines.append("## Appendix: source table snapshots\n")
    for t, order in [
        ("tp01_rows", "id"),
        ("tp02_keys", "id"),
        ("tp10_add", "id"),
        ("tp11_ddl", "id"),
        ("tp15_diff", "id"),
        ("tp16_diff", "id"),
        ("tp17_diff", "id"),
        ("tp18_diff", "id"),
        ("tp19_diff", "id"),
        ("tp20_diff", "id"),
        ("tp15_crt", "id"),
        ("tp16_crt", "id"),
        ("tp17_crt", "id"),
        ("tp18_crt", "id"),
        ("tp19_crt", "id"),
        ("tp20_crt", "id"),
    ]:
        lines += [f"### source {SRC_DB}.{t}\n", md_table(dump_table(SRC, SRC_DB, t, order)), ""]
    lines.append("## Appendix: sink table snapshots\n")
    for t, order in [
        ("tp01_rows_out", "id"),
        ("tp02_keys_out", "docid"),
        ("tp10_add_out", "id"),
        ("tp11_ddl_out", "id"),
        ("tp15_diff_out", "id"),
        ("tp15_crt_out", "id"),
        ("tp16_diff_out", "id"),
        ("tp16_crt_out", "id"),
        ("tp17_diff_out", "id"),
        ("tp17_crt_out", "id"),
        ("tp18_diff_out", "id"),
        ("tp18_crt_out", "id"),
        ("tp19_diff_out", "id"),
        ("tp19_crt_out", "id"),
        ("tp20_diff_out", "id"),
        ("tp20_crt_out", "id"),
    ]:
        lines += [f"### sink {SINK_DB}.{t}\n", md_table(dump_table(SNK, SINK_DB, t, order)), ""]

    path.write_text(cfg.sanitize_report_text("\n".join(lines)), encoding="utf-8")
    return path


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        stop_job()
