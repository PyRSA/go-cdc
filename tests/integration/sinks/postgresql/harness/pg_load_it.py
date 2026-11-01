#!/usr/bin/env python3
"""go-cdc mysql2postgresql load / peak harness (TP-PL01..TP-PL04).

Aligned point by point with ``sinks/mysql/harness/load_it.py`` (mysql2mysql): the same table
templates, the same baseline seeder, the same knobs (``LOAD_ROWS_PER_TABLE``,
``LOAD_LARGE_*``, ``LOAD_PEAK_*``) and the same 4 writers x 60 s peak window, so
the two targets stay comparable. **Every write happens on the MySQL source**; the
target is only read, through ``sinks/postgresql/harness/pg_client.py``, and compared by the
independent oracle ``sinks/postgresql/harness/pg_verify.py``.

Differences from the MySQL suite (listed here, in the run log and in the report -
never dropped silently, see docs/test/POSTGRESQL.md §4):

* ``tpl01_typed.c_enum`` and ``.c_bit`` are dropped after seeding, because the
  PostgreSQL sink refuses both types.
* Reconciliation is chunked by primary-key window and compares canonical values,
  not only row counts (PG requirement: never ``COUNT(*)``-only).
* Mega fields are compared by length + SHA-256 computed on both servers, so the
  2 MiB payloads never travel to the harness.

Run it with ``scripts/run.sh postgresql heavy pg16 func,load``; the report lands in
``docs/test/reports/mysql-postgresql-load.md``.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pymysql

from common import config as cfg
from ...mysql.harness import load_it as load
from . import pg_client, pg_verify
from common.logutil import log
from .pg_it import Case, _yaml_block, check, finish, my, my_exec, note, src_cfg
from common.sysstat import format_cpu_mem, format_disk, format_host_res

WORKDIR = cfg.WORKDIR
BINARY = cfg.BINARY
CK = Path(cfg.CHECKPOINT_PG_LOAD_PATH)
YAML = WORKDIR / "pipeline.pg.load.yaml"
LOG = WORKDIR / "cdc-pg-load.log"

# The MySQL side is the same source database the mysql2mysql load suite seeds, so
# ``load.ensure_schema()`` / ``load.seed_baseline()`` are reused verbatim (the
# Elasticsearch load suite does the same). The two suites must not run at the
# same time on one compose stack; that limit is recorded in POSTGRESQL.md §7.
SRC_DB = cfg.LOAD_SRC_DB
SCHEMA = cfg.PG_LOAD_SNK_SCHEMA

BIZ_TABLES = load.BIZ_TABLES
TYPED_TABLE = load.TYPED_TABLE
LARGE_TABLE = load.LARGE_TABLE
TABLES = load.TABLES
ROWS = load.ROWS
LARGE_ROWS = load.LARGE_ROWS
LARGE_BYTES = load.LARGE_BYTES
PEAK_SEC = load.PEAK_SEC
WORKERS = load.WORKERS
BATCH = load.BATCH

# Column order of ``load_it._typed_row()``. The two refused columns are skipped
# when that tuple is written back, which keeps every other value byte-identical
# to the MySQL suite.
TYPED_SRC_COLUMNS = (
    "id",
    "c_tinyint",
    "c_int",
    "c_bigint",
    "c_decimal",
    "c_float",
    "c_double",
    "c_varchar",
    "c_date",
    "c_datetime",
    "c_timestamp",
    "c_json",
    "c_enum",
    "c_bit",
    "c_bool",
)
DROPPED_COLUMNS = ("c_enum", "c_bit")
TYPED_COLUMNS = tuple(c for c in TYPED_SRC_COLUMNS if c not in DROPPED_COLUMNS)

# Oracle kinds per column (docs/test/POSTGRESQL.md §5).
KINDS: dict[str, dict[str, str]] = {
    "tpl01_t1": {"id": "int", "c_ref": "int", "c_amt": "decimal", "c_flag": "text"},
    "tpl01_t2": {"id": "int", "c_ref": "int", "c_code": "text", "c_qty": "int"},
    "tpl01_t3": {"id": "int", "c_name": "text", "c_city": "text"},
    "tpl01_t4": {"id": "int", "c_ref": "int", "c_method": "text", "c_paid": "decimal"},
    "tpl01_typed": {
        "id": "int",
        "c_tinyint": "int",
        "c_int": "int",
        "c_bigint": "int",
        "c_decimal": "decimal",
        "c_float": "float",
        "c_double": "float",
        "c_varchar": "text",
        "c_date": "date",
        "c_datetime": "datetime",
        "c_timestamp": "datetime",
        "c_json": "json",
        "c_bool": "int",
    },
}
COLUMNS = {table: tuple(columns) for table, columns in KINDS.items()}

# Expected pg_catalog types of tpl01_typed (docs/connectors/postgresql-sink.md).
EXPECTED_PG_TYPES = {
    "id": "bigint",
    "c_tinyint": "smallint",
    "c_int": "integer",
    "c_bigint": "bigint",
    "c_decimal": "numeric(10,4)",
    "c_float": "real",
    "c_double": "double precision",
    "c_varchar": "character varying(64)",
    "c_date": "date",
    "c_datetime": "timestamp(6) without time zone",
    "c_timestamp": "timestamp(6) without time zone",
    "c_json": "json",
    "c_bool": "smallint",
}

INSERT_T1 = f"INSERT INTO `{SRC_DB}`.tpl01_t1(id,c_ref,c_amt,c_flag) VALUES (%s,%s,%s,%s)"
INSERT_TYPED = (
    f"INSERT INTO `{SRC_DB}`.tpl01_typed({','.join(TYPED_COLUMNS)}) "
    f"VALUES ({','.join(['%s'] * len(TYPED_COLUMNS))})"
)

SINK: dict[str, object] = {
    "type": "postgresql",
    "hostname": cfg.PG_HOST,
    "port": cfg.PG_PORT,
    "username": cfg.PG_USER,
    "password": cfg.PG_PASSWORD,
    "database": cfg.PG_DB,
    "schema": SCHEMA,
    "sslmode": cfg.PG_SSLMODE,
    "connect-timeout": "30s",
    "auto-create-table": True,
    "write-batch-size": cfg.PG_LOAD_BATCH_SIZE,
    "write-batch-interval": cfg.PG_LOAD_BATCH_INTERVAL,
    "max-retries": 3,
    "schema.change.enabled": True,
    "schema.change.drop-truncate.enabled": False,
}

SOURCE: dict[str, object] = {
    "type": "mysql",
    "hostname": cfg.MYSQL_HOST,
    "port": cfg.MYSQL_PORT,
    "username": cfg.SRC_USER,
    "password": cfg.SRC_PASSWORD,
    "tables": "'" + f"{SRC_DB}\\.({'|'.join(TABLES)})" + "'",
    "server-id": cfg.CDC_SERVER_ID_PG_LOAD,
    "server-time-zone": "Asia/Shanghai",
    "scan.startup.mode": "initial",
    # Same snapshot shape as the mysql2mysql load fixture (4096 / 1024).
    "scan.incremental.snapshot.chunk.size": 4096,
    "scan.snapshot.fetch.size": 1024,
    "schema-change.enabled": False,
    "connect.timeout": "30s",
}

proc: subprocess.Popen | None = None
cases: list[Case] = []


# ------------------------------------------------------------------ reporting
def new_case(tp_id: str, title: str, action: str, expected: str) -> Case:
    case = Case(id=tp_id, title=title, action=action, expected=expected)
    cases.append(case)
    return case


# ------------------------------------------------------------------ MySQL side
def conn():
    return pymysql.connect(
        **src_cfg(SRC_DB), cursorclass=pymysql.cursors.DictCursor, autocommit=True
    )


def typed_row(i: int) -> tuple:
    """``load_it._typed_row(i)`` without the two columns the sink refuses."""
    row = load._typed_row(i)
    if len(row) != len(TYPED_SRC_COLUMNS):
        raise RuntimeError(
            f"load_it._typed_row() returned {len(row)} values, the harness expects "
            f"{len(TYPED_SRC_COLUMNS)}: the shared template changed"
        )
    return tuple(value for name, value in zip(TYPED_SRC_COLUMNS, row) if name not in DROPPED_COLUMNS)


def src_count(table: str) -> int:
    return load.count_rows(load.SRC, SRC_DB, table)


def snk_count(table: str) -> int:
    return pg_client.row_count(SCHEMA, f"{table}_out")


def lag_line(table: str) -> str:
    src_n, snk_n = src_count(table), snk_count(table)
    return f"{table} src={src_n} snk={snk_n}"


# ------------------------------------------------------------------ job control
def render_yaml() -> Path:
    lines = ["source:"]
    lines += _yaml_block(SOURCE)
    lines += ["", "sink:"]
    lines += _yaml_block(SINK)
    lines += ["", "route:"]
    for table in TABLES:
        lines.append(f"  - source-table: '{SRC_DB}\\.{table}'")
        lines.append(f"    sink-table: {SCHEMA}.{table}_out")
    lines += [
        "",
        "transform: []",
        "",
        "pipeline:",
        f"  name: {cfg.PIPELINE_PG_LOAD_NAME}",
        f"  parallelism: {cfg.LOAD_PARALLELISM}",
        "",
        "checkpoint:",
        "  storage: file",
        "  interval: 2s",
        f"  file-path: {cfg.it_rel_path(cfg.CHECKPOINT_PG_LOAD_DIR)}",
        "",
    ]
    YAML.parent.mkdir(parents=True, exist_ok=True)
    YAML.write_text("\n".join(lines), encoding="utf-8")
    return YAML


def start_job() -> None:
    global proc
    LOG.write_text("", encoding="utf-8")
    proc = subprocess.Popen(
        [BINARY, str(YAML)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    def pump() -> None:
        assert proc and proc.stdout
        with LOG.open("a", encoding="utf-8") as handle:
            for line in proc.stdout:
                handle.write(line)
                handle.flush()

    threading.Thread(target=pump, daemon=True).start()


def stop_job() -> None:
    global proc
    if proc and proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    proc = None


def job_alive() -> bool:
    return proc is not None and proc.poll() is None


def log_text() -> str:
    return LOG.read_text(encoding="utf-8", errors="replace") if LOG.exists() else ""


def log_tail(lines: int = 12) -> str:
    body = [ln for ln in log_text().splitlines() if ln.strip()]
    return " | ".join(body[-lines:])


def wait_alive(stable_for: float = 5.0) -> None:
    ready_at = time.time() + stable_for
    while time.time() < ready_at:
        if proc is None:
            raise RuntimeError("go-cdc: process not started")
        code = proc.poll()
        if code is not None:
            raise RuntimeError(f"go-cdc exited at startup ({code}): {log_tail()}")
        time.sleep(0.25)


def read_checkpoint() -> dict:
    if not CK.exists():
        return {}
    try:
        return json.loads(CK.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


# ------------------------------------------------------------ reconciliation
def reconcile(case: Case, table: str, *, timeout: float = 0.0) -> list[str]:
    """Compare one source table with its target, in primary-key windows.

    The source is walked with ``id > last LIMIT chunk`` (peak rows use sparse ids
    from 50k up to 30M, so a dense 1..max scan would fire thousands of empty
    queries). Each window is then read from the target with ``BETWEEN``, which
    keeps memory bounded on 50k-row tables and still reports the exact ids and
    columns that differ instead of a single "mismatch".
    """
    target = f"{table}_out"
    columns = COLUMNS[table]
    kinds = KINDS[table]
    chunk = cfg.PG_LOAD_VERIFY_CHUNK
    src_columns = ", ".join(f"`{name}`" for name in columns)
    snk_columns = ", ".join(f'"{name}"' for name in columns)

    def once() -> list[str]:
        if not pg_client.table_exists(SCHEMA, target):
            return [f"target {SCHEMA}.{target} does not exist"]
        failures: list[str] = []
        src_total, snk_total = src_count(table), snk_count(table)
        if src_total != snk_total:
            failures.append(f"row count differs: src={src_total} sink={snk_total}")
        last = 0
        while True:
            src_rows = my(
                SRC_DB,
                f"SELECT {src_columns} FROM `{SRC_DB}`.`{table}` "
                f"WHERE id > %s ORDER BY id LIMIT {chunk}",
                (last,),
            )
            if not src_rows:
                break
            low = int(src_rows[0]["id"])
            high = int(src_rows[-1]["id"])
            snk_rows = pg_client.q(
                f'SELECT {snk_columns} FROM "{SCHEMA}"."{target}" '
                "WHERE id BETWEEN %s AND %s ORDER BY id",
                (low, high),
            )
            failures += pg_verify.diff_rows(src_rows, snk_rows, columns, kinds, ("id",))
            if len(failures) > 200:
                failures.append(f"... stopped after {len(failures)} differences")
                break
            last = high
        return failures

    failures = once()
    if failures and timeout:
        deadline = time.time() + timeout
        while failures and time.time() < deadline:
            time.sleep(2)
            failures = once()
    label = f"{SRC_DB}.{table} -> {SCHEMA}.{target}"
    if failures:
        check(case, f"{label} reconciled", False, pg_verify.summarize(failures))
    else:
        check(case, f"{label} reconciled", True,
              f"{src_count(table)} rows, values equal in {chunk}-row windows")
    return failures


def wait_catchup(tables: tuple[str, ...], timeout: float) -> None:
    """Wait until source and target row counts are equal for ``tables``."""
    start = time.time()
    deadline = start + timeout
    last_log = 0.0
    format_cpu_mem()
    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"go-cdc exited during catch-up ({proc.returncode}): {log_tail()}")
        if all(src_count(t) == snk_count(t) for t in tables):
            log(f"TIMING  wait [pg load catch-up] done in {time.time() - start:.1f}s", flush=True)
            return
        now = time.time()
        if now - last_log >= 15:
            log(
                f"    catch-up {now - start:.0f}s: " + " | ".join(lag_line(t) for t in tables)
                + f" | {format_host_res(proc.pid if proc else None)}",
                flush=True,
            )
            last_log = now
        time.sleep(2)
    raise TimeoutError(
        f"catch-up budget {timeout:.0f}s exhausted: " + " | ".join(lag_line(t) for t in tables)
    )


# ------------------------------------------------------------------- seeding
def ensure_schema() -> None:
    """Reuse the MySQL load schema + seeder, then drop the refused columns."""
    load.ensure_schema()
    load.seed_baseline()
    for column in DROPPED_COLUMNS:
        my_exec(SRC_DB, f"ALTER TABLE `{SRC_DB}`.`{TYPED_TABLE}` DROP COLUMN `{column}`")
    live = {
        str(row["name"])
        for row in my(
            SRC_DB,
            "SELECT COLUMN_NAME AS name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s",
            (SRC_DB, TYPED_TABLE),
        )
    }
    still_there = [c for c in DROPPED_COLUMNS if c in live]
    if still_there:
        raise RuntimeError(
            f"{SRC_DB}.{TYPED_TABLE} still has {still_there}: the deviation was not applied"
        )
    if live != set(TYPED_COLUMNS):
        raise RuntimeError(
            f"{SRC_DB}.{TYPED_TABLE} columns {sorted(live)} != expected {sorted(TYPED_COLUMNS)}"
        )
    log(
        f"    dropped {TYPED_TABLE}.{', '.join(DROPPED_COLUMNS)} "
        "(refused by the PostgreSQL sink, docs/test/POSTGRESQL.md §4)",
        flush=True,
    )


# ------------------------------------------------------------------ peak load
def peak_writer(worker_id: int, stop_at: float, window: dict, lock: threading.Lock) -> dict:
    """INSERT batches into the MySQL source until ``stop_at``; never touches PG."""
    base = ROWS + 1 + worker_id * 10_000_000
    seq = 0
    written = 0
    failed = 0
    errors: list[str] = []
    db = conn()
    try:
        cur = db.cursor()
        while time.time() < stop_at:
            orders = []
            typed = []
            for _ in range(BATCH):
                seq += 1
                i = base + seq
                orders.append((i, i % 1000 + 1, float(i % 100) + 0.5, "peak"))
                typed.append(typed_row(i))
            try:
                cur.executemany(INSERT_T1, orders)
                cur.executemany(INSERT_TYPED, typed)
            except Exception as exc:  # noqa: BLE001 - reported as a failure counter
                failed += len(orders)
                errors.append(repr(exc))
                break
            written += len(orders)
            with lock:
                window["rows"] = int(window.get("rows", 0)) + len(orders)
    finally:
        db.close()
    return {"worker": worker_id, "written": written, "failed": failed, "errors": errors}


# ---------------------------------------------------------------- test points
def tp_pl01_snapshot() -> Case:
    case = new_case(
        "TP-PL01",
        f"Baseline multi-table sync (t1–t4 + typed, {ROWS} rows each)",
        f"seed {ROWS} rows x {len(BIZ_TABLES) + 1} tables + {LARGE_ROWS} mega-field rows, "
        "one pipeline, snapshot only",
        "every table reaches the planned row count and every row is canonically equal by "
        "primary key (chunked, diff-locating); no COUNT(*)-only check",
    )
    tables = BIZ_TABLES + (TYPED_TABLE,)
    wait_catchup(tables, timeout=cfg.PG_LOAD_CATCHUP_BUDGET_S)
    rows = {table: src_count(table) for table in tables}
    note(case, f"source rows: {rows}")
    check(
        case,
        "every table reached the planned row count",
        all(count == ROWS for count in rows.values()),
        f"want {ROWS} each, got {rows}",
    )
    for table in tables:
        reconcile(case, table)
    return finish(case)


def tp_pl02_peak() -> Case:
    case = new_case(
        "TP-PL02",
        f"Peak incremental stress ({WORKERS} writers x {PEAK_SEC}s, all writes on MySQL)",
        f"{WORKERS} threads INSERT {BATCH}/batch into {SRC_DB}.{BIZ_TABLES[0]} and "
        f"{SRC_DB}.{TYPED_TABLE} for {PEAK_SEC}s, then wait for catch-up",
        "writes all on the MySQL source; success/failure counters reported; both tables "
        "catch up within budget; final row diff empty; job up; checkpoint records an offset",
    )
    window: dict[str, int] = {"rows": 0}
    lock = threading.Lock()
    stop_at = time.time() + PEAK_SEC
    log(f"==> peak writers {WORKERS}x{PEAK_SEC}s (MySQL source only)", flush=True)
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(peak_writer, i, stop_at, window, lock) for i in range(WORKERS)]
        per_worker = [f.result() for f in as_completed(futures)]
    write_elapsed = max(time.time() - t0, 0.001)
    written = sum(int(w["written"]) for w in per_worker)
    failed = sum(int(w["failed"]) for w in per_worker)
    rps = written / write_elapsed
    log(f"TIMING  peak_write {write_elapsed:.1f}s ({written} rows ≈ {rps:.0f} rps)", flush=True)

    note(case, f"peak writes: {written} rows / {write_elapsed:.1f}s ≈ {rps:.0f} rps; failed={failed}")
    note(case, f"per worker: {per_worker}")
    check(case, "every writer wrote rows and none failed",
          all(int(w["written"]) > 0 for w in per_worker) and failed == 0,
          f"written={written} failed={failed} per_worker={[w['written'] for w in per_worker]}")
    # The window's length is enforced by the writer loop itself (``stop_at``), so it
    # is recorded rather than asserted: an assertion on ``write_elapsed`` would only
    # prove the clock moved. ``LOAD_PEAK_*`` may be overridden for a short smoke run,
    # so the documented shape is required only when the operator kept the defaults.
    note(case, f"peak window: {write_elapsed:.1f}s of the configured {PEAK_SEC}s")
    if os.environ.get("LOAD_PEAK_WORKERS") or os.environ.get("LOAD_PEAK_SECONDS"):
        note(case, f"peak shape overridden from the environment: {WORKERS} writers x {PEAK_SEC}s")
    else:
        check(case, "the peak window used the documented shape (4 writers x 60s)",
              WORKERS == 4 and PEAK_SEC == 60,
              f"workers={WORKERS} seconds={PEAK_SEC}")

    reached = True
    try:
        wait_catchup((BIZ_TABLES[0], TYPED_TABLE), timeout=cfg.PG_LOAD_CATCHUP_BUDGET_S)
    except TimeoutError as exc:
        reached = False
        check(case, "catch-up finished within budget", False, str(exc))
    if reached:
        check(case, "catch-up finished within budget", True,
              f"src={src_count(BIZ_TABLES[0])} snk={snk_count(BIZ_TABLES[0])}")
    reconcile(case, BIZ_TABLES[0])

    alive = job_alive()
    check(case, "the job is still running after the peak", alive, log_tail(4) if not alive else "yes")
    checkpoint = read_checkpoint()
    has_offset = bool(checkpoint.get("acked_offset"))
    check(case, "the checkpoint recorded a resume offset", has_offset,
          f"{len(checkpoint)} keys" if not has_offset else "yes")
    return finish(case)


def tp_pl03_typed() -> Case:
    case = new_case(
        "TP-PL03",
        "Typed columns under load (decimal / float / temporal / JSON / bool)",
        f"after the peak writes, compare every row of {SRC_DB}.{TYPED_TABLE} with "
        f"{SCHEMA}.{TYPED_TABLE}_out and assert the mapped column types",
        "mapped pg types match the documented matrix; every value (baseline and peak rows) "
        "is canonically equal - decimal/unsigned/temporal/JSON/bool stable",
    )
    live = pg_client.table_columns(SCHEMA, f"{TYPED_TABLE}_out")
    wrong = {
        column: (live.get(column), want)
        for column, want in EXPECTED_PG_TYPES.items()
        if live.get(column) != want
    }
    check(
        case,
        "mapped column types",
        not wrong,
        "; ".join(f"{c}: got {got!r} want {want!r}" for c, (got, want) in wrong.items()) or "ok",
    )

    reconcile(case, TYPED_TABLE)

    sample = [1, max(1, ROWS // 2), ROWS, ROWS + 1, ROWS + BATCH]
    ids = ",".join(str(i) for i in sample)
    src_rows = my(
        SRC_DB,
        f"SELECT {', '.join(f'`{c}`' for c in COLUMNS[TYPED_TABLE])} "
        f"FROM `{SRC_DB}`.`{TYPED_TABLE}` WHERE id IN ({ids}) ORDER BY id",
    )
    snk_rows = pg_client.q(
        f"SELECT {', '.join(f'{chr(34)}{c}{chr(34)}' for c in COLUMNS[TYPED_TABLE])} "
        f'FROM "{SCHEMA}"."{TYPED_TABLE}_out" WHERE id IN ({ids}) ORDER BY id',
    )
    failures = pg_verify.diff_rows(
        src_rows, snk_rows, COLUMNS[TYPED_TABLE], KINDS[TYPED_TABLE], ("id",)
    )
    check(case, f"spot-check ids {sample} equal", not failures, pg_verify.summarize(failures))
    note(case, f"spot-check rows: src={len(src_rows)} snk={len(snk_rows)}")
    return finish(case)


def tp_pl04_large() -> Case:
    case = new_case(
        "TP-PL04",
        "Mega-field sync (LONGBLOB + LONGTEXT, 1–5 MiB)",
        f"{LARGE_ROWS} rows x {LARGE_BYTES}B JPEG-like c_img + {LARGE_BYTES}-char c_text, "
        "compared by length + SHA-256 computed on each server",
        "byte/char length equal, SHA-256 equal, image keeps the JPEG SOI prefix; digests "
        "match for every row",
    )
    mysql_sql = (
        "SELECT id, LENGTH(c_img) AS img_len, HEX(SUBSTRING(c_img, 1, 2)) AS img_magic, "
        "SHA2(c_img, 256) AS img_sha, CHAR_LENGTH(c_text) AS text_len, "
        "SHA2(c_text, 256) AS text_sha, c_note "
        f"FROM `{SRC_DB}`.`{LARGE_TABLE}` ORDER BY id"
    )
    pg_sql = (
        "SELECT id, length(c_img) AS img_len, encode(substring(c_img from 1 for 2), 'hex') "
        "AS img_magic, encode(sha256(c_img), 'hex') AS img_sha, length(c_text) AS text_len, "
        "encode(sha256(convert_to(c_text, 'UTF8')), 'hex') AS text_sha, c_note "
        f'FROM "{SCHEMA}"."{LARGE_TABLE}_out" ORDER BY id'
    )
    kinds = {
        "id": "int",
        "img_len": "int",
        "img_magic": "text",
        "img_sha": "text",
        "text_len": "int",
        "text_sha": "text",
        "c_note": "text",
    }
    if not pg_client.table_exists(SCHEMA, f"{LARGE_TABLE}_out"):
        check(case, f"{SCHEMA}.{LARGE_TABLE}_out exists", False, "table missing")
        return finish(case)
    src_rows = my(SRC_DB, mysql_sql)
    snk_rows = pg_client.q(pg_sql)
    # The image magic comes back from two different hex encoders: MySQL
    # `HEX(...)` emits uppercase (``FFD8``) while PostgreSQL
    # `encode(...,'hex')` emits lowercase (``ffd8``). They are the same two bytes,
    # so normalise the case before the cross-server diff or every row would
    # "differ" on hex casing alone. (The sha columns already agree: both
    # `SHA2()` and `encode(sha256(...),'hex')` are lowercase.)
    for rows in (src_rows, snk_rows):
        for row in rows:
            if row.get("img_magic") is not None:
                row["img_magic"] = str(row["img_magic"]).lower()
    truncated = [
        str(row["id"])
        for row in snk_rows
        if int(row["img_len"] or 0) != LARGE_BYTES or int(row["text_len"] or 0) != LARGE_BYTES
    ]
    check(case, "no truncated payload", not truncated,
          f"rows with a wrong length: {truncated[:10]}" if truncated else f"{LARGE_BYTES}B each")
    bad_magic = [
        str(row["id"]) for row in snk_rows if not str(row["img_magic"]).lower().startswith("ffd8")
    ]
    check(case, "image keeps the JPEG SOI prefix", not bad_magic,
          f"rows without FF D8: {bad_magic[:10]}" if bad_magic else "ffd8")
    failures = pg_verify.diff_rows(src_rows, snk_rows, tuple(kinds), kinds, ("id",))
    check(case, f"{len(src_rows)} digests match source", not failures,
          pg_verify.summarize(failures))
    note(case, f"payload: {LARGE_ROWS} rows x {LARGE_BYTES}B x 2 fields "
               f"≈ {LARGE_ROWS * LARGE_BYTES * 2 / (1024 ** 2):.0f} MiB")
    return finish(case)


PLAN = (
    ("TP-PL01", tp_pl01_snapshot),
    ("TP-PL02", tp_pl02_peak),
    ("TP-PL03", tp_pl03_typed),
    ("TP-PL04", tp_pl04_large),
)


# ------------------------------------------------------------------ reporting
def _status_cell(status: str) -> str:
    return {"PASS": "✅ PASS", "FAIL": "❌ FAIL"}.get(status, f"⊘ {status}")


def write_report(timings: dict[str, float]) -> Path:
    cfg.REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = cfg.REPORT_PG_LOAD
    passed = sum(1 for case in cases if case.status == "PASS")
    failed = sum(1 for case in cases if case.status == "FAIL")
    other = len(cases) - passed - failed
    try:
        version = pg_client.server_version()
    except Exception:  # noqa: BLE001 - the report still has to be written
        version = "unknown"
    normal_tables = len(BIZ_TABLES) + 1
    mega_mib = LARGE_ROWS * LARGE_BYTES * 2 / (1024 ** 2)
    lines = [
        "# go-cdc mysql2postgresql load report (expected vs actual)\n",
        '<a id="overview"></a>\n',
        "## 0. Overview\n",
        f"- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "- Basis: 10-run CI average for timings; gates held across the sample window",
        f"- Source: `{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}` / `{cfg.SRC_USER}` / `{SRC_DB}` "
        "(all writes happen here)",
        f"- Target: `{cfg.PG_HOST}:{cfg.PG_PORT}` / `{cfg.PG_USER}` / `{cfg.PG_DB}` "
        f"(schema `{SCHEMA}`, read-only for this suite)",
        f"- PostgreSQL server: `{version}`",
        f"- Entry point: `{cfg.project_display_path(BINARY)} <pipeline.yaml>`",
        f"- **Summary: pass {passed} / fail {failed} / other {other} / total {len(cases)}** → "
        f"{'**all passed**' if failed == 0 else '**failures present**'}\n",
        "| TP | Name | Status |",
        "| --- | --- | --- |",
    ]
    for case in cases:
        lines.append(f"| {case.id} | {cfg.md_cell(case.title)} | {_status_cell(case.status)} |")
    lines += [
        "",
        "## 1. Volume plan (configured knobs)\n",
        "| Knob | Value | Meaning |",
        "| --- | --- | --- |",
        f"| `LOAD_ROWS_PER_TABLE` | {ROWS} | rows per normal table (t1–t4 + typed) |",
        f"| normal tables | {normal_tables} | t1,t2,t3,t4,typed |",
        f"| normal row total | {ROWS * normal_tables} | baseline snapshot rows |",
        f"| `LOAD_LARGE_ROWS` | {LARGE_ROWS} | rows in `{LARGE_TABLE}` |",
        f"| `LOAD_LARGE_FIELD_BYTES` | {LARGE_BYTES} ({LARGE_BYTES / (1024 ** 2):.2f} MiB) | "
        "per-field size for `c_img` and `c_text` |",
        f"| mega payload total | ~{mega_mib:.1f} MiB | {LARGE_ROWS} × {LARGE_BYTES}B × 2 fields |",
        f"| peak | {PEAK_SEC}s × {WORKERS} writers × batch {BATCH} | incremental stress |",
        f"| `LOAD_PARALLELISM` | {cfg.LOAD_PARALLELISM} | CDC pipeline.parallelism |",
        f"| `PG_LOAD_BATCH_SIZE` | {cfg.PG_LOAD_BATCH_SIZE} | sink write batch (rows) |",
        f"| `PG_LOAD_BATCH_INTERVAL` | {cfg.PG_LOAD_BATCH_INTERVAL} | sink flush interval |",
        f"| `PG_LOAD_VERIFY_CHUNK` | {cfg.PG_LOAD_VERIFY_CHUNK} | primary-key window per diff |",
        f"| `PG_LOAD_CATCHUP_BUDGET_S` | {cfg.PG_LOAD_CATCHUP_BUDGET_S:.0f} | catch-up bound |",
        "",
        "## 2. Phase timings\n",
        "| Phase | Seconds |",
        "| --- | ---: |",
    ]
    for name, seconds in timings.items():
        lines.append(f"| {name} | {seconds:.1f} |")
    lines += [
        "",
        "## 3. Deliberate deviations from the mysql2mysql load suite\n",
        f"- `{TYPED_TABLE}.c_enum` and `.c_bit` are dropped after seeding: the PostgreSQL sink "
        "refuses `ENUM`/`BIT`. Seeding fails loudly if the columns are still present, and every "
        "other column keeps the exact value `load_it._typed_row()` produces.",
        "- Reconciliation compares primary-key sets and canonical column values in "
        f"{cfg.PG_LOAD_VERIFY_CHUNK}-id windows, so a mismatch is reported with the ids and "
        "columns that differ instead of a row-count delta.",
        "- Mega fields are compared by `LENGTH`/`CHAR_LENGTH` and SHA-256 computed on each "
        "server, so 2 MiB payloads never travel to the harness.",
        f"- The MySQL source database is the same one the mysql2mysql load suite seeds "
        f"(`{SRC_DB}`); the two load suites must not run simultaneously on one stack.",
        "- `TIMESTAMP`/`DATETIME` are read with `SET time_zone = '+08:00'`, the pipeline's "
        "`server-time-zone`, so both sides are the same wall clock.",
        "- One source database only: the load suite does not claim multi-database coverage. "
        "Multi-DB capture is TP-P01 and TP-P10 in the functional suite; here the target is a "
        "single schema on one PostgreSQL database.",
        "- The peak window only INSERTs: UPDATE/DELETE under load are not part of TP-PL02, so the "
        "numbers describe the insert path. Mixed DML under sustained load is measured by "
        "`sinks/postgresql/harness/pg_bench_it.py` (workload `mixed`), not here.",
        "",
        "## 4. Data flow\n",
        "```mermaid",
        "flowchart LR",
        f"  src[{SRC_DB}] -->|binlog| job[go-cdc]",
        f"  job --> tgt[{cfg.PG_DB}.{SCHEMA}]",
        "```\n",
        "## 5. Test points\n",
    ]
    for case in cases:
        lines += [
            f"### {case.id} {case.title}\n",
            "- [↑ Overview](#overview)",
            f"- **Result: {_status_cell(case.status)}**",
            f"- **Action:** {cfg.md_cell(case.action)}",
            f"- **Expected:** {cfg.md_cell(case.expected)}\n",
            "| Check | Result | Detail |",
            "| --- | --- | --- |",
        ]
        for item in case.checks:
            lines.append(
                f"| {cfg.md_cell(item.name)} | {'✅' if item.ok else '❌'} | "
                f"{cfg.md_cell(item.detail)} |"
            )
        if case.evidence:
            lines += ["", "**Evidence**\n"]
            lines += [f"- {cfg.md_cell(text)}" for text in case.evidence]
        lines.append("")
    path.write_text(cfg.sanitize_report_text("\n".join(lines)), encoding="utf-8")
    return path


# ----------------------------------------------------------------------- main
def preflight() -> None:
    if not Path(BINARY).exists():
        raise RuntimeError(f"go-cdc binary not found at {BINARY} (run `make build`)")
    if not pg_client.ping():
        raise RuntimeError(
            f"PostgreSQL is not reachable at {cfg.PG_HOST}:{cfg.PG_PORT} "
            "(run `bash tests/integration/scripts/run.sh postgresql normal pg16`)"
        )
    if cfg.PG_EXPECT_MAJOR and str(pg_client.major_version()) != cfg.PG_EXPECT_MAJOR:
        raise RuntimeError(
            f"this run expects PostgreSQL {cfg.PG_EXPECT_MAJOR}, the server is "
            f"{pg_client.server_version()}"
        )
    try:
        my(SRC_DB, "SELECT 1 AS ok")
    except Exception as exc:  # noqa: BLE001 - any failure means MySQL is unusable
        raise RuntimeError(
            f"MySQL is not reachable at {cfg.MYSQL_HOST}:{cfg.MYSQL_PORT} or lacks "
            f"{SRC_DB} (run `bash tests/integration/scripts/run.sh mysql`): {exc}"
        ) from exc


def prepare() -> None:
    """Fresh target schema and no stale checkpoints before the first case."""
    pg_client.ensure_database()
    pg_client.drop_schema(SCHEMA)
    pg_client.create_schema(SCHEMA)
    for path in Path(cfg.CHECKPOINT_PG_LOAD_DIR).rglob("*.ckpt"):
        path.unlink()


def run_case(tp_id: str, run) -> None:
    before = len(cases)
    try:
        run()
    except Exception as exc:  # noqa: BLE001 - one case must not stop the others
        log(f"    [error] {tp_id}: {exc!r}", flush=True)
        if len(cases) > before:
            case = cases[-1]
            check(case, "the case completed without an exception", False, repr(exc))
            if not case.status:
                finish(case, "FAIL")
        else:
            case = new_case(tp_id, tp_id, "n/a", "n/a")
            check(case, "the case started", False, repr(exc))
            finish(case, "FAIL")


def main(argv: list[str] | None = None) -> int:
    wanted = {
        arg.upper()
        for arg in (sys.argv[1:] if argv is None else argv)
        if not arg.startswith("-")
    }
    cfg.REPORT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        preflight()
    except Exception as exc:  # noqa: BLE001 - an unusable environment is not a failure
        log(f"ENV BLOCKED: {exc}", flush=True)
        return 2
    prepare()
    suite_t0 = time.time()
    timings: dict[str, float] = {}

    t0 = time.time()
    ensure_schema()
    timings["schema_and_seed"] = time.time() - t0
    log(f"TIMING  schema_and_seed {timings['schema_and_seed']:.1f}s", flush=True)
    log(f"    {format_disk(WORKDIR)}", flush=True)
    log(
        f"==> seed baseline {ROWS} rows x {len(BIZ_TABLES) + 1} tables + "
        f"{LARGE_ROWS} large rows ({LARGE_BYTES}B)",
        flush=True,
    )

    render_yaml()
    log("==> start go-cdc (snapshot + incremental -> postgresql)", flush=True)
    log(f"    sink {cfg.PG_DB}.{SCHEMA}, batch {cfg.PG_LOAD_BATCH_SIZE}/"
        f"{cfg.PG_LOAD_BATCH_INTERVAL}, parallelism {cfg.LOAD_PARALLELISM}", flush=True)
    t0 = time.time()
    start_job()
    wait_alive()
    timings["cdc_start"] = time.time() - t0

    try:
        for tp_id, run in PLAN:
            if wanted and tp_id not in wanted:
                continue
            case_t0 = time.time()
            run_case(tp_id, run)
            timings[tp_id] = time.time() - case_t0
            log(f"TIMING  {tp_id} {timings[tp_id]:.1f}s", flush=True)
    finally:
        stop_job()

    timings["suite_total"] = time.time() - suite_t0
    path = write_report(timings)
    passed = sum(1 for case in cases if case.status == "PASS")
    failed = sum(1 for case in cases if case.status == "FAIL")
    log("REPORT", cfg.project_display_path(path), flush=True)
    log("PASS", passed, "FAIL", failed, "OTHER", len(cases) - passed - failed, flush=True)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        stop_job()
