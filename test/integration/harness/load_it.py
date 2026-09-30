#!/usr/bin/env python3
"""Load / peak integration suite (TP-L01..TP-L04).

TP-L01: multi-table snapshot (t1–t4 + typed + mega-field table).
TP-L02: concurrent peak inserts, assert sink catch-up.
TP-L03: typed-column round-trip under load (spot-check values).
TP-L04: 1–5MiB image-like LONGBLOB + LONGTEXT sync (LENGTH + magic bytes).
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
from datetime import datetime
from pathlib import Path

import pymysql

from . import config as cfg
from .logutil import log

SRC = cfg.conn_kwargs(cfg.SRC_USER, cfg.SRC_PASSWORD, cfg.LOAD_SRC_DB)
SNK = cfg.conn_kwargs(cfg.SINK_USER, cfg.SINK_PASSWORD, cfg.LOAD_SNK_DB)
SRC_DB = cfg.LOAD_SRC_DB
SINK_DB = cfg.LOAD_SNK_DB
WORKDIR = cfg.WORKDIR
BINARY = cfg.BINARY
CK = Path(cfg.CHECKPOINT_LOAD_PATH)
YAML = WORKDIR / "pipeline.load.yaml"
LOG = WORKDIR / "cdc-load.log"
REPORT_DIR = cfg.REPORT_DIR

# Normal tables: LOAD_ROWS_PER_TABLE each. Mega-field table: fewer rows × 1–5MiB.
BIZ_TABLES = ("tpl01_t1", "tpl01_t2", "tpl01_t3", "tpl01_t4")
TYPED_TABLE = "tpl01_typed"
LARGE_TABLE = "tpl01_large"
TABLES = BIZ_TABLES + (TYPED_TABLE, LARGE_TABLE)
OUTS = tuple(f"{t}_out" for t in TABLES)

ROWS = cfg.LOAD_ROWS_PER_TABLE
LARGE_ROWS = cfg.LOAD_LARGE_ROWS
LARGE_BYTES = cfg.LOAD_LARGE_FIELD_BYTES
PEAK_SEC = cfg.LOAD_PEAK_SECONDS
WORKERS = cfg.LOAD_PEAK_WORKERS
BATCH = cfg.LOAD_PEAK_BATCH

# JPEG SOI + APP0/JFIF stub; body is deterministic filler; EOI at end.
_JPEG_HDR = bytes(
    [
        0xFF,
        0xD8,
        0xFF,
        0xE0,
        0x00,
        0x10,
        0x4A,
        0x46,
        0x49,
        0x46,
        0x00,
        0x01,
        0x01,
        0x00,
        0x00,
        0x01,
        0x00,
        0x01,
        0x00,
        0x00,
    ]
)
_JPEG_EOI = bytes([0xFF, 0xD9])

proc: subprocess.Popen | None = None
results: list["TPResult"] = []


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


def conn(dbcfg):
    # Packet size comes from server my.cnf/compose (64M). Client reads it on handshake.
    return pymysql.connect(**dbcfg, cursorclass=pymysql.cursors.DictCursor, autocommit=True)


def exec_sql(dbcfg, sql: str, args=None):
    with conn(dbcfg) as c:
        with c.cursor() as cur:
            if args is None:
                cur.execute(sql)
            else:
                cur.execute(sql, args)


def q(dbcfg, sql: str, args=None) -> list[dict]:
    with conn(dbcfg) as c:
        with c.cursor() as cur:
            if args is None:
                cur.execute(sql)
            else:
                cur.execute(sql, args)
            return list(cur.fetchall()) if cur.description else []


def count_rows(dbcfg, database: str, table: str) -> int:
    rows = q({**dbcfg, "database": database}, f"SELECT COUNT(*) AS n FROM `{database}`.`{table}`")
    return int(rows[0]["n"]) if rows else 0


def table_exists(dbcfg, database: str, table: str) -> bool:
    rows = q(
        {**dbcfg, "database": database},
        "SELECT 1 AS ok FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
        (database, table),
    )
    return bool(rows)


def wait_until(predicate, timeout=300, interval=15.0, desc="condition") -> float:
    """Poll until predicate is true. Returns elapsed seconds."""
    t0 = time.time()
    deadline = t0 + timeout
    last_err = None
    while time.time() < deadline:
        try:
            if predicate():
                elapsed = time.time() - t0
                log(f"TIMING  wait [{desc}] done in {elapsed:.1f}s", flush=True)
                return elapsed
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(interval)
    raise TimeoutError(f"timeout waiting for {desc} after {time.time() - t0:.1f}s: {last_err}")


def _lag_line(table: str) -> str:
    src_n = count_rows(SRC, SRC_DB, table)
    snk_n = count_rows(SNK, SINK_DB, f"{table}_out")
    lag = abs(src_n - snk_n) / max(src_n, 1)
    return f"{table} src={src_n} snk={snk_n} lag={lag:.2%}"


def wait_peak_catchup(timeout: float = 1800, interval: float = 15.0, log_every: float = 15.0) -> float:
    """Wait until tpl01_t1 and typed are within 1% src/sink; log lag periodically."""
    t0 = time.time()
    deadline = t0 + timeout
    last_log = 0.0
    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"go-cdc exited during peak catch-up: {proc.returncode}")
        if _count_ok("tpl01_t1") and _count_ok(TYPED_TABLE):
            elapsed = time.time() - t0
            log(f"TIMING  wait [cdc_peak catch-up] done in {elapsed:.1f}s", flush=True)
            return elapsed
        now = time.time()
        if now - last_log >= log_every:
            log(
                f"    peak catch-up {now - t0:.0f}s: "
                f"{_lag_line('tpl01_t1')} | {_lag_line(TYPED_TABLE)}",
                flush=True,
            )
            last_log = now
        time.sleep(interval)
    log(
        f"    peak catch-up TIMEOUT: {_lag_line('tpl01_t1')} | {_lag_line(TYPED_TABLE)}",
        flush=True,
    )
    raise TimeoutError(
        f"timeout waiting for cdc_peak catch-up after {time.time() - t0:.1f}s "
        f"({_lag_line('tpl01_t1')}; {_lag_line(TYPED_TABLE)})"
    )


def start_job():
    global proc
    LOG.write_text("")
    proc = subprocess.Popen([BINARY, str(YAML)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

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
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    proc = None


def record(tp: TPResult, override: bool | None = None, compare_md: str | None = None):
    tp.passed = True if override is None else override
    if compare_md:
        tp.compare_md = compare_md
    results.append(tp)
    log(f"{'PASS' if tp.passed else 'FAIL'} {tp.id} {tp.title}")


def ensure_schema():
    ddl = {
        "tpl01_t1": (
            "(id BIGINT PRIMARY KEY, c_ref BIGINT NOT NULL, "
            "c_amt DECIMAL(12,2) NOT NULL, c_flag VARCHAR(16) NOT NULL)"
        ),
        "tpl01_t2": (
            "(id BIGINT PRIMARY KEY, c_ref BIGINT NOT NULL, "
            "c_code VARCHAR(32) NOT NULL, c_qty INT NOT NULL)"
        ),
        "tpl01_t3": (
            "(id BIGINT PRIMARY KEY, c_name VARCHAR(64) NOT NULL, c_city VARCHAR(32) NOT NULL)"
        ),
        "tpl01_t4": (
            "(id BIGINT PRIMARY KEY, c_ref BIGINT NOT NULL, "
            "c_method VARCHAR(16) NOT NULL, c_paid DECIMAL(12,2) NOT NULL)"
        ),
        "tpl01_typed": (
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
            "c_enum ENUM('red','green','blue') NOT NULL, "
            "c_bit BIT(8) NOT NULL, "
            "c_bool TINYINT(1) NOT NULL)"
        ),
        "tpl01_large": (
            "(id BIGINT PRIMARY KEY, "
            "c_img LONGBLOB NOT NULL COMMENT 'image-like JPEG payload 1-5MiB', "
            "c_text LONGTEXT NOT NULL COMMENT '1-5MiB text payload', "
            "c_note VARCHAR(64) NOT NULL)"
        ),
    }
    for name, cols in ddl.items():
        exec_sql(SRC, f"DROP TABLE IF EXISTS `{SRC_DB}`.`{name}`")
        exec_sql(SRC, f"CREATE TABLE `{SRC_DB}`.`{name}` {cols} ENGINE=InnoDB")
    for out in OUTS:
        try:
            exec_sql(SNK, f"DROP TABLE IF EXISTS `{SINK_DB}`.`{out}`")
        except Exception:
            pass


def _typed_row(i: int) -> tuple:
    colors = ("red", "green", "blue")
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
        colors[i % 3],
        i % 256,  # BIT(8) as integer 0..255
        i % 2,
    )


def _image_blob(i: int) -> bytes:
    """Deterministic JPEG-like binary of exactly LARGE_BYTES (header + body + EOI)."""
    if LARGE_BYTES < len(_JPEG_HDR) + len(_JPEG_EOI) + 8:
        raise ValueError(f"LOAD_LARGE_FIELD_BYTES too small: {LARGE_BYTES}")
    body_len = LARGE_BYTES - len(_JPEG_HDR) - len(_JPEG_EOI)
    unit = f"{i:08d}".encode("ascii") * 8
    body = (unit * ((body_len // len(unit)) + 2))[:body_len]
    return _JPEG_HDR + body + _JPEG_EOI


def _long_text(i: int) -> str:
    """Deterministic LONGTEXT of exactly LARGE_BYTES characters."""
    unit = f"T{i:08d}" * 8
    return (unit * ((LARGE_BYTES // len(unit)) + 2))[:LARGE_BYTES]


def seed_baseline():
    """Insert baseline rows into normal, typed, and mega-field tables (parallel by table)."""
    cities = ("hz", "sh", "bj", "sz")

    def _seed_range(table: str, sql: str, row_fn, id_start: int, id_end: int) -> None:
        db = conn(SRC)
        try:
            cur = db.cursor()
            cur.execute("SET time_zone = '+08:00'")
            for start in range(id_start, id_end + 1, BATCH):
                end = min(start + BATCH - 1, id_end)
                cur.executemany(sql, [row_fn(i) for i in range(start, end + 1)])
        finally:
            db.close()

    def seed_t1() -> None:
        _seed_range(
            "tpl01_t1",
            f"INSERT INTO `{SRC_DB}`.tpl01_t1(id,c_ref,c_amt,c_flag) VALUES (%s,%s,%s,%s)",
            lambda i: (i, i % 1000 + 1, float(i % 100) + 0.5, "ok"),
            1,
            ROWS,
        )

    def seed_t2() -> None:
        _seed_range(
            "tpl01_t2",
            f"INSERT INTO `{SRC_DB}`.tpl01_t2(id,c_ref,c_code,c_qty) VALUES (%s,%s,%s,%s)",
            lambda i: (i, i, f"C{i%500:04d}", (i % 5) + 1),
            1,
            ROWS,
        )

    def seed_t3() -> None:
        _seed_range(
            "tpl01_t3",
            f"INSERT INTO `{SRC_DB}`.tpl01_t3(id,c_name,c_city) VALUES (%s,%s,%s)",
            lambda i: (i, f"n{i}", cities[i % 4]),
            1,
            ROWS,
        )

    def seed_t4() -> None:
        _seed_range(
            "tpl01_t4",
            f"INSERT INTO `{SRC_DB}`.tpl01_t4(id,c_ref,c_method,c_paid) VALUES (%s,%s,%s,%s)",
            lambda i: (i, i, "m1" if i % 2 == 0 else "m2", float(i % 50) + 1.0),
            1,
            ROWS,
        )

    def seed_typed() -> None:
        _seed_range(
            "tpl01_typed",
            f"INSERT INTO `{SRC_DB}`.tpl01_typed("
            "id,c_tinyint,c_int,c_bigint,c_decimal,c_float,c_double,c_varchar,"
            "c_date,c_datetime,c_timestamp,c_json,c_enum,c_bit,c_bool"
            ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            _typed_row,
            1,
            ROWS,
        )

    def seed_large() -> None:
        db = conn(SRC)
        try:
            cur = db.cursor()
            cur.execute("SET time_zone = '+08:00'")
            for i in range(1, LARGE_ROWS + 1):
                cur.execute(
                    f"INSERT INTO `{SRC_DB}`.tpl01_large(id,c_img,c_text,c_note) VALUES (%s,%s,%s,%s)",
                    (i, _image_blob(i), _long_text(i), f"note-{i}"),
                )
                if i % 10 == 0:
                    log(f"    large seeded {i}/{LARGE_ROWS}", flush=True)
        finally:
            db.close()

    tasks = {
        "tpl01_t1": seed_t1,
        "tpl01_t2": seed_t2,
        "tpl01_t3": seed_t3,
        "tpl01_t4": seed_t4,
        "tpl01_typed": seed_typed,
        "tpl01_large": seed_large,
    }
    # Parallelize across tables; seed concurrency = workers * 2, hard cap 16.
    seed_cap = 16
    workers = max(1, min(WORKERS * 2, seed_cap, len(tasks)))
    log(
        f"    seed workers={workers} "
        f"(LOAD_PEAK_WORKERS={WORKERS} × 2, cap={seed_cap}, one conn/table)",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(fn): name for name, fn in tasks.items()}
        for fut in as_completed(futs):
            name = futs[fut]
            try:
                fut.result()
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(f"{SRC_DB}.{name}: seed failed: {exc}") from exc
            log(f"    seed done {name}", flush=True)


def peak_writer(worker_id: int, stop_at: float, counter: list[int], lock: threading.Lock) -> int:
    """Write batches until stop_at; returns rows written by this worker."""
    written = 0
    base = ROWS + 1 + worker_id * 10_000_000
    seq = 0
    db = conn(SRC)
    try:
        cur = db.cursor()
        cur.execute("SET time_zone = '+08:00'")
        while time.time() < stop_at:
            batch_orders = []
            batch_typed = []
            for _ in range(BATCH):
                seq += 1
                i = base + seq
                batch_orders.append((i, i % 1000 + 1, float(i % 100) + 0.5, "peak"))
                batch_typed.append(_typed_row(i))
            cur.executemany(
                f"INSERT INTO `{SRC_DB}`.tpl01_t1(id,c_ref,c_amt,c_flag) VALUES (%s,%s,%s,%s)",
                batch_orders,
            )
            cur.executemany(
                f"INSERT INTO `{SRC_DB}`.tpl01_typed("
                "id,c_tinyint,c_int,c_bigint,c_decimal,c_float,c_double,c_varchar,"
                "c_date,c_datetime,c_timestamp,c_json,c_enum,c_bit,c_bool"
                ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                batch_typed,
            )
            written += len(batch_orders)
            with lock:
                counter[0] += len(batch_orders)
    finally:
        db.close()
    return written


def write_report(timings: dict[str, float] | None = None) -> Path:
    passed = sum(1 for r in results if r.passed)
    failed = len(results) - passed
    path = cfg.REPORT_LOAD
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    normal_tables = len(BIZ_TABLES) + 1  # + typed
    mega_bytes = LARGE_ROWS * LARGE_BYTES * 2  # c_img + c_text
    lines = [
        "# go-cdc load integration test report\n",
        f"- Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Source DB: `{SRC_DB}` sink DB: `{SINK_DB}` (credentials redacted)",
        "## Volume plan (configured knobs)\n",
        f"| Knob | Value | Meaning |",
        f"| --- | --- | --- |",
        f"| `LOAD_PROFILE` | {cfg.LOAD_PROFILE or '(unset)'} | CI/local tier label |",
        f"| `LOAD_ROWS_PER_TABLE` | {ROWS} | rows per normal table (t1–t4 + typed) |",
        f"| normal tables | {normal_tables} | t1,t2,t3,t4,typed |",
        f"| normal row total | {ROWS * normal_tables} | baseline snapshot rows |",
        f"| `LOAD_LARGE_ROWS` | {LARGE_ROWS} | rows in `tpl01_large` |",
        f"| `LOAD_LARGE_FIELD_BYTES` | {LARGE_BYTES} ({LARGE_BYTES / (1024*1024):.2f} MiB) | "
        f"per-field size for `c_img` LONGBLOB and `c_text` LONGTEXT |",
        f"| mega payload total | ~{mega_bytes / (1024*1024):.1f} MiB | "
        f"{LARGE_ROWS} × {LARGE_BYTES}B × 2 fields |",
        f"| `LOAD_PARALLELISM` | {cfg.LOAD_PARALLELISM} | CDC pipeline.parallelism |",
        f"| peak | {PEAK_SEC}s × {WORKERS} workers × batch {BATCH} | incremental stress |",
        "",
    ]
    if timings:
        lines.extend(
            [
                "## Phase timings\n",
                "| Phase | Seconds |",
                "| --- | ---: |",
            ]
        )
        for name, sec in timings.items():
            lines.append(f"| {name} | {sec:.1f} |")
        lines.append("")
    lines.append(f"- Pass {passed} / fail {failed}\n")
    for r in results:
        lines.append(f"## {r.id} {r.title}\n")
        lines.append(f"- Result: {'✅ PASS' if r.passed else '❌ FAIL'}")
        lines.append(f"- Action: {r.action}")
        lines.append(f"- Expected: {r.expected_desc}\n")
        if r.compare_md:
            lines.append(r.compare_md + "\n")
        if r.extra_md:
            lines.append(r.extra_md + "\n")
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _count_ok(table: str) -> bool:
    src_n = count_rows(SRC, SRC_DB, table)
    snk_n = count_rows(SNK, SINK_DB, f"{table}_out")
    return abs(src_n - snk_n) / max(src_n, 1) <= 0.01


def main() -> int:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    if CK.exists():
        CK.unlink()
    suite_t0 = time.time()
    timings: dict[str, float] = {}

    t0 = time.time()
    ensure_schema()
    timings["schema_setup"] = time.time() - t0
    log(f"TIMING  schema_setup {timings['schema_setup']:.1f}s", flush=True)

    log(
        f"==> seed baseline {ROWS} rows x {len(BIZ_TABLES)+1} tables + "
        f"{LARGE_ROWS} large rows ({LARGE_BYTES}B)",
        flush=True,
    )
    t0 = time.time()
    seed_baseline()
    timings["seed_baseline"] = time.time() - t0
    log(f"TIMING  seed_baseline {timings['seed_baseline']:.1f}s", flush=True)

    cfg.render_template(
        "pipeline.load.yaml.tpl",
        YAML,
        extra={"LOAD_PARALLELISM": cfg.LOAD_PARALLELISM},
    )
    log("==> start go-cdc (snapshot + incremental)", flush=True)
    t0 = time.time()
    start_job()
    timings["cdc_start"] = time.time() - t0

    # TP-L01 multi-table snapshot (biz + typed + large)
    def snap_done():
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"go-cdc exited early: {proc.returncode}")
        for t in BIZ_TABLES + (TYPED_TABLE,):
            out = f"{t}_out"
            if not (table_exists(SNK, SINK_DB, out) and count_rows(SNK, SINK_DB, out) >= ROWS):
                return False
        out_l = f"{LARGE_TABLE}_out"
        return table_exists(SNK, SINK_DB, out_l) and count_rows(SNK, SINK_DB, out_l) >= LARGE_ROWS

    timings["cdc_snapshot"] = wait_until(snap_done, timeout=1800, desc="cdc_snapshot catch-up")
    log(f"TIMING  cdc_snapshot {timings['cdc_snapshot']:.1f}s", flush=True)
    cmp1 = (
        "| Table | Expected rows | Source rows | Sink rows | Delta | Verdict |\n"
        "| --- | --- | --- | --- | --- | --- |\n"
    )
    ok_l01 = True
    for t in TABLES:
        expect_n = LARGE_ROWS if t == LARGE_TABLE else ROWS
        s = count_rows(SRC, SRC_DB, t)
        d = count_rows(SNK, SINK_DB, f"{t}_out")
        lag = abs(s - d) / max(s, 1)
        hit = s == expect_n and lag <= 0.01
        ok_l01 = ok_l01 and hit
        cmp1 += (
            f"| {t} | {expect_n} | {s} | {d} | {lag:.2%} | {'✅' if hit else '❌'} |\n"
        )
    cmp1 += f"| Summary | | | | | **{'✅ PASS' if ok_l01 else '❌ FAIL'}** |"
    record(
        TPResult(
            id="TP-L01",
            title="Multi-table initial sync (t1–t4 + typed + mega-fields)",
            action=(
                f"{len(BIZ_TABLES)}+typed × {ROWS} rows; "
                f"tpl01_large × {LARGE_ROWS} rows × {LARGE_BYTES}B img+text"
            ),
            expected_desc="Each table reaches planned row count; src/sink delta <= 1%",
            expected_rows=[{"ok": "yes"}],
            actual_rows=[{"ok": "yes" if ok_l01 else "no"}],
            columns=["ok"],
        ),
        override=ok_l01,
        compare_md=cmp1,
    )

    # TP-L03 typed spot-check (after snapshot; before peak mutates ids beyond ROWS)
    sample_ids = [1, max(1, ROWS // 2), ROWS]
    typed_sql = (
        "SELECT id, CAST(c_tinyint AS CHAR) AS c_tinyint, CAST(c_int AS CHAR) AS c_int, "
        "CAST(c_bigint AS CHAR) AS c_bigint, CAST(c_decimal AS CHAR) AS c_decimal, "
        "CAST(c_varchar AS CHAR) AS c_varchar, "
        "DATE_FORMAT(c_date, '%Y-%m-%d') AS c_date, "
        "DATE_FORMAT(c_datetime, '%Y-%m-%d %H:%i:%s') AS c_datetime, "
        "CAST(c_json AS CHAR) AS c_json, CAST(c_enum AS CHAR) AS c_enum, "
        "LPAD(HEX(c_bit), 2, '0') AS c_bit, CAST(c_bool AS CHAR) AS c_bool "
        f"FROM `{{db}}`.`{{table}}` WHERE id IN ({','.join(str(i) for i in sample_ids)}) ORDER BY id"
    )
    src_typed = q(
        {**SRC, "database": SRC_DB},
        typed_sql.replace("{db}", SRC_DB).replace("{table}", TYPED_TABLE),
    )
    snk_typed = q(
        {**SNK, "database": SINK_DB},
        typed_sql.replace("{db}", SINK_DB).replace("{table}", f"{TYPED_TABLE}_out"),
    )

    def _norm(rows: list[dict]) -> list[dict]:
        out = []
        for r in rows:
            out.append({k: (None if v is None else str(v)) for k, v in r.items()})
        return out

    src_n, snk_n = _norm(src_typed), _norm(snk_typed)
    ok_l03 = src_n == snk_n and len(src_n) == len(sample_ids)
    cmp3 = (
        "| Check | Expected | Actual | Verdict |\n| --- | --- | --- | --- |\n"
        f"| sample rows | {len(sample_ids)} | src={len(src_n)} snk={len(snk_n)} | "
        f"{'✅' if len(src_n) == len(snk_n) == len(sample_ids) else '❌'} |\n"
        f"| values match | equal | {'equal' if src_n == snk_n else 'mismatch'} | "
        f"{'✅' if src_n == snk_n else '❌'} |\n"
        f"| Summary | | | **{'✅ PASS' if ok_l03 else '❌ FAIL'}** |"
    )
    record(
        TPResult(
            id="TP-L03",
            title="Typed columns under load (spot-check)",
            action=f"Compare typed sample ids {sample_ids} source vs sink",
            expected_desc="tinyint/int/bigint/decimal/varchar/date/datetime/json/enum/bit/bool match",
            expected_rows=src_n,
            actual_rows=snk_n,
            columns=list(src_n[0].keys()) if src_n else ["id"],
        ),
        override=ok_l03,
        compare_md=cmp3,
    )

    # TP-L04 mega fields: counts + LENGTH + JPEG magic on image blob
    large_sql = (
        "SELECT id, LENGTH(c_img) AS img_len, CHAR_LENGTH(c_text) AS text_len, "
        "HEX(SUBSTRING(c_img, 1, 4)) AS img_magic, c_note "
        f"FROM `{{db}}`.`{{table}}` WHERE id IN (1, {max(1, LARGE_ROWS // 2)}, {LARGE_ROWS}) "
        "ORDER BY id"
    )
    src_large = q(
        {**SRC, "database": SRC_DB},
        large_sql.replace("{db}", SRC_DB).replace("{table}", LARGE_TABLE),
    )
    snk_large = q(
        {**SNK, "database": SINK_DB},
        large_sql.replace("{db}", SINK_DB).replace("{table}", f"{LARGE_TABLE}_out"),
    )
    src_ln, snk_ln = _norm(src_large), _norm(snk_large)
    ok_len = src_ln == snk_ln and len(src_ln) > 0
    ok_size = all(
        int(r.get("img_len") or 0) == LARGE_BYTES and int(r.get("text_len") or 0) == LARGE_BYTES
        for r in src_ln
    )
    # JPEG SOI = FFD8; APP0 marker FF E0 → magic prefix FFD8FFE0
    ok_magic = all(str(r.get("img_magic", "")).upper().startswith("FFD8") for r in snk_ln)
    ok_l04 = _count_ok(LARGE_TABLE) and ok_len and ok_size and ok_magic
    cmp4 = (
        "| Check | Expected | Actual | Verdict |\n| --- | --- | --- | --- |\n"
        f"| row count | {LARGE_ROWS} (delta<=1%) | "
        f"src={count_rows(SRC, SRC_DB, LARGE_TABLE)} "
        f"snk={count_rows(SNK, SINK_DB, f'{LARGE_TABLE}_out')} | "
        f"{'✅' if _count_ok(LARGE_TABLE) else '❌'} |\n"
        f"| c_img LONGBLOB length | {LARGE_BYTES} ({LARGE_BYTES/(1024*1024):.2f} MiB) | "
        f"{src_ln[0].get('img_len') if src_ln else 'n/a'} | {'✅' if ok_size else '❌'} |\n"
        f"| c_text LONGTEXT length | {LARGE_BYTES} chars | "
        f"{src_ln[0].get('text_len') if src_ln else 'n/a'} | {'✅' if ok_size else '❌'} |\n"
        f"| image magic (JPEG SOI) | starts with FFD8 | "
        f"{snk_ln[0].get('img_magic') if snk_ln else 'n/a'} | {'✅' if ok_magic else '❌'} |\n"
        f"| sample LENGTH match src/sink | equal | {'equal' if ok_len else 'mismatch'} | "
        f"{'✅' if ok_len else '❌'} |\n"
        f"| Summary | | | **{'✅ PASS' if ok_l04 else '❌ FAIL'}** |"
    )
    record(
        TPResult(
            id="TP-L04",
            title="Mega-field sync (image LONGBLOB + LONGTEXT 1–5MiB)",
            action=(
                f"{LARGE_ROWS} rows × {LARGE_BYTES}B JPEG-like c_img + "
                f"{LARGE_BYTES}-char c_text"
            ),
            expected_desc=(
                "Row counts catch up; LENGTH(c_img)/CHAR_LENGTH(c_text) match; "
                "sink image starts with JPEG SOI FFD8"
            ),
            expected_rows=src_ln,
            actual_rows=snk_ln,
            columns=list(src_ln[0].keys()) if src_ln else ["id"],
        ),
        override=ok_l04,
        compare_md=cmp4,
    )

    # TP-L02 peak (orders + typed)
    counter = [0]
    lock = threading.Lock()
    stop_at = time.time() + PEAK_SEC
    log(f"==> peak writers {WORKERS}x{PEAK_SEC}s", flush=True)
    peak_t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futs = [pool.submit(peak_writer, i, stop_at, counter, lock) for i in range(WORKERS)]
        written_each = [f.result() for f in as_completed(futs)]
    peak_elapsed = max(time.time() - peak_t0, 0.001)
    timings["peak_write"] = peak_elapsed
    peak_rows = sum(written_each)
    peak_rps = peak_rows / peak_elapsed
    log(
        f"TIMING  peak_write {peak_elapsed:.1f}s "
        f"({peak_rows} rows ≈ {peak_rps:.0f} rps)",
        flush=True,
    )

    timings["cdc_peak_catchup"] = wait_peak_catchup(timeout=1800)
    log(f"TIMING  cdc_peak_catchup {timings['cdc_peak_catchup']:.1f}s", flush=True)
    time.sleep(3)
    src_n = count_rows(SRC, SRC_DB, "tpl01_t1")
    snk_n = count_rows(SNK, SINK_DB, "tpl01_t1_out")
    lag = abs(src_n - snk_n) / max(src_n, 1)
    src_t = count_rows(SRC, SRC_DB, TYPED_TABLE)
    snk_t = count_rows(SNK, SINK_DB, f"{TYPED_TABLE}_out")
    lag_t = abs(src_t - snk_t) / max(src_t, 1)
    job_alive = proc is not None and proc.poll() is None
    ck = json.loads(CK.read_text()) if CK.exists() else {}
    has_offset = "acked_offset" in ck and ck["acked_offset"]
    ok_l02 = lag <= 0.01 and lag_t <= 0.01 and job_alive and bool(has_offset)
    cmp2 = (
        "| Check | Expected | Actual | Verdict |\n| --- | --- | --- | --- |\n"
        f"| peak writes | >0 | {peak_rows} rows / {peak_elapsed:.1f}s ≈ {peak_rps:.0f} rps | "
        f"{'✅' if peak_rows > 0 else '❌'} |\n"
        f"| tpl01_t1 src/sink delta | <=1% | src={src_n} snk={snk_n} lag={lag:.2%} | "
        f"{'✅' if lag <= 0.01 else '❌'} |\n"
        f"| typed src/sink delta | <=1% | src={src_t} snk={snk_t} lag={lag_t:.2%} | "
        f"{'✅' if lag_t <= 0.01 else '❌'} |\n"
        f"| job still running | yes | {job_alive} | {'✅' if job_alive else '❌'} |\n"
        f"| resume offset recorded | yes | {'yes' if has_offset else 'no'} | "
        f"{'✅' if has_offset else '❌'} |\n"
        f"| Summary | | | **{'✅ PASS' if ok_l02 else '❌ FAIL'}** |"
    )
    record(
        TPResult(
            id="TP-L02",
            title="Peak incremental stress (tpl01_t1 + typed)",
            action=f"{WORKERS} workers x {PEAK_SEC}s concurrent INSERT tpl01_t1+typed",
            expected_desc="After catch-up delta <=1%; job stays up; resume offset recorded",
            expected_rows=[{"ok": "yes"}],
            actual_rows=[{"ok": "yes" if ok_l02 else "no"}],
            columns=["ok"],
            extra_md=f"- workers written: {written_each}\n",
        ),
        override=ok_l02,
        compare_md=cmp2,
    )

    timings["suite_total"] = time.time() - suite_t0
    log("TIMING  summary:", flush=True)
    for name, sec in timings.items():
        log(f"  {name:20s} {sec:8.1f}s", flush=True)
    report = write_report(timings)
    failed = [r for r in results if not r.passed]
    log("REPORT", report, flush=True)
    log("PASS", sum(1 for r in results if r.passed), "FAIL", len(failed), flush=True)
    return 0 if not failed else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        stop_job()
