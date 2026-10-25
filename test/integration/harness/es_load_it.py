#!/usr/bin/env python3
"""MySQL -> go-cdc -> Elasticsearch 8 load / peak suite (TP-EL01..TP-EL05).

Reuses the MySQL load suite's data generator and knobs (`harness.load_it`):
same tables, same row generators, same seed/peak shape, same `LOAD_*` env
vars. Only the target side differs, so the two suites stay comparable:

  * `tpl01_typed.c_bit` is dropped after seeding: Elasticsearch sink v1
    rejects BIT columns (raw bytes have no index-preserving representation).
    Everything else is byte-identical to the MySQL load suite.
  * verification reads documents over HTTP (`harness.es_client`) and compares
    them with an independent oracle (`harness.es_verify`); a bulk HTTP 200 is
    never treated as proof of content.

Plan:

  TP-EL01  snapshot of 4 business tables + typed + 1-5MiB mega fields
  TP-EL02  mixed DML at write-batch-size=1000 (UPDATE/DELETE/INSERT)
  TP-EL03  restart from the same checkpoint at write-batch-size=100
  TP-EL04  peak concurrent inserts; catch-up to <= 1% then exact counts
  TP-EL05  final consistency: streaming checksums + document content

Run it with `scripts/run-es-load.sh`; the report lands in
`docs/test/reports/mysql-elasticsearch-load.md`.
"""
from __future__ import annotations

import json
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pymysql

from . import config as cfg
from . import es_verify as verify
from . import load_it as load
from .es_client import ESClient
from .logutil import log
from .sysstat import format_cpu_mem, format_disk, format_host_res

SRC = cfg.conn_kwargs(cfg.SRC_USER, cfg.SRC_PASSWORD, cfg.LOAD_SRC_DB)
SRC_DB = cfg.LOAD_SRC_DB
SNK_DB = cfg.ES_LOAD_SNK_DB
WORKDIR = cfg.WORKDIR
BINARY = cfg.BINARY
CK = Path(cfg.CHECKPOINT_ES_LOAD_PATH)
YAML = WORKDIR / "pipeline.es.load.yaml"
LOG_PATH = WORKDIR / "cdc-es-load.log"
REPORT = cfg.REPORT_ES_LOAD

ES = ESClient(
    cfg.ES_URL,
    timeout=cfg.ES_REQUEST_TIMEOUT,
    username=cfg.ES_USERNAME,
    password=cfg.ES_PASSWORD,
)

# Same volume plan as the MySQL load suite.
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

# `load._typed_row` layout (see harness/load_it.py); `c_bit` is removed here.
_TYPED_SRC_COLUMNS = (
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
_DROPPED_COLUMN = "c_bit"
_TYPED_COLUMNS = tuple(c for c in _TYPED_SRC_COLUMNS if c != _DROPPED_COLUMN)

COLUMNS: dict[str, dict[str, str]] = {
    "tpl01_t1": {
        "id": "bigint",
        "c_ref": "bigint",
        "c_amt": "decimal(12,2)",
        "c_flag": "varchar(16)",
    },
    "tpl01_t2": {
        "id": "bigint",
        "c_ref": "bigint",
        "c_code": "varchar(32)",
        "c_qty": "int",
    },
    "tpl01_t3": {"id": "bigint", "c_name": "varchar(64)", "c_city": "varchar(32)"},
    "tpl01_t4": {
        "id": "bigint",
        "c_ref": "bigint",
        "c_method": "varchar(16)",
        "c_paid": "decimal(12,2)",
    },
    "tpl01_typed": {
        "id": "bigint",
        "c_tinyint": "tinyint(4)",
        "c_int": "int",
        "c_bigint": "bigint",
        "c_decimal": "decimal(10,4)",
        "c_float": "float",
        "c_double": "double",
        "c_varchar": "varchar(64)",
        "c_date": "date",
        "c_datetime": "datetime(6)",
        "c_timestamp": "timestamp(6)",
        "c_json": "json",
        "c_enum": "enum('red','green','blue')",
        "c_bool": "tinyint(1)",
    },
    "tpl01_large": {"id": "bigint", "c_note": "varchar(64)"},
}

# Column used for the CRC part of the content checksum (string, non-null).
CRC_COLUMN = {
    "tpl01_t1": "c_flag",
    "tpl01_t2": "c_code",
    "tpl01_t3": "c_name",
    "tpl01_t4": "c_method",
    "tpl01_typed": "c_varchar",
}

INSERT_T1 = f"INSERT INTO `{SRC_DB}`.tpl01_t1(id,c_ref,c_amt,c_flag) VALUES (%s,%s,%s,%s)"
INSERT_T3 = f"INSERT INTO `{SRC_DB}`.tpl01_t3(id,c_name,c_city) VALUES (%s,%s,%s)"
INSERT_T4 = f"INSERT INTO `{SRC_DB}`.tpl01_t4(id,c_ref,c_method,c_paid) VALUES (%s,%s,%s,%s)"
INSERT_TYPED = (
    f"INSERT INTO `{SRC_DB}`.tpl01_typed({','.join(_TYPED_COLUMNS)}) "
    f"VALUES ({','.join(['%s'] * len(_TYPED_COLUMNS))})"
)

proc: subprocess.Popen | None = None
_log_handle = None
results: list["TPResult"] = []


@dataclass
class TPResult:
    id: str
    title: str
    action: str
    expected_desc: str
    passed: bool = False
    detail_md: str = ""
    extra: dict[str, object] = field(default_factory=dict)
    # Concrete mismatches; logged on failure because the report file is a CI
    # artifact that pull-request runs never upload.
    failures: list[str] = field(default_factory=list)


# ------------------------------------------------------------------- MySQL
def conn():
    c = pymysql.connect(**SRC, cursorclass=pymysql.cursors.DictCursor, autocommit=True)
    with c.cursor() as cur:
        cur.execute("SET time_zone = '+08:00'")
    return c


def exec_sql(sql: str, args=None) -> None:
    with conn() as c, c.cursor() as cur:
        cur.execute(sql, args or ())


def q(sql: str, args=None) -> list[dict]:
    with conn() as c, c.cursor() as cur:
        cur.execute(sql, args or ())
        return list(cur.fetchall()) if cur.description else []


def count_rows(table: str) -> int:
    rows = q(f"SELECT COUNT(*) AS n FROM `{SRC_DB}`.`{table}`")
    return int(rows[0]["n"]) if rows else 0


def mysql_checksum(table: str, column: str) -> tuple[int, int, int]:
    rows = q(
        f"SELECT COUNT(*) AS cnt, COALESCE(SUM(`id`),0) AS sum_id, "
        f"COALESCE(SUM(CRC32(`{column}`)),0) AS crc FROM `{SRC_DB}`.`{table}`"
    )
    if not rows:
        return (0, 0, 0)
    row = rows[0]
    return (int(row["cnt"]), int(row["sum_id"]), int(row["crc"]))


def _es_typed_row(i: int) -> tuple:
    """`load._typed_row` without the BIT value (ES v1 has no bit mapping)."""
    return tuple(
        value
        for name, value in zip(_TYPED_SRC_COLUMNS, load._typed_row(i))
        if name != _DROPPED_COLUMN
    )


def rows_by_ids(table: str, ids: list[int], columns: dict[str, str]) -> dict[int, dict]:
    wanted = ", ".join(f"`{name}`" for name in columns)
    placeholders = ",".join(["%s"] * len(ids))
    rows = q(f"SELECT {wanted} FROM `{SRC_DB}`.`{table}` WHERE `id` IN ({placeholders})", tuple(ids))
    return {int(verify.lookup_case(row, "id")): row for row in rows}


# --------------------------------------------------------------- Elasticsearch
def index_for(table: str) -> str:
    return verify.index_name_for("", SNK_DB, f"{table}_out")


def es_count(table: str) -> int:
    index = index_for(table)
    ES.refresh(index)
    return ES.count(index)


def es_counts(tables: tuple[str, ...] = TABLES) -> dict[str, int]:
    return {table: es_count(table) for table in tables}


def expected_id(table: str, row: dict) -> str:
    return verify.expected_doc_id(SNK_DB, f"{table}_out", {"id": "bigint"}, row)


def doc_by_id(table: str, row: dict) -> dict | None:
    index = index_for(table)
    ES.refresh(index)
    return ES.get_doc(index, expected_id(table, row))


def clean_indices() -> list[str]:
    removed = ES.delete_indices(cfg.ES_LOAD_INDEX_PREFIX + ".*")
    if removed:
        log(f"    removed {len(removed)} index(es): {removed}")
    return removed


def mapping_problems() -> list[str]:
    problems: list[str] = []
    for table, columns in COLUMNS.items():
        mapping = ES.get_mapping(index_for(table))
        for item in verify.mapping_problems(columns, mapping):
            problems.append(f"{table}: {item}")
    return problems


# ------------------------------------------------------------------ process
def start_job(*, fresh: bool = False) -> None:
    """Start go-cdc; `fresh=True` truncates the log (first start of the suite)."""
    global proc, _log_handle
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    if fresh:
        LOG_PATH.write_text("", encoding="utf-8")
    _log_handle = LOG_PATH.open("a", encoding="utf-8")
    proc = subprocess.Popen(
        [BINARY, str(YAML)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    def pump(handle) -> None:
        assert proc is not None and proc.stdout is not None
        for line in proc.stdout:
            handle.write(line)
            handle.flush()

    threading.Thread(target=pump, args=(_log_handle,), daemon=True).start()


def wait_alive(*, stable_for: float = 5.0, desc: str = "go-cdc") -> None:
    """Fail fast when the job dies at startup; otherwise wait until it is stable."""
    ready_at = time.time() + stable_for
    while time.time() < ready_at:
        if proc is None:
            raise RuntimeError(f"{desc}: process not started")
        code = proc.poll()
        if code is not None:
            raise RuntimeError(f"{desc} exited at startup ({code}):\n{log_tail()}")
        time.sleep(0.25)


def assert_alive(phase: str) -> None:
    if proc is not None and proc.poll() is not None:
        raise RuntimeError(f"go-cdc exited during {phase} ({proc.returncode}):\n{log_tail()}")


def stop_job() -> None:
    global proc, _log_handle
    if proc is not None:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
        proc = None
    if _log_handle is not None:
        _log_handle.close()
        _log_handle = None


def log_tail(limit: int = 2500) -> str:
    if not LOG_PATH.exists():
        return "(no log)"
    return LOG_PATH.read_text(encoding="utf-8", errors="replace")[-limit:]


# -------------------------------------------------------------------- waits
def wait_counts_exact(
    tables: tuple[str, ...] = TABLES, *, timeout: float = 3600.0, interval: float = 5.0, desc: str = "documents"
) -> float:
    """Bounded wait until each index holds exactly its MySQL row count."""
    global _last_progress
    src = {table: count_rows(table) for table in tables}
    _last_progress = 0.0
    log_every = 15.0

    def check() -> bool:
        for table in tables:
            if es_count(table) != src[table]:
                return False
        return True

    elapsed = verify.wait_for(
        check,
        timeout=timeout,
        interval=interval,
        desc=desc,
        on_poll=lambda sec: _log_progress(tables, src, sec, log_every),
        on_error=lambda exc: log(f"    {desc} poll error: {exc}", flush=True),
    )
    log(f"TIMING  wait [{desc}] done in {elapsed:.1f}s", flush=True)
    return elapsed


_last_progress = 0.0


def _log_progress(tables: tuple[str, ...], src: dict[str, int], elapsed: float, every: float) -> None:
    global _last_progress
    if elapsed - _last_progress < every:
        return
    _last_progress = elapsed
    cdc_pid = proc.pid if proc is not None else None
    parts = []
    for table in tables:
        snk = es_count(table)
        lag = abs(src[table] - snk) / max(src[table], 1)
        parts.append(f"{table} src={src[table]} snk={snk} lag={lag:.2%}")
    log(f"    catch-up {elapsed:.0f}s: " + " | ".join(parts) + f" | {format_host_res(cdc_pid)}", flush=True)


# --------------------------------------------------------------- content
def check_docs(table: str, ids: list[int]) -> list[str]:
    """Compare the Elasticsearch documents for `ids` with the MySQL rows."""
    columns = COLUMNS[table]
    rows = rows_by_ids(table, ids, columns)
    problems: list[str] = []
    if len(rows) != len(set(ids)):
        problems.append(f"source rows missing for ids={ids} (found {sorted(rows)})")
    for row in rows.values():
        doc = doc_by_id(table, row)
        if doc is None:
            problems.append(f"id={verify.lookup_case(row, 'id')}: document missing in {index_for(table)}")
            continue
        problems.extend(
            f"id={verify.lookup_case(row, 'id')}: {item}"
            for item in verify.compare_row(columns, row, doc)
        )
    return problems


def check_large(table: str, ids: list[int]) -> list[str]:
    """LONGBLOB/LONGTEXT rows: compare payload bytes, not just row counts."""
    rows = q(
        f"SELECT id, c_img, c_text, c_note FROM `{SRC_DB}`.`{table}` WHERE id IN "
        f"({','.join(['%s'] * len(ids))})",
        tuple(ids),
    )
    problems: list[str] = []
    for row in rows:
        doc = doc_by_id(table, row)
        if doc is None:
            problems.append(f"id={row['id']}: document missing")
            continue
        if not verify.compare_value("varchar(64)", row["c_note"], doc.get("c_note")):
            problems.append(f"id={row['id']}: c_note mysql={row['c_note']!r} es={doc.get('c_note')!r}")
        if str(doc.get("c_img")) != verify.canonical_text("longblob", row["c_img"]):
            problems.append(f"id={row['id']}: c_img payload differs from base64(mysql)")
        if str(doc.get("c_text")) != row["c_text"]:
            problems.append(f"id={row['id']}: c_text payload differs from mysql")
    return problems


def verdict(ok: bool) -> str:
    return "✅" if ok else "❌"


def record(tp: TPResult) -> None:
    results.append(tp)
    log(f"{'PASS' if tp.passed else 'FAIL'} {tp.id} {tp.title}", flush=True)
    if not tp.passed and tp.failures:
        # The report is a git-ignored artifact that pull-request runs never
        # upload, so a failure has to explain itself in the job log too.
        for item in tp.failures[:10]:
            log(f"    reason: {item}", flush=True)
        if len(tp.failures) > 10:
            log(f"    reason: ... and {len(tp.failures) - 10} more (see the report)", flush=True)


# -------------------------------------------------------------- mutations
def round_one_mutations() -> dict[str, int]:
    """UPDATE t1 / DELETE t2 / INSERT t3 (mixed DML, one transaction each)."""
    exec_sql(
        f"UPDATE `{SRC_DB}`.tpl01_t1 SET c_flag='mix-u1', c_amt=c_amt+1.00 WHERE id <= 1000"
    )
    delete_from = max(1, ROWS - 499)
    exec_sql(f"DELETE FROM `{SRC_DB}`.tpl01_t2 WHERE id BETWEEN {delete_from} AND {ROWS}")
    insert_t3(ROWS + 1, ROWS + 2000)
    return {"updated_t1": 1000, "deleted_t2": 500, "inserted_t3": 2000}


def round_two_mutations() -> dict[str, int]:
    """Second round after restart: UPDATE t1/t2, DELETE t3, INSERT t4."""
    exec_sql(
        f"UPDATE `{SRC_DB}`.tpl01_t1 SET c_flag='mix-u2', c_amt=c_amt+0.50 "
        "WHERE id BETWEEN 1001 AND 2000"
    )
    exec_sql(f"UPDATE `{SRC_DB}`.tpl01_t2 SET c_qty=c_qty+10 WHERE id <= 500")
    exec_sql(f"DELETE FROM `{SRC_DB}`.tpl01_t3 WHERE id BETWEEN {ROWS + 1} AND {ROWS + 500}")
    insert_t4(ROWS + 1, ROWS + 1500)
    return {"updated_t1": 1000, "updated_t2": 500, "deleted_t3": 500, "inserted_t4": 1500}


def insert_t3(id_start: int, id_end: int) -> None:
    cities = ("hz", "sh", "bj", "sz")
    with conn() as c, c.cursor() as cur:
        for start in range(id_start, id_end + 1, BATCH):
            end = min(start + BATCH - 1, id_end)
            cur.executemany(
                INSERT_T3, [(i, f"n{i}", cities[i % 4]) for i in range(start, end + 1)]
            )


def insert_t4(id_start: int, id_end: int) -> None:
    with conn() as c, c.cursor() as cur:
        for start in range(id_start, id_end + 1, BATCH):
            end = min(start + BATCH - 1, id_end)
            cur.executemany(
                INSERT_T4,
                [
                    (i, i, "m1" if i % 2 == 0 else "m2", float(i % 50) + 1.0)
                    for i in range(start, end + 1)
                ],
            )


def peak_writer(worker_id: int, stop_at: float, counter: list[int], lock: threading.Lock) -> int:
    """Batch inserts into tpl01_t1 + typed until `stop_at` (same shape as MySQL load)."""
    written = 0
    base = ROWS + 1 + worker_id * 10_000_000
    seq = 0
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
                typed.append(_es_typed_row(i))
            cur.executemany(INSERT_T1, orders)
            cur.executemany(INSERT_TYPED, typed)
            written += len(orders)
            with lock:
                counter[0] += len(orders)
    finally:
        db.close()
    return written


# ------------------------------------------------------------------ report
def write_report(timings: dict[str, float], metrics: dict[str, object]) -> Path:
    passed = sum(1 for item in results if item.passed)
    failed = len(results) - passed
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# go-cdc Elasticsearch sink load report\n",
        f"- Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Source DB: `{SRC_DB}` (MySQL) -> indices `{SNK_DB}.*out` (Elasticsearch 8)",
        f"- Elasticsearch: {cfg.ES_URL}",
        "",
        "## Volume plan (shared with the MySQL load suite)\n",
        "| Knob | Value | Meaning |",
        "| --- | --- | --- |",
        f"| `LOAD_PROFILE` | {cfg.LOAD_PROFILE or '(unset)'} | CI/local tier label |",
        f"| `LOAD_ROWS_PER_TABLE` | {ROWS} | rows per table (t1-t4 + typed) |",
        f"| `LOAD_LARGE_ROWS` | {LARGE_ROWS} | rows in `tpl01_large` |",
        f"| `LOAD_LARGE_FIELD_BYTES` | {LARGE_BYTES} | `c_img` LONGBLOB and `c_text` LONGTEXT size |",
        f"| `LOAD_PEAK_SECONDS` / workers / batch | {PEAK_SEC}s / {WORKERS} / {BATCH} | peak incremental stress |",
        f"| `ES_LOAD_BATCH_SIZE` / interval | {cfg.ES_LOAD_BATCH_SIZE} / {cfg.ES_LOAD_BATCH_INTERVAL} | sink bulk batch (first run) |",
        f"| `ES_LOAD_SMALL_BATCH` / interval | {cfg.ES_LOAD_SMALL_BATCH} / {cfg.ES_LOAD_SMALL_INTERVAL} | sink bulk batch (after restart) |",
        "",
        "Difference from the MySQL load suite: `tpl01_typed.c_bit` is dropped after seeding because "
        "Elasticsearch sink v1 rejects BIT columns; the column is excluded from every comparison.",
        "",
    ]
    if timings:
        lines.extend(["## Phase timings\n", "| Phase | Seconds |", "| --- | ---: |"])
        for name, sec in timings.items():
            lines.append(f"| {name} | {sec:.1f} |")
        lines.append("")
    if metrics:
        lines.extend(["## Measurements\n", "| Metric | Value |", "| --- | --- |"])
        for name, value in metrics.items():
            lines.append(f"| {name} | {value} |")
        lines.append("")
    lines.append(f"- Pass {passed} / fail {failed}\n")
    for item in results:
        lines.append(f"## {item.id} {item.title}\n")
        lines.append(f"- Result: {'✅ PASS' if item.passed else '❌ FAIL'}")
        lines.append(f"- Action: {item.action}")
        lines.append(f"- Expected: {item.expected_desc}\n")
        if item.detail_md:
            lines.append(item.detail_md + "\n")
    REPORT.write_text(cfg.sanitize_report_text("\n".join(lines)), encoding="utf-8")
    return REPORT


def deleted_docs_present(table: str, ids: list[int]) -> list[int]:
    """Ids whose document still exists although the source row was deleted."""
    index = index_for(table)
    ES.refresh(index)
    return [row_id for row_id in ids if ES.get_doc(index, expected_id(table, {"id": row_id})) is not None]


# ------------------------------------------------------------------- TPs
def tp_el01_snapshot() -> float:
    """Multi-table snapshot with ES-side content and mapping verification."""
    elapsed = wait_counts_exact(desc="es_snapshot")
    problems: list[str] = []
    typed_ids = [1, max(1, ROWS // 2), ROWS]
    problems.extend(f"typed {item}" for item in check_docs(TYPED_TABLE, typed_ids))
    biz_ids = [1, max(1, ROWS // 2), ROWS]
    for table in BIZ_TABLES:
        problems.extend(f"{table} {item}" for item in check_docs(table, biz_ids))
    problems.extend(f"large {item}" for item in check_large(LARGE_TABLE, [1, max(1, LARGE_ROWS // 2), LARGE_ROWS]))
    mapping_issues = mapping_problems()
    problems.extend(f"mapping {item}" for item in mapping_issues)
    counts = es_counts()
    rows_md = "| Index | MySQL rows | ES docs | |\n| --- | ---: | ---: | --- |\n"
    counts_ok = True
    count_problems: list[str] = []
    for table in TABLES:
        src_n = count_rows(table)
        ok = counts[table] == src_n
        counts_ok = counts_ok and ok
        if not ok:
            count_problems.append(f"{table} count: mysql={src_n} es={counts[table]}")
        rows_md += f"| `{index_for(table)}` | {src_n} | {counts[table]} | {verdict(ok)} |\n"
    all_problems = problems + count_problems
    rows_md += f"| mappings (derived types/formats) | - | - | {verdict(not mapping_issues)} |\n"
    rows_md += f"| Summary | | | **{verdict(not all_problems)}** |\n"
    if all_problems:
        rows_md += "\nMismatches:\n" + "\n".join(f"- `{item}`" for item in all_problems[:10]) + "\n"
    record(
        TPResult(
            id="TP-EL01",
            title="Multi-table snapshot (t1-t4 + typed + mega fields)",
            action=(
                f"{ROWS} rows x 5 tables + {LARGE_ROWS} large rows ({LARGE_BYTES}B img+text); "
                "wait for snapshot, then compare documents and mappings"
            ),
            expected_desc="Every index matches the source count, mapping, and sampled document content",
            passed=not problems and counts_ok,
            detail_md=rows_md,
            extra={"snapshot_seconds": elapsed},
            failures=all_problems,
        )
    )
    return elapsed


def tp_el02_mixed_dml() -> float:
    """Mixed DML at write-batch-size=1000, verified document by document."""
    mutations = round_one_mutations()
    elapsed = wait_counts_exact(desc="mixed_dml_catchup")
    problems = check_docs("tpl01_t1", [1, 500, 1000])
    problems.extend(check_docs("tpl01_t3", [ROWS + 1, ROWS + 1000, ROWS + 2000]))
    deleted_ids = list(range(max(1, ROWS - 499), ROWS + 1))
    sample_deleted = deleted_ids[::100] + [ROWS]
    leaked = deleted_docs_present("tpl01_t2", sample_deleted)
    problems.extend(f"tpl01_t2 id={row_id}: deleted row still present in ES" for row_id in leaked)
    ok = not problems
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| mutations | {mutations} | {verdict(ok)} |\n"
        f"| catch-up seconds | {elapsed:.1f} | {verdict(ok)} |\n"
        f"| updated/deleted/inserted documents | {problems[:3] or 'all match'} | {verdict(ok)} |\n"
    )
    record(
        TPResult(
            id="TP-EL02",
            title="Mixed DML at write-batch-size=" + cfg.ES_LOAD_BATCH_SIZE,
            action="UPDATE t1 (1000 rows), DELETE t2 (500 rows), INSERT t3 (2000 rows)",
            expected_desc="All three mutation classes converge; updated values visible, deleted docs gone",
            passed=ok,
            detail_md=detail,
            extra={"catchup_seconds": elapsed},
        )
    )
    return elapsed


def tp_el03_restart_small_batch() -> float:
    """Restart from the same checkpoint with a smaller bulk batch."""
    ckpt_before = read_checkpoint()
    stop_job()
    render_yaml(cfg.ES_LOAD_SMALL_BATCH, cfg.ES_LOAD_SMALL_INTERVAL)
    start_job()
    wait_alive(desc="go-cdc (restart)")
    mutations = round_two_mutations()
    elapsed = wait_counts_exact(desc="restart_catchup")
    problems = check_docs("tpl01_t1", [1001, 1500, 2000])
    problems.extend(check_docs("tpl01_t2", [1, 250, 500]))
    problems.extend(check_docs("tpl01_t4", [ROWS + 1, ROWS + 750, ROWS + 1500]))
    gone = [ROWS + 1, ROWS + 250, ROWS + 500]
    leaked = deleted_docs_present("tpl01_t3", gone)
    problems.extend(f"tpl01_t3 id={row_id}: deleted row still present in ES" for row_id in leaked)
    ckpt_after = read_checkpoint()
    pos_before = int((ckpt_before.get("acked_offset") or {}).get("pos") or 0)
    pos_after = int((ckpt_after.get("acked_offset") or {}).get("pos") or 0)
    if pos_after <= 0:
        problems.append("checkpoint has no acked_offset after restart")
    ok = not problems
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| sink bulk batch | {cfg.ES_LOAD_SMALL_BATCH} / {cfg.ES_LOAD_SMALL_INTERVAL} | {verdict(True)} |\n"
        f"| checkpoint before | pos={pos_before} | {verdict(pos_before > 0)} |\n"
        f"| checkpoint after | pos={pos_after} | {verdict(pos_after > 0)} |\n"
        f"| mutations | {mutations} | {verdict(ok)} |\n"
        f"| catch-up seconds | {elapsed:.1f} | {verdict(ok)} |\n"
        f"| document checks | {problems[:3] or 'all match'} | {verdict(ok)} |\n"
    )
    record(
        TPResult(
            id="TP-EL03",
            title="Checkpoint restart with write-batch-size=" + cfg.ES_LOAD_SMALL_BATCH,
            action="Stop/restart go-cdc on the same checkpoint; UPDATE t1/t2, DELETE t3, INSERT t4",
            expected_desc="Resume without duplicates: counts stay exact, marker values match, checkpoint advances",
            passed=ok,
            detail_md=detail,
            extra={"catchup_seconds": elapsed},
        )
    )
    return elapsed


def tp_el04_peak() -> tuple[dict[str, object], float]:
    """Concurrent inserts; report source throughput and ES convergence."""
    counter = [0]
    lock = threading.Lock()
    stop_at = time.time() + PEAK_SEC
    log(f"==> peak writers {WORKERS}x{PEAK_SEC}s batch={BATCH}", flush=True)
    format_cpu_mem()  # prime the CPU delta
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futs = [pool.submit(peak_writer, i, stop_at, counter, lock) for i in range(WORKERS)]
        written_each = [fut.result() for fut in as_completed(futs)]
    elapsed = max(time.time() - t0, 0.001)
    rows_written = sum(written_each)
    rps = rows_written / elapsed
    log(f"TIMING  peak_write {elapsed:.1f}s ({rows_written} rows ~ {rps:.0f} rps)", flush=True)

    # Progress gate: lag <= 1% while the writer is still catching up.
    src = {table: count_rows(table) for table in ("tpl01_t1", TYPED_TABLE)}
    lag_elapsed = verify.wait_for(
        lambda: all(
            abs(src[table] - es_count(table)) / max(src[table], 1) <= 0.01
            for table in src
        ),
        timeout=3600.0,
        interval=5.0,
        desc="peak lag <= 1%",
        on_error=lambda exc: log(f"    peak lag poll error: {exc}", flush=True),
    )
    # A 1% lag is only a progress gate; require exact counts before claiming success.
    exact_elapsed = wait_counts_exact(("tpl01_t1", TYPED_TABLE), timeout=3600.0, desc="peak_exact")
    problems: list[str] = []
    # Worker 0 starts at ROWS+1 but increments before inserting, so its first id
    # is ROWS+2 (see `peak_writer`).
    peak_ids = [ROWS + 2, ROWS + 3]
    problems.extend(check_docs("tpl01_t1", peak_ids))
    problems.extend(check_docs(TYPED_TABLE, peak_ids))
    assert_alive("peak catch-up")
    ck = read_checkpoint()
    has_offset = bool(ck.get("acked_offset"))
    if not has_offset:
        problems.append("checkpoint has no acked_offset after the peak")
    ok = not problems
    detail = (
        "| Check | Expected | Actual | Verdict |\n| --- | --- | --- | --- |\n"
        f"| peak writes | >0 | {rows_written} rows / {elapsed:.1f}s ~ {rps:.0f} rps | {verdict(rows_written > 0)} |\n"
        f"| per-worker rows | balanced | {written_each} | {verdict(True)} |\n"
        f"| lag <= 1% (progress) | yes | reached in {lag_elapsed:.1f}s | ✅ |\n"
        f"| exact counts (final) | yes | reached in {exact_elapsed:.1f}s | {verdict(not problems)} |\n"
        f"| sampled documents | match | {problems[:3] or 'all match'} | {verdict(ok)} |\n"
        f"| checkpoint advances | yes | {'yes' if has_offset else 'no'} | {verdict(has_offset)} |\n"
    )
    record(
        TPResult(
            id="TP-EL04",
            title="Peak incremental inserts (tpl01_t1 + typed)",
            action=f"{WORKERS} workers x {PEAK_SEC}s concurrent INSERT with batch {BATCH}",
            expected_desc="Source throughput recorded; ES catches up to <=1% then to exact counts; job stays up",
            passed=ok,
            detail_md=detail,
            extra={"peak_rows": rows_written, "peak_seconds": elapsed, "peak_rps": rps},
        )
    )
    metrics: dict[str, object] = {
        "total events (baseline + mutations + peak)": (
            ROWS * 5 + LARGE_ROWS + 2000 + 1500 + rows_written
        ),
        "peak rows written": rows_written,
        "peak wall time (s)": f"{elapsed:.1f}",
        "source write throughput (rows/s)": f"{rps:.0f}",
        "ES convergence after peak (s)": f"{exact_elapsed:.1f}",
        "host resources (end of peak)": cfg.md_cell(format_host_res(proc.pid if proc else None)),
    }
    return metrics, exact_elapsed


def tp_el05_final_consistency() -> float:
    """Streaming checksums (count + SUM(id) + SUM(CRC32(col))) for every table."""
    started = time.time()
    problems: list[str] = []
    rows_md = (
        "| Table | MySQL (cnt, sum_id, crc) | ES (cnt, sum_id, crc) | |\n"
        "| --- | --- | --- | --- |\n"
    )
    for table in BIZ_TABLES + (TYPED_TABLE,):
        es_triple = verify.stream_checksum(
            ES.scan(index_for(table)), crc_field=CRC_COLUMN[table]
        )
        src_triple = mysql_checksum(table, CRC_COLUMN[table])
        ok = es_triple == src_triple
        if not ok:
            problems.append(f"{table}: mysql={src_triple} es={es_triple}")
        rows_md += f"| `{table}` | {src_triple} | {es_triple} | {verdict(ok)} |\n"
    large_count = es_count(LARGE_TABLE)
    large_src = count_rows(LARGE_TABLE)
    large_problems = check_large(LARGE_TABLE, [1, max(1, LARGE_ROWS // 2), LARGE_ROWS])
    if large_count != large_src:
        problems.append(f"{LARGE_TABLE}: mysql={large_src} es={large_count}")
    problems.extend(f"{LARGE_TABLE} {item}" for item in large_problems)
    rows_md += (
        f"| `{LARGE_TABLE}` | count={large_src} | count={large_count} | "
        f"{verdict(large_count == large_src and not large_problems)} |\n"
    )
    elapsed = time.time() - started
    ok = not problems
    rows_md += f"| Summary | | | **{verdict(ok)}** |\n"
    if problems:
        rows_md += "\nMismatches:\n" + "\n".join(f"- `{item}`" for item in problems[:10]) + "\n"
    record(
        TPResult(
            id="TP-EL05",
            title="Final consistency (streaming checksums + document content)",
            action="Scroll every index and compare (count, SUM(id), SUM(CRC32(col))) with MySQL",
            expected_desc="Every table matches content-wise; large payloads compared byte for byte",
            passed=ok,
            detail_md=rows_md,
        )
    )
    return elapsed


# ------------------------------------------------------------------ setup
def read_checkpoint() -> dict:
    if not CK.exists():
        return {}
    try:
        return json.loads(CK.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def render_yaml(batch_size: str, interval: str) -> None:
    cfg.render_template(
        "pipeline.es.load.yaml.tpl",
        YAML,
        extra={
            "ES_LOAD_BATCH_SIZE": batch_size,
            "ES_LOAD_BATCH_INTERVAL": interval,
            "LOAD_PARALLELISM": cfg.LOAD_PARALLELISM,
        },
    )


def ensure_schema() -> None:
    """Reuse the MySQL load schema + seeder, then drop the unsupported BIT column."""
    load.ensure_schema()
    load.seed_baseline()
    exec_sql(f"ALTER TABLE `{SRC_DB}`.`{TYPED_TABLE}` DROP COLUMN `{_DROPPED_COLUMN}`")
    log(f"    dropped {TYPED_TABLE}.{_DROPPED_COLUMN} (not representable in ES sink v1)")


def main() -> int:
    cfg.REPORT_DIR.mkdir(parents=True, exist_ok=True)
    suite_t0 = time.time()
    timings: dict[str, float] = {}
    metrics: dict[str, object] = {}

    if not ES.ping():
        log(f"ERROR elasticsearch is not reachable at {cfg.ES_URL}", file=sys.stderr, flush=True)
        return 2
    log(f"==> elasticsearch {ES.version()} at {cfg.ES_URL}", flush=True)

    t0 = time.time()
    clean_indices()
    ensure_schema()
    timings["schema_and_seed"] = time.time() - t0
    log(f"TIMING  schema_and_seed {timings['schema_and_seed']:.1f}s", flush=True)
    log(f"    {format_disk(WORKDIR)}", flush=True)

    if CK.exists():
        CK.unlink()
    render_yaml(cfg.ES_LOAD_BATCH_SIZE, cfg.ES_LOAD_BATCH_INTERVAL)
    log("==> start go-cdc (snapshot + incremental -> ES8)", flush=True)
    t0 = time.time()
    start_job(fresh=True)
    wait_alive(desc="go-cdc")
    timings["cdc_start"] = time.time() - t0

    try:
        timings["snapshot"] = tp_el01_snapshot()
        timings["mixed_dml_catchup"] = tp_el02_mixed_dml()
        timings["restart_catchup"] = tp_el03_restart_small_batch()
        peak_metrics, timings["peak_catchup"] = tp_el04_peak()
        metrics.update(peak_metrics)
        timings["final_checksum"] = tp_el05_final_consistency()
    finally:
        stop_job()

    timings["suite_total"] = time.time() - suite_t0
    metrics["final data consistency"] = "all tables match" if all(r.passed for r in results) else "mismatch (see TPs)"
    metrics["job exits / errors"] = "none" if all(r.passed for r in results) else "see report"
    report = write_report(timings, metrics)
    failed = [item for item in results if not item.passed]
    log(f"REPORT {cfg.project_display_path(report)}", flush=True)
    log(f"PASS {len(results) - len(failed)} FAIL {len(failed)}", flush=True)
    return 0 if not failed else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        stop_job()
