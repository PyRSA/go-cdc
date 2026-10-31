"""Doris scale suite orchestrator: multi-DB / multi-table bulk + mixed mutations + hotspot stress.

Mirrors ``scale.scale_it`` (MySQL→MySQL) with the Doris sink as target, so the
two profiles can be compared row for row. Differences are forced by Doris v1
semantics, not by preference:

  - DB names are re-prefixed to ``db_tp_dscale_*`` so a Doris run can never
    collide with the MySQL→MySQL scale databases when both execute in CI.
  - Tables whose event group is ``delete`` are remapped to ``insert_update``;
    the Doris sink rejects deletes and ``delete.enabled: true`` fails at parse.
  - Schema templates come from ``scale.doris_schemas`` (no TIME, no BLOB).
  - The content checksum is probed at startup: CRC32 sums are only trusted
    across engines if MySQL and Doris agree, otherwise the suite degrades to a
    character-length sum and says so in the report.

Requires MySQL (mysql-up) as the source and Doris FE/BE (doris-up) as the sink.
"""
from __future__ import annotations

import argparse
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pymysql

from common.config import (
    BINARY,
    CDC_SERVER_ID_DORIS_SCALE,
    CHECKPOINT_DORIS_SCALE_DIR,
    DORIS_HOST,
    DORIS_HTTP_PORT,
    DORIS_PASSWORD,
    DORIS_REPLICATION_NUM,
    DORIS_SQL_PORT,
    DORIS_USER,
    MYSQL_HOST,
    MYSQL_PORT,
    REPORT_DIR,
    ROOT,
    SRC_PASSWORD,
    SRC_USER,
    WORKDIR,
    it_rel_path,
    project_display_path,
    sanitize_report_text,
)
from common.logutil import log
from common.sysstat import format_cpu_mem, format_disk, format_host_res
from .doris_schemas import checksum_sql, create_table_sql, insert_batch
from common.profiles import Profile, TableSpec, hotspot_tables, load_profile

ROOT_USER = os.environ.get("ROOT_USER", "root")
ROOT_PASSWORD = os.environ.get("ROOT_PASSWORD", "rootpass")

# Hotspot knobs. DORIS_SCALE_* wins so the Doris tier can be tuned without
# touching the MySQL→MySQL numbers; otherwise it inherits the SCALE_* values.
SCALE_BURST_ROWS_PER_HOT = int(
    os.environ.get("DORIS_SCALE_BURST_ROWS_PER_HOT", os.environ.get("SCALE_BURST_ROWS_PER_HOT", "50000"))
)
SCALE_PEAK_SECONDS = int(
    os.environ.get("DORIS_SCALE_PEAK_SECONDS", os.environ.get("SCALE_PEAK_SECONDS", "45"))
)
SCALE_PEAK_BATCH = int(
    os.environ.get("DORIS_SCALE_PEAK_BATCH", os.environ.get("SCALE_PEAK_BATCH", "200"))
)
# Shared budget (seconds) spanning hotspot count convergence + final checksum
# verify. One deadline is created at peak-catchup start and reused by both so
# the two phases never re-time independently. Overridable so the CI `heavy` tier
# can wait longer on slower runners; the default is the historical 3600 s.
PEAK_CATCHUP_BUDGET_S = float(
    os.environ.get("DORIS_PEAK_CATCHUP_BUDGET_S", os.environ.get("PEAK_CATCHUP_BUDGET_S", "3600"))
)

SRC_DB_PREFIX = "db_tp_dscale_src"
SNK_DB_PREFIX = "db_tp_dscale_snk"

__all__ = [
    "ROOT_USER",
    "ROOT_PASSWORD",
    "SRC_DB_PREFIX",
    "SNK_DB_PREFIX",
    "doris_profile",
    "ensure_databases",
    "ensure_tables",
    "seed_all",
    "src_checksum",
    "snk_checksum",
    "verify_pair",
    "verify_pair_eventual",
    "wait_hotspot_convergence",
    "render_pipeline_yaml",
    "start_job",
    "stop_job",
    "wait_snapshot",
    "mutate_all",
    "wait_catchup",
    "probe_checksum_mode",
    "write_report",
    "main",
]


# --------------------------------------------------------------------------
# connections
# --------------------------------------------------------------------------
def admin_conn():
    """Privileged MySQL connection (compose root by default)."""
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=ROOT_USER,
        password=ROOT_PASSWORD,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def _user_conn(user: str, password: str, database: str | None = None):
    kwargs: dict = dict(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=user,
        password=password,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    if database:
        kwargs["database"] = database
    return pymysql.connect(**kwargs)


def doris_conn(database: str | None = None):
    kwargs: dict = dict(
        host=DORIS_HOST,
        port=DORIS_SQL_PORT,
        user=DORIS_USER,
        password=DORIS_PASSWORD,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )
    if database:
        kwargs["database"] = database
    return pymysql.connect(**kwargs)


# --------------------------------------------------------------------------
# profile adaptation
# --------------------------------------------------------------------------
def doris_profile(profile: Profile) -> Profile:
    """Re-prefix databases and drop DELETE groups for a Doris run.

    Table count, per-DB table count and row allocation stay untouched, so
    profiles remain directly comparable with the MySQL→MySQL scale report.
    """
    tables: list[TableSpec] = []
    for t in profile.tables:
        group = "insert_update" if t.event_group == "delete" else t.event_group
        tables.append(
            replace(
                t,
                src_db=f"{SRC_DB_PREFIX}_{t.db_index:02d}",
                snk_db=f"{SNK_DB_PREFIX}_{t.db_index:02d}",
                event_group=group,
            )
        )
    return replace(profile, tables=tables)


# --------------------------------------------------------------------------
# bootstrap
# --------------------------------------------------------------------------
def ensure_databases(profile: Profile) -> None:
    """CREATE Doris scale source DBs and GRANT the IT source user.

    Doris sink databases are created by the sink itself (auto-create-table).
    """
    src_dbs = sorted({t.src_db for t in profile.tables})
    with admin_conn() as conn:
        with conn.cursor() as cur:
            for db in src_dbs:
                cur.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{db}` DEFAULT CHARACTER SET utf8mb4"
                )
                cur.execute(f"GRANT ALL PRIVILEGES ON `{db}`.* TO %s@'%%'", (SRC_USER,))
            cur.execute("FLUSH PRIVILEGES")


def _doris_databases() -> set[str]:
    """Existing Doris databases (empty set tolerated on a clean run)."""
    conn = doris_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW DATABASES")
            rows = cur.fetchall()
    finally:
        conn.close()
    key = "Database"
    return {str(r[key] if key in r else next(iter(r.values()))) for r in rows}


def ensure_tables(profile: Profile) -> None:
    """DROP+CREATE source tables; drop any leftover Doris sink tables."""
    src_dbs = sorted({t.src_db for t in profile.tables})
    with admin_conn() as conn:
        with conn.cursor() as cur:
            # Wipe every table in the Doris scale source DBs so leftovers from
            # another profile cannot match this run's broad source.tables regex.
            for db in src_dbs:
                cur.execute(
                    "SELECT table_name AS name FROM information_schema.tables "
                    "WHERE table_schema=%s",
                    (db,),
                )
                for row in cur.fetchall():
                    cur.execute(f"DROP TABLE IF EXISTS `{db}`.`{row['name']}`")
            for t in profile.tables:
                cur.execute(f"USE `{t.src_db}`")
                cur.execute(create_table_sql(t.template, t.name))

    # Best-effort sink wipe; sink databases are created by the sink itself, so a
    # clean run has none of them and DROP on an unknown database is expected noise.
    existing = _doris_databases()
    for t in profile.tables:
        if t.snk_db not in existing:
            continue
        try:
            conn = doris_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS `{t.snk_db}`.`{t.sink_name}`")
            finally:
                conn.close()
        except pymysql.Error as exc:  # noqa: BLE001
            log(f"WARN drop Doris {t.snk_db}.{t.sink_name}: {exc}", flush=True)


def _seed_one_table(spec: TableSpec) -> None:
    conn = _user_conn(SRC_USER, SRC_PASSWORD, spec.src_db)
    try:
        if spec.rows > 0:
            insert_batch(
                conn,
                spec.src_db,
                spec.name,
                spec.template,
                1,
                spec.rows,
                spec.db_index,
                spec.ordinal,
            )
    finally:
        conn.close()


def seed_all(profile: Profile) -> float:
    """Seed all source tables with a worker pool; return elapsed seconds."""
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=profile.workers) as pool:
        future_to_spec = {pool.submit(_seed_one_table, t): t for t in profile.tables}
        for fut in as_completed(future_to_spec):
            spec = future_to_spec[fut]
            try:
                fut.result()
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(f"{spec.src_db}.{spec.name}: {exc}") from exc
    return time.time() - t0


# --------------------------------------------------------------------------
# checksums
# --------------------------------------------------------------------------
CHECKSUM_MODE = "crc32"


def probe_checksum_mode() -> str:
    """Return 'crc32' only if MySQL and Doris compute the same CRC32.

    Anything else (unsupported function, error, mismatch) degrades the suite
    to a character-length sum instead of silently comparing unlike aggregates.
    """
    sql = "SELECT CRC32('go-cdc-scale-probe') AS v"
    try:
        conn = _user_conn(SRC_USER, SRC_PASSWORD)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                src = int(row["v"]) if row and row["v"] is not None else None
        finally:
            conn.close()
        conn = doris_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                snk = int(row["v"]) if row and row["v"] is not None else None
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        log(f"WARN checksum probe failed, degrading to length: {exc}", flush=True)
        return "len"
    if src is None or src != snk:
        log(f"WARN CRC32 differs across engines (mysql={src} doris={snk}); degrading", flush=True)
        return "len"
    return "crc32"


def _checksum(spec: TableSpec, side: str) -> tuple[int, int, int]:
    """Return (cnt, sum_id, content) for one table on src (MySQL) or snk (Doris)."""
    sql = checksum_sql(
        spec.template,
        spec.src_db if side == "src" else spec.snk_db,
        spec.name if side == "src" else spec.sink_name,
        dialect="mysql" if side == "src" else "doris",
        mode=CHECKSUM_MODE,
    )
    conn = (
        _user_conn(SRC_USER, SRC_PASSWORD, spec.src_db)
        if side == "src"
        else doris_conn(spec.snk_db)
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return (0, 0, 0)
    return (int(row["cnt"]), int(row["sum_id"]), int(row["name_crc"]))


def src_checksum(spec: TableSpec) -> tuple[int, int, int]:
    return _checksum(spec, "src")


def snk_checksum(spec: TableSpec) -> tuple[int, int, int]:
    return _checksum(spec, "snk")


def _checksum_diff(
    profile: Profile, phase: str
) -> tuple[list[str], list[str]]:
    """Compare src vs Doris (cnt, sum_id, content) triples for every table.

    Returns (mismatches, fetch_errors); kept separate so the catch-up poller
    can tell a still-draining Doris replica (retryable) apart from a DB / SQL
    fault (not retryable, must not be masked by infinite retry).
    """
    mismatches: list[str] = []
    fetch_errors: list[str] = []
    for t in profile.tables:
        label = f"{t.src_db}.{t.name}"
        try:
            src = src_checksum(t)
            snk = snk_checksum(t)
        except Exception as exc:  # noqa: BLE001
            fetch_errors.append(f"{label}: {phase} fetch error: {exc}")
            continue
        if src != snk:
            mismatches.append(
                f"{label}: {phase} mismatch src(cnt,sum_id,name_crc)={src} "
                f"sink={snk}"
            )
    return mismatches, fetch_errors


def verify_pair(profile: Profile, phase: str) -> list[str]:
    """Compare source vs Doris aggregates; empty list means pass."""
    mismatches, fetch_errors = _checksum_diff(profile, phase)
    return fetch_errors + mismatches


def verify_pair_eventual(
    profile: Profile,
    deadline: float,
    interval: float = 15.0,
    proc: subprocess.Popen | None = None,
    phase: str = "final",
) -> list[str]:
    """Poll the exact (cnt, sum_id, content) verify until it passes or ``deadline``.

    Mirrors ``scale.scale_it.verify_pair_eventual``: count-equal but
    content-lagging Doris tables keep being retried within the peak-catchup
    budget (``deadline`` is shared with wait_hotspot_convergence, not re-timed
    here). Fetch / SQL errors raise immediately instead of retrying, and a
    go-cdc exit raises via _fail_fast; neither is masked as a transient
    mismatch. Deadline reached with mismatches returns the last-seen triples
    so the caller reports FAIL per table. On success, update markers are
    checked once and their failures returned.
    """
    t0 = time.time()
    last_log = 0.0
    last_failures: list[str] = []
    while True:
        _fail_fast(proc, profile, "final verify")
        mismatches, fetch_errors = _checksum_diff(profile, phase)
        if fetch_errors:
            raise RuntimeError(
                "final checksum fetch failed (not retried so a real fault is "
                "never masked as a transient mismatch):\n"
                + "\n".join(fetch_errors)
            )
        now = time.time()
        if now - last_log >= interval or not mismatches or now >= deadline:
            sample = "; ".join(mismatches[:3]) if mismatches else "ok"
            log(
                f"    final verify {now - t0:.0f}s: "
                f"mismatches={len(mismatches)} {sample}",
                flush=True,
            )
            last_log = now
        if not mismatches:
            elapsed = time.time() - t0
            log(f"TIMING  wait [final_verify] done in {elapsed:.1f}s", flush=True)
            return verify_update_markers(profile, phase)
        last_failures = mismatches
        if now >= deadline:
            return last_failures
        time.sleep(interval)


# --------------------------------------------------------------------------
# pipeline lifecycle
# --------------------------------------------------------------------------
def _yaml_path(profile: Profile) -> Path:
    return WORKDIR / f"pipeline.doris-scale-{profile.name}.yaml"


def _log_path(profile_name: str) -> Path:
    return WORKDIR / f"cdc-doris-scale-{profile_name}.log"


def _ckpt_dir(profile: Profile) -> Path:
    return Path(os.path.join(CHECKPOINT_DORIS_SCALE_DIR, profile.name))


def _profile_name_from_yaml(yaml_path: Path) -> str:
    stem = yaml_path.stem
    prefix = "pipeline.doris-scale-"
    if stem.startswith(prefix):
        return stem[len(prefix) :]
    return stem


def ensure_binary() -> Path:
    """Return go-cdc binary path (absolute under WORKDIR); build if missing."""
    bin_path = Path(BINARY)
    if bin_path.is_file():
        return bin_path
    bin_path.parent.mkdir(parents=True, exist_ok=True)
    log(f"==> building go-cdc -> {bin_path}", flush=True)
    subprocess.check_call(
        ["go", "build", "-o", str(bin_path), "./cmd/go-cdc"],
        cwd=str(ROOT),
    )
    return bin_path


def render_pipeline_yaml(profile: Profile, path: Path) -> None:
    """Write a Doris scale pipeline YAML: one route and one transform per table set."""
    src_dbs = sorted({t.src_db for t in profile.tables})
    tables_include = ",".join(f"{db}\\..*" for db in src_dbs)
    ckpt = _ckpt_dir(profile)
    lines: list[str] = [
        "source:",
        "  type: mysql",
        f"  hostname: {MYSQL_HOST}",
        f"  port: {MYSQL_PORT}",
        f"  username: {SRC_USER}",
        f'  password: "{SRC_PASSWORD}"',
        f"  tables: '{tables_include}'",
        f"  server-id: {CDC_SERVER_ID_DORIS_SCALE}",
        "  server-time-zone: Asia/Shanghai",
        "  scan.startup.mode: initial",
        "  scan.incremental.snapshot.chunk.size: 4096",
        "  scan.snapshot.fetch.size: 1024",
        "  schema-change.enabled: false",
        "  connect.timeout: 30s",
        "",
        "sink:",
        "  type: doris",
        f"  hostname: {DORIS_HOST}",
        f"  port: {DORIS_HTTP_PORT}",
        f"  sql-port: {DORIS_SQL_PORT}",
        f"  username: {DORIS_USER}",
        f'  password: "{DORIS_PASSWORD}"',
        "  auto-create-table: true",
        # Without this property the FE applies its own default of 3 replicas, which
        # a single-BE CI cluster rejects at table creation, so pass the env value.
        f"  replication-num: {DORIS_REPLICATION_NUM}",
        "  write-batch-size: 1000",
        "  write-batch-interval: 500ms",
        "  max-retries: 3",
        "  request-timeout: 60s",
        "",
        "route:",
    ]
    for t in profile.tables:
        lines.append(f"  - source-table: '{t.src_db}\\.{t.name}'")
        lines.append(f"    sink-table: {t.snk_db}.{t.sink_name}")
    lines.extend(
        [
            "",
            "transform:",
            # Single regex covers all Doris scale src tables (Go regexp fullMatch).
            r"  - source-table: 'db_tp_dscale_src_\d+\.tp_scale_\d+'",
            "    projection: '*'",
            # Doris auto-create needs the key list to emit a UNIQUE KEY table.
            "    primary-keys: id",
            "",
            "pipeline:",
            f"  name: tp_doris_scale_pipeline_{profile.name}",
            f"  parallelism: {profile.workers}",
            "",
            "checkpoint:",
            "  storage: file",
            "  interval: 3s",
            f"  file-path: {it_rel_path(ckpt)}",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def start_job(yaml_path: Path) -> subprocess.Popen:
    """Start go-cdc; pump stdout/stderr to workdir/cdc-doris-scale-{profile}.log."""
    bin_path = ensure_binary()
    profile_name = _profile_name_from_yaml(yaml_path)
    log_path = _log_path(profile_name)
    log_path.write_text("", encoding="utf-8")
    proc = subprocess.Popen(
        [str(bin_path), str(yaml_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    def pump() -> None:
        assert proc.stdout is not None
        with log_path.open("a", encoding="utf-8") as f:
            for line in proc.stdout:
                f.write(line)
                f.flush()

    threading.Thread(target=pump, daemon=True).start()
    return proc


def stop_job(proc: subprocess.Popen | None) -> None:
    """SIGTERM go-cdc; escalate to kill on timeout."""
    if proc is None or proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def _fail_fast(proc: subprocess.Popen | None, profile: Profile, phase: str) -> None:
    if proc is not None and proc.poll() is not None:
        log_path = _log_path(profile.name)
        tail = log_path.read_text(encoding="utf-8")[-2000:] if log_path.exists() else ""
        raise RuntimeError(f"go-cdc exited early during {phase}: code={proc.returncode}\n{tail}")


# --------------------------------------------------------------------------
# snapshot
# --------------------------------------------------------------------------
def _doris_count(db: str, table: str) -> int | None:
    """Row count, or None when the table is not queryable yet."""
    try:
        conn = doris_conn(db)
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) AS n FROM `{db}`.`{table}`")
                row = cur.fetchone()
        finally:
            conn.close()
    except pymysql.Error:
        return None
    return int(row["n"]) if row else None


def wait_snapshot(
    profile: Profile,
    timeout: float = 3600,
    proc: subprocess.Popen | None = None,
    interval: float = 15.0,
) -> float:
    """Poll until every Doris table answers and COUNT(*) matches planned rows."""
    t0 = time.time()
    deadline = t0 + timeout
    last_err: BaseException | None = None
    last_log = 0.0
    while time.time() < deadline:
        _fail_fast(proc, profile, "snapshot")
        try:
            ready = 0
            rows_src = 0
            rows_snk = 0
            missing = 0
            for t in profile.tables:
                rows_src += t.rows
                n = _doris_count(t.snk_db, t.sink_name)
                if n is None:
                    missing += 1
                    continue
                rows_snk += n
                if n == t.rows:
                    ready += 1
            now = time.time()
            if now - last_log >= interval:
                log(
                    f"    snapshot catch-up {now - t0:.0f}s: "
                    f"tables_ready={ready}/{len(profile.tables)} "
                    f"missing_sink={missing} rows_snk={rows_snk}/{rows_src}",
                    flush=True,
                )
                last_log = now
            if ready == len(profile.tables):
                elapsed = time.time() - t0
                log(f"TIMING  wait [cdc_snapshot] done in {elapsed:.1f}s", flush=True)
                return elapsed
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(interval)
    raise TimeoutError(
        f"timeout waiting for doris scale snapshot after {time.time() - t0:.1f}s: {last_err}"
    )


# --------------------------------------------------------------------------
# mutations
# --------------------------------------------------------------------------
def _mutated_name(db_index: int, ordinal: int, row_id: int) -> str:
    return f"mut-d{db_index:02d}-t{ordinal:03d}-id{row_id}"


def _mutated_status(row_id: int) -> int:
    return (row_id * 3 + 7) % 128


def _marker_column(template: str) -> str:
    if template in ("basic", "large"):
        return "name"
    if template == "indexed":
        return "status"
    if template == "wide":
        return "c_label"
    raise ValueError(f"unknown template: {template!r}")


def _update_rows(conn, spec: TableSpec, id_lo: int, id_hi: int) -> None:
    """Deterministically update the content column for ids in [id_lo, id_hi]."""
    if id_hi < id_lo:
        return
    col = _marker_column(spec.template)
    with conn.cursor() as cur:
        for row_id in range(id_lo, id_hi + 1):
            if col == "status":
                bind: object = _mutated_status(row_id)
            else:
                bind = _mutated_name(spec.db_index, spec.ordinal, row_id)
            cur.execute(
                f"UPDATE `{spec.src_db}`.`{spec.name}` SET `{col}`=%s WHERE `id`=%s",
                (bind, row_id),
            )
    conn.commit()


def _mutate_one_table(spec: TableSpec) -> None:
    """Apply event_group mutations for a single source table (no deletes)."""
    group = spec.event_group
    if group == "idle":
        return
    rows = spec.rows
    conn = _user_conn(SRC_USER, SRC_PASSWORD, spec.src_db)
    try:
        if group == "insert":
            n = max(1, rows // 20)
            insert_batch(conn, spec.src_db, spec.name, spec.template, rows + 1, rows + n,
                         spec.db_index, spec.ordinal)
        elif group == "update":
            id_hi = min(100, rows)
            if id_hi >= 1:
                _update_rows(conn, spec, 1, id_hi)
        elif group == "insert_update":
            ins = max(1, rows // 40)
            insert_batch(conn, spec.src_db, spec.name, spec.template, rows + 1, rows + ins,
                         spec.db_index, spec.ordinal)
            upd_hi = min(50, rows)
            if upd_hi >= 1:
                _update_rows(conn, spec, 1, upd_hi)
        elif group == "delete":
            raise ValueError("Doris scale must remap the delete group before mutating")
        else:
            raise ValueError(f"unknown event_group: {group!r}")
    finally:
        conn.close()


def mutate_all(profile: Profile) -> float:
    """Concurrent per-table mutations by event_group; return elapsed seconds."""
    t0 = time.time()
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=profile.workers) as pool:
        future_to_spec = {pool.submit(_mutate_one_table, t): t for t in profile.tables}
        for fut in as_completed(future_to_spec):
            spec = future_to_spec[fut]
            try:
                fut.result()
            except Exception as exc:  # noqa: BLE001
                msg = f"ERROR doris scale mutate {spec.src_db}.{spec.name}: {exc}"
                log(msg, file=sys.stderr, flush=True)
                errors.append(msg)
    if errors:
        raise RuntimeError(f"mutate_all failed ({len(errors)} tables):\n" + "\n".join(errors))
    return time.time() - t0


def _updated_id_hi(spec: TableSpec) -> int:
    if spec.event_group == "update":
        return min(100, spec.rows)
    if spec.event_group == "insert_update":
        return min(50, spec.rows)
    return 0


def _fetch_marker(side: str, spec: TableSpec, row_id: int):
    col = _marker_column(spec.template)
    db = spec.src_db if side == "src" else spec.snk_db
    table = spec.name if side == "src" else spec.sink_name
    conn = _user_conn(SRC_USER, SRC_PASSWORD, db) if side == "src" else doris_conn(db)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT `{col}` AS v FROM `{db}`.`{table}` WHERE `id`=%s", (row_id,))
            row = cur.fetchone()
    finally:
        conn.close()
    return None if not row else row["v"]


def verify_update_markers(profile: Profile, phase: str) -> list[str]:
    """Ensure update/insert_update mutations reached Doris (counts alone are not enough)."""
    failures: list[str] = []
    for t in profile.tables:
        id_hi = _updated_id_hi(t)
        if id_hi < 1:
            continue
        label = f"{t.src_db}.{t.name}"
        sample_ids = sorted({1, max(1, id_hi // 2), id_hi})
        for row_id in sample_ids:
            try:
                src_v = _fetch_marker("src", t, row_id)
                snk_v = _fetch_marker("snk", t, row_id)
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{label}: {phase} update-marker fetch error id={row_id}: {exc}")
                break
            if str(src_v) != str(snk_v):
                failures.append(
                    f"{label}: {phase} update-marker mismatch id={row_id} "
                    f"src={src_v!r} sink={snk_v!r}"
                )
                break
    return failures


def wait_catchup(
    profile: Profile,
    timeout: float = 3600,
    proc: subprocess.Popen | None = None,
    interval: float = 15.0,
) -> float:
    """Poll until every checksum and update marker matches after mutations."""
    t0 = time.time()
    deadline = t0 + timeout
    last_failures: list[str] = []
    last_log = 0.0
    while time.time() < deadline:
        _fail_fast(proc, profile, "catch-up")
        last_failures = verify_pair(profile, "catchup")
        if not last_failures:
            last_failures = verify_update_markers(profile, "catchup")
        now = time.time()
        if now - last_log >= interval or not last_failures:
            sample = "; ".join(last_failures[:3]) if last_failures else "ok"
            log(
                f"    mutate catch-up {now - t0:.0f}s: mismatches={len(last_failures)} {sample}",
                flush=True,
            )
            last_log = now
        if not last_failures:
            elapsed = time.time() - t0
            log(f"TIMING  wait [cdc_catchup] done in {elapsed:.1f}s", flush=True)
            return elapsed
        time.sleep(interval)
    sample = "\n".join(last_failures[:5])
    raise TimeoutError(
        f"timeout waiting for doris scale catch-up after {time.time() - t0:.1f}s "
        f"({len(last_failures)} mismatches):\n{sample}"
    )


# --------------------------------------------------------------------------
# idle invariants
# --------------------------------------------------------------------------
def _idle_checksums(profile: Profile) -> dict[str, tuple[int, int, int]]:
    return {
        f"{t.src_db}.{t.name}": src_checksum(t)
        for t in profile.tables
        if t.event_group == "idle"
    }


def _verify_idle_unchanged(
    profile: Profile, before: dict[str, tuple[int, int, int]]
) -> list[str]:
    failures: list[str] = []
    for t in profile.tables:
        if t.event_group != "idle":
            continue
        label = f"{t.src_db}.{t.name}"
        after = src_checksum(t)
        expected = before.get(label)
        if expected is None:
            failures.append(f"{label}: idle missing pre-mutation checksum")
        elif after != expected:
            failures.append(
                f"{label}: idle changed after mutations before={expected} after={after}"
            )
    return failures


# --------------------------------------------------------------------------
# hotspot burst / peak
# --------------------------------------------------------------------------
def _table_key(spec: TableSpec) -> str:
    return f"{spec.src_db}.{spec.name}"


def _max_id(spec: TableSpec) -> int:
    conn = _user_conn(SRC_USER, SRC_PASSWORD, spec.src_db)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COALESCE(MAX(`id`), 0) AS m FROM `{spec.src_db}`.`{spec.name}`")
            row = cur.fetchone()
            return int(row["m"]) if row else 0
    finally:
        conn.close()


def _burst_one_table(spec: TableSpec, n_rows: int) -> int:
    if n_rows <= 0:
        return _max_id(spec)
    start = _max_id(spec) + 1
    end = start + n_rows - 1
    conn = _user_conn(SRC_USER, SRC_PASSWORD, spec.src_db)
    try:
        insert_batch(conn, spec.src_db, spec.name, spec.template, start, end,
                     spec.db_index, spec.ordinal)
    finally:
        conn.close()
    return end


def burst_hotspots(
    profile: Profile,
    hotspots: list[TableSpec],
    proc: subprocess.Popen | None = None,
) -> tuple[float, dict[str, int]]:
    """Fixed-row burst into hotspot tables; return (elapsed, next_id_by_key)."""
    n_rows = SCALE_BURST_ROWS_PER_HOT
    log(
        f"==> hotspot burst rows_per_hot={n_rows} tables={len(hotspots)} "
        f"workers={profile.workers}",
        flush=True,
    )
    for h in hotspots:
        log(f"    hotspot {h.src_db}.{h.name} (ordinal={h.ordinal} template={h.template})", flush=True)
    t0 = time.time()
    next_ids: dict[str, int] = {}
    errors: list[str] = []
    _fail_fast(proc, profile, "burst")
    with ThreadPoolExecutor(max_workers=profile.workers) as pool:
        futs = {pool.submit(_burst_one_table, h, n_rows): h for h in hotspots}
        pending = set(futs)
        while pending:
            _fail_fast(proc, profile, "burst")
            done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
            for fut in done:
                h = futs[fut]
                try:
                    high = fut.result()
                    next_ids[_table_key(h)] = high + 1
                    log(f"    burst done {h.src_db}.{h.name} high_id={high}", flush=True)
                except Exception as exc:  # noqa: BLE001
                    msg = f"ERROR burst {h.src_db}.{h.name}: {exc}"
                    log(msg, file=sys.stderr, flush=True)
                    errors.append(msg)
    if errors:
        raise RuntimeError("burst_hotspots failed:\n" + "\n".join(errors))
    return time.time() - t0, next_ids


def _peak_writer(
    worker_id: int,
    hotspots: list[TableSpec],
    next_ids: dict[str, int],
    lock: threading.Lock,
    stop_at: float,
    batch: int,
) -> int:
    written = 0
    rr = worker_id
    conns: dict[str, object] = {}
    try:
        while time.time() < stop_at:
            spec = hotspots[rr % len(hotspots)]
            rr += 1
            key = _table_key(spec)
            with lock:
                start = next_ids[key]
                next_ids[key] = start + batch
            end = start + batch - 1
            conn = conns.get(spec.src_db)
            if conn is None:
                conn = _user_conn(SRC_USER, SRC_PASSWORD, spec.src_db)
                conns[spec.src_db] = conn
            insert_batch(conn, spec.src_db, spec.name, spec.template, start, end,  # type: ignore[arg-type]
                         spec.db_index, spec.ordinal)
            written += batch
    finally:
        for c in conns.values():
            c.close()  # type: ignore[union-attr]
    return written


def peak_write_hotspots(
    profile: Profile,
    hotspots: list[TableSpec],
    next_ids: dict[str, int],
    proc: subprocess.Popen | None = None,
) -> tuple[float, int]:
    """Time-based peak into hotspots; return (elapsed, total_rows_written)."""
    peak_workers = max(1, int(os.environ.get("DORIS_SCALE_PEAK_WORKERS", str(profile.workers))))
    seconds = SCALE_PEAK_SECONDS
    batch = SCALE_PEAK_BATCH
    log(
        f"==> hotspot peak seconds={seconds} workers={peak_workers} batch={batch}",
        flush=True,
    )
    stop_at = time.time() + seconds
    lock = threading.Lock()
    t0 = time.time()
    totals: list[int] = []
    _fail_fast(proc, profile, "peak")
    with ThreadPoolExecutor(max_workers=peak_workers) as pool:
        futs = [
            pool.submit(_peak_writer, i, hotspots, next_ids, lock, stop_at, batch)
            for i in range(peak_workers)
        ]
        pending = set(futs)
        while pending:
            _fail_fast(proc, profile, "peak")
            done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
            for fut in done:
                totals.append(fut.result())
    elapsed = max(time.time() - t0, 0.001)
    rows = sum(totals)
    log(f"TIMING peak_write {elapsed:.1f}s ({rows} rows ≈ {rows / elapsed:.0f} rps)", flush=True)
    return elapsed, rows


def _mysql_count(db: str, table: str) -> int:
    """Source (MySQL) row count."""
    conn = _user_conn(SRC_USER, SRC_PASSWORD, db)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM `{db}`.`{table}`")
            row = cur.fetchone()
    finally:
        conn.close()
    return int(row["n"]) if row else 0


def _hotspot_pair_counts(h: TableSpec) -> tuple[int, int]:
    """Return (src, sink) row counts for one hotspot table (sink not-ready = 0)."""
    return _mysql_count(h.src_db, h.name), _doris_count(h.snk_db, h.sink_name) or 0


def wait_hotspot_convergence(
    profile: Profile,
    hotspots: list[TableSpec],
    deadline: float,
    interval: float = 15.0,
    proc: subprocess.Popen | None = None,
) -> float:
    """Poll until each hotspot src/sink row COUNT matches exactly.

    ``deadline`` is an absolute time.time() budget shared with
    verify_pair_eventual (created once at peak-catchup start). Counts must be
    equal — no percentage tolerance — but content equality is deliberately NOT
    checked here (cheap COUNT only); verify_pair_eventual does the exact
    (cnt, sum_id, content) check afterwards within the remaining budget. The
    delta percent in the progress log is trend display only.
    """
    t0 = time.time()
    last_log = 0.0
    cdc_pid = proc.pid if proc is not None else None
    format_cpu_mem()  # baseline /proc/stat so the first periodic line has cpu=%
    while time.time() < deadline:
        _fail_fast(proc, profile, "peak catch-up")
        ok = True
        lines: list[str] = []
        for h in hotspots:
            src_rows, snk_rows = _hotspot_pair_counts(h)
            delta = abs(src_rows - snk_rows) / max(src_rows, 1)
            lines.append(f"{h.src_db}.{h.name} src={src_rows} snk={snk_rows} delta={delta:.2%}")
            if src_rows != snk_rows:
                ok = False
        now = time.time()
        if now - last_log >= interval or ok:
            log(
                f"    hotspot catch-up {now - t0:.0f}s: "
                + " | ".join(lines)
                + f" | {format_host_res(cdc_pid)}",
                flush=True,
            )
            last_log = now
        if ok:
            elapsed = time.time() - t0
            log(f"TIMING peak_catchup {elapsed:.1f}s", flush=True)
            return elapsed
        time.sleep(interval)
    log(f"    hotspot catch-up TIMEOUT: {format_host_res(cdc_pid)}", flush=True)
    raise TimeoutError(
        f"timeout waiting for hotspot count convergence after {time.time() - t0:.1f}s"
    )


# --------------------------------------------------------------------------
# report
# --------------------------------------------------------------------------
def _report_path(profile: Profile) -> Path:
    return REPORT_DIR / f"mysql-doris-scale-{profile.name}.md"


def write_report(
    profile: Profile,
    timings: dict[str, float],
    failures: list[str],
    path: Path | None = None,
    extra: dict[str, object] | None = None,
) -> Path:
    """Write the Markdown scale report; return the path written."""
    out = path if path is not None else _report_path(profile)
    out.parent.mkdir(parents=True, exist_ok=True)
    passed = len(failures) == 0
    src_dbs = sorted({t.src_db for t in profile.tables})
    snk_dbs = sorted({t.snk_db for t in profile.tables})
    group_counts: dict[str, int] = {}
    for t in profile.tables:
        group_counts[t.event_group] = group_counts.get(t.event_group, 0) + 1

    lines = [
        f"# go-cdc MySQL → Doris scale integration test report ({profile.name})\n",
        f"- Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Result: {'✅ PASS' if passed else '❌ FAIL'}",
        f"- Profile: `{profile.name}`",
        f"- Workers / parallelism: {profile.workers}",
        f"- Tables: {len(profile.tables)}",
        f"- Total rows (planned seed): {profile.total_rows}",
        f"- Source DBs ({len(src_dbs)}): `{', '.join(src_dbs)}`",
        f"- Sink DBs ({len(snk_dbs)}): `{', '.join(snk_dbs)}`",
        f"- Doris FE: `{DORIS_HOST}:{DORIS_HTTP_PORT}` (HTTP) / `{DORIS_SQL_PORT}` (SQL)",
        f"- Content checksum: `{CHECKSUM_MODE}`",
        "",
        "## Doris v1 deviations from the MySQL→MySQL scale suite\n",
        "- `delete` event group: remapped to `insert_update` (Doris sink v1 rejects DELETE).",
        "- Schema templates: `wide` drops `c_time TIME`, `large` uses `payload LONGTEXT` "
        "instead of `binary_data BLOB`.",
        "- Sink databases are created by the sink (`auto-create-table: true`).",
        "",
        "## Event groups\n",
        "| Group | Tables |",
        "| --- | ---: |",
    ]
    if CHECKSUM_MODE == "len":
        lines.append(
            "> NOTE: content check degraded to character-length sums "
            "(MySQL and Doris CRC32 did not agree)."
        )
        lines.append("")
    for g in ("insert", "update", "insert_update", "idle"):
        lines.append(f"| {g} | {group_counts.get(g, 0)} |")
    lines.append("")

    if extra and extra.get("hotspots"):
        peak_workers = int(os.environ.get("DORIS_SCALE_PEAK_WORKERS", str(profile.workers)))
        lines.append("## Hotspot burst / peak\n")
        lines.append(f"- Burst rows/hot table: `{SCALE_BURST_ROWS_PER_HOT}`")
        lines.append(f"- Peak seconds: `{SCALE_PEAK_SECONDS}`")
        lines.append(f"- Peak workers: `{peak_workers}`")
        lines.append(f"- Peak batch: `{SCALE_PEAK_BATCH}`")
        if "peak_rows" in extra:
            lines.append(f"- Peak rows written (measured): `{extra['peak_rows']}`")
        lines.append("- Hotspot tables:")
        for name in extra["hotspots"]:  # type: ignore[union-attr]
            lines.append(f"  - `{name}`")
        lines.append("")

    if timings:
        lines.extend(["## Phase timings\n", "| Phase | Seconds |", "| --- | ---: |"])
        for name, sec in timings.items():
            lines.append(f"| {name} | {sec:.1f} |")
        lines.append("")

    lines.append(f"- Failures: {len(failures)}\n")
    if failures:
        lines.append("## Failures\n")
        for msg in failures:
            lines.append(f"- `{msg}`")
        lines.append("")
    else:
        lines.append("## Summary\n")
        content = (
            "COUNT + SUM(id) + CRC32" if CHECKSUM_MODE == "crc32" else "COUNT + SUM(id) + length"
        )
        summary = (
            f"All {len(profile.tables)} source/sink pairs matched "
            f"({content}) after snapshot and post-mutation catch-up."
        )
        if extra and extra.get("hotspots"):
            summary += " Heavy hotspot burst + peak count convergence also passed."
        lines.append(summary + "\n")

    lines.append("## Tables\n")
    lines.append("| ordinal | src | sink | template | rows | event_group |")
    lines.append("| ---: | --- | --- | --- | ---: | --- |")
    for t in profile.tables:
        lines.append(
            f"| {t.ordinal} | `{t.src_db}.{t.name}` | `{t.snk_db}.{t.sink_name}` | "
            f"{t.template} | {t.rows} | {t.event_group} |"
        )
    lines.append("")

    out.write_text(sanitize_report_text("\n".join(lines)), encoding="utf-8")
    return out


# --------------------------------------------------------------------------
# driver
# --------------------------------------------------------------------------
def _clear_checkpoint(profile: Profile) -> None:
    ckpt = _ckpt_dir(profile)
    if ckpt.exists():
        shutil.rmtree(ckpt)
    ckpt.mkdir(parents=True, exist_ok=True)


def _print_plan(profile: Profile) -> None:
    log(f"profile={profile.name}")
    log(f"tables={len(profile.tables)}")
    log(f"total_rows={profile.total_rows}")
    log(f"workers={profile.workers}")
    log(f"sum_table_rows={sum(t.rows for t in profile.tables)}")
    log(f"source_dbs={len({t.src_db for t in profile.tables})}")
    log(f"sink_dbs={len({t.snk_db for t in profile.tables})}")


def run_profile(profile: Profile, until: str) -> int:
    """Run Doris scale phases through ``until`` (seed|snapshot|mutate|all)."""
    log(
        f"==> doris scale profile={profile.name} tables={len(profile.tables)} "
        f"total_rows={profile.total_rows} workers={profile.workers} until={until}",
        flush=True,
    )
    timings: dict[str, float] = {}
    all_failures: list[str] = []
    report_path = _report_path(profile)

    ensure_databases(profile)
    ensure_tables(profile)

    log("==> seed baseline", flush=True)
    log(f"    {format_disk(WORKDIR)}", flush=True)
    seed_s = seed_all(profile)
    timings["seed_baseline"] = seed_s
    log(f"TIMING seed_baseline {seed_s:.1f}s", flush=True)
    if until == "seed":
        return 0

    yaml_path = _yaml_path(profile)
    _clear_checkpoint(profile)
    render_pipeline_yaml(profile, yaml_path)
    log(f"==> rendered {yaml_path}", flush=True)

    log("==> start go-cdc (snapshot)", flush=True)
    proc = start_job(yaml_path)
    try:
        snap_s = wait_snapshot(profile, timeout=3600, proc=proc)
        timings["cdc_snapshot"] = snap_s
        log(f"TIMING cdc_snapshot {snap_s:.1f}s", flush=True)

        snap_failures = verify_pair(profile, "snapshot")
        if snap_failures:
            all_failures.extend(snap_failures)
            for msg in snap_failures:
                log(f"FAIL {msg}", file=sys.stderr, flush=True)
            write_report(profile, timings, all_failures, report_path)
            log(f"REPORT {project_display_path(report_path)}", flush=True)
            return 1
        log(f"==> snapshot verify ok ({len(profile.tables)} tables)", flush=True)
        if until == "snapshot":
            return 0

        idle_before = _idle_checksums(profile)

        log("==> concurrent mutations", flush=True)
        mut_s = mutate_all(profile)
        timings["mutate"] = mut_s
        log(f"TIMING mutate {mut_s:.1f}s", flush=True)

        idle_failures = _verify_idle_unchanged(profile, idle_before)
        if idle_failures:
            all_failures.extend(idle_failures)
            for msg in idle_failures:
                log(f"FAIL {msg}", file=sys.stderr, flush=True)

        log("==> wait catch-up", flush=True)
        catch_s = wait_catchup(profile, timeout=3600, proc=proc)
        timings["cdc_catchup"] = catch_s
        log(f"TIMING cdc_catchup {catch_s:.1f}s", flush=True)

        hotspot_extra: dict[str, object] = {}
        final_failures: list[str] = []
        if profile.name == "heavy" and until == "all":
            hotspots = hotspot_tables(profile)
            burst_s, next_ids = burst_hotspots(profile, hotspots, proc=proc)
            timings["burst"] = burst_s
            log(f"TIMING burst {burst_s:.1f}s", flush=True)

            peak_s, peak_rows = peak_write_hotspots(profile, hotspots, next_ids, proc=proc)
            timings["peak_write"] = peak_s
            hotspot_extra["peak_rows"] = peak_rows
            hotspot_extra["hotspots"] = [f"{h.src_db}.{h.name}" for h in hotspots]

            # One budget shared by count convergence + exact checksum verify.
            # Source is static now (peak writes have stopped); only CDC drain
            # remains, so both phases count down the same deadline.
            catchup_deadline = time.time() + PEAK_CATCHUP_BUDGET_S
            catch_peak_s = wait_hotspot_convergence(
                profile, hotspots, catchup_deadline, interval=15.0, proc=proc
            )
            timings["peak_catchup"] = catch_peak_s
            fv0 = time.time()
            final_failures = verify_pair_eventual(
                profile, catchup_deadline, interval=15.0, proc=proc
            )
            timings["final_verify"] = time.time() - fv0
        else:
            final_failures = verify_pair(profile, "final")
            final_failures.extend(verify_update_markers(profile, "final"))
        all_failures.extend(final_failures)
        if final_failures:
            for msg in final_failures:
                log(f"FAIL {msg}", file=sys.stderr, flush=True)
        else:
            log(f"==> final verify ok ({len(profile.tables)} tables)", flush=True)

        write_report(profile, timings, all_failures, report_path, extra=hotspot_extra or None)
        log(f"REPORT {project_display_path(report_path)}", flush=True)
        if all_failures:
            log("SUMMARY FAIL", flush=True)
            return 1
        log("SUMMARY PASS", flush=True)
        return 0
    except Exception:
        try:
            if timings and until not in ("seed", "snapshot"):
                write_report(
                    profile,
                    timings,
                    all_failures + ["aborted: see logs / exception"],
                    report_path,
                )
                log(f"REPORT {project_display_path(report_path)}", flush=True)
        except Exception:  # noqa: BLE001
            pass
        raise
    finally:
        stop_job(proc)


def main(argv: list[str] | None = None) -> int:
    global CHECKSUM_MODE
    parser = argparse.ArgumentParser(
        description="go-cdc MySQL → Doris scale integration orchestrator"
    )
    parser.add_argument(
        "--print-plan",
        "--dry-topology",
        dest="print_plan",
        metavar="PROFILE",
        help="print table count / total rows / db split and exit (no DB access)",
    )
    parser.add_argument("profile", nargs="?", default=None, help="profile name (normal|heavy)")
    parser.add_argument(
        "--until",
        choices=("seed", "snapshot", "mutate", "all"),
        default="all",
        help="stop after phase (default: all)",
    )
    parser.add_argument("--total-rows", type=int, default=None, help="override profile total_rows")
    parser.add_argument("--workers", type=int, default=None, help="override profile workers")
    parser.add_argument(
        "--checksum-mode",
        choices=("auto", "crc32", "len"),
        default="auto",
        help="content aggregate; auto probes CRC32 agreement across engines",
    )
    args = parser.parse_args(argv)

    total_rows = args.total_rows
    if total_rows is None and os.environ.get("DORIS_SCALE_TOTAL_ROWS"):
        total_rows = int(os.environ["DORIS_SCALE_TOTAL_ROWS"])
    workers = args.workers
    if workers is None and os.environ.get("DORIS_SCALE_WORKERS"):
        workers = int(os.environ["DORIS_SCALE_WORKERS"])

    if args.print_plan:
        p = doris_profile(load_profile(args.print_plan, total_rows, workers))
        _print_plan(p)
        return 0

    if args.profile:
        if args.checksum_mode == "auto":
            CHECKSUM_MODE = probe_checksum_mode()
        else:
            CHECKSUM_MODE = args.checksum_mode
        log(f"==> content checksum mode: {CHECKSUM_MODE}", flush=True)
        return run_profile(doris_profile(load_profile(args.profile, total_rows, workers)), args.until)

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
