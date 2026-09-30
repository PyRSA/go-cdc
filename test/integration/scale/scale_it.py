"""Scale suite orchestrator: bootstrap, seed, CDC lifecycle, mutate, verify, report."""
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
from datetime import datetime
from pathlib import Path

import pymysql

from harness.config import (
    BINARY,
    MYSQL_HOST,
    MYSQL_PORT,
    REPORT_DIR,
    ROOT,
    SINK_PASSWORD,
    SINK_USER,
    SRC_PASSWORD,
    SRC_USER,
    WORKDIR,
)
from scale.profiles import Profile, TableSpec, hotspot_tables, load_profile
from scale.schemas import checksum_sql, create_table_sql, insert_batch
from harness.logutil import log

ROOT_USER = os.environ.get("ROOT_USER", "root")
ROOT_PASSWORD = os.environ.get("ROOT_PASSWORD", "rootpass")
CDC_SERVER_ID_SCALE = os.environ.get("CDC_SERVER_ID_SCALE", "5616")

# Heavy-only hotspot burst / peak knobs (env-overridable).
SCALE_BURST_ROWS_PER_HOT = int(os.environ.get("SCALE_BURST_ROWS_PER_HOT", "50000"))
SCALE_PEAK_SECONDS = int(os.environ.get("SCALE_PEAK_SECONDS", "45"))
SCALE_PEAK_BATCH = int(os.environ.get("SCALE_PEAK_BATCH", "200"))

# Re-export for callers / future tasks.
__all__ = [
    "ROOT_USER",
    "ROOT_PASSWORD",
    "BINARY",
    "WORKDIR",
    "CDC_SERVER_ID_SCALE",
    "admin_conn",
    "ensure_databases",
    "ensure_tables",
    "seed_all",
    "fetch_checksum",
    "verify_pair",
    "render_pipeline_yaml",
    "start_job",
    "stop_job",
    "wait_snapshot",
    "mutate_all",
    "wait_catchup",
    "write_report",
    "ensure_binary",
    "main",
]


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


def ensure_databases(profile: Profile) -> None:
    """CREATE scale src/snk databases and GRANT to IT source/sink users."""
    src_dbs = sorted({t.src_db for t in profile.tables})
    snk_dbs = sorted({t.snk_db for t in profile.tables})
    with admin_conn() as conn:
        with conn.cursor() as cur:
            for db in src_dbs:
                cur.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{db}` DEFAULT CHARACTER SET utf8mb4"
                )
                cur.execute(f"GRANT ALL PRIVILEGES ON `{db}`.* TO %s@'%%'", (SRC_USER,))
            for db in snk_dbs:
                cur.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{db}` DEFAULT CHARACTER SET utf8mb4"
                )
                cur.execute(f"GRANT ALL PRIVILEGES ON `{db}`.* TO %s@'%%'", (SINK_USER,))
            cur.execute("FLUSH PRIVILEGES")


def ensure_tables(profile: Profile) -> None:
    """DROP+CREATE source tables; DROP all tables in profile sink DBs (clears leftovers)."""
    src_dbs = sorted({t.src_db for t in profile.tables})
    snk_dbs = sorted({t.snk_db for t in profile.tables})
    with admin_conn() as conn:
        with conn.cursor() as cur:
            # Wipe every table in profile src/snk DBs so a prior profile's
            # leftovers (e.g. normal tp_scale_009 in src_02) cannot match
            # the next run's broad source.tables regex.
            for db in src_dbs + snk_dbs:
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


def _seed_one_table(spec: TableSpec) -> None:
    """Seed a single table; insert_batch streams ids in chunks of 500."""
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
            except Exception as exc:
                raise RuntimeError(f"{spec.src_db}.{spec.name}: {exc}") from exc
    return time.time() - t0


def fetch_checksum(
    user: str, password: str, db: str, table: str, template: str
) -> tuple[int, int, int]:
    """Return (cnt, sum_id, name_crc) for db.table."""
    sql = checksum_sql(template, db, table)
    conn = _user_conn(user, password, db)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return (0, 0, 0)
    return (int(row["cnt"]), int(row["sum_id"]), int(row["name_crc"]))


def verify_pair(profile: Profile, phase: str) -> list[str]:
    """Compare source vs sink checksums; empty list means pass.

    Failure strings look like ``db.table: reason``.
    """
    failures: list[str] = []
    for t in profile.tables:
        label = f"{t.src_db}.{t.name}"
        try:
            src = fetch_checksum(SRC_USER, SRC_PASSWORD, t.src_db, t.name, t.template)
            snk = fetch_checksum(
                SINK_USER, SINK_PASSWORD, t.snk_db, t.sink_name, t.template
            )
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{label}: {phase} fetch error: {exc}")
            continue
        if src != snk:
            failures.append(
                f"{label}: {phase} mismatch src(cnt,sum_id,name_crc)={src} "
                f"sink={snk}"
            )
    return failures


def _yaml_path(profile: Profile) -> Path:
    return WORKDIR / f"pipeline.scale-{profile.name}.yaml"


def _log_path(profile_name: str) -> Path:
    return WORKDIR / f"cdc-scale-{profile_name}.log"


def _ckpt_dir(profile: Profile) -> Path:
    return WORKDIR / f"ckpts-scale-{profile.name}"


def _profile_name_from_yaml(yaml_path: Path) -> str:
    stem = yaml_path.stem  # pipeline.scale-{name}
    prefix = "pipeline.scale-"
    if stem.startswith(prefix):
        return stem[len(prefix) :]
    return stem


def ensure_binary() -> Path:
    """Return go-cdc binary path; build into workdir if missing."""
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
    """Write a scale pipeline YAML for the profile tables."""
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
        f"  server-id: {CDC_SERVER_ID_SCALE}",
        "  server-time-zone: Asia/Shanghai",
        "  scan.startup.mode: initial",
        "  scan.incremental.snapshot.chunk.size: 4096",
        "  scan.snapshot.fetch.size: 1024",
        "  schema-change.enabled: false",
        "  connect.timeout: 30s",
        "",
        "sink:",
        "  type: mysql",
        f"  hostname: {MYSQL_HOST}",
        f"  port: {MYSQL_PORT}",
        f"  username: {SINK_USER}",
        f'  password: "{SINK_PASSWORD}"',
        "  auto-create-table: true",
        "  write-batch-size: 1000",
        "  write-batch-interval: 500ms",
        "  max-retries: 3",
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
            # Single regex covers all scale src tables (Go regexp fullMatch).
            r"  - source-table: 'db_tp_scale_src_\d+\.tp_scale_\d+'",
            "    projection: '*'",
            "    primary-keys: id",
            "",
            "pipeline:",
            f"  name: tp_scale_pipeline_{profile.name}",
            f"  parallelism: {profile.workers}",
            "",
            "checkpoint:",
            "  storage: file",
            "  interval: 3s",
            f"  file-path: {ckpt}",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def start_job(yaml_path: Path) -> subprocess.Popen:
    """Start go-cdc; pump stdout/stderr to workdir/cdc-scale-{profile}.log."""
    ensure_binary()
    profile_name = _profile_name_from_yaml(yaml_path)
    log_path = _log_path(profile_name)
    log_path.write_text("", encoding="utf-8")
    proc = subprocess.Popen(
        [str(Path(BINARY)), str(yaml_path)],
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


def _table_exists(user: str, password: str, db: str, table: str) -> bool:
    conn = _user_conn(user, password, db)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 AS ok FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name=%s",
                (db, table),
            )
            return bool(cur.fetchone())
    finally:
        conn.close()


def _count_rows(user: str, password: str, db: str, table: str) -> int:
    conn = _user_conn(user, password, db)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM `{db}`.`{table}`")
            row = cur.fetchone()
    finally:
        conn.close()
    return int(row["n"]) if row else 0


def wait_snapshot(
    profile: Profile,
    timeout: float = 1800,
    proc: subprocess.Popen | None = None,
) -> float:
    """Poll until every sink table exists and COUNT(*) matches planned seed rows."""
    t0 = time.time()
    deadline = t0 + timeout
    last_err: BaseException | None = None
    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            log = _log_path(profile.name)
            tail = log.read_text(encoding="utf-8")[-2000:] if log.exists() else ""
            raise RuntimeError(f"go-cdc exited early: code={proc.returncode}\n{tail}")
        try:
            done = True
            for t in profile.tables:
                if not _table_exists(SINK_USER, SINK_PASSWORD, t.snk_db, t.sink_name):
                    done = False
                    break
                n = _count_rows(SINK_USER, SINK_PASSWORD, t.snk_db, t.sink_name)
                if n != t.rows:
                    done = False
                    break
            if done:
                elapsed = time.time() - t0
                log(f"TIMING  wait [cdc_snapshot] done in {elapsed:.1f}s", flush=True)
                return elapsed
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(15.0)
    raise TimeoutError(
        f"timeout waiting for scale snapshot after {time.time() - t0:.1f}s: {last_err}"
    )


def _mutated_name(db_index: int, ordinal: int, row_id: int) -> str:
    return f"mut-d{db_index:02d}-t{ordinal:03d}-id{row_id}"


def _mutated_status(row_id: int) -> int:
    return (row_id * 3 + 7) % 128


def _update_rows(conn, spec: TableSpec, id_lo: int, id_hi: int) -> None:
    """Deterministically update name (or status/label) for ids in [id_lo, id_hi]."""
    if id_hi < id_lo:
        return
    with conn.cursor() as cur:
        if spec.template in ("basic", "large"):
            for row_id in range(id_lo, id_hi + 1):
                cur.execute(
                    f"UPDATE `{spec.src_db}`.`{spec.name}` SET `name`=%s WHERE `id`=%s",
                    (_mutated_name(spec.db_index, spec.ordinal, row_id), row_id),
                )
        elif spec.template == "indexed":
            for row_id in range(id_lo, id_hi + 1):
                cur.execute(
                    f"UPDATE `{spec.src_db}`.`{spec.name}` SET `status`=%s WHERE `id`=%s",
                    (_mutated_status(row_id), row_id),
                )
        elif spec.template == "wide":
            for row_id in range(id_lo, id_hi + 1):
                cur.execute(
                    f"UPDATE `{spec.src_db}`.`{spec.name}` SET `c_label`=%s WHERE `id`=%s",
                    (_mutated_name(spec.db_index, spec.ordinal, row_id), row_id),
                )
        else:
            raise ValueError(f"unknown template: {spec.template!r}")
    conn.commit()


def _mutate_one_table(spec: TableSpec) -> None:
    """Apply event_group mutations for a single source table."""
    group = spec.event_group
    if group == "idle":
        return
    rows = spec.rows
    conn = _user_conn(SRC_USER, SRC_PASSWORD, spec.src_db)
    try:
        if group == "insert":
            n = max(1, rows // 20)
            insert_batch(
                conn,
                spec.src_db,
                spec.name,
                spec.template,
                rows + 1,
                rows + n,
                spec.db_index,
                spec.ordinal,
            )
        elif group == "update":
            id_hi = min(100, rows)
            if id_hi >= 1:
                _update_rows(conn, spec, 1, id_hi)
        elif group == "delete":
            n = min(50, rows)
            if n > 0:
                id_lo = rows - n + 1
                with conn.cursor() as cur:
                    cur.execute(
                        f"DELETE FROM `{spec.src_db}`.`{spec.name}` "
                        f"WHERE `id` BETWEEN %s AND %s",
                        (id_lo, rows),
                    )
                conn.commit()
        elif group == "insert_update":
            ins = max(1, rows // 40)
            insert_batch(
                conn,
                spec.src_db,
                spec.name,
                spec.template,
                rows + 1,
                rows + ins,
                spec.db_index,
                spec.ordinal,
            )
            upd_hi = min(50, rows)
            if upd_hi >= 1:
                _update_rows(conn, spec, 1, upd_hi)
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
                msg = f"ERROR scale mutate {spec.src_db}.{spec.name}: {exc}"
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


def _fetch_update_marker(
    user: str, password: str, db: str, table: str, template: str, row_id: int
):
    """Return the mutated column value for row_id (None if missing)."""
    if template in ("basic", "large"):
        col = "name"
    elif template == "indexed":
        col = "status"
    elif template == "wide":
        col = "c_label"
    else:
        raise ValueError(f"unknown template: {template!r}")
    conn = _user_conn(user, password, db)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT `{col}` AS v FROM `{db}`.`{table}` WHERE `id`=%s", (row_id,))
            row = cur.fetchone()
    finally:
        conn.close()
    return None if not row else row["v"]


def verify_update_markers(profile: Profile, phase: str) -> list[str]:
    """Ensure update/insert_update mutations are visible on sink (cnt alone is insufficient)."""
    failures: list[str] = []
    for t in profile.tables:
        id_hi = _updated_id_hi(t)
        if id_hi < 1:
            continue
        label = f"{t.src_db}.{t.name}"
        # Spot-check first, mid, last updated ids to avoid scanning all rows every poll.
        sample_ids = sorted({1, max(1, id_hi // 2), id_hi})
        for row_id in sample_ids:
            try:
                src_v = _fetch_update_marker(
                    SRC_USER, SRC_PASSWORD, t.src_db, t.name, t.template, row_id
                )
                snk_v = _fetch_update_marker(
                    SINK_USER, SINK_PASSWORD, t.snk_db, t.sink_name, t.template, row_id
                )
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{label}: {phase} update-marker fetch error id={row_id}: {exc}")
                break
            if src_v != snk_v:
                failures.append(
                    f"{label}: {phase} update-marker mismatch id={row_id} src={src_v!r} sink={snk_v!r}"
                )
                break
    return failures


def wait_catchup(
    profile: Profile,
    timeout: float = 1800,
    proc: subprocess.Popen | None = None,
) -> float:
    """Poll until every source/sink checksum (+ update markers) match after mutations."""
    t0 = time.time()
    deadline = t0 + timeout
    last_failures: list[str] = []
    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            log = _log_path(profile.name)
            tail = log.read_text(encoding="utf-8")[-2000:] if log.exists() else ""
            raise RuntimeError(f"go-cdc exited early: code={proc.returncode}\n{tail}")
        last_failures = verify_pair(profile, "catchup")
        if not last_failures:
            last_failures = verify_update_markers(profile, "catchup")
        if not last_failures:
            elapsed = time.time() - t0
            log(f"TIMING  wait [cdc_catchup] done in {elapsed:.1f}s", flush=True)
            return elapsed
        time.sleep(15.0)
    sample = "\n".join(last_failures[:5])
    raise TimeoutError(
        f"timeout waiting for scale catch-up after {time.time() - t0:.1f}s "
        f"({len(last_failures)} mismatches):\n{sample}"
    )


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
    """Insert n_rows after MAX(id); return new high-water id."""
    if n_rows <= 0:
        return _max_id(spec)
    start = _max_id(spec) + 1
    end = start + n_rows - 1
    conn = _user_conn(SRC_USER, SRC_PASSWORD, spec.src_db)
    try:
        insert_batch(
            conn,
            spec.src_db,
            spec.name,
            spec.template,
            start,
            end,
            spec.db_index,
            spec.ordinal,
        )
    finally:
        conn.close()
    return end


def _assert_cdc_alive(proc: subprocess.Popen | None, phase: str) -> None:
    """Fail immediately if go-cdc exited during hotspot burst / peak / catch-up."""
    if proc is not None and proc.poll() is not None:
        raise RuntimeError(f"go-cdc exited during hotspot {phase}: {proc.returncode}")


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
    _assert_cdc_alive(proc, "burst")
    with ThreadPoolExecutor(max_workers=profile.workers) as pool:
        futs = {pool.submit(_burst_one_table, h, n_rows): h for h in hotspots}
        pending = set(futs)
        while pending:
            _assert_cdc_alive(proc, "burst")
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
        raise RuntimeError(f"burst_hotspots failed:\n" + "\n".join(errors))
    return time.time() - t0, next_ids


def _peak_writer(
    worker_id: int,
    hotspots: list[TableSpec],
    next_ids: dict[str, int],
    lock: threading.Lock,
    stop_at: float,
    batch: int,
) -> int:
    """Time-based inserts into hotspot set; returns rows written by this worker."""
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
            insert_batch(
                conn,
                spec.src_db,
                spec.name,
                spec.template,
                start,
                end,
                spec.db_index,
                spec.ordinal,
            )
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
    peak_workers = int(os.environ.get("SCALE_PEAK_WORKERS", str(profile.workers)))
    peak_workers = max(1, peak_workers)
    seconds = SCALE_PEAK_SECONDS
    batch = SCALE_PEAK_BATCH
    log(
        f"==> hotspot peak seconds={seconds} workers={peak_workers} batch={batch}",
        flush=True,
    )
    stop_at = time.time() + seconds
    lock = threading.Lock()
    t0 = time.time()
    _assert_cdc_alive(proc, "peak")
    totals: list[int] = []
    with ThreadPoolExecutor(max_workers=peak_workers) as pool:
        futs = [
            pool.submit(_peak_writer, i, hotspots, next_ids, lock, stop_at, batch)
            for i in range(peak_workers)
        ]
        pending = set(futs)
        while pending:
            _assert_cdc_alive(proc, "peak")
            done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
            for fut in done:
                totals.append(fut.result())
    elapsed = max(time.time() - t0, 0.001)
    rows = sum(totals)
    log(
        f"TIMING peak_write {elapsed:.1f}s ({rows} rows ≈ {rows / elapsed:.0f} rps)",
        flush=True,
    )
    return elapsed, rows


def wait_hotspot_convergence(
    hotspots: list[TableSpec],
    timeout: float = 3600,
    interval: float = 15.0,
    proc: subprocess.Popen | None = None,
) -> float:
    """Poll until each hotspot src/sink COUNT converges within 1%."""
    t0 = time.time()
    deadline = t0 + timeout
    last_log = 0.0
    while time.time() < deadline:
        _assert_cdc_alive(proc, "catch-up")
        ok = True
        lines: list[str] = []
        for h in hotspots:
            src_n = _count_rows(SRC_USER, SRC_PASSWORD, h.src_db, h.name)
            snk_n = _count_rows(SINK_USER, SINK_PASSWORD, h.snk_db, h.sink_name)
            delta = abs(src_n - snk_n) / max(src_n, 1)
            lines.append(
                f"{h.src_db}.{h.name} src={src_n} snk={snk_n} delta={delta:.2%}"
            )
            if delta > 0.01:
                ok = False
        now = time.time()
        if now - last_log >= interval or ok:
            log(
                f"    hotspot catch-up {now - t0:.0f}s: " + " | ".join(lines),
                flush=True,
            )
            last_log = now
        if ok:
            elapsed = time.time() - t0
            log(f"TIMING peak_catchup {elapsed:.1f}s", flush=True)
            return elapsed
        time.sleep(interval)
    raise TimeoutError(
        f"timeout waiting for hotspot count convergence after {time.time() - t0:.1f}s"
    )


def _report_path(profile: Profile) -> Path:
    return REPORT_DIR / f"mysql-mysql-scale-{profile.name}.md"


def write_report(
    profile: Profile,
    timings: dict[str, float],
    failures: list[str],
    path: Path | None = None,
    extra: dict[str, object] | None = None,
) -> Path:
    """Write Markdown scale report; return path written."""
    out = path if path is not None else _report_path(profile)
    out.parent.mkdir(parents=True, exist_ok=True)
    passed = len(failures) == 0
    src_dbs = sorted({t.src_db for t in profile.tables})
    snk_dbs = sorted({t.snk_db for t in profile.tables})
    group_counts: dict[str, int] = {}
    for t in profile.tables:
        group_counts[t.event_group] = group_counts.get(t.event_group, 0) + 1

    lines = [
        f"# go-cdc scale integration test report ({profile.name})\n",
        f"- Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Result: {'✅ PASS' if passed else '❌ FAIL'}",
        f"- Profile: `{profile.name}`",
        f"- Workers / parallelism: {profile.workers}",
        f"- Tables: {len(profile.tables)}",
        f"- Total rows (planned seed): {profile.total_rows}",
        f"- Source DBs ({len(src_dbs)}): `{', '.join(src_dbs)}`",
        f"- Sink DBs ({len(snk_dbs)}): `{', '.join(snk_dbs)}`",
        "",
        "## Event groups\n",
        "| Group | Tables |",
        "| --- | ---: |",
    ]
    for g in ("insert", "update", "delete", "insert_update", "idle"):
        lines.append(f"| {g} | {group_counts.get(g, 0)} |")
    lines.append("")

    if extra and extra.get("hotspots"):
        lines.append("## Hotspot burst / peak\n")
        lines.append(
            f"- Burst rows/hot table: `{SCALE_BURST_ROWS_PER_HOT}`"
        )
        lines.append(f"- Peak seconds: `{SCALE_PEAK_SECONDS}`")
        peak_workers = int(os.environ.get("SCALE_PEAK_WORKERS", str(profile.workers)))
        lines.append(f"- Peak workers: `{peak_workers}`")
        lines.append(f"- Peak batch: `{SCALE_PEAK_BATCH}`")
        if "peak_rows" in extra:
            lines.append(f"- Peak rows written (measured): `{extra['peak_rows']}`")
        lines.append("- Hotspot tables:")
        for name in extra["hotspots"]:  # type: ignore[union-attr]
            lines.append(f"  - `{name}`")
        lines.append("")

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

    lines.append(f"- Failures: {len(failures)}\n")
    if failures:
        lines.append("## Failures\n")
        for msg in failures:
            lines.append(f"- `{msg}`")
        lines.append("")
    else:
        lines.append("## Summary\n")
        summary = (
            f"All {len(profile.tables)} source/sink pairs matched "
            "(COUNT + SUM(id) + name_crc) after snapshot and post-mutation catch-up."
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

    out.write_text("\n".join(lines), encoding="utf-8")
    return out


def _idle_checksums(profile: Profile) -> dict[str, tuple[int, int, int]]:
    """Capture source checksums for idle tables (must stay unchanged)."""
    out: dict[str, tuple[int, int, int]] = {}
    for t in profile.tables:
        if t.event_group != "idle":
            continue
        label = f"{t.src_db}.{t.name}"
        out[label] = fetch_checksum(
            SRC_USER, SRC_PASSWORD, t.src_db, t.name, t.template
        )
    return out


def _verify_idle_unchanged(
    profile: Profile, before: dict[str, tuple[int, int, int]]
) -> list[str]:
    failures: list[str] = []
    for t in profile.tables:
        if t.event_group != "idle":
            continue
        label = f"{t.src_db}.{t.name}"
        after = fetch_checksum(SRC_USER, SRC_PASSWORD, t.src_db, t.name, t.template)
        expected = before.get(label)
        if expected is None:
            failures.append(f"{label}: idle missing pre-mutation checksum")
        elif after != expected:
            failures.append(
                f"{label}: idle changed after mutations before={expected} after={after}"
            )
    return failures


def _print_plan(profile: Profile) -> None:
    log(f"profile={profile.name}")
    log(f"tables={len(profile.tables)}")
    log(f"total_rows={profile.total_rows}")
    log(f"workers={profile.workers}")
    log(f"sum_table_rows={sum(t.rows for t in profile.tables)}")


def _clear_checkpoint(profile: Profile) -> None:
    ckpt = _ckpt_dir(profile)
    if ckpt.exists():
        shutil.rmtree(ckpt)
    ckpt.mkdir(parents=True, exist_ok=True)


def run_profile(profile: Profile, until: str) -> int:
    """Run scale phases through ``until`` (seed|snapshot|mutate|all)."""
    log(
        f"==> scale profile={profile.name} tables={len(profile.tables)} "
        f"total_rows={profile.total_rows} workers={profile.workers} until={until}",
        flush=True,
    )
    timings: dict[str, float] = {}
    all_failures: list[str] = []
    report_path = _report_path(profile)

    ensure_databases(profile)
    ensure_tables(profile)

    log("==> seed baseline", flush=True)
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
        snap_s = wait_snapshot(profile, timeout=1800, proc=proc)
        timings["cdc_snapshot"] = snap_s
        log(f"TIMING cdc_snapshot {snap_s:.1f}s", flush=True)

        snap_failures = verify_pair(profile, "snapshot")
        if snap_failures:
            all_failures.extend(snap_failures)
            for msg in snap_failures:
                log(f"FAIL {msg}", file=sys.stderr, flush=True)
            write_report(profile, timings, all_failures, report_path)
            log(f"REPORT {report_path}", flush=True)
            return 1
        log(
            f"==> snapshot verify ok ({len(profile.tables)} tables)",
            flush=True,
        )
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

        if until == "mutate":
            # Still wait catch-up so CDC drains before stop; report final state.
            pass

        log("==> wait catch-up", flush=True)
        catch_s = wait_catchup(profile, timeout=1800, proc=proc)
        timings["cdc_catchup"] = catch_s
        log(f"TIMING cdc_catchup {catch_s:.1f}s", flush=True)

        hotspot_extra: dict[str, object] = {}
        if profile.name == "heavy" and until == "all":
            hotspots = hotspot_tables(profile)
            burst_s, next_ids = burst_hotspots(profile, hotspots, proc=proc)
            timings["burst"] = burst_s
            log(f"TIMING burst {burst_s:.1f}s", flush=True)

            peak_s, peak_rows = peak_write_hotspots(
                profile, hotspots, next_ids, proc=proc
            )
            timings["peak_write"] = peak_s
            hotspot_extra["peak_rows"] = peak_rows
            hotspot_extra["hotspots"] = [
                f"{h.src_db}.{h.name}" for h in hotspots
            ]

            catch_peak_s = wait_hotspot_convergence(
                hotspots, timeout=3600, interval=15.0, proc=proc
            )
            timings["peak_catchup"] = catch_peak_s

        final_failures = verify_pair(profile, "final")
        final_failures.extend(verify_update_markers(profile, "final"))
        all_failures.extend(final_failures)
        if final_failures:
            for msg in final_failures:
                log(f"FAIL {msg}", file=sys.stderr, flush=True)
        else:
            log(
                f"==> final verify ok ({len(profile.tables)} tables)",
                flush=True,
            )

        write_report(
            profile,
            timings,
            all_failures,
            report_path,
            extra=hotspot_extra or None,
        )
        log(f"REPORT {report_path}", flush=True)
        if all_failures:
            log("SUMMARY FAIL", flush=True)
            return 1
        log("SUMMARY PASS", flush=True)
        return 0
    except Exception:
        # Best-effort report on hard failures when we have timings.
        try:
            if timings and until not in ("seed", "snapshot"):
                write_report(
                    profile,
                    timings,
                    all_failures + ["aborted: see logs / exception"],
                    report_path,
                )
                log(f"REPORT {report_path}", flush=True)
        except Exception:  # noqa: BLE001
            pass
        raise
    finally:
        stop_job(proc)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="go-cdc scale integration orchestrator")
    parser.add_argument(
        "--print-plan",
        "--dry-topology",
        dest="print_plan",
        metavar="PROFILE",
        help="print table count / total rows and exit (no MySQL)",
    )
    parser.add_argument(
        "profile",
        nargs="?",
        default=None,
        help="profile name (normal|heavy)",
    )
    parser.add_argument(
        "--until",
        choices=("seed", "snapshot", "mutate", "all"),
        default="all",
        help="stop after phase (default: all)",
    )
    parser.add_argument(
        "--total-rows",
        type=int,
        default=None,
        help="override profile total_rows",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="override profile workers",
    )
    args = parser.parse_args(argv)

    total_rows = args.total_rows
    if total_rows is None and os.environ.get("SCALE_TOTAL_ROWS"):
        total_rows = int(os.environ["SCALE_TOTAL_ROWS"])
    workers = args.workers
    if workers is None and os.environ.get("SCALE_WORKERS"):
        workers = int(os.environ["SCALE_WORKERS"])

    if args.print_plan:
        name = args.print_plan
        p = load_profile(name, total_rows, workers)
        _print_plan(p)
        return 0

    if args.profile:
        p = load_profile(args.profile, total_rows, workers)
        return run_profile(p, args.until)

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
