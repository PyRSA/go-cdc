#!/usr/bin/env python3
"""go-cdc mysql2postgresql benchmark harness (PG-210, plan T3.21..T3.24).

This is a **performance benchmark**, not an acceptance test. It produces
throughput, latency and resource numbers for the PostgreSQL sink write path and
sets no pass threshold (spec §9.3: no unfounded target is invented here). It
reuses the infrastructure the other suites already own — ``pg_it`` job control,
``pg_client`` target reads, ``pg_verify`` canonical row diff and ``sysstat``
resource sampling — but it owns its source database, target schema, checkpoint
directory, server-id and report, so a benchmark run can never be reported as an
E2E / Load / Scale result.

Matrix (spec §9.1): two workloads x three batch sizes (100 / 1000 / 5000).

* ``insert`` — pure INSERT/UPSERT traffic at the configured batch size.
* ``mixed``  — UPDATE + DELETE + INSERT against a pre-seeded row pool.

**Every write happens on the MySQL source**; the PostgreSQL side is only read
back. One cell has three phases: (1) snapshot of the seeded rows, *not measured*;
(2) the measured write window; (3) catch-up after the writes stop, followed by a
full row-by-row reconciliation by primary key.

Run it with ``scripts/run.sh postgresql heavy pg16 bench``; the report lands in
``docs/test/reports/mysql-postgresql-benchmark.md``. ``--dry-run`` renders the
matrix and the report without contacting a database, and every cell stays
``NOT RUN`` - a benchmark that did not run proves nothing about performance.
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import math
import os
import platform
import random
import statistics
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Sequence

import pymysql

from common import config as cfg
from . import pg_client, pg_it, pg_verify
from common.logutil import log
from .pg_compat import ci_pg_image, go_mod_versions
from .pg_it import Check, _yaml_block, my, my_exec, src_cfg
from common.sysstat import cpu_busy_pct, mem_mib, proc_rss_mib

WORKDIR = cfg.WORKDIR
BINARY = cfg.BINARY
CK = Path(cfg.CHECKPOINT_PG_BENCH_PATH)
YAML = WORKDIR / "pipeline.pg.bench.yaml"
LOG = WORKDIR / "cdc-pg-bench.log"

SRC_DB = cfg.PG_BENCH_SRC_DB
SCHEMA = cfg.PG_BENCH_SNK_SCHEMA
DATA = "tpb01_bench"
PROBE = "tpb01_probe"
PROBE_DDL = f"CREATE TABLE `{PROBE}` (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB"

DATA_DDL = f"""CREATE TABLE `{DATA}` (
  id BIGINT NOT NULL,
  c_seq INT NOT NULL,
  c_payload VARCHAR(200) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB"""

COLUMNS = ("id", "c_seq", "c_payload")
KINDS = {"id": "int", "c_seq": "int", "c_payload": "text"}

WORKLOADS = ("insert", "mixed")
STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_NOT_RUN = "NOT RUN"
EXIT_OK = 0
EXIT_FAIL = 1
EXIT_BLOCKED = 2

INSERT_SQL = f"INSERT INTO `{SRC_DB}`.`{DATA}`(id,c_seq,c_payload) VALUES (%s,%s,%s)"
UPSERT_SQL = (
    f"INSERT INTO `{SRC_DB}`.`{DATA}`(id,c_seq,c_payload) VALUES (%s,%s,%s) "
    "ON DUPLICATE KEY UPDATE c_seq=VALUES(c_seq), c_payload=VALUES(c_payload)"
)
DELETE_SQL = f"DELETE FROM `{SRC_DB}`.`{DATA}` WHERE id=%s"
PROBE_INSERT = f"INSERT INTO `{SRC_DB}`.`{PROBE}`(id) VALUES (%s)"


# ------------------------------------------------------------------ matrix model
def parse_workloads(raw: str) -> tuple[str, ...]:
    """Workloads named on the command line / in the environment, in matrix order."""
    wanted = [part.strip().lower() for part in raw.split(",") if part.strip()]
    unknown = [name for name in wanted if name not in WORKLOADS]
    if unknown:
        raise ValueError(f"unknown workload(s) {unknown}; known: {list(WORKLOADS)}")
    ordered = [name for name in WORKLOADS if name in wanted]
    if not ordered:
        raise ValueError("no workload selected")
    return tuple(ordered)


def parse_batches(raw: str) -> tuple[int, ...]:
    """Batch sizes of the matrix, ascending and de-duplicated."""
    values: list[int] = []
    for part in raw.split(","):
        text = part.strip()
        if not text:
            continue
        try:
            size = int(text)
        except ValueError as exc:
            raise ValueError(f"batch size {text!r} is not an integer") from exc
        if size <= 0:
            raise ValueError(f"batch size {size} must be >= 1")
        if size not in values:
            values.append(size)
    if not values:
        raise ValueError("no batch size selected")
    return tuple(sorted(values))


@dataclass
class Cell:
    """One (workload, batch size) matrix row."""

    workload: str
    batch: int
    status: str = STATUS_NOT_RUN
    checks: list[Check] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    metrics: dict[str, float | int | None] = field(default_factory=dict)
    reason: str = ""

    @property
    def label(self) -> str:
        return f"{self.workload}/batch-{self.batch}"

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        self.checks.append(Check(name=name, ok=bool(ok), detail=detail))
        log(f"    [{'ok' if ok else 'BAD'}] {self.label} {name}" + (f" - {detail}" if detail else ""),
            flush=True)
        return bool(ok)

    def note(self, text: str) -> None:
        self.notes.append(text)

    def finish(self) -> "Cell":
        if self.status == STATUS_NOT_RUN and self.checks:
            self.status = STATUS_PASS if all(c.ok for c in self.checks) else STATUS_FAIL
        log(f"[{self.label}] {self.status}", flush=True)
        return self


def build_matrix(workloads: Sequence[str], batches: Sequence[int]) -> list[Cell]:
    """The full configured matrix: workload-major, batch ascending.

    Unselected rows stay in the matrix as ``NOT RUN`` so a report always shows
    which cells exist and which of them actually ran.
    """
    return [Cell(workload=w, batch=b) for w in workloads for b in batches]


# ---------------------------------------------------------------------- metrics
def percentile(values: Sequence[float], p: float) -> float | None:
    """Percentile with linear interpolation between the two closest ranks.

    Same definition as ``numpy.percentile`` with its default ``linear`` method,
    spelled out here so the report can state exactly how a number was produced.
    """
    if not values:
        return None
    if not 0.0 <= p <= 100.0:
        raise ValueError(f"percentile {p} outside 0..100")
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * (p / 100.0)
    low, high = math.floor(position), math.ceil(position)
    if low == high:
        return ordered[int(position)]
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def latency_summary(samples: Sequence[float]) -> dict[str, float | None]:
    """P50 / P95 / max / mean of the probe latencies (ms)."""
    return {
        "count": len(samples),
        "p50": percentile(samples, 50),
        "p95": percentile(samples, 95),
        "max": max(samples) if samples else None,
        "mean": statistics.fmean(samples) if samples else None,
    }


def throughput(rows: int, seconds: float) -> float | None:
    """Rows per second over the measured write window (None when not measurable)."""
    if rows < 0 or seconds <= 0:
        return None
    return rows / seconds


def failure_pct(failed: int, attempted: int) -> float | None:
    """Share of attempted rows the generator could not commit, in percent."""
    if attempted <= 0:
        return None
    return 100.0 * failed / attempted


def pool_size(rows: int) -> int:
    """Rows the mixed workload can update or delete (a quarter of a cell's rows)."""
    return max(1000, rows // 4)


def payload(seed: int, row_id: int) -> str:
    """Deterministic payload: the same (seed, id) always produces the same text."""
    return hashlib.sha256(f"{seed}:{row_id}".encode("utf-8")).hexdigest()[:32]


def fmt_num(value: object, digits: int = 1) -> str:
    """Table cell for a measured number; ``-`` when nothing was measured."""
    if value is None:
        return "-"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return "-"
        return f"{value:.{digits}f}"
    return str(value)


class BatchPlanner:
    """Deterministic batch generator for one writer (fixed seed, no wall clock).

    Pure bookkeeping: it decides *what* a batch contains, the driver decides
    whether it commits. That split is what the offline tests exercise.
    """

    def __init__(
        self,
        workload: str,
        batch: int,
        *,
        seed: int,
        base_id: int,
        pool: int,
    ) -> None:
        if workload not in WORKLOADS:
            raise ValueError(f"unknown workload {workload!r}")
        if batch <= 0:
            raise ValueError(f"batch size {batch} must be >= 1")
        self.workload = workload
        self.batch = batch
        self.seed = seed
        self.next_id = base_id
        self.pool = max(1, pool)
        self.seq = 0
        self.rng = random.Random(seed)
        # Ids this writer inserted and has not deleted yet: a delete can only
        # remove a row that exists, and never the same id twice.
        self.pending_delete: collections.deque[int] = collections.deque()

    def next(self) -> tuple[list[tuple], list[tuple], list[int]]:
        """Next batch as ``(inserts, upserts, delete_ids)``."""
        if self.workload == "insert":
            rows = []
            for _ in range(self.batch):
                row_id = self.next_id
                self.next_id += 1
                rows.append((row_id, self.seq, payload(self.seed, row_id)))
            self.seq += 1
            return rows, [], []

        upserts_n = self.batch // 2
        inserts_n = max(1, self.batch // 4)
        deletes_n = self.batch - upserts_n - inserts_n
        self.seq += 1

        upserts = []
        for _ in range(upserts_n):
            row_id = self.rng.randint(1, self.pool)
            upserts.append((row_id, self.seq, payload(self.seed, row_id)))

        inserts = []
        for _ in range(inserts_n):
            row_id = self.next_id
            self.next_id += 1
            inserts.append((row_id, self.seq, payload(self.seed, row_id)))
            self.pending_delete.append(row_id)

        deletes: list[int] = []
        while len(deletes) < deletes_n and self.pending_delete:
            deletes.append(self.pending_delete.popleft())
        # A delete list shorter than ``deletes_n`` only happens before this writer
        # inserted enough rows; the batch is then smaller and reported as such.
        return inserts, upserts, deletes


# ----------------------------------------------------------------- code versions
def environment(command: str, *, mode: str) -> dict[str, str]:
    """Reproduction metadata (T3.23): machine, software versions, command."""
    versions = go_mod_versions()
    usage = mem_mib()
    info = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": mode,
        "command": command,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": str(os.cpu_count() or "unknown"),
        "mem_total_mib": f"{usage[1]:.0f}" if usage else "unknown",
        "go": versions.get("go", "unknown"),
        "pgx": versions.get("pgx", "unknown"),
        "ci_pg_image": ci_pg_image(),
        "pg_image": cfg.env("PG_IMAGE", "postgres:16.4"),
    }
    return info


def pg_settings() -> dict[str, str]:
    """Server version plus the settings that most affect write throughput."""
    names = (
        "server_version",
        "max_connections",
        "shared_buffers",
        "work_mem",
        "maintenance_work_mem",
        "wal_level",
        "synchronous_commit",
        "fsync",
    )
    found: dict[str, str] = {}
    for name in names:
        try:
            found[name] = str(pg_client.q(f"SHOW {name}")[0][name])
        except Exception:  # noqa: BLE001 - a report still has to be written
            found[name] = "unknown"
    return found


# -------------------------------------------------------------------- MySQL side
def _admin_conn():
    return pymysql.connect(
        host=cfg.MYSQL_HOST,
        port=cfg.MYSQL_PORT,
        user=cfg.env("ROOT_USER", "root"),
        password=cfg.env("ROOT_PASSWORD", "rootpass"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def ensure_source_database() -> None:
    """Create the benchmark's own source database and grant the source user."""
    with _admin_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{SRC_DB}` DEFAULT CHARACTER SET utf8mb4")
            cur.execute(f"GRANT ALL PRIVILEGES ON `{SRC_DB}`.* TO %s@'%%'", (cfg.SRC_USER,))
            cur.execute("FLUSH PRIVILEGES")


def reset_source() -> None:
    """Recreate both source tables, so a rerun starts from the same state."""
    my_exec(SRC_DB, f"DROP TABLE IF EXISTS `{SRC_DB}`.`{DATA}`")
    my_exec(SRC_DB, f"DROP TABLE IF EXISTS `{SRC_DB}`.`{PROBE}`")
    my_exec(SRC_DB, DATA_DDL)
    my_exec(SRC_DB, PROBE_DDL)


def seed_pool(rows: int, seed: int) -> None:
    """Seed the rows the mixed workload will update and delete."""
    conn = pymysql.connect(**src_cfg(SRC_DB), autocommit=False)
    try:
        with conn.cursor() as cur:
            step = 1000
            for start in range(1, rows + 1, step):
                chunk = [
                    (row_id, 0, payload(seed, row_id))
                    for row_id in range(start, min(start + step, rows + 1))
                ]
                cur.executemany(f"INSERT INTO `{SRC_DB}`.`{DATA}`(id,c_seq,c_payload) "
                                "VALUES (%s,%s,%s)", chunk)
            conn.commit()
    finally:
        conn.close()


def src_count() -> int:
    return int(my(SRC_DB, f"SELECT COUNT(*) AS n FROM `{SRC_DB}`.`{DATA}`")[0]["n"])


def snk_count() -> int:
    return pg_client.row_count(SCHEMA, DATA)


# ----------------------------------------------------------------- target side
def reset_target() -> None:
    """Fresh schema and no checkpoint, so a cell never inherits another's state."""
    pg_client.ensure_database()
    pg_client.drop_schema(SCHEMA)
    pg_client.create_schema(SCHEMA)
    if CK.exists():
        CK.unlink()
    Path(cfg.CHECKPOINT_PG_BENCH_DIR).mkdir(parents=True, exist_ok=True)


def render_yaml(batch: int) -> Path:
    """One pipeline per cell: only the sink batch size differs between cells."""
    source = {
        "type": "mysql",
        "hostname": cfg.MYSQL_HOST,
        "port": cfg.MYSQL_PORT,
        "username": cfg.SRC_USER,
        "password": cfg.SRC_PASSWORD,
        "tables": "'" + f"{SRC_DB}\\.({DATA}|{PROBE})" + "'",
        "server-id": cfg.CDC_SERVER_ID_PG_BENCH,
        "server-time-zone": "Asia/Shanghai",
        "scan.startup.mode": "initial",
        "scan.incremental.snapshot.chunk.size": 4096,
        "scan.snapshot.fetch.size": 1024,
        "schema-change.enabled": False,
        "connect.timeout": "30s",
    }
    sink = {
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
        "write-batch-size": batch,
        "write-batch-interval": cfg.PG_BENCH_BATCH_INTERVAL,
        "max-retries": 3,
        "schema.change.enabled": False,
        "schema.change.drop-truncate.enabled": False,
    }
    lines = ["source:"]
    lines += _yaml_block(source)
    lines += ["", "sink:"]
    lines += _yaml_block(sink)
    lines += ["", "route:"]
    for table in (DATA, PROBE):
        lines.append(f"  - source-table: '{SRC_DB}\\.{table}'")
        lines.append(f"    sink-table: {SCHEMA}.{table}")
    lines += [
        "",
        "transform: []",
        "",
        "pipeline:",
        f"  name: {cfg.PIPELINE_PG_BENCH_NAME}",
        f"  parallelism: {cfg.PG_BENCH_PARALLELISM}",
        "",
        "checkpoint:",
        "  storage: file",
        "  interval: 2s",
        f"  file-path: {cfg.it_rel_path(cfg.CHECKPOINT_PG_BENCH_DIR)}",
        "",
    ]
    YAML.parent.mkdir(parents=True, exist_ok=True)
    YAML.write_text("\n".join(lines), encoding="utf-8")
    return YAML


def wait_catchup(
    timeout: float,
    desc: str = "catch-up",
    *,
    verify: Callable[[], list[str]] | None = None,
) -> float:
    """Wait until the target has caught up; return the elapsed seconds.

    Row counts are a valid barrier only while the source row count grows, which is
    the case for the ``insert`` workload. ``mixed`` inserts and deletes the same
    number of rows per batch, so equal counts also hold while a backlog is still
    being applied; there the caller passes ``verify`` and the wait ends only on a
    clean full reconciliation.
    """
    started = time.time()
    deadline = started + timeout
    differences: list[str] | None = None
    while time.time() < deadline:
        if pg_it.proc is not None and pg_it.proc.poll() is not None:
            raise RuntimeError(
                f"go-cdc exited during {desc} ({pg_it.proc.returncode}): {pg_it.log_tail()}"
            )
        if src_count() == snk_count():
            differences = verify() if verify is not None else []
            if not differences:
                elapsed = time.time() - started
                log(f"TIMING  {desc} {elapsed:.1f}s", flush=True)
                return elapsed
        # A row-count probe opens two connections; 20 ms would hammer the servers
        # without making the number more precise than the checkpoint interval.
        time.sleep(max(cfg.PG_BENCH_POLL_S, 0.5))
    detail = f"; last differences: {differences[:3]}" if differences else ""
    raise TimeoutError(
        f"{desc} budget {timeout:.0f}s exhausted: src={src_count()} sink={snk_count()}{detail}"
    )


def reconcile() -> list[str]:
    """Full primary-key walk: every row and every column, in bounded windows."""
    if not pg_client.table_exists(SCHEMA, DATA):
        return [f"target {SCHEMA}.{DATA} does not exist"]
    failures: list[str] = []
    src_total, snk_total = src_count(), snk_count()
    if src_total != snk_total:
        failures.append(f"row count differs: src={src_total} sink={snk_total}")
    chunk = cfg.PG_BENCH_VERIFY_CHUNK
    columns = ", ".join(f"`{name}`" for name in COLUMNS)
    target_columns = ", ".join(f'"{name}"' for name in COLUMNS)
    last = -1
    while True:
        src_rows = my(
            SRC_DB,
            f"SELECT {columns} FROM `{SRC_DB}`.`{DATA}` WHERE id > %s ORDER BY id LIMIT {chunk}",
            (last,),
        )
        if not src_rows:
            break
        low, high = int(src_rows[0]["id"]), int(src_rows[-1]["id"])
        snk_rows = pg_client.q(
            f'SELECT {target_columns} FROM "{SCHEMA}"."{DATA}" '
            "WHERE id BETWEEN %s AND %s ORDER BY id",
            (low, high),
        )
        failures += pg_verify.diff_rows(src_rows, snk_rows, COLUMNS, KINDS, ("id",))
        if len(failures) > 50:
            failures.append(f"... stopped after {len(failures)} differences")
            break
        last = high
    return failures


# --------------------------------------------------------------------- sampler
class Sampler(threading.Thread):
    """Background resource sampler: host CPU, go-cdc RSS, target connections."""

    def __init__(self, pid: int | None, interval: float = 1.0) -> None:
        super().__init__(daemon=True)
        self.pid = pid
        self.interval = max(0.2, interval)
        self.stop_event = threading.Event()
        self.cpu: list[float] = []
        self.rss: list[float] = []
        self.conns: list[int] = []
        self._conn = None
        self._me: int | None = None

    def _count_connections(self) -> int | None:
        try:
            if self._conn is None:
                self._conn = pg_client.connect()
                self._me = pg_client.backend_pid(self._conn)
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity "
                    "WHERE datname = %s AND pid <> %s",
                    (cfg.PG_DB, self._me),
                )
                return int(cur.fetchone()[0])
        except Exception:  # noqa: BLE001 - a lost sample is not a failed benchmark
            return None

    def run(self) -> None:
        while not self.stop_event.wait(self.interval):
            busy = cpu_busy_pct()
            if busy is not None:
                self.cpu.append(busy)
            rss = proc_rss_mib(self.pid)
            if rss is not None:
                self.rss.append(rss)
            count = self._count_connections()
            if count is not None:
                self.conns.append(count)

    def stop(self) -> None:
        self.stop_event.set()
        if self.is_alive():
            self.join(timeout=5)
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    def peak_cpu(self) -> float | None:
        return max(self.cpu) if self.cpu else None

    def peak_rss(self) -> float | None:
        return max(self.rss) if self.rss else None

    def peak_conns(self) -> int | None:
        return max(self.conns) if self.conns else None


class Probe(threading.Thread):
    """Single-row latency probe: MySQL commit -> row visible on the target.

    A bulk batch cannot be timed row by row without distorting the load, so the
    latency metric is measured on this channel: one dedicated table, one row per
    probe, ``PG_BENCH_PROBE_INTERVAL_S`` apart. Throughput comes from the bulk
    channel, latency from this one; the report says so.
    """

    def __init__(
        self,
        *,
        interval: float,
        timeout: float,
        poll: float,
        ready_timeout: float = 60.0,
    ) -> None:
        super().__init__(daemon=True)
        self.interval = max(0.05, interval)
        self.timeout = timeout
        self.poll = max(0.005, poll)
        self.ready_timeout = max(0.0, ready_timeout)
        self.stop_event = threading.Event()
        self.samples: list[float] = []
        self.timeouts = 0
        self.errors: list[str] = []
        self._next_id = 1

    def _visible(self, cur, row_id: int) -> bool:
        cur.execute(f'SELECT 1 AS ok FROM "{SCHEMA}"."{PROBE}" WHERE id = %s', (row_id,))
        return cur.fetchone() is not None

    def _wait_ready(self) -> bool:
        """Block until go-cdc has created the target probe table.

        The probe table is empty during the snapshot and only materialises on the
        sink's first event for that table, so the very first ``_visible`` query can
        race a ``UndefinedTable error``. That is a startup race, not a slow query:
        wait for the table to exist before measuring any latency, and give up (with
        an error, never a silent wrong number) if it never appears.
        """
        deadline = time.time() + self.ready_timeout
        while time.time() < deadline:
            if self.stop_event.is_set():
                return False
            if pg_client.table_exists(SCHEMA, PROBE):
                return True
            time.sleep(max(self.poll, 0.5))
        # One final check so a table created just before the deadline is not missed.
        return pg_client.table_exists(SCHEMA, PROBE)

    def run(self) -> None:
        source = pymysql.connect(**src_cfg(SRC_DB), autocommit=True)
        target = pg_client.connect()
        try:
            if not self._wait_ready():
                self.errors.append(
                    f"probe table {SCHEMA}.{PROBE} not created within "
                    f"{self.ready_timeout:.0f}s; latency not measured"
                )
                return
            while not self.stop_event.is_set():
                row_id = self._next_id
                self._next_id += 1
                started = time.time()
                try:
                    with source.cursor() as cur:
                        cur.execute(PROBE_INSERT, (row_id,))
                except Exception as exc:  # noqa: BLE001 - reported, never silent
                    self.errors.append(repr(exc))
                    break
                deadline = started + self.timeout
                seen = False
                while time.time() < deadline:
                    with target.cursor() as cur:
                        if self._visible(cur, row_id):
                            seen = True
                            break
                    time.sleep(self.poll)
                if seen:
                    self.samples.append((time.time() - started) * 1000.0)
                else:
                    self.timeouts += 1
                self.stop_event.wait(self.interval)
        finally:
            source.close()
            target.close()

    def stop(self) -> None:
        self.stop_event.set()
        if self.is_alive():
            self.join(timeout=max(5.0, self.timeout))


# --------------------------------------------------------------------- writers
def write_batches(
    planner: BatchPlanner,
    *,
    target_rows: int,
    deadline: float,
    stats: dict[str, int],
    lock: threading.Lock,
) -> list[str]:
    """Commit batches on MySQL until the row target or the deadline is reached."""
    errors: list[str] = []
    conn = pymysql.connect(**src_cfg(SRC_DB), autocommit=False)
    try:
        with conn.cursor() as cur:
            while time.time() < deadline:
                with lock:
                    if stats["attempted"] >= target_rows:
                        break
                inserts, upserts, deletes = planner.next()
                size = len(inserts) + len(upserts) + len(deletes)
                if size == 0:
                    break
                try:
                    if inserts:
                        cur.executemany(INSERT_SQL, inserts)
                    if upserts:
                        cur.executemany(UPSERT_SQL, upserts)
                    if deletes:
                        cur.executemany(DELETE_SQL, [(row_id,) for row_id in deletes])
                    conn.commit()
                except Exception as exc:  # noqa: BLE001 - counted as a failed batch
                    conn.rollback()
                    with lock:
                        stats["failed"] += size
                    errors.append(repr(exc))
                    break
                with lock:
                    stats["attempted"] += size
                    stats["rows"] += size
    finally:
        conn.close()
    return errors


# ----------------------------------------------------------------------- cells
def run_cell(cell: Cell, *, rows: int, seed: int, workers: int, seconds: float) -> None:
    """Execute one matrix cell; fills ``cell.metrics`` / ``cell.checks``."""
    batch = cell.batch
    pool = pool_size(rows)
    reset_target()
    reset_source()
    if cell.workload == "mixed":
        seed_pool(pool, seed)
        cell.note(f"seeded a {pool}-row pool the mixed workload updates and deletes")
    log(f"==> {cell.label}: {rows} rows, workers={workers}, "
        f"interval={cfg.PG_BENCH_BATCH_INTERVAL}", flush=True)

    render_yaml(batch)
    pg_it.start_job(YAML, LOG)
    stats = {"attempted": 0, "rows": 0, "failed": 0}
    lock = threading.Lock()
    errors: list[str] = []
    sampler = Sampler(pg_it.proc.pid if pg_it.proc else None)
    probe: Probe | None = None
    alive = False
    catchup_s = 0.0
    write_seconds = 0.0
    try:
        # Phase 1: the snapshot of the seeded rows is setup, never measured.
        cell.metrics["snapshot_s"] = wait_catchup(cfg.PG_BENCH_CATCHUP_BUDGET_S, "snapshot")
        sampler.start()
        if cfg.PG_BENCH_PROBE_INTERVAL_S > 0:
            probe = Probe(
                interval=cfg.PG_BENCH_PROBE_INTERVAL_S,
                timeout=cfg.PG_BENCH_PROBE_TIMEOUT_S,
                poll=cfg.PG_BENCH_POLL_S,
            )
            probe.start()

        # Phase 2: the measured write window. All writers share one row target, so
        # the cell stops at ``rows`` and never at ``rows / workers``.
        base_id = 1_000_000 + batch
        deadline = time.time() + (seconds if seconds > 0 else 3600.0)

        def run_writer(wid: int) -> None:
            found = write_batches(
                BatchPlanner(
                    cell.workload, batch, seed=seed + wid,
                    base_id=base_id + wid * 10_000_000, pool=pool,
                ),
                target_rows=rows,
                deadline=deadline,
                stats=stats,
                lock=lock,
            )
            with lock:
                errors.extend(found)

        threads = [
            threading.Thread(target=run_writer, args=(wid,), daemon=True)
            for wid in range(workers)
        ]
        started = time.time()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        write_seconds = max(time.time() - started, 0.001)
        if probe is not None:
            probe.stop()
        # Phase 3: catch-up after the writers stop. Equal row counts are not a
        # barrier for ``mixed`` (each batch inserts as many rows as it deletes), so
        # that workload ends the wait on a clean full reconciliation instead.
        catchup_s = wait_catchup(
            cfg.PG_BENCH_CATCHUP_BUDGET_S,
            "catch-up",
            verify=reconcile if cell.workload == "mixed" else None,
        )
        alive = pg_it.job_alive()
    finally:
        if probe is not None:
            probe.stop()
        sampler.stop()
        pg_it.stop_job()

    rows_written = int(stats["rows"])
    attempted = int(stats["attempted"])
    failures = reconcile()
    latencies = latency_summary(probe.samples if probe is not None else [])
    cell.metrics.update(
        {
            "rows": rows_written,
            "attempted": attempted,
            "seconds": round(write_seconds, 1),
            "throughput": throughput(rows_written, write_seconds),
            "catchup_s": round(catchup_s, 1),
            "probes": latencies["count"],
            "p50": latencies["p50"],
            "p95": latencies["p95"],
            "lat_max": latencies["max"],
            "probe_timeouts": probe.timeouts if probe is not None else None,
            "cpu_max": sampler.peak_cpu(),
            "rss_max": sampler.peak_rss(),
            "conns_max": sampler.peak_conns(),
            "failure_pct": failure_pct(int(stats["failed"]), attempted),
        }
    )
    cell.note(f"snapshot phase {cell.metrics['snapshot_s']:.1f}s is setup, not measured")
    cell.note(f"write window {write_seconds:.1f}s, catch-up {catchup_s:.1f}s")
    if probe is not None:
        cell.note(f"latency channel: {latencies['count']} single-row probes, "
                  f"{probe.timeouts} timeout(s)")
        if probe.errors:
            cell.note(f"probe errors: {probe.errors[:3]}")
    if errors:
        cell.note(f"writer errors: {errors[:3]}")

    src_total, snk_total = src_count(), snk_count()
    cell.check("the planned rows were written and none failed",
               rows_written >= rows and int(stats["failed"]) == 0,
               f"rows={rows_written} want>={rows} failed={stats['failed']}")
    cell.check("the job stayed up for the whole cell", alive,
               "yes" if alive else pg_it.log_tail(4))
    cell.check("source and target row counts agree after catch-up",
               src_total == snk_total, f"src={src_total} sink={snk_total}")
    cell.check("every row is canonically equal to the source", not failures,
               pg_verify.summarize(failures) if failures else f"{snk_total} rows")
    if probe is not None:
        cell.check("the latency probe never timed out", probe.timeouts == 0,
                   f"{probe.timeouts} timeout(s) of {latencies['count']} probes")
    cell.finish()


# ----------------------------------------------------------------------- report
def _status_cell(status: str) -> str:
    return {STATUS_PASS: "✅ PASS", STATUS_FAIL: "❌ FAIL"}.get(status, f"⊘ {status}")


def write_report(
    cells: Sequence[Cell],
    env: dict[str, str],
    *,
    settings: dict[str, str] | None = None,
    path: Path | None = None,
) -> Path:
    """Render the matrix, the metadata and the per-cell detail to Markdown."""
    target = Path(path or cfg.REPORT_PG_BENCH)
    target.parent.mkdir(parents=True, exist_ok=True)
    passed = sum(1 for c in cells if c.status == STATUS_PASS)
    failed = sum(1 for c in cells if c.status == STATUS_FAIL)
    not_run = sum(1 for c in cells if c.status == STATUS_NOT_RUN)
    complete = not_run == 0 and failed == 0
    settings = settings or {}
    lines = [
        "# go-cdc mysql2postgresql benchmark report (PG-210)\n",
        '<a id="overview"></a>\n',
        "## 0. Overview\n",
        f"- Generated: {env.get('generated_at', '')}",
        f"- Mode: **{env.get('mode', 'benchmark')}**",
        f"- Source (all writes happen here): "
        f"`{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}` / `{SRC_DB}`",
        f"- Target (read back only): `{cfg.PG_HOST}:{cfg.PG_PORT}` / `{cfg.PG_DB}` "
        f"(schema `{SCHEMA}`)",
        f"- **Cells: pass {passed} / fail {failed} / not run {not_run} of {len(cells)}** → "
        f"{'**complete**' if complete else '**incomplete**'}",
        "- This is a performance benchmark, not an acceptance test: it sets no throughput "
        "target and a `NOT RUN` cell is not evidence of anything.\n",
        "| Workload | Batch | Rows | Throughput (rows/s) | P50 (ms) | P95 (ms) | "
        "Catch-up (s) | CPU max (%) | RSS max (MiB) | Connections max | Retries | "
        "Failure (%) | Status |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | "
        "---: | --- |",
    ]
    for cell in cells:
        metrics = cell.metrics
        lines.append(
            f"| {cell.workload} | {cell.batch} | {fmt_num(metrics.get('rows'))} | "
            f"{fmt_num(metrics.get('throughput'))} | {fmt_num(metrics.get('p50'))} | "
            f"{fmt_num(metrics.get('p95'))} | {fmt_num(metrics.get('catchup_s'))} | "
            f"{fmt_num(metrics.get('cpu_max'))} | {fmt_num(metrics.get('rss_max'))} | "
            f"{fmt_num(metrics.get('conns_max'))} | n/a | "
            f"{fmt_num(metrics.get('failure_pct'), 2)} | {_status_cell(cell.status)} |"
        )
    lines += [
        "",
        "`Retries` is `n/a`: the PostgreSQL sink does not emit a retry event, so a retry "
        "count can only be invented. See §5.\n",
        "## 1. Reproduction metadata\n",
        "| Item | Value |",
        "| --- | --- |",
    ]
    for key in (
        "platform", "cpu_count", "mem_total_mib", "python", "go", "pgx",
        "pg_image", "ci_pg_image", "command",
    ):
        lines.append(f"| {key} | {cfg.md_cell(env.get(key, 'unknown'), code=True)} |")
    for key in ("server_version", "max_connections", "shared_buffers", "work_mem",
                "maintenance_work_mem", "wal_level", "synchronous_commit", "fsync"):
        lines.append(f"| PostgreSQL `{key}` | {cfg.md_cell(settings.get(key, 'not collected'), code=True)} |")
    lines += [
        "",
        "### Dataset and knobs\n",
        "| Item | Value |",
        "| --- | --- |",
        f"| tables | `{SRC_DB}.{DATA}` → `{SCHEMA}.{DATA}`, `{SRC_DB}.{PROBE}` → "
        f"`{SCHEMA}.{PROBE}` |",
        f"| columns | `{', '.join(COLUMNS)}` |",
        "| indexes | primary key on `id` only (no secondary index) |",
        "| transaction boundary | one MySQL transaction per batch; one PostgreSQL "
        "`Write` per batch |",
        f"| `PG_BENCH_ROWS` | {cfg.PG_BENCH_ROWS} rows per cell |",
        f"| `PG_BENCH_WORKERS` | {cfg.PG_BENCH_WORKERS} |",
        f"| `PG_BENCH_SEED` | {cfg.PG_BENCH_SEED} (same seed → same ids and payloads) |",
        f"| `PG_BENCH_BATCH_INTERVAL` | {cfg.PG_BENCH_BATCH_INTERVAL} |",
        f"| `PG_BENCH_PARALLELISM` | {cfg.PG_BENCH_PARALLELISM} |",
        f"| `PG_BENCH_PROBE_INTERVAL_S` | {cfg.PG_BENCH_PROBE_INTERVAL_S} |",
        f"| `PG_BENCH_CATCHUP_BUDGET_S` | {cfg.PG_BENCH_CATCHUP_BUDGET_S:.0f} |",
        "",
        "## 2. Metric definitions\n",
        "- **Rows**: rows the generator committed on the MySQL source inside the measured "
        "window (snapshot rows are excluded).",
        "- **Throughput**: `rows / write window seconds`; the window covers the writers "
        "only, not snapshot or catch-up.",
        "- **P50 / P95**: percentiles of the probe latency, i.e. the time from a MySQL "
        "single-row commit until that row is visible on the target. Interpolation is "
        "linear between the two closest ranks, and the poll resolution "
        f"(`PG_BENCH_POLL_S={cfg.PG_BENCH_POLL_S}`) bounds the smallest measurable value.",
        "- **Catch-up**: after the writers stop, the time until the target has "
        "consumed the whole window: equal row counts for `insert`, and a clean "
        "row-by-row reconciliation for `mixed`, whose batches insert and delete the "
        "same number of rows so counts alone prove nothing. The final full "
        "reconciliation follows it for every cell.",
        "- **CPU / RSS**: highest host CPU busy percentage and highest go-cdc resident "
        "set size sampled once per second (`/proc` only: `-` elsewhere).",
        "- **Connections**: highest number of PostgreSQL client backends on the target "
        "database, excluding the sampler's own session.",
        "- **Failure**: share of attempted rows whose batch rolled back.",
        "",
        "## 3. Per-cell detail\n",
    ]
    for cell in cells:
        lines += [
            f"### {cell.workload} / batch {cell.batch}\n",
            "- [↑ Overview](#overview)",
            f"- **Result: {_status_cell(cell.status)}**"
            + (f" — {cfg.md_cell(cell.reason)}" if cell.reason else ""),
        ]
        if cell.checks:
            lines += ["", "| Check | Result | Detail |", "| --- | --- | --- |"]
            for item in cell.checks:
                lines.append(
                    f"| {cfg.md_cell(item.name)} | {'✅' if item.ok else '❌'} | "
                    f"{cfg.md_cell(item.detail)} |"
                )
        if cell.notes:
            lines += ["", "**Notes**\n"]
            lines += [f"- {cfg.md_cell(text)}" for text in cell.notes]
        lines.append("")
    lines += [
        "## 4. Data flow\n",
        "```mermaid",
        "flowchart LR",
        f"  src[{SRC_DB}<br/>{DATA} + {PROBE}] -->|binlog| job[go-cdc]",
        f"  job --> tgt[{cfg.PG_DB}.{SCHEMA}]",
        "```\n",
        "## 5. What this benchmark does not measure\n",
        "- **Retries**: the sink logs no retry event, so the column stays `n/a` rather "
        "than reporting a fabricated zero.",
        "- **Absolute latency of a 5000-row batch**: latency comes from the probe channel "
        "(single-row transactions), because timing every bulk batch would need a marker "
        "table that changes the very number it measures. Throughput covers the batch "
        "dimension.",
        "- **Container quotas**: the compose services carry no CPU/memory limit in "
        "`tests/integration/sinks/postgresql/docker-compose.yml`, so the runner's "
        "resources are the limit. "
        "A machine with different hardware will produce different numbers.",
        "- **NOT RUN / FAIL cells**: a cell that did not run or failed proves nothing; "
        "the matrix stays `incomplete` until every cell has a real PASS.",
        "",
    ]
    target.write_text(cfg.sanitize_report_text("\n".join(lines)), encoding="utf-8")
    return target


# ------------------------------------------------------------------------- main
def preflight() -> None:
    """Refuse to start when the environment cannot produce a real measurement."""
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
    except Exception as exc:  # noqa: BLE001
        # The database is created by this harness, so a missing one is not fatal;
        # an unreachable server is.
        try:
            ensure_source_database()
        except Exception as admin_exc:  # noqa: BLE001
            raise RuntimeError(
                f"MySQL is not reachable at {cfg.MYSQL_HOST}:{cfg.MYSQL_PORT} and the "
                f"benchmark database {SRC_DB} cannot be created "
                "(run `bash tests/integration/scripts/run.sh mysql`): "
                f"{exc} / {admin_exc}"
            ) from admin_exc


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m harness.pg_bench_it",
        description="PostgreSQL sink benchmark (batch x workload matrix, PG-210).",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="render the matrix and the report without touching a database")
    parser.add_argument("--list", action="store_true", help="print the matrix and exit")
    parser.add_argument("--workloads", default=cfg.PG_BENCH_WORKLOADS)
    parser.add_argument("--batches", default=cfg.PG_BENCH_BATCHES)
    parser.add_argument("--rows", type=int, default=cfg.PG_BENCH_ROWS)
    parser.add_argument("--seconds", type=float, default=cfg.PG_BENCH_SECONDS)
    parser.add_argument("--report", default=str(cfg.REPORT_PG_BENCH))
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        selected_workloads = parse_workloads(args.workloads)
        selected_batches = parse_batches(args.batches)
    except ValueError as exc:
        parser.error(str(exc))
        raise  # pragma: no cover - argparse exits before this line
    full = build_matrix(WORKLOADS, parse_batches(cfg.PG_BENCH_BATCHES))
    chosen = {(w, b) for w in selected_workloads for b in selected_batches}

    command = "python -m harness.pg_bench_it " + " ".join(
        str(part) for part in (sys.argv[1:] if argv is None else argv)
    )
    mode = "dry run" if args.dry_run else "benchmark"

    if args.list:
        for cell in full:
            mark = "*" if (cell.workload, cell.batch) in chosen else " "
            print(f"{mark} {cell.workload:6s} batch={cell.batch}")
        return EXIT_OK

    report = Path(args.report)
    if args.dry_run:
        # Nothing was executed, so no cell may carry a number or a PASS.
        for cell in full:
            cell.reason = (
                "dry run: selected but not executed"
                if (cell.workload, cell.batch) in chosen
                else "dry run: not selected"
            )
        path = write_report(full, environment(command, mode="dry run"), path=report)
        log("REPORT", cfg.project_display_path(path), flush=True)
        log("NOT RUN", len(full), "of", len(full), flush=True)
        return EXIT_OK

    cfg.REPORT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        preflight()
    except Exception as exc:  # noqa: BLE001 - an unusable environment is not a failure
        log(f"ENV BLOCKED: {exc}", flush=True)
        for cell in full:
            cell.reason = f"ENV BLOCKED: {exc}"
        write_report(full, environment(command, mode="ENV BLOCKED"), path=report)
        return EXIT_BLOCKED

    settings = pg_settings()
    ensure_source_database()
    env = environment(command, mode=mode)
    try:
        for cell in full:
            if (cell.workload, cell.batch) not in chosen:
                cell.reason = "not selected in this run"
                continue
            try:
                run_cell(cell, rows=args.rows, seed=cfg.PG_BENCH_SEED,
                         workers=max(1, cfg.PG_BENCH_WORKERS), seconds=args.seconds)
            except Exception as exc:  # noqa: BLE001 - one cell must not stop the rest
                log(f"    [error] {cell.label}: {exc!r}", flush=True)
                cell.reason = repr(exc)
                cell.check("the cell completed without an exception", False, repr(exc))
                cell.finish()
    finally:
        pg_it.stop_job()

    path = write_report(full, env, settings=settings, path=report)
    passed = sum(1 for c in full if c.status == STATUS_PASS)
    failed = sum(1 for c in full if c.status == STATUS_FAIL)
    skipped = len(full) - passed - failed
    log("REPORT", cfg.project_display_path(path), flush=True)
    log("PASS", passed, "FAIL", failed, "NOT RUN", skipped, flush=True)
    return EXIT_OK if failed == 0 else EXIT_FAIL


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        pg_it.stop_job()
