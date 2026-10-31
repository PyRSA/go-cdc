"""PostgreSQL scale suite orchestrator (MySQL source -> PostgreSQL sink).

Mirrors ``scale.scale_it`` phase for phase - seed, snapshot, mixed mutations,
hotspot burst/peak (heavy only), checkpoint recovery, final reconciliation - and
reuses ``scale.profiles`` for the data model, so a PostgreSQL run and a
MySQL->MySQL run allocate the same tables and rows.

What differs is forced by PostgreSQL, not by preference:

  - Source databases and target schemas are re-prefixed (``db_tp_pg_scale_src_*``
    / ``tp_pg_scale_<profile>_*``), so this suite can never touch the databases
    or schemas of the MySQL->MySQL, Doris or Elasticsearch suites. The profile
    name is part of every target schema, which is what keeps a normal run and a
    heavy run from overwriting each other's tables.
  - Every run starts by dropping only the schemas that match
    ``tp_pg_scale_<profile>_<nn>`` (never ``public``, never the other profile's)
    and by clearing only its own checkpoint directory. Checkpoint file paths,
    checkpoint directories and MySQL server ids are all per profile.
  - Source templates come from ``scale.pg_schemas``: ``wide`` drops ``c_time``
    because the sink refuses ``TIME``. The removal is asserted and reported.
  - Convergence is gated on a cheap aggregate both engines compute identically
    (``COUNT(*)``, ``SUM(id)``, a character-length sum). Acceptance is a chunked
    row-level comparison through ``harness.pg_verify``: primary-key sets, every
    projected column, NULLs, binary values, timestamps, JSON, duplicate keys on
    the target. A count-only result is never reported as a pass.
  - The restart phase stops the job, writes to the source while it is down, and
    restarts on the checkpoint the stopped job wrote - which is PG-205's
    "resume from an existing checkpoint" path, where the sink is not re-sent the
    captured schemas (PR2 M3).

Requires MySQL (``scripts/mysql-up.sh``) as the source and PostgreSQL
(``scripts/pg-up.sh``) as the sink. Entry point: ``scripts/run-pg-scale.sh``.
"""
from __future__ import annotations

import argparse
import os
import re
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

from harness import pg_client, pg_verify
from harness.config import (
    BINARY,
    CDC_SERVER_ID_PG_SCALE,
    CDC_SERVER_ID_PG_SCALE_HEAVY,
    CHECKPOINT_PG_SCALE_DIR,
    MYSQL_HOST,
    MYSQL_PORT,
    PG_DB,
    PG_HOST,
    PG_PASSWORD,
    PG_PORT,
    PG_SSLMODE,
    PG_USER,
    REPORT_DIR,
    REPORT_PG_SCALE_TMPL,
    ROOT,
    SRC_PASSWORD,
    SRC_USER,
    WORKDIR,
    checkpoint_file,
    it_rel_path,
    pg_pipeline_scale_name,
    project_display_path,
    sanitize_report_text,
)
from harness.logutil import log
from harness.sysstat import format_disk
from scale import pg_schemas
from scale.profiles import Profile, TableSpec, hotspot_tables, load_profile
from scale.scale_it import ROOT_PASSWORD, ROOT_USER

SRC_DB_PREFIX = "db_tp_pg_scale_src"
SINK_SCHEMA_PREFIX = "tp_pg_scale"

# Hotspot knobs. PG_SCALE_* wins so this tier can be tuned without touching the
# MySQL->MySQL numbers; otherwise the shared SCALE_* values apply, which keeps
# the historical 50 000 rows / 45 s / batch 200 model in force.
SCALE_BURST_ROWS_PER_HOT = int(
    os.environ.get("PG_SCALE_BURST_ROWS_PER_HOT", os.environ.get("SCALE_BURST_ROWS_PER_HOT", "50000"))
)
SCALE_PEAK_SECONDS = int(
    os.environ.get("PG_SCALE_PEAK_SECONDS", os.environ.get("SCALE_PEAK_SECONDS", "45"))
)
SCALE_PEAK_BATCH = int(
    os.environ.get("PG_SCALE_PEAK_BATCH", os.environ.get("SCALE_PEAK_BATCH", "200"))
)
# One budget (seconds) spanning hotspot count convergence and the aggregate +
# row-level verification that follows it. Created once when the peak writes
# stop, so the phases never re-time independently. Mirrors
# ``scale.scale_it.PEAK_CATCHUP_BUDGET_S``; the PR3 spec's historical 1800 s
# ceiling is recorded in the report instead of being hardcoded a second time.
PEAK_CATCHUP_BUDGET_S = 3600.0
SNAPSHOT_TIMEOUT_S = float(os.environ.get("PG_SCALE_SNAPSHOT_TIMEOUT", "3600"))
CATCHUP_TIMEOUT_S = float(os.environ.get("PG_SCALE_CATCHUP_TIMEOUT", "3600"))
# Primary-key window of the row-level reconciliation (bounds memory; the
# difference it reports is still per id and per column).
VERIFY_CHUNK = int(os.environ.get("PG_SCALE_VERIFY_CHUNK", "20000"))
# Rows written to the first source table while the job is stopped (PG-205).
RESTART_ROWS = int(os.environ.get("PG_SCALE_RESTART_ROWS", "100"))

# Only schemas this suite created are ever dropped: prefix + profile + 2 digits.
_SCHEMA_RE = re.compile(rf"^{SINK_SCHEMA_PREFIX}_(?P<profile>[a-z]+)_(?P<db>\d{{2}})$")

__all__ = [
    "SRC_DB_PREFIX",
    "SINK_SCHEMA_PREFIX",
    "PEAK_CATCHUP_BUDGET_S",
    "pg_scale_profile",
    "sink_schema",
    "stale_schemas",
    "clean_target_schemas",
    "ensure_source",
    "seed_all",
    "src_aggregate",
    "snk_aggregate",
    "verify_pair",
    "verify_pair_eventual",
    "verify_rows",
    "verify_contract",
    "verify_update_markers",
    "render_pipeline_yaml",
    "start_job",
    "stop_job",
    "wait_snapshot",
    "mutate_all",
    "wait_catchup",
    "burst_hotspots",
    "peak_write_hotspots",
    "wait_hotspot_convergence",
    "restart_recovery",
    "write_report",
    "main",
]


# ------------------------------------------------------------------ profile map
def sink_schema(profile_name: str, db_index: int) -> str:
    """Target schema of one source database; the profile name is part of it."""
    return f"{SINK_SCHEMA_PREFIX}_{profile_name}_{db_index:02d}"


def pg_scale_profile(profile: Profile) -> Profile:
    """Re-prefix source databases and target schemas for a PostgreSQL run.

    Table count, per-database table count, templates, event groups and row
    allocation are untouched, so the profile stays comparable with the
    MySQL->MySQL and Doris scale reports.
    """
    tables: list[TableSpec] = []
    for t in profile.tables:
        tables.append(
            replace(
                t,
                src_db=f"{SRC_DB_PREFIX}_{t.db_index:02d}",
                snk_db=sink_schema(profile.name, t.db_index),
            )
        )
    return replace(profile, tables=tables)


def _profile_server_id(profile_name: str) -> str:
    return (
        CDC_SERVER_ID_PG_SCALE_HEAVY
        if profile_name == "heavy"
        else CDC_SERVER_ID_PG_SCALE
    )


def _yaml_path(profile: Profile) -> Path:
    return WORKDIR / f"pipeline.pg-scale-{profile.name}.yaml"


def _log_path(profile: Profile) -> Path:
    return WORKDIR / f"cdc-pg-scale-{profile.name}.log"


def _ckpt_dir(profile: Profile) -> Path:
    return Path(CHECKPOINT_PG_SCALE_DIR) / profile.name


def _ckpt_file(profile: Profile) -> Path:
    return checkpoint_file(str(_ckpt_dir(profile)), pg_pipeline_scale_name(profile.name))


# -------------------------------------------------------------------- bootstrap
def _admin_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=ROOT_USER,
        password=ROOT_PASSWORD,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def _src_conn(db: str):
    """Source connection pinned to the pipeline's ``server-time-zone``."""
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=SRC_USER,
        password=SRC_PASSWORD,
        database=db,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        init_command="SET time_zone = '+08:00'",
    )


def ensure_source(profile: Profile) -> None:
    """Create the scale source databases and tables; wipe leftovers first."""
    src_dbs = sorted({t.src_db for t in profile.tables})
    with _admin_conn() as conn:
        with conn.cursor() as cur:
            for db in src_dbs:
                cur.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{db}` DEFAULT CHARACTER SET utf8mb4"
                )
                cur.execute(f"GRANT ALL PRIVILEGES ON `{db}`.* TO %s@'%%'", (SRC_USER,))
                cur.execute(
                    "SELECT table_name AS name FROM information_schema.tables "
                    "WHERE table_schema=%s",
                    (db,),
                )
                for row in cur.fetchall():
                    cur.execute(f"DROP TABLE IF EXISTS `{db}`.`{row['name']}`")
            for spec in profile.tables:
                cur.execute(f"USE `{spec.src_db}`")
                cur.execute(pg_schemas.create_table_sql(spec.template, spec.name))
            cur.execute("FLUSH PRIVILEGES")


def stale_schemas(profile: Profile) -> list[str]:
    """Existing target schemas of *this* profile (never another profile's).

    A schema of this profile that the current topology no longer uses is still
    ours to clean; nothing outside ``tp_pg_scale_<profile>_<nn>`` is matched.
    """
    found: list[str] = []
    for name in pg_client.schemas():
        match = _SCHEMA_RE.match(name)
        if match and match.group("profile") == profile.name:
            found.append(name)
    return sorted(found)


def clean_target_schemas(profile: Profile) -> list[str]:
    """Drop this profile's target schemas (and only those) before a run."""
    dropped: list[str] = []
    for name in stale_schemas(profile):
        # Defensive: the name must match the pattern *and* carry this profile, so
        # a bug in ``stale_schemas`` can never drop another run's data.
        match = _SCHEMA_RE.match(name)
        if match is None or match.group("profile") != profile.name:
            raise RuntimeError(
                f"refusing to drop schema {name!r}: not a {profile.name} pg scale schema"
            )
        pg_client.drop_schema(name)
        dropped.append(name)
    if dropped:
        log(f"    dropped {len(dropped)} stale schema(s): {', '.join(dropped)}", flush=True)
    return dropped


def clear_checkpoint(profile: Profile) -> None:
    """Clear this profile's checkpoint directory only."""
    ckpt = _ckpt_dir(profile)
    if ckpt.exists():
        shutil.rmtree(ckpt)
    ckpt.mkdir(parents=True, exist_ok=True)


def check_pg_target() -> None:
    if not pg_client.ping():
        raise RuntimeError(
            f"PostgreSQL is not reachable at {PG_HOST}:{PG_PORT} "
            "(run `bash test/integration/scripts/pg-up.sh`)"
        )


# ------------------------------------------------------------------------- seed
def _seed_one_table(spec: TableSpec) -> None:
    conn = _src_conn(spec.src_db)
    try:
        if spec.rows > 0:
            pg_schemas.insert_batch(
                conn, spec.src_db, spec.name, spec.template,
                1, spec.rows, spec.db_index, spec.ordinal,
            )
    finally:
        conn.close()


def seed_all(profile: Profile) -> float:
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


# ------------------------------------------------------------------ aggregates
def src_aggregate(spec: TableSpec) -> tuple[int, int, int]:
    """(count, sum(id), content length) on the MySQL source."""
    conn = _src_conn(spec.src_db)
    try:
        with conn.cursor() as cur:
            cur.execute(pg_schemas.checksum_sql(spec.template, spec.src_db, spec.name))
            row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return (0, 0, 0)
    return (int(row["cnt"]), int(row["sum_id"]), int(row["content_len"]))


def snk_aggregate(spec: TableSpec) -> tuple[int, int, int]:
    """(count, sum(id), content length) on the PostgreSQL target."""
    if not pg_client.table_exists(spec.snk_db, spec.sink_name):
        return (0, 0, 0)
    rows = pg_client.q(
        pg_schemas.checksum_sql(spec.template, spec.snk_db, spec.sink_name, dialect="pg")
    )
    if not rows:
        return (0, 0, 0)
    row = rows[0]
    return (int(row["cnt"]), int(row["sum_id"]), int(row["content_len"]))


def _aggregate_diff(profile: Profile, phase: str) -> tuple[list[str], list[str]]:
    """Compare aggregates for every table; (mismatches, fetch_errors)."""
    mismatches: list[str] = []
    fetch_errors: list[str] = []
    for t in profile.tables:
        label = f"{t.src_db}.{t.name}"
        try:
            src = src_aggregate(t)
            snk = snk_aggregate(t)
        except Exception as exc:  # noqa: BLE001
            fetch_errors.append(f"{label}: {phase} fetch error: {exc}")
            continue
        if src != snk:
            mismatches.append(
                f"{label}: {phase} aggregate mismatch "
                f"src(cnt,sum_id,content_len)={src} sink={snk}"
            )
    return mismatches, fetch_errors


def verify_pair(profile: Profile, phase: str) -> list[str]:
    """Aggregate gate: empty list means every table agrees (count + content length)."""
    mismatches, fetch_errors = _aggregate_diff(profile, phase)
    return fetch_errors + mismatches


def verify_update_markers(profile: Profile, phase: str) -> list[str]:
    """The mutated column must be visible on the target, not only the row count."""
    failures: list[str] = []
    for t in profile.tables:
        id_hi = _updated_id_hi(t)
        if id_hi < 1:
            continue
        label = f"{t.src_db}.{t.name}"
        col = pg_schemas.update_column(t.template)
        kind = pg_schemas.kinds_of(t.template)[col]
        for row_id in sorted({1, max(1, id_hi // 2), id_hi}):
            try:
                src_v = _src_value(t, col, row_id)
                snk_v = _snk_value(t, col, row_id)
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{label}: {phase} marker fetch error id={row_id}: {exc}")
                break
            if pg_verify.canon(src_v, kind) != pg_verify.canon(snk_v, kind):
                failures.append(
                    f"{label}: {phase} marker mismatch id={row_id} "
                    f"src={src_v!r} sink={snk_v!r}"
                )
                break
    return failures


def _src_value(spec: TableSpec, column: str, row_id: int):
    conn = _src_conn(spec.src_db)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT `{column}` AS v FROM `{spec.src_db}`.`{spec.name}` WHERE `id`=%s",
                (row_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    return None if not row else row["v"]


def _snk_value(spec: TableSpec, column: str, row_id: int):
    rows = pg_client.q(
        f'SELECT "{column}" AS v FROM "{spec.snk_db}"."{spec.sink_name}" WHERE "id"=%s',
        (row_id,),
    )
    if not rows:
        return None
    return rows[0]["v"]


# ------------------------------------------------------- row-level exact check
def _src_window(spec: TableSpec, lo: int, hi: int) -> list[dict]:
    conn = _src_conn(spec.src_db)
    try:
        with conn.cursor() as cur:
            cur.execute(
                pg_schemas.select_rows_sql(spec.template, spec.src_db, spec.name)
                + " WHERE `id` BETWEEN %s AND %s ORDER BY `id`",
                (lo, hi),
            )
            return list(cur.fetchall())
    finally:
        conn.close()


def _snk_window(spec: TableSpec, lo: int, hi: int) -> list[dict]:
    if not pg_client.table_exists(spec.snk_db, spec.sink_name):
        return []
    return pg_client.q(
        pg_schemas.select_rows_sql(spec.template, spec.snk_db, spec.sink_name, dialect="pg")
        + ' WHERE "id" BETWEEN %s AND %s ORDER BY "id"',
        (lo, hi),
    )


def _id_bounds(spec: TableSpec) -> tuple[int, int]:
    """(min, max) id over both sides, so a target-only id is inside the windows."""
    conn = _src_conn(spec.src_db)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COALESCE(MIN(`id`), 0) AS lo, COALESCE(MAX(`id`), 0) AS hi "
                f"FROM `{spec.src_db}`.`{spec.name}`"
            )
            row = cur.fetchone() or {"lo": 0, "hi": 0}
    finally:
        conn.close()
    lo, hi = int(row["lo"]), int(row["hi"])
    if pg_client.table_exists(spec.snk_db, spec.sink_name):
        bounds = pg_client.q(
            f'SELECT COALESCE(min("id"), 0) AS lo, COALESCE(max("id"), 0) AS hi '
            f'FROM "{spec.snk_db}"."{spec.sink_name}"'
        )[0]
        lo = min(lo, int(bounds["lo"]))
        hi = max(hi, int(bounds["hi"]))
    return lo, hi


def compare_table(spec: TableSpec) -> list[str]:
    """Chunked row-level comparison of one table; per-id, per-column failures."""
    if not pg_client.table_exists(spec.snk_db, spec.sink_name):
        return [f"{spec.snk_db}.{spec.sink_name}: target table does not exist"]
    columns = pg_schemas.columns_of(spec.template)
    kinds = pg_schemas.kinds_of(spec.template)
    failures: list[str] = []
    lo, hi = _id_bounds(spec)
    window = lo
    while window <= hi:
        end = min(window + VERIFY_CHUNK - 1, hi)
        src_rows = _src_window(spec, window, end)
        snk_rows = _snk_window(spec, window, end)
        if src_rows or snk_rows:
            diff = pg_verify.diff_rows(src_rows, snk_rows, columns, kinds, ("id",))
            if diff:
                failures.append(
                    f"{spec.src_db}.{spec.name} -> {spec.snk_db}.{spec.sink_name} "
                    f"ids {window}..{end}:"
                )
                failures.extend(f"  {line}" for line in diff[:20])
                if len(failures) > 200:
                    failures.append("  ... further windows not read (too many differences)")
                    return failures
        window = end + 1
    return failures


def verify_rows(profile: Profile, phase: str) -> list[str]:
    """Exact row-level reconciliation of every table (one pass)."""
    failures: list[str] = []
    for spec in profile.tables:
        try:
            diff = compare_table(spec)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"{spec.src_db}.{spec.name}: {phase} row comparison failed: {exc}"
            ) from exc
        if diff:
            failures.append(f"[{phase}]")
            failures.extend(diff)
    return failures


def verify_rows_eventual(profile: Profile, deadline: float, phase: str,
                         interval: float = 15.0, proc: subprocess.Popen | None = None) -> list[str]:
    """Repeat the exact row-level compare until it passes or ``deadline``."""
    t0 = time.time()
    last: list[str] = []
    while True:
        _fail_fast(proc, profile, phase)
        last = verify_rows(profile, phase)
        if not last:
            log(f"TIMING  wait [row_verify:{phase}] done in {time.time() - t0:.1f}s", flush=True)
            return verify_update_markers(profile, phase)
        log(
            f"    row verify {phase} {time.time() - t0:.0f}s: "
            f"{len(last)} line(s), retrying",
            flush=True,
        )
        if time.time() >= deadline:
            return last
        time.sleep(interval)


def verify_pair_eventual(
    profile: Profile,
    deadline: float,
    interval: float = 15.0,
    proc: subprocess.Popen | None = None,
    phase: str = "final",
) -> list[str]:
    """Poll the aggregate gate until it passes or ``deadline``; errors are fatal.

    The aggregate is the convergence gate only; ``verify_rows_eventual`` decides
    acceptance. Keeping them apart is deliberate: the gate is cheap enough to
    poll every 15 s, the row-level pass is not.
    """
    t0 = time.time()
    last_log = 0.0
    last_failures: list[str] = []
    while True:
        _fail_fast(proc, profile, phase)
        mismatches, fetch_errors = _aggregate_diff(profile, phase)
        if fetch_errors:
            raise RuntimeError(
                "aggregate fetch failed (not retried so a real fault is never "
                "masked as a transient mismatch):\n" + "\n".join(fetch_errors)
            )
        now = time.time()
        if now - last_log >= interval or not mismatches or now >= deadline:
            sample = "; ".join(mismatches[:3]) if mismatches else "ok"
            log(
                f"    aggregate verify {now - t0:.0f}s: "
                f"mismatches={len(mismatches)} {sample}",
                flush=True,
            )
            last_log = now
        if not mismatches:
            log(f"TIMING  wait [aggregate:{phase}] done in {time.time() - t0:.1f}s", flush=True)
            return []
        last_failures = mismatches
        if now >= deadline:
            return last_failures
        time.sleep(interval)


# ------------------------------------------------------------------- contracts
def verify_contract(profile: Profile) -> list[str]:
    """Topology, schema mapping and type mapping of the target."""
    failures: list[str] = []
    expected_tables = len(profile.tables)
    seen: set[str] = set()
    for spec in profile.tables:
        schema, table = spec.snk_db, spec.sink_name
        if not pg_client.table_exists(schema, table):
            failures.append(f"{schema}.{table}: target table is missing")
            continue
        seen.add(f"{schema}.{table}")
        live = pg_client.table_columns(schema, table)
        want = pg_schemas.sink_types(spec.template)
        wrong = {c: (live.get(c), w) for c, w in want.items() if live.get(c) != w}
        if wrong:
            failures.append(
                f"{schema}.{table}: type map mismatch "
                + "; ".join(f"{c}: got {g!r} want {w!r}" for c, (g, w) in wrong.items())
            )
        extra = sorted(set(live) - set(want))
        if extra:
            failures.append(f"{schema}.{table}: unexpected column(s) {extra}")
        keys = pg_client.primary_keys(schema, table)
        if keys != ["id"]:
            failures.append(f"{schema}.{table}: primary key {keys} != ['id']")
    if len(seen) != expected_tables:
        failures.append(
            f"target exposes {len(seen)} of {expected_tables} planned tables"
        )
    # The suite owns only its own schemas: nothing else may have appeared.
    foreign = [
        name for name in pg_client.schemas()
        if _SCHEMA_RE.match(name) and _SCHEMA_RE.match(name).group("profile") == profile.name
        and name not in {t.snk_db for t in profile.tables}
    ]
    if foreign:
        failures.append(f"unexpected schema(s) for this profile: {sorted(foreign)}")
    return failures


# --------------------------------------------------------------------- process
def ensure_binary() -> Path:
    bin_path = Path(BINARY)
    if bin_path.is_file():
        return bin_path
    bin_path.parent.mkdir(parents=True, exist_ok=True)
    log(f"==> building go-cdc -> {bin_path}", flush=True)
    subprocess.check_call(["go", "build", "-o", str(bin_path), "./cmd/go-cdc"], cwd=str(ROOT))
    return bin_path


def render_pipeline_yaml(profile: Profile, path: Path) -> None:
    """Write the scale pipeline: one route per table, one schema per source DB."""
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
        f"  server-id: {_profile_server_id(profile.name)}",
        "  server-time-zone: Asia/Shanghai",
        "  scan.startup.mode: initial",
        "  scan.incremental.snapshot.chunk.size: 4096",
        "  scan.snapshot.fetch.size: 1024",
        "  schema-change.enabled: false",
        "  connect.timeout: 30s",
        "",
        "sink:",
        "  type: postgresql",
        f"  hostname: {PG_HOST}",
        f"  port: {PG_PORT}",
        f"  username: {PG_USER}",
        f'  password: "{PG_PASSWORD}"',
        f"  database: {PG_DB}",
        f"  schema: {sink_schema(profile.name, 1)}",
        f"  sslmode: {PG_SSLMODE}",
        "  connect-timeout: 30s",
        "  auto-create-table: true",
        "  write-batch-size: 1000",
        "  write-batch-interval: 500ms",
        "  max-retries: 3",
        # The scale model has no DDL: a source schema change must stop the job,
        # not reshape a target that the profile is measuring.
        "  schema.change.enabled: false",
        "  schema.change.drop-truncate.enabled: false",
        "",
        "route:",
    ]
    for spec in profile.tables:
        lines.append(f"  - source-table: '{spec.src_db}\\.{spec.name}'")
        lines.append(f"    sink-table: {spec.snk_db}.{spec.sink_name}")
    lines.extend(
        [
            "",
            "transform:",
            # One regex covers every scale source table (Go regexp fullMatch);
            # the primary key is what the sink's ON CONFLICT needs.
            f"  - source-table: '{SRC_DB_PREFIX}_\\d+\\.tp_scale_\\d+'",
            "    projection: '*'",
            "    primary-keys: id",
            "",
            "pipeline:",
            f"  name: {pg_pipeline_scale_name(profile.name)}",
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


def start_job(profile: Profile, append_log: bool = False) -> subprocess.Popen:
    """Start go-cdc; stdout/stderr are pumped into the profile's log file."""
    bin_path = ensure_binary()
    log_path = _log_path(profile)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    if not append_log:
        log_path.write_text("", encoding="utf-8")
    proc = subprocess.Popen(
        [str(bin_path), str(_yaml_path(profile))],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    def pump() -> None:
        assert proc.stdout is not None
        with log_path.open("a", encoding="utf-8") as handle:
            for line in proc.stdout:
                handle.write(line)
                handle.flush()

    threading.Thread(target=pump, daemon=True).start()
    return proc


def stop_job(proc: subprocess.Popen | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)


def _tail(profile: Profile, limit: int = 2500) -> str:
    path = _log_path(profile)
    if not path.exists():
        return "(no log)"
    return path.read_text(encoding="utf-8", errors="replace")[-limit:]


def _fail_fast(proc: subprocess.Popen | None, profile: Profile, phase: str) -> None:
    if proc is not None and proc.poll() is not None:
        raise RuntimeError(
            f"go-cdc exited during {phase} (code={proc.returncode}):\n{_tail(profile)}"
        )


# -------------------------------------------------------------------- snapshot
def wait_snapshot(
    profile: Profile,
    proc: subprocess.Popen | None = None,
    timeout: float = SNAPSHOT_TIMEOUT_S,
    interval: float = 15.0,
) -> float:
    """Poll until every target table exists and holds the planned row count."""
    t0 = time.time()
    deadline = t0 + timeout
    last_log = 0.0
    while time.time() < deadline:
        _fail_fast(proc, profile, "snapshot")
        ready = 0
        rows_src = 0
        rows_snk = 0
        missing = 0
        for t in profile.tables:
            rows_src += t.rows
            if not pg_client.table_exists(t.snk_db, t.sink_name):
                missing += 1
                continue
            n = pg_client.row_count(t.snk_db, t.sink_name)
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
        time.sleep(interval)
    raise TimeoutError(
        f"timeout waiting for the PostgreSQL scale snapshot after "
        f"{time.time() - t0:.1f}s (ready={ready}/{len(profile.tables)}, missing={missing})"
    )


# ------------------------------------------------------------------- mutations
def _mutated_name(db_index: int, ordinal: int, row_id: int) -> str:
    return f"mut-d{db_index:02d}-t{ordinal:03d}-id{row_id}"


def _mutated_status(row_id: int) -> int:
    return (row_id * 3 + 7) % 128


def _update_rows(conn, spec: TableSpec, id_lo: int, id_hi: int) -> None:
    """Deterministic update of the marker column for ids in [id_lo, id_hi].

    Same values as ``scale.scale_it`` so the two scale reports stay comparable.
    """
    if id_hi < id_lo:
        return
    column = pg_schemas.update_column(spec.template)
    with conn.cursor() as cur:
        for row_id in range(id_lo, id_hi + 1):
            bind: object = (
                _mutated_status(row_id)
                if column == "status"
                else _mutated_name(spec.db_index, spec.ordinal, row_id)
            )
            cur.execute(
                f"UPDATE `{spec.src_db}`.`{spec.name}` SET `{column}`=%s WHERE `id`=%s",
                (bind, row_id),
            )
    conn.commit()


def _updated_id_hi(spec: TableSpec) -> int:
    if spec.event_group == "update":
        return min(100, spec.rows)
    if spec.event_group == "insert_update":
        return min(50, spec.rows)
    return 0


def _mutate_one_table(spec: TableSpec) -> None:
    group = spec.event_group
    if group == "idle":
        return
    rows = spec.rows
    conn = _src_conn(spec.src_db)
    try:
        if group == "insert":
            n = max(1, rows // 20)
            pg_schemas.insert_batch(conn, spec.src_db, spec.name, spec.template,
                                    rows + 1, rows + n, spec.db_index, spec.ordinal)
        elif group == "update":
            id_hi = min(100, rows)
            if id_hi >= 1:
                _update_rows(conn, spec, 1, id_hi)
        elif group == "delete":
            n = min(50, rows)
            if n > 0:
                with conn.cursor() as cur:
                    cur.execute(
                        f"DELETE FROM `{spec.src_db}`.`{spec.name}` "
                        f"WHERE `id` BETWEEN %s AND %s",
                        (rows - n + 1, rows),
                    )
                conn.commit()
        elif group == "insert_update":
            ins = max(1, rows // 40)
            pg_schemas.insert_batch(conn, spec.src_db, spec.name, spec.template,
                                    rows + 1, rows + ins, spec.db_index, spec.ordinal)
            id_hi = min(50, rows)
            if id_hi >= 1:
                _update_rows(conn, spec, 1, id_hi)
        else:
            raise ValueError(f"unknown event_group: {group!r}")
    finally:
        conn.close()


def mutate_all(profile: Profile) -> float:
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


def wait_catchup(
    profile: Profile,
    proc: subprocess.Popen | None = None,
    timeout: float = CATCHUP_TIMEOUT_S,
    interval: float = 15.0,
) -> float:
    """Poll the aggregate gate plus the update markers after the mutations."""
    t0 = time.time()
    deadline = t0 + timeout
    last_failures: list[str] = []
    last_log = 0.0
    while time.time() < deadline:
        _fail_fast(proc, profile, "catchup")
        last_failures = verify_pair(profile, "catchup")
        if not last_failures:
            last_failures = verify_update_markers(profile, "catchup")
        now = time.time()
        if now - last_log >= interval or not last_failures:
            sample = "; ".join(last_failures[:3]) if last_failures else "ok"
            log(
                f"    mutate catch-up {now - t0:.0f}s: "
                f"mismatches={len(last_failures)} {sample}",
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
        f"timeout waiting for the PostgreSQL scale catch-up after "
        f"{time.time() - t0:.1f}s ({len(last_failures)} mismatches):\n{sample}"
    )


# -------------------------------------------------------------- hotspots (heavy)
def _table_key(spec: TableSpec) -> str:
    return f"{spec.src_db}.{spec.name}"


def _max_id(spec: TableSpec) -> int:
    conn = _src_conn(spec.src_db)
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
    conn = _src_conn(spec.src_db)
    try:
        pg_schemas.insert_batch(conn, spec.src_db, spec.name, spec.template,
                                start, end, spec.db_index, spec.ordinal)
    finally:
        conn.close()
    return end


def burst_hotspots(
    profile: Profile,
    hotspots: list[TableSpec],
    proc: subprocess.Popen | None = None,
) -> tuple[float, dict[str, int]]:
    """Fixed-row burst into the hotspot tables; returns (elapsed, next_id_by_key)."""
    n_rows = SCALE_BURST_ROWS_PER_HOT
    log(
        f"==> hotspot burst rows_per_hot={n_rows} tables={len(hotspots)} "
        f"workers={profile.workers}",
        flush=True,
    )
    for h in hotspots:
        log(f"    hotspot {h.src_db}.{h.name} (ordinal={h.ordinal} template={h.template})",
            flush=True)
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
            conn = conns.get(spec.src_db)
            if conn is None:
                conn = _src_conn(spec.src_db)
                conns[spec.src_db] = conn
            pg_schemas.insert_batch(conn, spec.src_db, spec.name, spec.template,
                                    start, start + batch - 1, spec.db_index, spec.ordinal)
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
    """Time-based peak into the hotspot set; returns (elapsed, rows written)."""
    peak_workers = max(1, int(os.environ.get("PG_SCALE_PEAK_WORKERS",
                                             os.environ.get("SCALE_PEAK_WORKERS",
                                                            str(profile.workers)))))
    seconds = SCALE_PEAK_SECONDS
    batch = SCALE_PEAK_BATCH
    log(f"==> hotspot peak seconds={seconds} workers={peak_workers} batch={batch}", flush=True)
    stop_at = time.time() + seconds
    lock = threading.Lock()
    t0 = time.time()
    _fail_fast(proc, profile, "peak")
    totals: list[int] = []
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
    log(f"TIMING peak_write {elapsed:.1f}s ({rows} rows ~ {rows / elapsed:.0f} rps)", flush=True)
    return elapsed, rows


def wait_hotspot_convergence(
    profile: Profile,
    hotspots: list[TableSpec],
    deadline: float,
    interval: float = 15.0,
    proc: subprocess.Popen | None = None,
) -> float:
    """Poll until every hotspot's source and target row counts are exactly equal.

    Counts are the progress gate only (the delta percentage is trend display);
    content equality is decided by ``verify_rows_eventual`` within the same
    ``deadline``. No percentage tolerance is accepted here.
    """
    t0 = time.time()
    last_log = 0.0
    while time.time() < deadline:
        _fail_fast(proc, profile, "catch-up")
        ok = True
        lines: list[str] = []
        for h in hotspots:
            src_n = _count_src(h)
            snk_n = pg_client.row_count(h.snk_db, h.sink_name)
            delta = abs(src_n - snk_n) / max(src_n, 1)
            lines.append(f"{h.src_db}.{h.name} src={src_n} snk={snk_n} delta={delta:.2%}")
            if src_n != snk_n:
                ok = False
        now = time.time()
        if now - last_log >= interval or ok:
            log(f"    hotspot catch-up {now - t0:.0f}s: " + " | ".join(lines), flush=True)
            last_log = now
        if ok:
            elapsed = time.time() - t0
            log(f"TIMING peak_catchup {elapsed:.1f}s", flush=True)
            return elapsed
        time.sleep(interval)
    raise TimeoutError(
        f"timeout waiting for hotspot count convergence after {time.time() - t0:.1f}s"
    )


def _count_src(spec: TableSpec) -> int:
    conn = _src_conn(spec.src_db)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM `{spec.src_db}`.`{spec.name}`")
            row = cur.fetchone()
    finally:
        conn.close()
    return int(row["n"]) if row else 0


# ------------------------------------------------------- checkpoint recovery
def restart_recovery(
    profile: Profile,
    proc: subprocess.Popen,
    deadline: float,
) -> tuple[subprocess.Popen, dict[str, object]]:
    """PG-205: stop the job, write to the source, restart on the same checkpoint.

    The checkpoint file the stopped job wrote must still be there before the
    restart (that is what "resume from an existing checkpoint" means), and the
    rows written while the job was down must reach the target. A missing file
    fails the phase: without it the restart only re-snapshots from scratch and
    the resume path was never exercised, so a green row comparison would say
    nothing about checkpoint recovery. The pass/fail decision otherwise is the
    same row-level comparison as everywhere else.

    Assumes a graceful SIGTERM lets the job flush its checkpoint (POSIX).
    """
    log("==> restart recovery check (stop, write at source, resume)", flush=True)
    stop_job(proc)
    ckpt = _ckpt_file(profile)
    pre_failures: list[str] = []
    if not ckpt.exists():
        pre_failures.append(
            f"no checkpoint file at {project_display_path(ckpt)} before the restart: "
            "the resume path was not exercised"
        )
    info: dict[str, object] = {
        "checkpoint": project_display_path(ckpt),
        "checkpoint_present": ckpt.exists(),
        "checkpoint_bytes": ckpt.stat().st_size if ckpt.exists() else 0,
    }
    spec = profile.tables[0]
    start = _max_id(spec) + 1
    conn = _src_conn(spec.src_db)
    try:
        pg_schemas.insert_batch(conn, spec.src_db, spec.name, spec.template,
                                start, start + RESTART_ROWS - 1, spec.db_index, spec.ordinal)
    finally:
        conn.close()
    info["rows_written_while_down"] = RESTART_ROWS
    info["table"] = f"{spec.src_db}.{spec.name}"
    log(
        f"    checkpoint kept: {ckpt.name} ({info['checkpoint_bytes']} bytes); "
        f"inserted {RESTART_ROWS} rows into {spec.src_db}.{spec.name} while the job was down",
        flush=True,
    )
    t_restart = time.time()
    proc = start_job(profile, append_log=True)
    gate = verify_pair_eventual(profile, deadline, proc=proc, phase="restart")
    if gate:
        info["gate_failures"] = gate[:5]
        info["failures"] = pre_failures[:10]
        info["seconds"] = round(time.time() - t_restart, 1)
        info["ok"] = False
        return proc, info
    failures = verify_rows_eventual(profile, deadline, "restart", proc=proc)
    info["failures"] = (pre_failures + failures)[:10]
    info["seconds"] = round(time.time() - t_restart, 1)
    info["ok"] = not info["failures"]
    return proc, info


# ---------------------------------------------------------------------- report
def report_path(profile: Profile) -> Path:
    name = REPORT_PG_SCALE_TMPL.format(profile=profile.name)
    return REPORT_DIR / name


def write_report(
    profile: Profile,
    timings: dict[str, float],
    failures: list[str],
    extra: dict[str, object] | None = None,
) -> Path:
    out = report_path(profile)
    out.parent.mkdir(parents=True, exist_ok=True)
    extra = extra or {}
    passed = not failures
    src_dbs = sorted({t.src_db for t in profile.tables})
    schemas = sorted({t.snk_db for t in profile.tables})
    group_counts: dict[str, int] = {}
    for t in profile.tables:
        group_counts[t.event_group] = group_counts.get(t.event_group, 0) + 1

    lines = [
        f"# go-cdc mysql2postgresql scale report ({profile.name})\n",
        f"- Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Result: {'✅ PASS' if passed else '❌ FAIL'}",
        f"- Profile: `{profile.name}` (workers / parallelism: {profile.workers})",
        f"- Stopped after phase: `{extra.get('until', 'all')}`",
        f"- Tables / planned rows: {len(profile.tables)} / {profile.total_rows}",
        f"- Source DBs ({len(src_dbs)}): `{', '.join(src_dbs)}`",
        f"- Target schemas ({len(schemas)}): `{', '.join(schemas)}`",
        f"- Target: `{PG_HOST}:{PG_PORT}` / `{PG_DB}`",
        f"- Pipeline: `{pg_pipeline_scale_name(profile.name)}`, "
        f"checkpoint `{it_rel_path(_ckpt_dir(profile))}`",
        "",
        "## Event groups\n",
        "| Group | Tables |",
        "| --- | ---: |",
    ]
    for g in ("insert", "update", "delete", "insert_update", "idle"):
        lines.append(f"| {g} | {group_counts.get(g, 0)} |")
    lines.append("")

    lines.append("## Verification\n")
    lines.append(
        "- Gate: per-table aggregate `(COUNT(*), SUM(id), SUM(CHAR_LENGTH/length(marker)))` "
        "on both engines; polling only, never the pass criterion on its own."
    )
    lines.append(
        f"- Acceptance: row-level compare per table in primary-key windows of "
        f"`{VERIFY_CHUNK}` ids (PK set, every projected column, NULL, binary, "
        f"timestamp, JSON, duplicate keys on the target) through `harness.pg_verify`."
    )
    lines.append(
        f"- Update markers: sampled ids 1 / mid / max per mutated table on both sides."
    )
    lines.append(
        "- Contract: target schema set, table count, per-column `format_type`, "
        "primary key `(id)`."
    )
    lines.append(
        f"- Timeouts: snapshot `{SNAPSHOT_TIMEOUT_S:.0f}s`, mutation catch-up "
        f"`{CATCHUP_TIMEOUT_S:.0f}s`, peak / restart / final budget "
        f"`{PEAK_CATCHUP_BUDGET_S:.0f}s`. The PR3 spec recorded 1800 s for peak "
        "catch-up; the shared 3600 s budget is kept and the difference is recorded "
        "here instead of re-timing the phases."
    )
    lines.append(
        f"- Isolation: run `{profile.name}` owns only `{SINK_SCHEMA_PREFIX}_{profile.name}_*` "
        f"schemas and `{it_rel_path(_ckpt_dir(profile))}`; source databases are "
        f"`{SRC_DB_PREFIX}_*`. Other profiles' schemas, `public` and the "
        "MySQL->MySQL / Doris / Elasticsearch scale objects are never touched."
    )
    if extra.get("dropped_schemas"):
        lines.append(
            "- Schemas dropped before the run: "
            + ", ".join(f"`{s}`" for s in extra["dropped_schemas"])  # type: ignore[union-attr]
        )
    lines.append("")

    lines.append("## Template and type differences\n")
    lines.extend(pg_schemas.report_deviations())
    lines.append("")

    if extra.get("hotspots"):
        lines.append("## Hotspot burst / peak (heavy)\n")
        lines.append(f"- Burst rows per hotspot table: `{SCALE_BURST_ROWS_PER_HOT}`")
        lines.append(f"- Peak seconds: `{SCALE_PEAK_SECONDS}`")
        lines.append(f"- Peak workers: `{extra.get('peak_workers')}`")
        lines.append(f"- Peak batch: `{SCALE_PEAK_BATCH}`")
        lines.append(f"- Peak rows written (measured): `{extra.get('peak_rows')}`")
        lines.append(f"- Peak catch-up budget: `{PEAK_CATCHUP_BUDGET_S:.0f}s`")
        lines.append("- Hotspot tables:")
        for name in extra["hotspots"]:  # type: ignore[union-attr]
            lines.append(f"  - `{name}`")
        lines.append("")

    if extra.get("restart"):
        info = extra["restart"]  # type: ignore[assignment]
        lines.append("## Checkpoint recovery (PG-205)\n")
        lines.append(f"- Source table written while the job was down: `{info.get('table')}`")
        lines.append(f"- Rows written while down: `{info.get('rows_written_while_down')}`")
        lines.append(f"- Checkpoint kept before the restart: `{info.get('checkpoint')}` "
                     f"({'present' if info.get('checkpoint_present') else 'MISSING'}, "
                     f"{info.get('checkpoint_bytes')} bytes)")
        lines.append(f"- Restart + catch-up + row-level verify: `{info.get('seconds')}`s")
        lines.append(f"- Outcome: {'✅ equal' if info.get('ok') else '❌ differences'}")
        if not info.get("checkpoint_present"):
            lines.append(
                "- Outcome note: no checkpoint was reused, so the restart re-snapshotted from "
                "scratch and the rows above do not prove the resume path"
            )
        for line in (info.get("gate_failures") or []) + (info.get("failures") or []):
            lines.append(f"  - `{line}`")
        lines.append("")

    if timings:
        lines.extend(["## Phase timings\n", "| Phase | Seconds |", "| --- | ---: |"])
        for name, sec in timings.items():
            lines.append(f"| {name} | {sec:.1f} |")
        lines.append("")

    if failures:
        lines.append("## Failures\n")
        for msg in failures:
            lines.append(f"- `{msg}`")
        lines.append("")
    else:
        lines.append("## Summary\n")
        stages = ["the snapshot"]
        if "cdc_catchup" in timings:
            stages.append("the mutations")
        if extra.get("hotspots"):
            stages.append("the heavy hotspot burst + peak")
        if extra.get("restart"):
            stages.append("the checkpoint restart")
        lines.append(
            f"All {len(profile.tables)} source/target pairs matched at row level "
            "(primary keys, every column, NULL/binary/timestamp/JSON) after "
            + ", ".join(stages)
            + "."
        )
        lines.append("")

    lines.append("## Tables\n")
    lines.append("| ordinal | src | target | template | rows | event_group |")
    lines.append("| ---: | --- | --- | --- | ---: | --- |")
    for t in profile.tables:
        lines.append(
            f"| {t.ordinal} | `{t.src_db}.{t.name}` | `{t.snk_db}.{t.sink_name}` | "
            f"{t.template} | {t.rows} | {t.event_group} |"
        )
    lines.append("")
    out.write_text(sanitize_report_text("\n".join(lines)), encoding="utf-8")
    return out


# ----------------------------------------------------------------------- drill
def run_profile(profile: Profile, until: str) -> int:
    """Run the scale phases through ``until`` (seed|snapshot|mutate|restart|all)."""
    log(
        f"==> pg scale profile={profile.name} tables={len(profile.tables)} "
        f"total_rows={profile.total_rows} workers={profile.workers} until={until}",
        flush=True,
    )
    check_pg_target()
    log(f"==> postgresql {pg_client.server_version()} at {PG_HOST}:{PG_PORT}/{PG_DB}", flush=True)

    timings: dict[str, float] = {}
    failures: list[str] = []
    extra: dict[str, object] = {}
    proc: subprocess.Popen | None = None

    dropped = clean_target_schemas(profile)
    extra["dropped_schemas"] = dropped
    extra["until"] = until
    t0 = time.time()
    log("==> create source schema", flush=True)
    ensure_source(profile)
    log(f"    {format_disk(WORKDIR)}", flush=True)
    log("==> seed baseline", flush=True)
    timings["seed_baseline"] = seed_all(profile)
    timings["schema_setup"] = time.time() - t0
    log(f"TIMING seed_baseline {timings['seed_baseline']:.1f}s", flush=True)
    if until == "seed":
        log("==> stopped after seed: nothing was verified, so no report is written", flush=True)
        return 0

    clear_checkpoint(profile)
    yaml_path = _yaml_path(profile)
    render_pipeline_yaml(profile, yaml_path)
    log(f"==> rendered {yaml_path}", flush=True)

    log("==> start go-cdc (snapshot -> postgresql)", flush=True)
    proc = start_job(profile)
    try:
        timings["snapshot"] = wait_snapshot(profile, proc)
        gate = verify_pair(profile, "snapshot")
        # The aggregate is the gate to the (expensive) row pass; the contract is
        # metadata only, so it is always checked and can never be masked by it.
        snapshot_failures = list(gate)
        if not gate:
            snapshot_failures.extend(verify_rows(profile, "snapshot"))
        snapshot_failures.extend(verify_contract(profile))
        if snapshot_failures:
            failures.extend(snapshot_failures)
            for msg in snapshot_failures:
                log(f"FAIL {msg}", file=sys.stderr, flush=True)
            write_report(profile, timings, failures, extra)
            log(f"REPORT {project_display_path(report_path(profile))}", flush=True)
            return 1
        log(f"==> snapshot verify ok ({len(profile.tables)} tables, row level)", flush=True)
        if until == "snapshot":
            return 0

        log("==> concurrent mutations", flush=True)
        timings["mutate"] = mutate_all(profile)
        log(f"TIMING mutate {timings['mutate']:.1f}s", flush=True)
        timings["cdc_catchup"] = wait_catchup(profile, proc)
        log(f"TIMING cdc_catchup {timings['cdc_catchup']:.1f}s", flush=True)
        if until == "mutate":
            return 0

        if profile.name == "heavy":
            hotspots = hotspot_tables(profile)
            burst_s, next_ids = burst_hotspots(profile, hotspots, proc=proc)
            timings["burst"] = burst_s
            log(f"TIMING burst {burst_s:.1f}s", flush=True)

            peak_s, peak_rows = peak_write_hotspots(profile, hotspots, next_ids, proc=proc)
            timings["peak_write"] = peak_s
            extra.update(
                {
                    "hotspots": [f"{h.src_db}.{h.name}" for h in hotspots],
                    "peak_rows": peak_rows,
                    "peak_workers": max(1, int(os.environ.get(
                        "PG_SCALE_PEAK_WORKERS",
                        os.environ.get("SCALE_PEAK_WORKERS", str(profile.workers)),
                    ))),
                }
            )
            # One budget shared by count convergence, the aggregate gate and the
            # row-level pass; the source is static from here on.
            catchup_deadline = time.time() + PEAK_CATCHUP_BUDGET_S
            timings["peak_catchup"] = wait_hotspot_convergence(
                profile, hotspots, catchup_deadline, proc=proc
            )
            gate = verify_pair_eventual(profile, catchup_deadline, proc=proc, phase="peak")
            if gate:
                failures.extend(gate)
                for msg in gate:
                    log(f"FAIL {msg}", file=sys.stderr, flush=True)
                write_report(profile, timings, failures, extra)
                log(f"REPORT {project_display_path(report_path(profile))}", flush=True)
                return 1
            row_failures = verify_rows_eventual(profile, catchup_deadline, "peak", proc=proc)
            if row_failures:
                failures.extend(row_failures)
                for msg in row_failures[:10]:
                    log(f"FAIL {msg}", file=sys.stderr, flush=True)

        # PG-205: stop writing, add rows while the job is down, resume on the
        # checkpoint it left behind, then require exact equality.
        recovery_deadline = time.time() + PEAK_CATCHUP_BUDGET_S
        proc, info = restart_recovery(profile, proc, recovery_deadline)
        extra["restart"] = info
        timings["restart"] = float(info.get("seconds") or 0.0)
        if not info.get("ok"):
            reasons = (info.get("failures") or []) + (info.get("gate_failures") or [])
            failures.append("restart recovery: " + "; ".join(reasons[:3] or ["unknown"]))
        if until == "restart":
            # The `finally` block writes the report for every exit path.
            return 0 if not failures else 1

        final_deadline = time.time() + PEAK_CATCHUP_BUDGET_S
        final_failures = verify_pair_eventual(profile, final_deadline, proc=proc, phase="final")
        if not final_failures:
            final_failures = verify_rows_eventual(profile, final_deadline, "final", proc=proc)
        final_failures.extend(verify_contract(profile))
        failures.extend(final_failures)
        for msg in final_failures[:10]:
            log(f"FAIL {msg}", file=sys.stderr, flush=True)
        if not final_failures:
            log(f"==> final verify ok ({len(profile.tables)} tables, row level)", flush=True)
    except Exception as exc:  # noqa: BLE001
        failures.append(f"aborted: {exc}")
        raise
    finally:
        timings["suite_total"] = time.time() - t0
        write_report(profile, timings, failures, extra)
        log(f"REPORT {project_display_path(report_path(profile))}", flush=True)
        stop_job(proc)

    if failures:
        log("SUMMARY FAIL", flush=True)
        return 1
    log("SUMMARY PASS", flush=True)
    return 0


def _print_plan(profile: Profile) -> None:
    log(f"profile={profile.name}")
    log(f"tables={len(profile.tables)}")
    log(f"total_rows={profile.total_rows}")
    log(f"workers={profile.workers}")
    log(f"src_dbs={len({t.src_db for t in profile.tables})}")
    log(f"target_schemas={len({t.snk_db for t in profile.tables})}")
    log(f"hotspots={len(hotspot_tables(profile))}")
    log(f"checkpoint={it_rel_path(_ckpt_dir(profile))}")
    log(f"pipeline={pg_pipeline_scale_name(profile.name)}")
    dropped = {t.template: pg_schemas.dropped_columns(t.template) for t in profile.tables}
    log(f"dropped_columns={ {k: v for k, v in dropped.items() if v} }")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="go-cdc PostgreSQL scale suite")
    parser.add_argument("profile", nargs="?", default=None, help="profile name (normal|heavy)")
    parser.add_argument(
        "--print-plan",
        dest="print_plan",
        metavar="PROFILE",
        help="print the topology and exit (no MySQL/PostgreSQL needed)",
    )
    parser.add_argument(
        "--until",
        choices=("seed", "snapshot", "mutate", "restart", "all"),
        default="all",
        help="stop after a phase (default: all)",
    )
    parser.add_argument("--total-rows", type=int, default=None, help="override profile total_rows")
    parser.add_argument("--workers", type=int, default=None, help="override profile workers")
    args = parser.parse_args(argv)

    total_rows = args.total_rows
    if total_rows is None and os.environ.get("SCALE_TOTAL_ROWS"):
        total_rows = int(os.environ["SCALE_TOTAL_ROWS"])
    workers = args.workers
    if workers is None and os.environ.get("SCALE_WORKERS"):
        workers = int(os.environ["SCALE_WORKERS"])

    if args.print_plan:
        _print_plan(pg_scale_profile(load_profile(args.print_plan, total_rows, workers)))
        return 0
    if args.profile:
        return run_profile(
            pg_scale_profile(load_profile(args.profile, total_rows, workers)), args.until
        )
    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
