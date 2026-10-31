#!/usr/bin/env python3
"""go-cdc mysql2postgresql E2E harness (TP-P01..TP-P20).

Reuses the mysql2mysql shape: the same job control, fixture rendering, report
layout and row-level verification style as ``sinks/mysql/harness/run_it.py``, with the
target read through psycopg2 (``sinks/postgresql/harness/pg_client.py``) and compared by the
independent oracle (``sinks/postgresql/harness/pg_verify.py``). MySQL stays the only source; the
sink is one PostgreSQL database and suites are isolated by schema.

Run it with `scripts/run.sh postgresql`; the report lands in
`docs/test/reports/mysql-postgresql-func.md`. Requirement mapping and the status
vocabulary live in `docs/test/POSTGRESQL.md`.
"""
from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pymysql

from common import config as cfg
from . import pg_client, pg_verify
from common.logutil import log

WORKDIR = cfg.WORKDIR
BINARY = cfg.BINARY
YAML = WORKDIR / "pipeline.pg.yaml"
LOG = WORKDIR / "cdc-pg.log"
REPORT_DIR = cfg.REPORT_DIR

SRC_A = cfg.PG_SRC_DB_A
SRC_B = cfg.PG_SRC_DB_B
SNK_A = cfg.PG_SNK_SCHEMA_A
SNK_B = cfg.PG_SNK_SCHEMA_B

# Session key -> the tables captured by that pipeline. Each session renders its
# own YAML (and most of them its own checkpoint), because a refused DDL or a
# permanent write error stops the job by design.
SESSIONS = ("main", "switch", "resume", "gate", "refused", "fault", "unsupported", "perm",
            "restart")


@dataclass
class TableSpec:
    name: str
    session: str
    ddl: str
    columns: tuple[str, ...]
    kinds: dict[str, str]
    key: tuple[str, ...] = ("id",)
    dbs: tuple[str, ...] = (SRC_A,)
    order: str = "id"

    @property
    def sink_name(self) -> str:
        return f"{self.name}_out"


def _schema_of(db: str) -> str:
    return SNK_B if db == SRC_B else SNK_A


TINYINT = "int"
INTS = {
    "c_tiny": TINYINT, "c_utiny": TINYINT, "c_small": TINYINT, "c_usmall": TINYINT,
    "c_med": TINYINT, "c_int": TINYINT, "c_uint": TINYINT, "c_big": TINYINT,
    "c_ubig": "decimal", "c_year": TINYINT, "c_bool": TINYINT,
}

TABLES: dict[str, TableSpec] = {}


def _register(spec: TableSpec) -> TableSpec:
    TABLES[spec.name] = spec
    return spec


_register(TableSpec(
    name="tp_p01_snap", session="main", dbs=(SRC_A, SRC_B),
    ddl="""CREATE TABLE `{t}` (
  id BIGINT NOT NULL PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  amount DECIMAL(12,4) NOT NULL DEFAULT 0.0000,
  c_dt DATETIME(3) NULL,
  c_json JSON NULL,
  c_bin BLOB NULL,
  note VARCHAR(64) NULL,
  c_default VARCHAR(16) NOT NULL DEFAULT 'dflt'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    columns=("id", "name", "amount", "c_dt", "c_json", "c_bin", "note", "c_default"),
    kinds={"id": "int", "name": "text", "amount": "decimal", "c_dt": "datetime",
           "c_json": "json", "c_bin": "bytes", "note": "text", "c_default": "text"},
))

_register(TableSpec(
    name="tp_p01_empty", session="main",
    ddl="CREATE TABLE `{t}` (id BIGINT NOT NULL PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB",
    columns=("id", "v"), kinds={"id": "int", "v": "int"},
))

_register(TableSpec(
    name="tp_p02_switch", session="switch",
    ddl="CREATE TABLE `{t}` (id BIGINT NOT NULL PRIMARY KEY, v INT NOT NULL, tag VARCHAR(32) NULL) ENGINE=InnoDB",
    columns=("id", "v", "tag"), kinds={"id": "int", "v": "int", "tag": "text"},
))

_register(TableSpec(
    name="tp_p03_crud", session="main",
    ddl="CREATE TABLE `{t}` (id BIGINT NOT NULL PRIMARY KEY, v INT NOT NULL, note VARCHAR(64) NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    columns=("id", "v", "note"), kinds={"id": "int", "v": "int", "note": "text"},
))

_register(TableSpec(
    name="tp_p04_pk", session="main", key=("docid",), order="docid",
    ddl="CREATE TABLE `{t}` (docid VARCHAR(32) NOT NULL PRIMARY KEY, title VARCHAR(64) NOT NULL, seq INT NOT NULL DEFAULT 0) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    # The projection keeps docid,title, so the oracle must not ask for seq.
    columns=("docid", "title"), kinds={"docid": "text", "title": "text"},
))

_register(TableSpec(
    name="tp_p05_filter", session="main",
    ddl="CREATE TABLE `{t}` (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(64) NOT NULL, city VARCHAR(16) NOT NULL, amount INT NOT NULL DEFAULT 0) ENGINE=InnoDB",
    # The projection drops `amount`, so the comparison must not ask for it.
    columns=("id", "name", "city"), kinds={"id": "int", "name": "text", "city": "text"},
))

_register(TableSpec(
    name="tp_p06_del", session="main", key=("id", "sub"),
    ddl="CREATE TABLE `{t}` (id BIGINT NOT NULL, sub INT NOT NULL, v INT NOT NULL, PRIMARY KEY (id, sub)) ENGINE=InnoDB",
    columns=("id", "sub", "v"), kinds={"id": "int", "sub": "int", "v": "int"}, order="id, sub",
))

_register(TableSpec(
    name="tp_p07_ddl", session="main",
    ddl="CREATE TABLE `{t}` (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(64) NOT NULL) ENGINE=InnoDB",
    columns=("id", "name"), kinds={"id": "int", "name": "text"},
))

_register(TableSpec(
    name="tp_p08_off", session="refused",
    ddl="""CREATE TABLE `{t}` (
  id BIGINT NOT NULL PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  doomed INT NULL,
  keepme INT NOT NULL DEFAULT 1,
  code VARCHAR(32) NULL,
  UNIQUE KEY uq_code (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    columns=("id", "name", "doomed", "keepme"),
    kinds={"id": "int", "name": "text", "doomed": "int", "keepme": "int"},
))

_register(TableSpec(
    name="tp_p09_gate", session="gate",
    ddl="CREATE TABLE `{t}` (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(64) NOT NULL, doomed INT NULL) ENGINE=InnoDB",
    columns=("id", "name", "doomed"), kinds={"id": "int", "name": "text", "doomed": "int"},
))

_register(TableSpec(
    name="tp_p10_route", session="main", dbs=(SRC_A, SRC_B),
    ddl="CREATE TABLE `{t}` (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL) ENGINE=InnoDB",
    columns=("id", "payload"), kinds={"id": "int", "payload": "text"},
))

# Mixed-case identifier: the sink quotes every identifier, so PostgreSQL keeps
# the case and the routing must not silently fold it to lowercase.
_register(TableSpec(
    name="tp_P10_Mixed", session="main", dbs=(SRC_B,), order="id",
    ddl="CREATE TABLE `{t}` (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL) ENGINE=InnoDB",
    columns=("id", "payload"), kinds={"id": "int", "payload": "text"},
))

for _name in ("tp_p11_resume", "tp_p12_kill", "tp_p13_m3"):
    _register(TableSpec(
        name=_name, session="resume",
        ddl="CREATE TABLE `{t}` (id BIGINT NOT NULL PRIMARY KEY, v INT NOT NULL, note VARCHAR(32) NULL) ENGINE=InnoDB",
        columns=("id", "v", "note"), kinds={"id": "int", "v": "int", "note": "text"},
    ))

_register(TableSpec(
    name="tp_p14_fault", session="fault",
    ddl="CREATE TABLE `{t}` (id BIGINT NOT NULL PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB",
    columns=("id", "v"), kinds={"id": "int", "v": "int"},
))

_register(TableSpec(
    name="tp_p15_replay", session="fault",
    ddl="CREATE TABLE `{t}` (id BIGINT NOT NULL PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB",
    columns=("id", "v"), kinds={"id": "int", "v": "int"},
))

_register(TableSpec(
    name="tp_p16_types", session="main",
    ddl="""CREATE TABLE `{t}` (
  id BIGINT NOT NULL PRIMARY KEY,
  c_tiny TINYINT NULL, c_utiny TINYINT UNSIGNED NULL,
  c_small SMALLINT NULL, c_usmall SMALLINT UNSIGNED NULL,
  c_med MEDIUMINT NULL, c_int INT NULL, c_uint INT UNSIGNED NULL,
  c_big BIGINT NULL, c_ubig BIGINT UNSIGNED NULL,
  c_dec DECIMAL(18,6) NULL, c_float FLOAT NULL, c_double DOUBLE NULL,
  c_char CHAR(8) NULL, c_vc VARCHAR(120) NULL, c_text TEXT NULL,
  c_bin BINARY(4) NULL, c_vbin VARBINARY(16) NULL, c_blob BLOB NULL,
  c_date DATE NULL, c_dt DATETIME(6) NULL, c_ts TIMESTAMP(6) NULL,
  c_year YEAR NULL, c_json JSON NULL, c_bool TINYINT(1) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    columns=("id", "c_tiny", "c_utiny", "c_small", "c_usmall", "c_med", "c_int",
             "c_uint", "c_big", "c_ubig", "c_dec", "c_float", "c_double", "c_char",
             "c_vc", "c_text", "c_bin", "c_vbin", "c_blob", "c_date", "c_dt", "c_ts",
             "c_year", "c_json", "c_bool"),
    kinds={**INTS, "id": "int", "c_dec": "decimal", "c_float": "float",
           "c_double": "float", "c_char": "char", "c_vc": "text", "c_text": "text",
           "c_bin": "bytes", "c_vbin": "bytes", "c_blob": "bytes", "c_date": "date",
           "c_dt": "datetime", "c_ts": "datetime", "c_json": "json"},
))

# Expected pg_catalog types for tp_p16_types (docs/connectors/postgresql-sink.md
# "Column Type Mapping"), asserted after the table is created by the sink.
EXPECTED_PG_TYPES = {
    "id": "bigint", "c_tiny": "smallint", "c_utiny": "smallint",
    "c_small": "integer", "c_usmall": "integer", "c_med": "integer",
    "c_int": "integer", "c_uint": "bigint", "c_big": "bigint",
    "c_ubig": "numeric(20,0)", "c_dec": "numeric(18,6)", "c_float": "real",
    "c_double": "double precision", "c_char": "character(8)",
    "c_vc": "character varying(120)", "c_text": "text", "c_bin": "bytea",
    "c_vbin": "bytea", "c_blob": "bytea", "c_date": "date",
    "c_dt": "timestamp(6) without time zone",
    "c_ts": "timestamp(6) without time zone", "c_year": "smallint",
    "c_json": "json", "c_bool": "smallint",
}

_register(TableSpec(
    name="tp_p16_refused", session="unsupported",
    ddl="CREATE TABLE `{t}` (id BIGINT NOT NULL PRIMARY KEY, c_time TIME NULL) ENGINE=InnoDB",
    columns=("id",), kinds={"id": "int"},
))

_register(TableSpec(
    name="tp_p17_restart", session="restart",
    ddl="CREATE TABLE `{t}` (id BIGINT NOT NULL PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB",
    columns=("id", "v"), kinds={"id": "int", "v": "int"},
))

# No primary key at the source and none declared by a transform: the sink renders a
# plain INSERT, which is the documented at-least-once limit (TP-P20). The source
# still needs a NOT NULL column to split the snapshot on, so the table runs in its
# own session with `scan.incremental.snapshot.chunk.key-column` (a chunk key column
# must exist on every table of the session, and tp_p04_pk has no `id`).
_register(TableSpec(
    name="tp_p20_nokey", session="nokey",
    ddl="CREATE TABLE `{t}` (id INT NOT NULL, v VARCHAR(32) NOT NULL) ENGINE=InnoDB",
    columns=("id", "v"), kinds={"id": "int", "v": "text"},
))

_register(TableSpec(
    name="tp_p19_perm", session="perm",
    ddl="CREATE TABLE `{t}` (id BIGINT NOT NULL PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB",
    columns=("id", "v"), kinds={"id": "int", "v": "int"},
))


# ---------------------------------------------------------------- report shape

@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""


@dataclass
class Case:
    id: str
    title: str
    action: str
    expected: str
    checks: list[Check] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    status: str = ""

    @property
    def passed(self) -> bool:
        return self.status == "PASS"


cases: list[Case] = []


def new_case(tp_id: str, title: str, action: str, expected: str) -> Case:
    case = Case(id=tp_id, title=title, action=action, expected=expected)
    cases.append(case)
    return case


def check(case: Case, name: str, ok: bool, detail: str = "") -> bool:
    case.checks.append(Check(name=name, ok=bool(ok), detail=detail))
    log(f"    [{'ok' if ok else 'BAD'}] {case.id} {name}" + (f" - {detail}" if detail else ""), flush=True)
    return bool(ok)


def note(case: Case, text: str) -> None:
    case.evidence.append(text)


def finish(case: Case, status: str = "") -> Case:
    """Score a case: PASS when every check held; SKIPPED/NOT RUN are explicit."""
    if status:
        case.status = status
    else:
        case.status = "PASS" if case.checks and all(c.ok for c in case.checks) else "FAIL"
    log(f"[{case.id}] {case.status} {case.title}", flush=True)
    return case


# ------------------------------------------------------------------- job control

proc: subprocess.Popen | None = None
active_log = LOG
active_yaml = YAML


def start_job(yaml_path: Path = YAML, log_path: Path = LOG) -> subprocess.Popen:
    global proc, active_log, active_yaml
    active_log = log_path
    active_yaml = yaml_path
    log_path.write_text("")
    proc = subprocess.Popen(
        [BINARY, str(yaml_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    def pump() -> None:
        assert proc and proc.stdout
        with log_path.open("a") as handle:
            for line in proc.stdout:
                handle.write(line)
                handle.flush()

    threading.Thread(target=pump, daemon=True).start()
    return proc


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


def kill_job_hard() -> int | None:
    """SIGKILL: no graceful checkpoint flush, no drain of the held batch."""
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


def wait_until(predicate, timeout: float = 120, interval: float = 1.0, desc: str = "condition") -> None:
    deadline = time.time() + timeout
    last: object | None = None
    while time.time() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(interval)
    raise TimeoutError(f"timeout waiting for {desc}: {last}")


def wait_exit(timeout: float = 60, desc: str = "job exit") -> int:
    assert proc is not None
    try:
        code = proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(f"{desc}: job still running after {timeout}s") from exc
    stop_job()
    return code if code is not None else -1


def job_alive() -> bool:
    return proc is not None and proc.poll() is None


def log_text() -> str:
    return active_log.read_text(encoding="utf-8", errors="replace") if active_log.exists() else ""


def log_contains(needle: str) -> bool:
    return needle in log_text()


def log_tail(lines: int = 12) -> str:
    body = [ln for ln in log_text().splitlines() if ln.strip()]
    return " | ".join(body[-lines:])


def log_errors(limit: int = 4, stack: int = 8) -> str:
    """Return the pipeline's own fatal records, not the last lines written.

    On a failure the process logs `pipeline failed` at level=ERROR and only then
    closes the source, which appends three INFO shutdown lines (`closing canal`,
    `syncer is closing...`, `syncer is closed`). A plain tail therefore hides the
    one line that says why the job died, so every failure report quotes the
    ERROR record instead and only falls back to the tail when there is none.

    A Go crash is quoted together with the goroutine stack that follows it: the
    `panic:` line names the value but never the frame, so dropping the frames
    makes the report less useful than the tail it replaced.
    """
    if limit <= 0:
        limit = 4
    body = [ln.strip() for ln in log_text().splitlines() if ln.strip()]
    for i, line in enumerate(body):
        if "panic:" in line or line.startswith("fatal error:"):
            return " | ".join(body[i:i + stack])
    fatal = [ln for ln in body if "level=ERROR" in ln]
    if fatal:
        return " | ".join(fatal[-limit:])
    return " | ".join(["no level=ERROR line", *body[-limit:]])


# ---------------------------------------------------------------- pipeline YAML

DEFAULT_SINK: dict[str, object] = {
    "type": "postgresql",
    "hostname": cfg.PG_HOST,
    "port": cfg.PG_PORT,
    "username": cfg.PG_USER,
    "password": cfg.PG_PASSWORD,
    "database": cfg.PG_DB,
    "schema": SNK_A,
    "sslmode": cfg.PG_SSLMODE,
    "connect-timeout": "30s",
    "auto-create-table": True,
    "write-batch-size": 100,
    "write-batch-interval": "200ms",
    "max-retries": 3,
    "schema.change.enabled": True,
    "schema.change.drop-truncate.enabled": False,
}

DEFAULT_SOURCE: dict[str, object] = {
    "type": "mysql",
    "hostname": cfg.MYSQL_HOST,
    "port": cfg.MYSQL_PORT,
    "username": cfg.SRC_USER,
    "password": cfg.SRC_PASSWORD,
    "server-id": cfg.CDC_SERVER_ID_PG,
    "server-time-zone": "Asia/Shanghai",
    "scan.startup.mode": "initial",
    "scan.incremental.snapshot.chunk.size": 200,
    "scan.snapshot.fetch.size": 100,
    "schema-change.enabled": True,
    "connect.timeout": "30s",
}

# Per-table transform entries, emitted only for the tables a session captures.
TRANSFORMS: dict[str, dict[str, str]] = {
    "tp_p05_filter": {"projection": "id, name, city", "filter": "city = 'hz'", "primary-keys": "id"},
    "tp_p04_pk": {"projection": "docid, title", "primary-keys": "docid"},
}

# Sink-side table names that differ from the source name: (db, table) -> sink table.
RENAMES: dict[tuple[str, str], str] = {
    (SRC_A, "tp_p10_route"): "tp_p10_route_from_a",
    (SRC_B, "tp_p10_route"): "tp_p10_route_from_b",
}


def tables_of(session: str) -> list[TableSpec]:
    return [spec for spec in TABLES.values() if spec.session == session]


def _tables_pattern(session: str) -> str:
    """Comma-separated `db\\.(t1|t2)` regex list, one entry per captured database."""
    specs = tables_of(session)
    parts: list[str] = []
    for db in (SRC_A, SRC_B):
        names = [s.name for s in specs if db in s.dbs]
        if names:
            parts.append(f"{db}\\.({'|'.join(names)})")
    if not parts:
        raise ValueError(f"session {session!r} captures no table")
    return "'" + ",".join(parts) + "'"


def _yaml_block(mapping: dict[str, object], indent: str = "  ") -> list[str]:
    lines: list[str] = []
    for key, value in mapping.items():
        if isinstance(value, bool):
            lines.append(f"{indent}{key}: {'true' if value else 'false'}")
        elif isinstance(value, (list, tuple)):
            rendered = ", ".join(str(item) for item in value)
            lines.append(f"{indent}{key}: [{rendered}]")
        elif isinstance(value, str) and key == "password":
            lines.append(f'{indent}{key}: "{value}"')
        else:
            lines.append(f"{indent}{key}: {value}")
    return lines


def build_yaml(
    session: str,
    *,
    path: Path = YAML,
    pipeline_name: str = cfg.PIPELINE_PG_NAME,
    ckpt_dir: str | None = None,
    sink: dict[str, object] | None = None,
    source: dict[str, object] | None = None,
    parallelism: int = 1,
) -> Path:
    """Render a PostgreSQL pipeline YAML for one session's tables."""
    sink_cfg = {**DEFAULT_SINK, **(sink or {})}
    src_cfg = {**DEFAULT_SOURCE, **(source or {})}
    specs = tables_of(session)

    lines = ["source:"]
    lines += _yaml_block({**src_cfg, "tables": _tables_pattern(session)})
    lines += ["", "sink:"]
    lines += _yaml_block(sink_cfg)

    lines += ["", "route:"]
    for spec in specs:
        for db in spec.dbs:
            sink_table = RENAMES.get((db, spec.name), spec.sink_name)
            lines.append(f"  - source-table: '{db}\\.{spec.name}'")
            lines.append(f"    sink-table: {_schema_of(db)}.{sink_table}")

    entries = [name for name in TRANSFORMS if any(s.name == name for s in specs)]
    if entries:
        lines += ["", "transform:"]
        for name in sorted(entries):
            spec = TABLES[name]
            for db in spec.dbs:
                lines.append(f"  - source-table: '{db}\\.{name}'")
                for key, value in TRANSFORMS[name].items():
                    rendered = f'"{value}"' if key == "filter" else value
                    lines.append(f"    {key}: {rendered}")
    else:
        lines += ["transform: []"]

    lines += ["", "pipeline:", f"  name: {pipeline_name}", f"  parallelism: {parallelism}"]
    lines += [
        "", "checkpoint:", "  storage: file", "  interval: 2s",
        f"  file-path: {cfg.it_rel_path(ckpt_dir or cfg.CHECKPOINT_PG_DIR)}",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def checkpoint_path(pipeline_name: str, ckpt_dir: str) -> Path:
    return cfg.checkpoint_file(ckpt_dir, pipeline_name)


@contextmanager
def session(
    name: str,
    *,
    pipeline_name: str = cfg.PIPELINE_PG_NAME,
    ckpt_dir: str | None = None,
    sink: dict[str, object] | None = None,
    source: dict[str, object] | None = None,
    log_path: Path | None = None,
    reset_checkpoint: bool = True,
    parallelism: int = 1,
    expect_exit: bool = False,
):
    """Render + start one pipeline; the job is always stopped on exit."""
    ckpt = ckpt_dir or cfg.CHECKPOINT_PG_DIR
    path = build_yaml(name, pipeline_name=pipeline_name, ckpt_dir=ckpt, sink=sink,
                      source=source, parallelism=parallelism)
    if reset_checkpoint:
        ck_file = checkpoint_path(pipeline_name, ckpt)
        if ck_file.exists():
            ck_file.unlink()
    log(f"==> session {name} (pipeline={pipeline_name}, yaml={path.name})", flush=True)
    start_job(path, log_path or (WORKDIR / f"cdc-pg-{name}.log"))
    if not expect_exit:
        wait_until(job_alive, timeout=5, desc=f"{name} job started")
    try:
        yield path
    finally:
        stop_job()


# --------------------------------------------------------------- data accessors

def src_cfg(db: str) -> dict:
    # TIMESTAMP columns come back as the wall clock the pipeline is told to use
    # (`server-time-zone: Asia/Shanghai`), not the container's UTC default.
    return {
        **cfg.conn_kwargs(cfg.SRC_USER, cfg.SRC_PASSWORD, db),
        "init_command": "SET time_zone = '+08:00'",
    }


def my(db: str, sql: str, args: tuple | None = None) -> list[dict]:
    conn = pymysql.connect(**src_cfg(db), cursorclass=pymysql.cursors.DictCursor, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args or ())
            return list(cur.fetchall()) if cur.description else []
    finally:
        conn.close()


def my_exec(db: str, sql: str, args: tuple | None = None) -> None:
    conn = pymysql.connect(**src_cfg(db), autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args or ())
    finally:
        conn.close()


def src_rows(db: str, table: str, order: str = "id") -> list[dict]:
    return my(db, f"SELECT * FROM `{db}`.`{table}` ORDER BY {order}")


def snk_rows(schema: str, table: str, order: str = "id") -> list[dict]:
    return pg_client.dump_table(schema, table, order)


def target_of(spec: TableSpec, db: str) -> tuple[str, str]:
    """(schema, table) the pipeline writes ``db.spec.name`` into."""
    return _schema_of(db), RENAMES.get((db, spec.name), spec.sink_name)


def reconcile(case: Case, label: str, db: str, table: str, *, timeout: float = 0.0) -> list[str]:
    """Compare one source table against its PostgreSQL target."""
    spec = TABLES[table]
    schema, sink_table = target_of(spec, db)

    def once() -> list[str]:
        if not pg_client.table_exists(schema, sink_table):
            return [f"target {schema}.{sink_table} does not exist"]
        return pg_verify.diff_rows(
            src_rows(db, spec.name, spec.order),
            snk_rows(schema, sink_table, spec.order),
            spec.columns, spec.kinds, spec.key,
        )

    failures = once()
    if failures and timeout:
        # A pipeline that died cannot catch up, so stop waiting as soon as the
        # process is gone and report its last log lines instead of the diff.
        alive_at_entry = job_alive()
        deadline = time.time() + timeout
        while failures and time.time() < deadline:
            if alive_at_entry and not job_alive():
                # The log is attached once, by the failure report below; quoting it
                # here too would print the same ERROR record twice in one detail.
                failures.append("the job stopped while waiting")
                break
            time.sleep(2)
            failures = once()
    if failures:
        # A diff alone does not say whether the sink is behind or already gone, so
        # the process state and its last words travel with every failure.
        detail = (f"{pg_verify.summarize(failures)} | job "
                  f"{'alive' if job_alive() else 'stopped'} | error: {log_errors()}")
        check(case, f"{label}: {db}.{table} -> {schema}.{sink_table}", False, detail)
    else:
        check(case, f"{label}: {db}.{table} -> {schema}.{sink_table} reconciled", True,
              f"{src_count(spec, db)} rows")
    return failures


def wait_for_sink_rows(case: Case, db: str, table: str, count: int, timeout: float = 120) -> None:
    spec = TABLES[table]
    schema, sink_table = target_of(spec, db)
    wait_until(
        lambda: pg_client.row_count(schema, sink_table) >= count,
        timeout=timeout,
        desc=f"{schema}.{sink_table} >= {count} rows ({case.id})",
    )


def expect_column_types(case: Case, db: str, table: str, expected: dict[str, str]) -> None:
    spec = TABLES[table]
    schema, sink_table = target_of(spec, db)
    live = pg_client.table_columns(schema, sink_table)
    wrong = {col: (live.get(col), want) for col, want in expected.items() if live.get(col) != want}
    check(case, f"{schema}.{sink_table} mapped types", not wrong,
          "; ".join(f"{c}: got {got!r} want {want!r}" for c, got, want in
                    ((k, v[0], v[1]) for k, v in wrong.items())) if wrong else "ok")


# ------------------------------------------------------------------- fixtures
#
# Every case owns its rows: the source table is recreated from TableSpec.ddl and
# the routed target is dropped before the job starts, so no case inherits state
# from the one before it, and a re-run of a single case is meaningful.

def wait_alive(stable_for: float = 4.0, desc: str = "go-cdc") -> None:
    """Fail fast when the job dies at startup; otherwise wait until it is stable."""
    ready_at = time.time() + stable_for
    while time.time() < ready_at:
        if proc is None:
            raise RuntimeError(f"{desc}: process not started")
        code = proc.poll()
        if code is not None:
            raise RuntimeError(f"{desc} exited at startup ({code}):\n{log_tail()}")
        time.sleep(0.25)


def reset_source_table(spec: TableSpec) -> None:
    for db in spec.dbs:
        my_exec(db, f"DROP TABLE IF EXISTS `{db}`.`{spec.name}`")
        my_exec(db, spec.ddl.format(t=spec.name))


def reset_target_table(spec: TableSpec, db: str) -> None:
    schema, table = target_of(spec, db)
    pg_client.exec_sql(f'DROP TABLE IF EXISTS "{schema}"."{table}"')


def fresh(spec: TableSpec) -> None:
    """Recreate the source table(s) and drop the routed target table(s)."""
    reset_source_table(spec)
    for db in spec.dbs:
        reset_target_table(spec, db)


def insert_rows(db: str, spec: TableSpec, columns: str, values: str) -> None:
    my_exec(db, f"INSERT INTO `{db}`.`{spec.name}` ({columns}) VALUES {values}")


def count_of(spec: TableSpec, db: str = SRC_A) -> int:
    schema, table = target_of(spec, db)
    return pg_client.row_count(schema, table)


def src_count(spec: TableSpec, db: str = SRC_A) -> int:
    """Row count on the MySQL source, so a target count is compared to the origin."""
    return int(my(db, f"SELECT COUNT(*) AS n FROM `{db}`.`{spec.name}`")[0]["n"])


def wait_count(spec: TableSpec, want: int, db: str = SRC_A, timeout: float = 120) -> None:
    wait_until(lambda: count_of(spec, db) == want, timeout=timeout,
               desc=f"{spec.name} target holds {want} rows")


def sink_rows_of(spec: TableSpec, db: str = SRC_A) -> list[dict]:
    schema, table = target_of(spec, db)
    return pg_client.dump_table(schema, table, spec.order)


def sink_keys(spec: TableSpec, db: str = SRC_A) -> set[tuple]:
    return pg_verify.pk_set(sink_rows_of(spec, db), spec.key, spec.kinds)


def reconcile_expected(case: Case, label: str, spec: TableSpec, db: str,
                       expected: list[dict]) -> list[str]:
    """Compare the target against rows the test computed, not against the source.

    Used by the filter case, where the target is *supposed* to differ from the
    source table.
    """
    schema, table = target_of(spec, db)
    failures = pg_verify.diff_rows(expected, pg_client.dump_table(schema, table, spec.order),
                                   spec.columns, spec.kinds, spec.key)
    check(case, f"{label}: {schema}.{table} matches {len(expected)} expected rows",
          not failures, pg_verify.summarize(failures))
    return failures


def check_columns(case: Case, spec: TableSpec, db: str, expected: tuple[str, ...]) -> None:
    schema, table = target_of(spec, db)
    live = tuple(pg_client.table_columns(schema, table))
    check(case, f"{schema}.{table} columns", live == expected,
          f"got {list(live)} want {list(expected)}")


def wait_column(schema: str, table: str, column: str, present: bool = True,
                timeout: float = 60) -> None:
    def ok() -> bool:
        return (column in pg_client.table_columns(schema, table)) is present

    wait_until(ok, timeout=timeout,
               desc=f"{schema}.{table} column {column!r} {'present' if present else 'gone'}")


# ---------------------------------------------------------------- test points

def tp_p01() -> Case:
    case = new_case(
        "TP-P01",
        "Snapshot of two databases: same table name, empty table, NULL and default values",
        "seed tp_p01_snap in db A (5 rows) and db B (3 rows), leave tp_p01_empty empty, one pipeline",
        "both source tables land in their own schema with equal primary-key sets and canonically "
        "equal values; the empty table is created with 0 rows; no COUNT(*)-only check",
    )
    spec = TABLES["tp_p01_snap"]
    empty = TABLES["tp_p01_empty"]
    fresh(spec)
    fresh(empty)
    insert_rows(SRC_A, spec, "id, name, amount, c_dt, c_json, c_bin, note", """
        (1, 'alice', 12.3400, '2024-01-02 03:04:05.123', '{"k": 1}', X'0001', 'a-note'),
        (2, 'bob', 0.0000, NULL, NULL, NULL, NULL),
        (3, 'cara', -5.5000, '2024-02-29 23:59:59.999', '[1, 2, 3]', X'FF00', 'c-note'),
        (4, 'dàn', 1.0001, '2024-12-31 00:00:00.000', '{"n": null}', X'', NULL),
        (5, 'eve', 99999999.9999, '1970-01-01 00:00:01.000', '{}', X'DEADBEEF', 'e-note')
    """)
    insert_rows(SRC_B, spec, "id, name, amount, note", """
        (1, 'zoe', 1.0000, 'b-one'),
        (2, 'yan', 2.0000, NULL),
        (7, 'xue', 7.0000, 'b-seven')
    """)
    with session("main"):
        wait_alive(6)
        wait_for_sink_rows(case, SRC_A, spec.name, 5)
        wait_for_sink_rows(case, SRC_B, spec.name, 3)
        reconcile(case, "snapshot", SRC_A, spec.name, timeout=60)
        reconcile(case, "snapshot", SRC_B, spec.name, timeout=60)
        empty_schema, empty_table = target_of(empty, SRC_A)
        check(case, f"{empty_schema}.{empty_table} created by auto-create-table",
              pg_client.table_exists(empty_schema, empty_table))
        check(case, f"{empty_schema}.{empty_table} holds 0 rows", count_of(empty) == 0,
              f"{count_of(empty)} rows")
        check_columns(case, spec, SRC_A, spec.columns)
    return finish(case)


def tp_p02() -> Case:
    case = new_case(
        "TP-P02",
        "Snapshot to binlog switchover with writes landing while the snapshot is still streaming",
        "seed 6'000 rows, start the job one row per transaction, and while the target still holds "
        "only part of the snapshot run INSERT/UPDATE/DELETE on source rows",
        "the DML provably happens before the snapshot finished (0 < target rows < baseline), and "
        "after catch-up every row is canonically equal",
    )
    spec = TABLES["tp_p02_switch"]
    baseline = 6000
    fresh(spec)
    batch: list[str] = []
    for start in range(1, baseline + 1, 1000):
        rows = ", ".join(f"({i}, {i}, 's{i}')" for i in range(start, min(start + 1000, baseline + 1)))
        batch.append(rows)
    for rows in batch:
        insert_rows(SRC_A, spec, "id, v, tag", rows)
    # One row per transaction is what makes the snapshot observably partial: with
    # the default batch of 100 the whole snapshot can land between two polls, and
    # the window this case has to prove is gone before the DML runs.
    with session("switch", sink={"write-batch-size": 1}):
        wait_alive(2)
        try:
            wait_until(lambda: 0 < count_of(spec) < baseline, timeout=180, interval=0.2,
                       desc="target holds a partial snapshot")
            partial = count_of(spec)
            check(case, "snapshot was still streaming when the DML ran", 0 < partial < baseline,
                  f"{partial} of {baseline} rows already on the target")
        except TimeoutError as exc:
            note(case, f"overlap not observed: {exc}")
            check(case, "snapshot was still streaming when the DML ran", False,
                  "the target jumped from empty to complete; the window was not observable")
            partial = count_of(spec)
        # Rows below the partial watermark were snapshotted, rows above are still
        # to come: updating both directions is what the switchover has to survive.
        my_exec(SRC_A, f"UPDATE `{SRC_A}`.`{spec.name}` SET v = v + 100000, tag = 'pre' WHERE id <= 100")
        my_exec(SRC_A, f"UPDATE `{SRC_A}`.`{spec.name}` SET v = v + 200000, tag = 'post' "
                       f"WHERE id > {baseline - 100}")
        my_exec(SRC_A, f"DELETE FROM `{SRC_A}`.`{spec.name}` WHERE id IN (500, 6000)")
        insert_rows(SRC_A, spec, "id, v, tag", f"({baseline + 1}, 1, 'new1'), "
                                                        f"({baseline + 2}, 2, 'new2')")
        reconcile(case, "after switchover", SRC_A, spec.name, timeout=300)
        check(case, "final row count equals the source", count_of(spec) == baseline,
              f"target={count_of(spec)} source={baseline}")
    return finish(case)


def tp_p03() -> Case:
    case = new_case(
        "TP-P03",
        "Binlog INSERT/UPDATE/DELETE: consecutive updates, delete then insert same key, insert then delete",
        "three rows, three back-to-back updates on one key, delete+re-insert on another, insert+delete a third",
        "exactly one target row per source row, final values equal, deleted keys stay deleted",
    )
    spec = TABLES["tp_p03_crud"]
    fresh(spec)
    insert_rows(SRC_A, spec, "id, v, note", "(1, 1, 'first'), (2, 2, 'second'), (3, 3, NULL)")
    with session("main"):
        wait_alive(6)
        wait_for_sink_rows(case, SRC_A, spec.name, 3)
        my_exec(SRC_A, f"UPDATE `{SRC_A}`.`{spec.name}` SET v = 10, note = 'u1' WHERE id = 1")
        my_exec(SRC_A, f"UPDATE `{SRC_A}`.`{spec.name}` SET v = 11, note = 'u2' WHERE id = 1")
        my_exec(SRC_A, f"UPDATE `{SRC_A}`.`{spec.name}` SET v = 12, note = NULL WHERE id = 1")
        my_exec(SRC_A, f"DELETE FROM `{SRC_A}`.`{spec.name}` WHERE id = 2")
        insert_rows(SRC_A, spec, "id, v, note", "(2, 22, 'resurrected')")
        insert_rows(SRC_A, spec, "id, v, note", "(4, 4, NULL)")
        my_exec(SRC_A, f"DELETE FROM `{SRC_A}`.`{spec.name}` WHERE id = 4")
        reconcile(case, "final state", SRC_A, spec.name, timeout=120)
        check(case, "target holds exactly 3 rows", count_of(spec) == 3, f"{count_of(spec)} rows")
        check(case, "never-inserted key 4 is absent", (4,) not in sink_keys(spec))
    return finish(case)


def tp_p04() -> Case:
    case = new_case(
        "TP-P04",
        "Primary key change on the business key",
        "update docid 'd1' to 'd9' on a table whose only key is docid",
        "the old key is gone, the new key carries the row, the row count is unchanged",
    )
    spec = TABLES["tp_p04_pk"]
    fresh(spec)
    insert_rows(SRC_A, spec, "docid, title, seq", "('d1', 'first', 1), ('d2', 'second', 2)")
    with session("main"):
        wait_alive(6)
        wait_for_sink_rows(case, SRC_A, spec.name, 2)
        my_exec(SRC_A, f"UPDATE `{SRC_A}`.`{spec.name}` SET docid = 'd9', seq = 9 WHERE docid = 'd1'")
        wait_until(lambda: sink_keys(spec) == {('d2',), ('d9',)}, timeout=90,
                   desc="target keys become d2 + d9")
        reconcile(case, "after key change", SRC_A, spec.name, timeout=60)
        check(case, "old key d1 is gone", ('d1',) not in sink_keys(spec))
        check(case, "row count unchanged", count_of(spec) == 2, f"{count_of(spec)} rows")
    return finish(case)


def tp_p05() -> Case:
    case = new_case(
        "TP-P05",
        "Projection plus filter: fall-in, fall-out and stay-out",
        "filter city = 'hz' with projection id,name,city: one row starts in, one starts out, then both cross the filter",
        "fall-in creates the row, fall-out retracts it (the sink deletes it), stay-out never appears, "
        "and the projected-out column is absent from the target",
    )
    spec = TABLES["tp_p05_filter"]
    fresh(spec)
    insert_rows(SRC_A, spec, "id, name, city, amount", "(1, 'ann', 'hz', 10), (2, 'ben', 'sh', 20)")
    with session("main"):
        wait_alive(6)
        wait_until(lambda: count_of(spec) == 1, timeout=90, desc="only the in-filter row arrives")
        check(case, "only the matching row is created", sink_keys(spec) == {(1,)}, f"{sink_keys(spec)}")
        check(case, "projected-out column amount is absent",
              "amount" not in pg_client.table_columns(*target_of(spec, SRC_A)),
              f"{list(pg_client.table_columns(*target_of(spec, SRC_A)))}")
        check_columns(case, spec, SRC_A, spec.columns)
        # fall-out: the row leaves the filter and must be retracted from the target
        my_exec(SRC_A, f"UPDATE `{SRC_A}`.`{spec.name}` SET city = 'sh' WHERE id = 1")
        wait_until(lambda: count_of(spec) == 0, timeout=90, desc="fall-out retracts id=1")
        check(case, "fall-out deletes the target row", count_of(spec) == 0)
        # fall-in: the other row enters the filter
        my_exec(SRC_A, f"UPDATE `{SRC_A}`.`{spec.name}` SET city = 'hz' WHERE id = 2")
        wait_until(lambda: count_of(spec) == 1, timeout=90, desc="fall-in creates id=2")
        reconcile_expected(case, "fall-in", spec, SRC_A, [{"id": 2, "name": "ben", "city": "hz"}])
        # stay-out: an update that does not touch the filter must not resurrect id=1
        my_exec(SRC_A, f"UPDATE `{SRC_A}`.`{spec.name}` SET amount = 11 WHERE id = 1")
        time.sleep(3)
        check(case, "stay-out update does not resurrect id=1", (1,) not in sink_keys(spec))
        # final fall-out: the table ends up empty on the target
        my_exec(SRC_A, f"UPDATE `{SRC_A}`.`{spec.name}` SET city = 'sh' WHERE id = 2")
        wait_until(lambda: count_of(spec) == 0, timeout=90, desc="final fall-out")
        reconcile_expected(case, "final state", spec, SRC_A, [])
    return finish(case)


def tp_p06() -> Case:
    case = new_case(
        "TP-P06",
        "DELETE carried only by the before image, composite key",
        "delete (id=1, sub=2) from a two-column primary key table",
        "only that row is removed; the sibling with the same id and the other id both survive",
    )
    spec = TABLES["tp_p06_del"]
    fresh(spec)
    insert_rows(SRC_A, spec, "id, sub, v", "(1, 1, 10), (1, 2, 20), (2, 1, 30)")
    with session("main"):
        wait_alive(6)
        wait_for_sink_rows(case, SRC_A, spec.name, 3)
        my_exec(SRC_A, f"DELETE FROM `{SRC_A}`.`{spec.name}` WHERE id = 1 AND sub = 2")
        wait_until(lambda: count_of(spec) == 2, timeout=90, desc="target loses exactly one row")
        check(case, "siblings survive", sink_keys(spec) == {(1, 1), (2, 1)}, f"{sink_keys(spec)}")
        reconcile(case, "after delete", SRC_A, spec.name, timeout=60)
    return finish(case)


def tp_p07() -> Case:
    case = new_case(
        "TP-P07",
        "Supported DDL followed immediately by DML: add, modify and rename a column",
        "ALTER TABLE ADD note, then MODIFY it wider, then CHANGE it to memo; write rows after each step",
        "the target column list and mapped type follow each change, and the rows written right "
        "after the DDL are equal",
    )
    spec = TABLES["tp_p07_ddl"]
    fresh(spec)
    original_columns, original_kinds = spec.columns, dict(spec.kinds)
    schema, table = target_of(spec, SRC_A)
    insert_rows(SRC_A, spec, "id, name", "(1, 'a'), (2, 'b')")
    try:
        with session("main"):
            wait_alive(6)
            wait_for_sink_rows(case, SRC_A, spec.name, 2)

            my_exec(SRC_A, f"ALTER TABLE `{SRC_A}`.`{spec.name}` ADD COLUMN note VARCHAR(32) NULL")
            wait_column(schema, table, "note")
            spec.columns = spec.columns + ("note",)
            spec.kinds["note"] = "text"
            insert_rows(SRC_A, spec, "id, name, note", "(3, 'c', 'n3')")
            my_exec(SRC_A, f"UPDATE `{SRC_A}`.`{spec.name}` SET note = 'n1' WHERE id = 1")
            reconcile(case, "after add.column", SRC_A, spec.name, timeout=90)

            my_exec(SRC_A, f"ALTER TABLE `{SRC_A}`.`{spec.name}` MODIFY COLUMN note VARCHAR(64) NULL")
            wait_until(lambda: pg_client.table_columns(schema, table).get("note")
                       == "character varying(64)", timeout=90, desc="note widens to varchar(64)")
            my_exec(SRC_A, f"UPDATE `{SRC_A}`.`{spec.name}` SET note = REPEAT('m', 40) WHERE id = 2")
            reconcile(case, "after modify.column", SRC_A, spec.name, timeout=90)

            my_exec(SRC_A, f"ALTER TABLE `{SRC_A}`.`{spec.name}` "
                           f"CHANGE COLUMN note memo VARCHAR(64) NULL")
            wait_column(schema, table, "note", present=False)
            wait_column(schema, table, "memo")
            spec.columns = tuple("memo" if col == "note" else col for col in spec.columns)
            spec.kinds["memo"] = spec.kinds.pop("note")
            my_exec(SRC_A, f"UPDATE `{SRC_A}`.`{spec.name}` SET memo = 'm2' WHERE id = 3")
            reconcile(case, "after rename.column", SRC_A, spec.name, timeout=90)
            check_columns(case, spec, SRC_A, ("id", "name", "memo"))
    finally:
        spec.columns, spec.kinds = original_columns, original_kinds
    return finish(case)


# Each entry: label, DDL template, sink overrides, fragment the refusal must name.
REFUSED_ATTEMPTS = (
    (
        "drop.column with schema.change.enabled=false",
        "ALTER TABLE `{db}`.`{t}` DROP COLUMN `doomed`",
        {"schema.change.enabled": False},
        "schema changes are disabled",
    ),
    (
        "TRUNCATE with drop-truncate disabled",
        "TRUNCATE TABLE `{db}`.`{t}`",
        {"schema.change.enabled": True},
        "dropping and truncating are disabled",
    ),
    (
        "DROP TABLE with drop-truncate disabled",
        "DROP TABLE `{db}`.`{t}`",
        {"schema.change.enabled": True},
        "dropping and truncating are disabled",
    ),
    (
        "RENAME TABLE",
        "RENAME TABLE `{db}`.`{t}` TO `{db}`.`{t}_renamed`",
        {"schema.change.enabled": True},
        "not supported by this sink",
    ),
)


def tp_p08() -> Case:
    case = new_case(
        "TP-P08",
        "Schema changes the sink refuses: drop.column, TRUNCATE, DROP TABLE, RENAME TABLE",
        "one fresh job per refused DDL, each applied at the source with the switch that refuses it",
        "every run stops with a non-zero exit that names the refusal, and nothing is applied "
        "silently: the target keeps its column and its rows",
    )
    spec = TABLES["tp_p08_off"]
    schema, table = target_of(spec, SRC_A)
    for label, ddl, sink, needle in REFUSED_ATTEMPTS:
        fresh(spec)
        insert_rows(SRC_A, spec, "id, name, doomed, keepme",
                    "(1, 'a', 1, 1), (2, 'b', 2, 2)")
        with session("refused", sink=sink):
            wait_alive(6)
            wait_for_sink_rows(case, SRC_A, spec.name, 2)
            my_exec(SRC_A, ddl.format(db=SRC_A, t=spec.name))
            code = wait_exit(timeout=90, desc=f"refused {label}")
        check(case, f"{label}: job stops with a non-zero exit", code != 0, f"exit={code}")
        check(case, f"{label}: the refusal is named in the log", needle in log_text(), log_tail(3))
        check(case, f"{label}: target table still exists", pg_client.table_exists(schema, table))
        check(case, f"{label}: target keeps its 2 rows", pg_client.row_count(schema, table) == 2,
              f"{pg_client.row_count(schema, table)} rows")
        if "drop.column" in label:
            check(case, f"{label}: column doomed was not dropped",
                  "doomed" in pg_client.table_columns(schema, table))
        my_exec(SRC_A, f"DROP TABLE IF EXISTS `{SRC_A}`.`{spec.name}_renamed`")
    return finish(case)


def tp_p09() -> Case:
    case = new_case(
        "TP-P09",
        "Destructive DDL with schema.change.drop-truncate.enabled: true",
        "TRUNCATE, ADD COLUMN, DROP COLUMN and finally DROP TABLE at the source, with DML after each step",
        "each change is applied to the target, the rows written after it are equal, and the job "
        "keeps running through the whole sequence",
    )
    spec = TABLES["tp_p09_gate"]
    fresh(spec)
    insert_rows(SRC_A, spec, "id, name, doomed", "(1, 'a', 1), (2, 'b', 2), (3, 'c', 3)")
    sink = {"schema.change.enabled": True, "schema.change.drop-truncate.enabled": True}
    schema, table = target_of(spec, SRC_A)
    original_columns, original_kinds = spec.columns, dict(spec.kinds)
    try:
        with session("gate", sink=sink):
            wait_alive(6)
            wait_for_sink_rows(case, SRC_A, spec.name, 3)

            my_exec(SRC_A, f"TRUNCATE TABLE `{SRC_A}`.`{spec.name}`")
            wait_count(spec, 0, timeout=90)
            check(case, "TRUNCATE applied to the target", count_of(spec) == 0)
            insert_rows(SRC_A, spec, "id, name, doomed", "(4, 'd', 4), (5, 'e', 5)")
            reconcile(case, "DML after TRUNCATE", SRC_A, spec.name, timeout=90)

            my_exec(SRC_A, f"ALTER TABLE `{SRC_A}`.`{spec.name}` ADD COLUMN extra INT NULL")
            wait_column(schema, table, "extra")
            spec.columns = spec.columns + ("extra",)
            spec.kinds["extra"] = "int"
            my_exec(SRC_A, f"UPDATE `{SRC_A}`.`{spec.name}` SET extra = 44 WHERE id = 4")
            reconcile(case, "DML after add.column", SRC_A, spec.name, timeout=90)

            my_exec(SRC_A, f"ALTER TABLE `{SRC_A}`.`{spec.name}` DROP COLUMN doomed")
            wait_column(schema, table, "doomed", present=False)
            spec.columns = tuple(col for col in spec.columns if col != "doomed")
            spec.kinds.pop("doomed", None)
            my_exec(SRC_A, f"UPDATE `{SRC_A}`.`{spec.name}` SET extra = 55 WHERE id = 5")
            reconcile(case, "DML after drop.column", SRC_A, spec.name, timeout=90)
            check(case, "job still running after the destructive DDL", job_alive(), log_tail(4))

            my_exec(SRC_A, f"DROP TABLE `{SRC_A}`.`{spec.name}`")
            wait_until(lambda: not pg_client.table_exists(schema, table), timeout=90,
                       desc="target table dropped")
            time.sleep(2)
            check(case, "job survives DROP TABLE", job_alive(), log_tail(4))
    finally:
        spec.columns, spec.kinds = original_columns, original_kinds
    return finish(case)


def tp_p10() -> Case:
    case = new_case(
        "TP-P10",
        "Route: source database to target schema, renamed target tables, mixed-case identifiers",
        "the same table name in two source databases plus a mixed-case table, all in one pipeline",
        "each database lands in its own schema under its routed name, rows do not leak across "
        "schemas, and a mixed-case identifier is not folded to lowercase",
    )
    spec = TABLES["tp_p10_route"]
    mixed = TABLES["tp_P10_Mixed"]
    fresh(spec)
    fresh(mixed)
    insert_rows(SRC_A, spec, "id, payload", "(1, 'from-a')")
    insert_rows(SRC_B, spec, "id, payload", "(1, 'from-b')")
    insert_rows(SRC_B, mixed, "id, payload", "(1, 'mixed')")
    with session("main"):
        wait_alive(6)
        wait_for_sink_rows(case, SRC_A, spec.name, 1)
        wait_for_sink_rows(case, SRC_B, spec.name, 1)
        reconcile(case, "route db A", SRC_A, spec.name, timeout=90)
        reconcile(case, "route db B", SRC_B, spec.name, timeout=90)
        schema_a, table_a = target_of(spec, SRC_A)
        schema_b, table_b = target_of(spec, SRC_B)
        check(case, f"db A wrote {schema_a}.{table_a}", pg_client.table_exists(schema_a, table_a))
        check(case, f"db B wrote {schema_b}.{table_b}", pg_client.table_exists(schema_b, table_b))
        check(case, "the two databases map to different schemas", schema_a != schema_b,
              f"{schema_a} vs {schema_b}")
        payloads_a = [row["payload"] for row in pg_client.dump_table(schema_a, table_a, "id")]
        payloads_b = [row["payload"] for row in pg_client.dump_table(schema_b, table_b, "id")]
        check(case, "db A rows stay in schema A", payloads_a == ["from-a"], f"{payloads_a}")
        check(case, "db B rows stay in schema B", payloads_b == ["from-b"], f"{payloads_b}")
        mixed_schema, mixed_table = target_of(mixed, SRC_B)
        check(case, f"mixed-case table name preserved ({mixed_table})",
              pg_client.table_exists(mixed_schema, mixed_table))
        check(case, "no lower-cased twin appears",
              not pg_client.table_exists(mixed_schema, mixed_table.lower()),
              mixed_table.lower())
        reconcile(case, "mixed case", SRC_B, mixed.name, timeout=60)
    return finish(case)


def tp_p11() -> Case:
    case = new_case(
        "TP-P11",
        "Graceful stop after a checkpoint, then resume",
        "stop with SIGTERM, write more rows while the job is down, resume from the stored checkpoint",
        "the checkpoint file exists after the stop and the target ends up equal with no lost or "
        "duplicated row",
    )
    spec = TABLES["tp_p11_resume"]
    fresh(spec)
    insert_rows(SRC_A, spec, "id, v, note", "(1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c')")
    name, ckpt_dir = cfg.PIPELINE_PG_RESTART_NAME, cfg.CHECKPOINT_PG_RESTART_DIR
    with session("resume", pipeline_name=name, ckpt_dir=ckpt_dir):
        wait_alive(6)
        wait_for_sink_rows(case, SRC_A, spec.name, 3)
    ckpt = checkpoint_path(name, ckpt_dir)
    check(case, "graceful stop leaves a checkpoint file", ckpt.exists(), str(ckpt))
    insert_rows(SRC_A, spec, "id, v, note", "(4, 4, 'd'), (5, 5, 'e')")
    with session("resume", pipeline_name=name, ckpt_dir=ckpt_dir, reset_checkpoint=False):
        wait_alive(6)
        reconcile(case, "after resume", SRC_A, spec.name, timeout=180)
        check(case, "the target holds one row per source row",
              count_of(spec) == src_count(spec),
              f"target={count_of(spec)} source={src_count(spec)}")
    return finish(case)


def tp_p12() -> Case:
    case = new_case(
        "TP-P12",
        "SIGKILL then resume (at-least-once replay)",
        "hard-kill the job mid-stream, write more rows, resume from the last checkpoint",
        "the target ends up equal; a replayed INSERT may duplicate only transiently and the "
        "final state holds one row per key",
    )
    spec = TABLES["tp_p12_kill"]
    fresh(spec)
    insert_rows(SRC_A, spec, "id, v, note", "(1, 1, 'a'), (2, 2, 'b')")
    name, ckpt_dir = cfg.PIPELINE_PG_RESTART_NAME, cfg.CHECKPOINT_PG_RESTART_DIR
    with session("resume", pipeline_name=name, ckpt_dir=ckpt_dir):
        wait_alive(6)
        wait_for_sink_rows(case, SRC_A, spec.name, 2)
        insert_rows(SRC_A, spec, "id, v, note", "(3, 3, 'c')")
        code = kill_job_hard()
    check(case, "SIGKILL ends the job without a graceful shutdown", code != 0, f"exit={code}")
    insert_rows(SRC_A, spec, "id, v, note", "(4, 4, 'd')")
    with session("resume", pipeline_name=name, ckpt_dir=ckpt_dir, reset_checkpoint=False):
        wait_alive(6)
        reconcile(case, "after hard kill and resume", SRC_A, spec.name, timeout=240)
        check(case, "the target holds one row per source row",
              count_of(spec) == src_count(spec),
              f"target={count_of(spec)} source={src_count(spec)}")
    return finish(case)


def tp_p13() -> Case:
    case = new_case(
        "TP-P13",
        "Resume with a schema change that is still only in the binlog (PR2 M3)",
        "stop gracefully, ADD COLUMN and insert a row using it while the job is down, then resume "
        "from the existing checkpoint without re-emitting the source catalog",
        "no silent divergence: either the replayed DDL is applied and the rows are equal, or the "
        "job stops with a diagnosable error and recovers after the column is applied by hand",
    )
    spec = TABLES["tp_p13_m3"]
    fresh(spec)
    insert_rows(SRC_A, spec, "id, v, note", "(1, 1, 'a'), (2, 2, 'b')")
    name, ckpt_dir = cfg.PIPELINE_PG_RESTART_NAME, cfg.CHECKPOINT_PG_RESTART_DIR
    schema, table = target_of(spec, SRC_A)
    original_columns, original_kinds = spec.columns, dict(spec.kinds)
    try:
        with session("resume", pipeline_name=name, ckpt_dir=ckpt_dir):
            wait_alive(6)
            wait_for_sink_rows(case, SRC_A, spec.name, 2)
        note(case, "PR2 M3: a resume does not re-emit the captured catalog "
                   "(cmd/go-cdc/main.go re-emits it for Doris only)")
        my_exec(SRC_A, f"ALTER TABLE `{SRC_A}`.`{spec.name}` ADD COLUMN c_new VARCHAR(32) NULL")
        insert_rows(SRC_A, spec, "id, v, note, c_new", "(3, 30, 'n3', 'new-col')")
        spec.columns = spec.columns + ("c_new",)
        spec.kinds["c_new"] = "text"

        start_job(build_yaml("resume", pipeline_name=name, ckpt_dir=ckpt_dir),
                  WORKDIR / "cdc-pg-m3.log")
        outcome = "pending"
        deadline = time.time() + 120
        try:
            while time.time() < deadline:
                code = proc.poll() if proc is not None else None
                if code is not None:
                    outcome = f"exit {code}"
                    break
                if "c_new" in pg_client.table_columns(schema, table) and count_of(spec) >= 3:
                    outcome = "ddl replayed and applied"
                    break
                time.sleep(1)
            check(case, "the resume reaches a defined outcome within 120s",
                  outcome != "pending", outcome)
            note(case, f"observed outcome: {outcome}")
            if outcome == "ddl replayed and applied":
                check(case, "replayed DDL is verified on the target",
                      pg_client.table_columns(schema, table).get("c_new") == "character varying(32)",
                      f"{pg_client.table_columns(schema, table).get('c_new')}")
                reconcile(case, "after replayed DDL", SRC_A, spec.name, timeout=120)
            elif outcome.startswith("exit"):
                check(case, "the divergence is diagnosable, not silent",
                      "ERROR" in log_text() and spec.name in log_text(), log_tail(4))
                # Documented recovery: apply the column by hand; the replay of the
                # ADD COLUMN then verifies as a no-op (ADD COLUMN IF NOT EXISTS).
                pg_client.exec_sql(
                    f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "c_new" VARCHAR(32)')
                start_job(build_yaml("resume", pipeline_name=name, ckpt_dir=ckpt_dir),
                          WORKDIR / "cdc-pg-m3-recover.log")
                wait_alive(6)
                reconcile(case, "after manual recovery", SRC_A, spec.name, timeout=180)
            else:
                check(case, "the target does not silently keep the old shape", False,
                      f"outcome={outcome} columns={list(pg_client.table_columns(schema, table))}")
        finally:
            stop_job()
    finally:
        spec.columns, spec.kinds = original_columns, original_kinds
    return finish(case)


def tp_p14() -> Case:
    case = new_case(
        "TP-P14",
        "Target-side SQL faults: terminated backend (57P01), cancelled statement (57014), refused login (28000)",
        "terminate the sink's backend during writes; lock the target and cancel the blocked statement; "
        "disable LOGIN for a non-superuser sink role and start the job",
        "57P01 is retried or recovers on restart, 57014 and 28000 stop the job with a diagnosable "
        "error, and every path reconciles after recovery; all injections are undone",
    )
    spec = TABLES["tp_p14_fault"]
    schema, table = target_of(spec, SRC_A)

    # (a) 57P01: the pool's backend dies under an in-flight write.
    fresh(spec)
    insert_rows(SRC_A, spec, "id, v", "(1, 1), (2, 2), (3, 3)")
    stop = threading.Event()
    written = {"n": 0}

    def writer() -> None:
        row = 100
        while not stop.is_set():
            my_exec(SRC_A, f"INSERT INTO `{SRC_A}`.`{spec.name}` (id, v) VALUES ({row}, {row})")
            written["n"] += 1
            row += 1
            time.sleep(0.05)

    killed = 0
    with session("fault"):
        wait_alive(6)
        wait_for_sink_rows(case, SRC_A, spec.name, 3)
        thread = threading.Thread(target=writer, daemon=True)
        thread.start()
        try:
            for _ in range(3):
                time.sleep(2)
                killed += pg_client.terminate_backends()
        finally:
            stop.set()
            thread.join(timeout=10)
        check(case, "the sink's backend was terminated during writes", killed > 0,
              f"{killed} backends terminated, {written['n']} rows written")
        survived = job_alive()
        note(case, f"57P01: job {'survived' if survived else 'exited'} after {killed} terminations")
        if not survived:
            check(case, "an exit on 57P01 is diagnosable", "ERROR" in log_text(), log_tail(4))
            start_job(build_yaml("fault"), WORKDIR / "cdc-pg-fault-57p01.log")
            wait_alive(6)
        reconcile(case, "after terminated backends", SRC_A, spec.name, timeout=240)

    # (b) 57014: block the target, then cancel the statement the sink is running.
    fresh(spec)
    insert_rows(SRC_A, spec, "id, v", "(1, 1)")
    with session("fault"):
        wait_alive(6)
        wait_for_sink_rows(case, SRC_A, spec.name, 1)
        locker = pg_client.connect(autocommit=False)
        try:
            locker_pid = pg_client.backend_pid(locker)
            with locker.cursor() as cur:
                cur.execute(f'LOCK TABLE "{schema}"."{table}" IN ACCESS EXCLUSIVE MODE')
            insert_rows(SRC_A, spec, "id, v", "(2, 2)")
            time.sleep(3)
            cancelled = pg_client.cancel_backends(exclude=[locker_pid])
            check(case, "the blocked statement was cancelled", cancelled > 0, f"{cancelled} backends")
            locker.rollback()
        finally:
            locker.close()
        code = wait_exit(timeout=120, desc="job after 57014")
        check(case, "57014 is not retried: the job stops", code != 0, f"exit={code}")
        check(case, "the 57014 path is diagnosable",
              "ERROR" in log_text() and "57014" in log_text(), log_tail(4))
    start_job(build_yaml("fault"), WORKDIR / "cdc-pg-fault-57014-recover.log")
    try:
        wait_alive(6)
        reconcile(case, "after 57014 recovery", SRC_A, spec.name, timeout=240)
    finally:
        stop_job()

    # (c) 28000: a non-superuser sink role loses the right to log in.
    role, password = "cdc_pg_it_sink", "it-sink-pw"
    pg_client.drop_login_role(role)
    pg_client.create_login_role(role, password)
    pg_client.exec_sql(f'GRANT USAGE, CREATE ON SCHEMA "{schema}" TO "{role}"')
    fresh(spec)
    insert_rows(SRC_A, spec, "id, v", "(1, 1), (2, 2)")
    sink = {"username": role, "password": password}
    try:
        with session("fault", sink=sink):
            wait_alive(8)
            wait_for_sink_rows(case, SRC_A, spec.name, 2)
        pg_client.set_role_login(role, False)
        start_job(build_yaml("fault", sink=sink), WORKDIR / "cdc-pg-fault-login.log")
        code = wait_exit(timeout=120, desc="job with a disabled role")
        check(case, "a refused login stops the job", code != 0, f"exit={code}")
        check(case, "the login refusal is diagnosable", "ERROR" in log_text(), log_tail(4))
    finally:
        pg_client.set_role_login(role, True)
    start_job(build_yaml("fault", sink=sink), WORKDIR / "cdc-pg-fault-login-recover.log")
    try:
        wait_alive(8)
        reconcile(case, "after restoring the login", SRC_A, spec.name, timeout=240)
    finally:
        stop_job()
        pg_client.drop_login_role(role)
    return finish(case)


def tp_p15() -> Case:
    case = new_case(
        "TP-P15",
        "Replay of an already-applied window and four writers on overlapping keys",
        "copy the checkpoint, write a window of DML, put the old checkpoint back and resume; then "
        "hammer six keys from four threads",
        "the replay reproduces the same final state without duplicating keys, and the concurrent "
        "writers converge to the source state (ordering limited to binlog commit order)",
    )
    spec = TABLES["tp_p15_replay"]
    fresh(spec)
    insert_rows(SRC_A, spec, "id, v", ", ".join(f"({i}, {i})" for i in range(1, 21)))
    ckpt = checkpoint_path(cfg.PIPELINE_PG_NAME, cfg.CHECKPOINT_PG_DIR)
    backup = ckpt.with_suffix(".window")
    with session("fault"):
        wait_alive(6)
        wait_for_sink_rows(case, SRC_A, spec.name, 20)
        check(case, "checkpoint exists before the window is replayed", ckpt.exists(), str(ckpt))
        if ckpt.exists():
            shutil.copy2(ckpt, backup)
    my_exec(SRC_A, f"UPDATE `{SRC_A}`.`{spec.name}` SET v = 100 WHERE id <= 5")
    my_exec(SRC_A, f"DELETE FROM `{SRC_A}`.`{spec.name}` WHERE id = 20")
    insert_rows(SRC_A, spec, "id, v", "(21, 21), (22, 22)")
    if backup.exists():
        shutil.copy2(backup, ckpt)
    with session("fault", reset_checkpoint=False):
        wait_alive(6)
        wait_until(lambda: count_of(spec) == 21, timeout=240, desc="replayed window lands")
        reconcile(case, "after replay", SRC_A, spec.name, timeout=120)
        check(case, "the replay does not duplicate keys", count_of(spec) == src_count(spec),
              f"target={count_of(spec)} source={src_count(spec)}")

    def hammer(worker: int) -> None:
        for step in range(40):
            key = (step + worker) % 6 + 1
            my_exec(SRC_A, f"UPDATE `{SRC_A}`.`{spec.name}` SET v = v + 1 WHERE id = {key}")

    with session("fault"):
        wait_alive(6)
        wait_for_sink_rows(case, SRC_A, spec.name, 21)
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(hammer, range(4)))
        note(case, "ordering claim: cross-writer order is binlog commit order only")
        reconcile(case, "after four concurrent writers", SRC_A, spec.name, timeout=240)
    return finish(case)


# Row 1 uses an extreme for every supported type; row 2 leaves every nullable
# column NULL so the snapshot also covers the NULL path.
TYPED_SEED = """
    (1, -128, 255, -32768, 65535, -8388608, -2147483648, 4294967295,
     -9223372036854775808, 18446744073709551615,
     123456789012.123456, 1.5, 2.25,
     'ab', 'value-χ', 'text value',
     X'01020304', X'FFEE', X'00FF00',
     '2024-02-29', '2024-02-29 12:34:56.123456', '2024-02-29 12:34:56.654321',
     2024, '{"a": [1, 2], "b": "χ"}', 1),
    (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
     NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
"""


def tp_p16() -> Case:
    case = new_case(
        "TP-P16",
        "Type round-trip over snapshot and binlog, and the types the sink refuses",
        "every supported column type on one row plus an all-NULL row, then a full-row UPDATE; "
        "a second pipeline captures a table carrying TIME, which the sink must refuse",
        "pg_catalog types match the documented mapping, every value is canonically equal after "
        "the snapshot and after the update, and the TIME pipeline exits non-zero with a refusal "
        "naming the type, leaving no target column behind",
    )
    spec = TABLES["tp_p16_types"]
    refused = TABLES["tp_p16_refused"]
    fresh(spec)
    fresh(refused)
    insert_rows(SRC_A, spec, ", ".join(spec.columns), TYPED_SEED)
    insert_rows(SRC_A, refused, "id, c_time", "(1, '12:34:56')")
    with session("main"):
        wait_alive(6)
        wait_until(lambda: count_of(spec) == 2, timeout=120, desc="both typed rows arrive")
        expect_column_types(case, SRC_A, spec.name, EXPECTED_PG_TYPES)
        reconcile(case, "snapshot", SRC_A, spec.name, timeout=120)
        my_exec(
            SRC_A,
            f"UPDATE `{SRC_A}`.`{spec.name}` SET "
            "c_tiny = 127, c_utiny = 0, c_med = 1, c_uint = 7, c_ubig = 1, "
            "c_dec = -0.000001, c_float = -1.5, c_double = 1e300, "
            "c_char = 'zz', c_vc = 'updated', c_text = NULL, c_bin = X'FFFFFFFF', "
            "c_vbin = X'00', c_blob = X'', c_date = '1970-01-01', "
            "c_dt = '2038-01-19 03:14:07.999999', c_ts = '2024-06-01 00:00:00.000001', "
            "c_year = 1999, c_json = '{\"z\": 0}', c_bool = 0 WHERE id = 1",
        )
        # The widest row event in this suite: prove it reaches the target before
        # diffing 25 columns. A pipeline that is alive but never applies it and a
        # pipeline that died produce the same diff, so record which one it was.
        schema, sink_table = target_of(spec, SRC_A)
        try:
            wait_until(
                lambda: pg_client.q(f'SELECT c_tiny FROM "{schema}"."{sink_table}" WHERE id = 1')
                [0]["c_tiny"] == 127,
                timeout=60, interval=0.5, desc="the typed UPDATE reaches the target",
            )
        except TimeoutError as exc:
            note(case, f"the typed UPDATE never landed: {exc} | job "
                       f"{'alive' if job_alive() else 'stopped'} | {log_errors(6)}")
        reconcile(case, "after binlog update", SRC_A, spec.name, timeout=120)
    # TIME/SET/ENUM/BIT are refused, so a pipeline that captures such a table has
    # to stop rather than create a target column the mapping does not define. It
    # runs as its own session: `main` never captures tp_p16_refused, so a check
    # made while only that pipeline was running could only ever pass.
    refused_schema, refused_table = target_of(refused, SRC_A)
    with session("unsupported", expect_exit=True):
        code = wait_exit(timeout=120, desc="job on a refused TIME column")
    check(case, "a type the sink refuses stops the job", code != 0, f"exit={code}")
    check(case, "the refusal names the unsupported MySQL type",
          "mysql type time is not supported" in log_text().lower(), log_tail(6))
    check(case, "a refused TIME column never reaches the target",
          not pg_client.table_exists(refused_schema, refused_table)
          or "c_time" not in pg_client.table_columns(refused_schema, refused_table),
          f"{pg_client.table_columns(refused_schema, refused_table)}")
    return finish(case)


def tp_p17() -> Case:
    case = new_case(
        "TP-P17",
        "PostgreSQL restart while the pipeline is writing (opt-in)",
        "PG_RESTART_ENABLED=1: restart the compose service during a write stream and reconcile",
        "the job recovers or is restarted, the final state is equal, and the target is reachable again; "
        "SKIPPED when the switch is off, which is the default",
    )
    if cfg.PG_RESTART_ENABLED != "1":
        note(case, "PG_RESTART_ENABLED != 1: the container-restart scenario is not part of this run")
        note(case, "enable with PG_RESTART_ENABLED=1 (and optionally PG_RESTART_CMD)")
        return finish(case, "SKIPPED")
    spec = TABLES["tp_p17_restart"]
    fresh(spec)
    insert_rows(SRC_A, spec, "id, v", "(1, 1), (2, 2)")
    stop = threading.Event()

    def writer() -> None:
        row = 100
        while not stop.is_set():
            my_exec(SRC_A, f"INSERT INTO `{SRC_A}`.`{spec.name}` (id, v) VALUES ({row}, {row})")
            row += 1
            time.sleep(0.05)

    with session("restart"):
        wait_alive(6)
        wait_for_sink_rows(case, SRC_A, spec.name, 2)
        thread = threading.Thread(target=writer, daemon=True)
        thread.start()
        try:
            time.sleep(5)
            restart_pg()
            time.sleep(10)
        finally:
            stop.set()
            thread.join(timeout=15)
        check(case, "the target accepts connections again", pg_client.ping())
        survived = job_alive()
        note(case, f"restart: job {'survived' if survived else 'exited'}")
        if not survived:
            note(case, log_tail(4))
            start_job(build_yaml("restart"), WORKDIR / "cdc-pg-restart-recover.log")
            wait_alive(10)
        reconcile(case, "after the server restart", SRC_A, spec.name, timeout=300)
    return finish(case)


def tp_p18() -> Case:
    case = new_case(
        "TP-P18",
        "Network interruption between the pipeline and the target",
        "no injector is run",
        "recorded as SKIPPED: this repository has no facility to cut the network path, and "
        "a SQL permission error is not a network fault, so the scenario stays uncovered",
    )
    note(case, "no network-fault injector exists in tests/integration; not simulated with a "
               "permission or authentication error")
    note(case, "covered by other means: TP-P14 injects 57P01/57014/28000 at the SQL layer, "
               "TP-P17 restarts the server")
    return finish(case, "SKIPPED")


def tp_p19() -> Case:
    case = new_case(
        "TP-P19",
        "Permanent target error: ON CONFLICT without a unique constraint (42P10)",
        "pre-create the target table without a primary key, then let the sink upsert into it",
        "the job stops instead of spinning, the error names the missing constraint, no row is "
        "written, and the pipeline reconciles once the key exists",
    )
    spec = TABLES["tp_p19_perm"]
    fresh(spec)
    schema, table = target_of(spec, SRC_A)
    pg_client.exec_sql(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
    pg_client.exec_sql(f'CREATE TABLE "{schema}"."{table}" (id bigint NOT NULL, v integer NOT NULL)')
    insert_rows(SRC_A, spec, "id, v", "(1, 1), (2, 2)")
    with session("perm"):
        code = wait_exit(timeout=120, desc="job on 42P10")
    check(case, "the job stops instead of retrying forever", code != 0, f"exit={code}")
    # The sink validates the target structure before the first write, so the
    # refusal may name the missing key instead of surfacing ON CONFLICT's 42P10.
    check(case, "the error names the missing key constraint",
          any(needle in log_text() for needle in
              ("42P10", "unique or exclusion constraint", "no primary key or unique constraint")),
          log_tail(4))
    check(case, "no row was written to the incompatible target",
          pg_client.row_count(schema, table) == 0, f"{pg_client.row_count(schema, table)} rows")
    pg_client.exec_sql(f'ALTER TABLE "{schema}"."{table}" ADD PRIMARY KEY (id)')
    start_job(build_yaml("perm"), WORKDIR / "cdc-pg-perm-recover.log")
    try:
        wait_alive(6)
        reconcile(case, "after adding the key constraint", SRC_A, spec.name, timeout=180)
    finally:
        stop_job()
    return finish(case)


def tp_p20() -> Case:
    case = new_case(
        "TP-P20",
        "Keyless source table: plain INSERT and no conflict target on the target",
        "a source table with no primary key and no primary-keys transform",
        "the rows arrive and are equal; the target has no primary key, which is the documented "
        "at-least-once limit (a replayed window duplicates rows) and is recorded, not hidden",
    )
    spec = TABLES["tp_p20_nokey"]
    fresh(spec)
    insert_rows(SRC_A, spec, "id, v", "(1, 'a'), (2, 'b'), (3, 'c')")
    with session("nokey", source={"scan.incremental.snapshot.chunk.key-column": "id"}):
        wait_alive(6)
        wait_for_sink_rows(case, SRC_A, spec.name, 3)
        reconcile(case, "keyless snapshot", SRC_A, spec.name, timeout=90)
        schema, table = target_of(spec, SRC_A)
        keys = pg_client.primary_keys(schema, table)
        check(case, "the target has no primary key (plain INSERT path)", keys == [], f"{keys}")
        note(case, "an UPDATE on a keyless table renders a plain INSERT, so it cannot be "
                   "expressed as a single row; replayed windows duplicate rows (at-least-once)")
    return finish(case)


# ------------------------------------------------------------ run and report

def restart_pg() -> None:
    """Restart the integration PostgreSQL service and wait until it answers."""
    command = cfg.PG_RESTART_CMD or "docker compose restart postgres"
    # The compose project comes from run.sh, which exports COMPOSE_PROJECT_NAME
    # and COMPOSE_FILE. Guessing a name here would restart the wrong stack (or
    # fail with "no such project" instead of saying why).
    project = os.environ.get("COMPOSE_PROJECT_NAME", "")
    if not project:
        raise RuntimeError(
            "COMPOSE_PROJECT_NAME is not set; run this suite through "
            "`bash tests/integration/scripts/run.sh postgresql ...` so the "
            "compose project is resolvable"
        )
    environment = {**os.environ, "COMPOSE_PROJECT_NAME": project}
    log(f"    restart: {command}", flush=True)
    subprocess.run(command, shell=True, cwd=str(cfg.IT_ROOT), env=environment, check=True)
    deadline = time.time() + 180
    while time.time() < deadline:
        if pg_client.ping():
            return
        time.sleep(2)
    raise TimeoutError("PostgreSQL did not answer after the restart")


def preflight() -> None:
    if not Path(BINARY).exists():
        raise RuntimeError(f"go-cdc binary not found at {BINARY} (run `make build`)")
    if not pg_client.ping():
        raise RuntimeError(
            f"PostgreSQL is not reachable at {cfg.PG_HOST}:{cfg.PG_PORT} "
            "(run `bash tests/integration/scripts/run.sh postgresql normal pg16`)"
        )
    major = pg_client.major_version()
    if cfg.PG_EXPECT_MAJOR and str(major) != cfg.PG_EXPECT_MAJOR:
        raise RuntimeError(
            f"this run expects PostgreSQL {cfg.PG_EXPECT_MAJOR}, the server is "
            f"{pg_client.server_version()}"
        )
    try:
        my(SRC_A, "SELECT 1 AS ok")
    except Exception as exc:  # noqa: BLE001 - any failure means MySQL is unusable
        raise RuntimeError(
            f"MySQL is not reachable at {cfg.MYSQL_HOST}:{cfg.MYSQL_PORT} or lacks "
            f"{SRC_A} (run `bash tests/integration/scripts/run.sh mysql`): {exc}"
        ) from exc


def prepare() -> None:
    """Fresh target schemas and no stale checkpoints before the first case."""
    pg_client.ensure_database()
    for schema in (SNK_A, SNK_B):
        pg_client.drop_schema(schema)
        pg_client.create_schema(schema)
    for directory in (cfg.CHECKPOINT_PG_DIR, cfg.CHECKPOINT_PG_RESTART_DIR):
        for path in Path(directory).rglob("*.ckpt"):
            path.unlink()


def run_case(tp_id: str, run) -> None:
    """Run one test point; an exception is a FAIL of that point, not of the suite."""
    before = len(cases)
    try:
        run()
    except Exception as exc:  # noqa: BLE001 - one case must not stop the others
        log(f"    [error] {tp_id}: {exc!r}", flush=True)
        if len(cases) > before:
            case = cases[-1]
            check(case, "the case completed without an exception", False, f"{exc!r} | {log_errors()}")
            if not case.status:
                finish(case, "FAIL")
        else:
            case = new_case(tp_id, tp_id, "n/a", "n/a")
            check(case, "the case started", False, repr(exc))
            finish(case, "FAIL")
    finally:
        stop_job()


def _status_cell(status: str) -> str:
    return {"PASS": "✅ PASS", "FAIL": "❌ FAIL"}.get(status, f"⊘ {status}")


def write_report() -> Path:
    cfg.REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = cfg.REPORT_PG_FUNC
    passed = sum(1 for case in cases if case.status == "PASS")
    failed = sum(1 for case in cases if case.status == "FAIL")
    other = len(cases) - passed - failed
    try:
        version = pg_client.server_version()
    except Exception:  # noqa: BLE001 - the report still has to be written
        version = "unknown"
    lines = [
        "# go-cdc mysql2postgresql E2E report (expected vs actual)\n",
        '<a id="overview"></a>\n',
        "## 0. Overview\n",
        f"- Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Source: `{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}` / `{cfg.SRC_USER}` / `{SRC_A}`, `{SRC_B}`",
        f"- Target: `{cfg.PG_HOST}:{cfg.PG_PORT}` / `{cfg.PG_USER}` / `{cfg.PG_DB}` "
        f"(schemas `{SNK_A}`, `{SNK_B}`)",
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
        "## 1. Data flow\n",
        "```mermaid",
        "flowchart LR",
        f"  a[{SRC_A}] --> job[go-cdc]",
        f"  b[{SRC_B}] --> job",
        f"  job --> sa[{cfg.PG_DB}.{SNK_A}]",
        f"  job --> sb[{cfg.PG_DB}.{SNK_B}]",
        "```\n",
        "## 2. Deliberate deviations from the mysql2mysql templates\n",
        "- `ENUM`, `SET`, `BIT`, `TIME` and the spatial types are refused by the PostgreSQL "
        "sink, so TP-P16 seeds only the supported types, then runs a second pipeline over a "
        "`TIME` table and asserts that it exits non-zero with a refusal naming the type and "
        "never reaches the target.",
        "- A bare JSON `null` is indistinguishable from SQL NULL through psycopg2, so the seed "
        "uses a nested null (`{\"n\": null}`) instead.",
        "- `TIMESTAMP` is read with `SET time_zone = '+08:00'`, the pipeline's "
        "`server-time-zone`, so both sides are compared as the same wall clock.",
        "- TP-P02 runs its pipeline with `write-batch-size: 1`. One row per transaction is "
        "what makes the snapshot observably partial: with the default batch of 100 the "
        "whole snapshot lands between two polls and the overlap can never be proven.",
        "- TP-P20 runs alone with `scan.incremental.snapshot.chunk.key-column: id`: the "
        "source needs a NOT NULL chunk key to split a keyless table, and the option names "
        "one column, so it cannot be set for a session that also captures `tp_p04_pk`.",
        "",
        "## 3. Test points\n",
    ]
    for case in cases:
        lines += [
            f"### {case.id} {case.title}\n",
            f"- [↑ Overview](#overview)",
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


PLAN = (
    ("TP-P01", tp_p01),
    ("TP-P02", tp_p02),
    ("TP-P03", tp_p03),
    ("TP-P04", tp_p04),
    ("TP-P05", tp_p05),
    ("TP-P06", tp_p06),
    ("TP-P07", tp_p07),
    ("TP-P08", tp_p08),
    ("TP-P09", tp_p09),
    ("TP-P10", tp_p10),
    ("TP-P11", tp_p11),
    ("TP-P12", tp_p12),
    ("TP-P13", tp_p13),
    ("TP-P14", tp_p14),
    ("TP-P15", tp_p15),
    ("TP-P16", tp_p16),
    ("TP-P17", tp_p17),
    ("TP-P18", tp_p18),
    ("TP-P19", tp_p19),
    ("TP-P20", tp_p20),
)


def main(argv: list[str] | None = None) -> int:
    wanted = {
        arg.upper()
        for arg in (sys.argv[1:] if argv is None else argv)
        if not arg.startswith("-")
    }
    try:
        preflight()
    except Exception as exc:  # noqa: BLE001 - an unusable environment is not a failure
        log(f"ENV BLOCKED: {exc}", flush=True)
        return 2
    prepare()
    for tp_id, run in PLAN:
        if wanted and tp_id not in wanted:
            continue
        run_case(tp_id, run)
    path = write_report()
    passed = sum(1 for case in cases if case.status == "PASS")
    failed = sum(1 for case in cases if case.status == "FAIL")
    log("REPORT", cfg.project_display_path(path))
    log("PASS", passed, "FAIL", failed, "OTHER", len(cases) - passed - failed)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        stop_job()
