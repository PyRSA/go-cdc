"""Elasticsearch 8 scale suite: every MySQL scale table lands in one ES index.

The MySQL scale suite (`common/scale_it.py`) is the reference implementation:
same profiles (`normal` / `heavy`), same source databases and templates, same
seed / mutate / burst / peak helpers. This module only swaps the target side:

  * the pipeline renders an `elasticsearch` sink instead of `mysql`,
  * snapshot / catch-up / final checks read documents over HTTP
    (`harness.es_client`) and compare them with an independent oracle
    (`harness.es_verify`),
  * checksums are `(count, SUM(id), SUM(CRC32(col)))` computed by scrolling the
    whole index, so `_count` is never treated as proof of content.

Profiles (unchanged from the MySQL suite):

  normal  5 source DBs / 20 tables / 50k rows, light mutate + restart
  heavy   10 source DBs / 50 tables / 300k rows, light mutate, then 50k extra
          rows per hotspot (one per DB), a 45s peak with 8 writers, and a
          bounded catch-up. Writes must converge before the final checksums
          are accepted.

Run it with `scripts/run.sh elasticsearch`; reports land in
`docs/test/reports/mysql-elasticsearch-scale-{profile}.md`.
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
from datetime import datetime
from pathlib import Path

from ..harness import es_verify as verify
from common.config import (
    CDC_SERVER_ID_ES_SCALE,
    ES_PASSWORD,
    ES_REQUEST_TIMEOUT,
    ES_SCALE_INDEX_PREFIX,
    ES_URL,
    ES_USERNAME,
    MYSQL_HOST,
    MYSQL_PORT,
    REPORT_DIR,
    SRC_PASSWORD,
    SRC_USER,
    WORKDIR,
    it_rel_path,
    md_cell,
    project_display_path,
    sanitize_report_text,
)
from ..harness.es_client import ESClient
from common.logutil import log
from common.sysstat import format_cpu_mem, format_disk, format_host_res
from common import scale_it
from common.profiles import Profile, TableSpec, hotspot_tables, load_profile
from common.schemas import checksum_column, create_table_sql, table_columns

ES = ESClient(
    ES_URL,
    timeout=ES_REQUEST_TIMEOUT,
    username=ES_USERNAME,
    password=ES_PASSWORD,
)

# Total wall-clock budget for a profile run (same limit as the MySQL suite).
SCALE_TOTAL_TIMEOUT = int(os.environ.get("SCALE_TOTAL_TIMEOUT", "1800"))
# Rows inserted before the restart phase of the normal profile.
RESTART_ROWS = int(os.environ.get("ES_SCALE_RESTART_ROWS", "100"))

_proc: subprocess.Popen | None = None
_log_handle = None
_final_checksum_rows: dict[str, tuple[tuple[int, int, int], tuple[int, int, int], bool]] = {}
_content_samples: dict[str, list[str]] = {}


# ------------------------------------------------------------------ helpers
def index_for(spec: TableSpec) -> str:
    """Index name the sink derives for this final table (ES v1 rule)."""
    return verify.index_name_for("", spec.snk_db, spec.sink_name)


def report_path(profile: Profile) -> Path:
    return REPORT_DIR / f"mysql-elasticsearch-scale-{profile.name}.md"


def remaining(deadline: float, floor: float = 60.0) -> float:
    return max(floor, deadline - time.time())


def _ensure_source_schema(profile: Profile) -> None:
    """Create the source DBs/tables and wipe leftovers (source side only).

    The MySQL scale suite wipes its sink DBs as well; Elasticsearch has no
    such state, so only the source databases are touched here.
    """
    src_dbs = sorted({t.src_db for t in profile.tables})
    with scale_it.admin_conn() as conn:
        with conn.cursor() as cur:
            for db in src_dbs:
                cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db}` DEFAULT CHARACTER SET utf8mb4")
                cur.execute(f"GRANT ALL PRIVILEGES ON `{db}`.* TO %s@'%%'", (SRC_USER,))
                cur.execute(
                    "SELECT table_name AS name FROM information_schema.tables WHERE table_schema=%s",
                    (db,),
                )
                for row in cur.fetchall():
                    cur.execute(f"DROP TABLE IF EXISTS `{db}`.`{row['name']}`")
            for spec in profile.tables:
                cur.execute(f"USE `{spec.src_db}`")
                cur.execute(create_table_sql(spec.template, spec.name))
            cur.execute("FLUSH PRIVILEGES")


def _clean_indices() -> list[str]:
    removed = ES.delete_indices(ES_SCALE_INDEX_PREFIX)
    if removed:
        log(f"    removed {len(removed)} stale index(es)")
    return removed


# ------------------------------------------------------------------ process
def _log_path(profile: Profile) -> Path:
    return WORKDIR / f"cdc-es-scale-{profile.name}.log"


def _yaml_path(profile: Profile) -> Path:
    return WORKDIR / f"pipeline.es-scale-{profile.name}.yaml"


def _ckpt_dir(profile: Profile) -> Path:
    return WORKDIR / f"ckpts-es-scale-{profile.name}"


def _start_job(profile: Profile) -> subprocess.Popen:
    global _proc, _log_handle
    bin_path = scale_it.ensure_binary()
    log_path = _log_path(profile)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    _log_handle = log_path.open("a", encoding="utf-8")
    handle = _log_handle
    _proc = subprocess.Popen(
        [str(bin_path), str(_yaml_path(profile))],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    def pump() -> None:
        assert _proc is not None and _proc.stdout is not None
        for line in _proc.stdout:
            handle.write(line)
            handle.flush()

    threading.Thread(target=pump, daemon=True).start()
    return _proc


def _stop_job(proc: subprocess.Popen | None) -> None:
    global _proc, _log_handle
    if proc is not None and proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
    if proc is _proc:
        _proc = None
    if _log_handle is not None:
        _log_handle.close()
        _log_handle = None


def _tail(profile: Profile, limit: int = 2500) -> str:
    path = _log_path(profile)
    if not path.exists():
        return "(no log)"
    return path.read_text(encoding="utf-8", errors="replace")[-limit:]


def _assert_alive(proc: subprocess.Popen | None, profile: Profile, phase: str) -> None:
    if proc is not None and proc.poll() is not None:
        raise RuntimeError(f"go-cdc exited during {phase} ({proc.returncode}):\n{_tail(profile)}")


# -------------------------------------------------------------- pipeline yaml
def render_pipeline_yaml(profile: Profile, path: Path) -> None:
    """Same source/route/transform as the MySQL scale pipeline, ES sink target."""
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
        f"  server-id: {CDC_SERVER_ID_ES_SCALE}",
        "  server-time-zone: Asia/Shanghai",
        "  scan.startup.mode: initial",
        "  scan.incremental.snapshot.chunk.size: 4096",
        "  scan.snapshot.fetch.size: 1024",
        "  schema-change.enabled: false",
        "  connect.timeout: 30s",
        "",
        "sink:",
        "  type: elasticsearch",
        f'  hosts: ["{ES_URL}"]',
        "  auto-create: true",
        "  write-batch-size: 1000",
        "  write-batch-interval: 500ms",
        "  max-retries: 3",
        "  request-timeout: 60s",
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
            r"  - source-table: 'db_tp_scale_src_\d+\.tp_scale_\d+'",
            "    projection: '*'",
            "    primary-keys: id",
            "",
            "pipeline:",
            f"  name: tp_es_scale_pipeline_{profile.name}",
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


# ------------------------------------------------------------------ reading
def _es_count(spec: TableSpec) -> int:
    index = index_for(spec)
    ES.refresh(index)
    return ES.count(index)


_MARKER_COLUMN = {"basic": "name", "large": "name", "indexed": "status", "wide": "c_label"}


def _es_doc(spec: TableSpec, row: dict) -> dict | None:
    index = index_for(spec)
    doc_id = verify.expected_doc_id(spec.snk_db, spec.sink_name, {"id": "bigint"}, row)
    ES.refresh(index)
    return ES.get_doc(index, doc_id)


def _user_conn(spec: TableSpec):
    """Read-only source connection pinned to the pipeline's server time zone.

    `TIMESTAMP` columns are converted with `server-time-zone`
    (`Asia/Shanghai` = `+08:00`) when they become documents, so the oracle has to
    read them in the same zone. `DATETIME` columns are zone-agnostic.
    """
    conn = scale_it._user_conn(SRC_USER, SRC_PASSWORD, spec.src_db)
    with conn.cursor() as cur:
        cur.execute("SET time_zone = '+08:00'")
    return conn


def _rows_by_ids(spec: TableSpec, ids: list[int], columns: dict[str, str]) -> dict[int, dict]:
    wanted = ", ".join(f"`{name}`" for name in columns)
    placeholders = ",".join(["%s"] * len(ids))
    conn = _user_conn(spec)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {wanted} FROM `{spec.src_db}`.`{spec.name}` WHERE `id` IN ({placeholders})",
                tuple(ids),
            )
            rows = list(cur.fetchall())
    finally:
        conn.close()
    return {int(row["id"]): row for row in rows}


# ------------------------------------------------------------------ checks
def verify_mappings(profile: Profile) -> list[str]:
    failures: list[str] = []
    for spec in profile.tables:
        mapping = ES.get_mapping(index_for(spec))
        for item in verify.mapping_problems(table_columns(spec.template), mapping):
            failures.append(f"{spec.src_db}.{spec.name}: mapping {item}")
    return failures


def verify_counts(profile: Profile) -> list[str]:
    failures: list[str] = []
    for spec in profile.tables:
        src_n = scale_it._count_rows(SRC_USER, SRC_PASSWORD, spec.src_db, spec.name)
        snk_n = _es_count(spec)
        if src_n != snk_n:
            failures.append(
                f"{spec.src_db}.{spec.name}: count mismatch src={src_n} es={snk_n} "
                f"(index={index_for(spec)})"
            )
    return failures


def _sample_ids(spec: TableSpec) -> list[int]:
    hi = scale_it._updated_id_hi(spec)
    if hi < 1:
        hi = spec.rows
    return sorted({1, max(1, hi // 2), hi})


def verify_markers(profile: Profile) -> list[str]:
    """Sample updated rows: ES must hold the post-update value, not just a count."""
    failures: list[str] = []
    for spec in profile.tables:
        if scale_it._updated_id_hi(spec) < 1:
            continue
        columns = table_columns(spec.template)
        column = _MARKER_COLUMN[spec.template]
        for row_id in _sample_ids(spec):
            src_row = _rows_by_ids(spec, [row_id], columns).get(row_id)
            if src_row is None:
                failures.append(f"{spec.src_db}.{spec.name}: id={row_id} missing in source")
                continue
            doc = _es_doc(spec, {"id": row_id})
            if doc is None:
                failures.append(f"{spec.src_db}.{spec.name}: id={row_id} missing in {index_for(spec)}")
                continue
            if not verify.compare_value(columns[column], src_row[column], doc.get(column)):
                failures.append(
                    f"{spec.src_db}.{spec.name}: id={row_id} {column} "
                    f"src={src_row[column]!r} es={doc.get(column)!r}"
                )
    return failures


def verify_checksums(profile: Profile) -> list[str]:
    """Streaming content checksum per table (scroll paging, never `_count`)."""
    failures: list[str] = []
    for spec in profile.tables:
        column = checksum_column(spec.template)
        src_triple = scale_it.fetch_checksum(SRC_USER, SRC_PASSWORD, spec.src_db, spec.name, spec.template)
        es_triple = verify.stream_checksum(ES.scan(index_for(spec)), crc_field=column)
        ok = src_triple == es_triple
        label = f"{spec.src_db}.{spec.name}"
        _final_checksum_rows[label] = (src_triple, es_triple, ok)
        if not ok:
            failures.append(f"{label}: checksum src={src_triple} es={es_triple}")
    return failures


def verify_content_samples(profile: Profile) -> list[str]:
    """Full document comparison for two rows per table (beyond the checksum)."""
    failures: list[str] = []
    for spec in profile.tables:
        columns = table_columns(spec.template)
        ids = sorted({1, max(1, scale_it._count_rows(SRC_USER, SRC_PASSWORD, spec.src_db, spec.name))})
        rows = _rows_by_ids(spec, ids, columns)
        problems: list[str] = []
        for row_id, row in rows.items():
            doc = _es_doc(spec, row)
            if doc is None:
                problems.append(f"id={row_id}: document missing")
                continue
            problems.extend(f"id={row_id}: {item}" for item in verify.compare_row(columns, row, doc))
        _content_samples[f"{spec.src_db}.{spec.name}"] = problems
        failures.extend(f"{spec.src_db}.{spec.name}: {item}" for item in problems)
    return failures


def verify_index_isolation(profile: Profile) -> list[str]:
    """A shared index must not merge documents from different final tables.

    ES v1's `_id` hashes the *routed* table name, so two final tables that share
    one index keep distinct ids even when their primary keys collide. The
    internal names never collide either (normalization appends a hash whenever
    it changes a name), which is asserted here as a regression guard.
    """
    failures: list[str] = []
    indices = [index_for(spec) for spec in profile.tables]
    if len(set(indices)) != len(indices):
        duplicates = sorted({name for name in indices if indices.count(name) > 1})
        failures.append(f"derived index names collide: {duplicates}")
    by_id: dict[str, list[str]] = {}
    for spec in profile.tables:
        by_id.setdefault(spec.sink_name, []).append(spec.snk_db)
    for sink_name, dbs in by_id.items():
        if len(dbs) > 1:
            failures.append(f"final table {sink_name} is routed from several DBs: {dbs}")
    return failures


def verify_contract(profile: Profile) -> list[str]:
    """Cheap checks (`mapping` + index isolation) used after the full wait.

    `wait_final` already proves count / marker / checksum / content agreement, so
    the tail of a run must not scroll every index a second time.
    """
    return verify_mappings(profile) + verify_index_isolation(profile)


# -------------------------------------------------------------------- waits
def wait_snapshot(profile: Profile, proc: subprocess.Popen | None, *, timeout: float) -> float:
    t0 = time.time()
    deadline = t0 + timeout
    last_log = 0.0
    while time.time() < deadline:
        _assert_alive(proc, profile, "snapshot")
        ready = 0
        missing = 0
        rows_snk = 0
        for spec in profile.tables:
            index = index_for(spec)
            if not ES.index_exists(index):
                missing += 1
                continue
            count = _es_count(spec)
            rows_snk += count
            if count == spec.rows:
                ready += 1
        now = time.time()
        if now - last_log >= 15.0 or ready == len(profile.tables):
            log(
                f"    snapshot catch-up {now - t0:.0f}s: ready={ready}/{len(profile.tables)} "
                f"missing_index={missing} docs={rows_snk}/{profile.total_rows}",
                flush=True,
            )
            last_log = now
        if ready == len(profile.tables):
            elapsed = time.time() - t0
            log(f"TIMING  wait [es_snapshot] done in {elapsed:.1f}s", flush=True)
            return elapsed
        time.sleep(15.0)
    raise TimeoutError(f"timeout waiting for ES snapshot after {time.time() - t0:.1f}s")


def wait_final(profile: Profile, proc: subprocess.Popen | None, *, phase: str, timeout: float) -> float:
    """Bounded wait for count + marker + checksum + content agreement."""
    t0 = time.time()
    deadline = t0 + timeout
    last_failures: list[str] = []
    last_log = 0.0
    format_cpu_mem()  # baseline for the periodic CPU delta
    while time.time() < deadline:
        _assert_alive(proc, profile, phase)
        failures = verify_counts(profile)
        if not failures:
            failures = verify_markers(profile)
        if not failures:
            failures = verify_checksums(profile)
            if not failures:
                failures = verify_content_samples(profile)
        last_failures = failures
        now = time.time()
        if now - last_log >= 15.0 or not failures:
            sample = "; ".join(failures[:3]) if failures else "ok"
            cdc_pid = proc.pid if proc is not None else None
            log(
                f"    {phase} catch-up {now - t0:.0f}s: mismatches={len(failures)} {sample} "
                f"| {format_host_res(cdc_pid)}",
                flush=True,
            )
            last_log = now
        if not failures:
            elapsed = time.time() - t0
            log(f"TIMING  wait [{phase}] done in {elapsed:.1f}s", flush=True)
            return elapsed
        time.sleep(15.0)
    sample = "\n".join(last_failures[:5])
    raise TimeoutError(
        f"timeout waiting for ES {phase} catch-up after {time.time() - t0:.1f}s "
        f"({len(last_failures)} mismatches):\n{sample}"
    )


def wait_hotspot_lag(profile: Profile, hotspots: list[TableSpec], proc: subprocess.Popen | None, *, timeout: float) -> float:
    """Progress gate only: hotspot document counts within 1% of the source."""
    t0 = time.time()
    deadline = t0 + timeout
    last_log = 0.0
    while time.time() < deadline:
        _assert_alive(proc, profile, "hotspot catch-up")
        ok = True
        parts: list[str] = []
        for spec in hotspots:
            src_n = scale_it._count_rows(SRC_USER, SRC_PASSWORD, spec.src_db, spec.name)
            snk_n = _es_count(spec)
            delta = abs(src_n - snk_n) / max(src_n, 1)
            parts.append(f"{spec.src_db}.{spec.name} src={src_n} es={snk_n} delta={delta:.2%}")
            if delta > 0.01:
                ok = False
        now = time.time()
        if now - last_log >= 15.0 or ok:
            cdc_pid = proc.pid if proc is not None else None
            log(f"    hotspot lag {now - t0:.0f}s: " + " | ".join(parts) + f" | {format_host_res(cdc_pid)}", flush=True)
            last_log = now
        if ok:
            elapsed = time.time() - t0
            log(f"TIMING hotspot_lag {elapsed:.1f}s", flush=True)
            return elapsed
        time.sleep(15.0)
    raise TimeoutError(f"timeout waiting for hotspot lag <= 1% after {time.time() - t0:.1f}s")


# ------------------------------------------------------------------- report
def write_report(profile: Profile, timings: dict[str, float], failures: list[str], extra: dict[str, object]) -> Path:
    out = report_path(profile)
    out.parent.mkdir(parents=True, exist_ok=True)
    passed = not failures
    src_dbs = sorted({t.src_db for t in profile.tables})
    lines = [
        f"# go-cdc Elasticsearch sink scale report ({profile.name})\n",
        f"- Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Result: {'✅ PASS' if passed else '❌ FAIL'}",
        f"- Profile: `{profile.name}`",
        f"- Sink: Elasticsearch 8 at `{ES_URL}` (index per final table, `auto-create: true`)",
        f"- Tables: {len(profile.tables)} over {len(src_dbs)} source DBs",
        f"- Total rows (seed): {profile.total_rows}",
        f"- Workers / parallelism: {profile.workers}",
        f"- Total timeout budget: {SCALE_TOTAL_TIMEOUT}s",
        "",
        "## Verification method\n",
        "- Snapshot and catch-up waits poll `_count` only as a progress gate.",
        "- Final verification per table: mapping contract, exact count, update markers,",
        "  streaming checksum `(COUNT, SUM(id), SUM(CRC32(col)))` over scroll pages of 1000,",
        "  plus a full two-row document comparison (NULL, DECIMAL, JSON, BLOB, date/time).",
        "- `_id` is recomputed independently (`harness.es_verify`) from the routed table name",
        "  and primary key, so a wrong id fails the marker/content check.",
        "",
    ]
    if extra.get("hotspots"):
        lines.extend(
            [
                "## Hotspot burst / peak\n",
                f"- Rows per hotspot table: `{extra.get('burst_rows')}`",
                f"- Peak seconds / workers / batch: `{extra.get('peak_seconds')}` / "
                f"`{extra.get('peak_workers')}` / `{extra.get('peak_batch')}`",
                f"- Peak rows written (measured): `{extra.get('peak_rows', 'n/a')}`",
                f"- Lag <= 1% reached in: `{extra.get('lag_seconds', 'n/a')}` s",
                "- Hotspot tables:",
            ]
        )
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
        for message in failures[:50]:
            lines.append(f"- `{message}`")
        lines.append("")
    if _final_checksum_rows:
        lines.extend(
            [
                "## Final checksums (src vs ES)\n",
                "| Table | MySQL (cnt, sum_id, crc) | ES (cnt, sum_id, crc) | |",
                "| --- | --- | --- | --- |",
            ]
        )
        for label, (src_triple, es_triple, ok) in list(_final_checksum_rows.items())[:60]:
            lines.append(f"| `{label}` | {src_triple} | {es_triple} | {'✅' if ok else '❌'} |")
        lines.append("")
    if _content_samples:
        problem_tables = {label: items for label, items in _content_samples.items() if items}
        lines.append("## Document content samples\n")
        lines.append(
            f"- Tables sampled: {len(_content_samples)}; two rows per table compared field by field."
        )
        lines.append(f"- Tables with mismatches: {len(problem_tables)}")
        for label, items in list(problem_tables.items())[:10]:
            lines.append(f"  - `{label}`: {md_cell(items[:3])}")
        lines.append("")
    lines.extend(
        [
            "## Tables\n",
            "| ordinal | src | final table | index | template | rows | event_group |",
            "| ---: | --- | --- | --- | --- | ---: | --- |",
        ]
    )
    for spec in profile.tables:
        lines.append(
            f"| {spec.ordinal} | `{spec.src_db}.{spec.name}` | `{spec.snk_db}.{spec.sink_name}` | "
            f"`{index_for(spec)}` | {spec.template} | {spec.rows} | {spec.event_group} |"
        )
    lines.append("")
    out.write_text(sanitize_report_text("\n".join(lines)), encoding="utf-8")
    return out


# ------------------------------------------------------------------ profile
def _new_mutation_rows(profile: Profile) -> int:
    """Insert a few rows into the first table (restart / recovery check)."""
    spec = profile.tables[0]
    start = scale_it._max_id(spec) + 1
    end = start + RESTART_ROWS - 1
    conn = scale_it._user_conn(SRC_USER, SRC_PASSWORD, spec.src_db)
    try:
        scale_it.insert_batch(
            conn, spec.src_db, spec.name, spec.template, start, end, spec.db_index, spec.ordinal
        )
    finally:
        conn.close()
    return RESTART_ROWS


def run_profile(profile: Profile, until: str) -> int:
    log(
        f"==> es scale profile={profile.name} tables={len(profile.tables)} "
        f"total_rows={profile.total_rows} workers={profile.workers} until={until}",
        flush=True,
    )
    suite_t0 = time.time()
    deadline = suite_t0 + SCALE_TOTAL_TIMEOUT
    timings: dict[str, float] = {}
    failures: list[str] = []
    extra: dict[str, object] = {}
    out = report_path(profile)
    proc: subprocess.Popen | None = None

    if not ES.ping():
        log(f"ERROR elasticsearch is not reachable at {ES_URL}", file=sys.stderr, flush=True)
        return 2
    log(f"==> elasticsearch {ES.version()} at {ES_URL}", flush=True)

    _final_checksum_rows.clear()
    _content_samples.clear()

    _clean_indices()
    t0 = time.time()
    log("==> create source schema", flush=True)
    _ensure_source_schema(profile)
    log(f"    {format_disk(WORKDIR)}", flush=True)
    log("==> seed baseline", flush=True)
    timings["seed_baseline"] = scale_it.seed_all(profile)
    timings["schema_setup"] = time.time() - t0
    log(f"TIMING seed_baseline {timings['seed_baseline']:.1f}s", flush=True)
    if until == "seed":
        return 0

    ckpt = _ckpt_dir(profile)
    if ckpt.exists():
        shutil.rmtree(ckpt)
    ckpt.mkdir(parents=True, exist_ok=True)
    yaml_path = _yaml_path(profile)
    render_pipeline_yaml(profile, yaml_path)
    log(f"==> rendered {yaml_path}", flush=True)

    log("==> start go-cdc (snapshot -> ES8)", flush=True)
    _log_path(profile).write_text("", encoding="utf-8")
    proc = _start_job(profile)
    try:
        timings["snapshot"] = wait_snapshot(profile, proc, timeout=remaining(deadline))
        snapshot_failures = verify_mappings(profile) + verify_counts(profile) + verify_checksums(profile)
        if snapshot_failures:
            failures.extend(snapshot_failures)
            for message in snapshot_failures:
                log(f"FAIL {message}", file=sys.stderr, flush=True)
            return 1
        log(f"==> snapshot verify ok ({len(profile.tables)} tables)", flush=True)
        if until == "snapshot":
            return 0

        log("==> concurrent mutations", flush=True)
        timings["mutate"] = scale_it.mutate_all(profile)
        log(f"TIMING mutate {timings['mutate']:.1f}s", flush=True)
        timings["catchup"] = wait_final(profile, proc, phase="mutate", timeout=remaining(deadline))
        if until == "mutate":
            return 0

        if profile.name == "normal":
            # Recovery check: a restart on the same checkpoint must keep the data
            # identical and pick up rows written while the job was down.
            log("==> restart recovery check", flush=True)
            _stop_job(proc)
            inserted = _new_mutation_rows(profile)
            log(f"    inserted {inserted} rows into {profile.tables[0].src_db}.{profile.tables[0].name}", flush=True)
            t_restart = time.time()
            proc = _start_job(profile)
            timings["restart_catchup"] = wait_final(
                profile, proc, phase="restart", timeout=remaining(deadline)
            )
            timings["restart"] = time.time() - t_restart
            extra["restart_rows"] = inserted
            if until == "restart":
                return 0

        if profile.name == "heavy":
            hotspots = hotspot_tables(profile)
            burst_seconds, next_ids = scale_it.burst_hotspots(profile, hotspots, proc=proc)
            timings["burst"] = burst_seconds
            log(f"TIMING burst {burst_seconds:.1f}s", flush=True)

            peak_seconds, peak_rows = scale_it.peak_write_hotspots(profile, hotspots, next_ids, proc=proc)
            timings["peak_write"] = peak_seconds
            extra.update(
                {
                    "hotspots": [f"{h.src_db}.{h.name}" for h in hotspots],
                    "burst_rows": scale_it.SCALE_BURST_ROWS_PER_HOT,
                    "peak_seconds": scale_it.SCALE_PEAK_SECONDS,
                    "peak_workers": int(os.environ.get("SCALE_PEAK_WORKERS", str(profile.workers))),
                    "peak_batch": scale_it.SCALE_PEAK_BATCH,
                    "peak_rows": peak_rows,
                }
            )
            lag_seconds = wait_hotspot_lag(profile, hotspots, proc, timeout=remaining(deadline))
            extra["lag_seconds"] = f"{lag_seconds:.1f}"
            timings["hotspot_lag"] = lag_seconds
            # <= 1% is a progress gate; the final checksums decide correctness.
            timings["peak_catchup"] = wait_final(profile, proc, phase="peak", timeout=remaining(deadline))

        timings["final_verify"] = wait_final(profile, proc, phase="final", timeout=remaining(deadline))
        # `wait_final` already proved count/marker/checksum/content agreement; only
        # the cheap contract checks are repeated here (no second full scroll).
        final_failures = verify_contract(profile)
        failures.extend(final_failures)
        for message in final_failures:
            log(f"FAIL {message}", file=sys.stderr, flush=True)
        if not final_failures:
            log(f"==> final verify ok ({len(profile.tables)} tables)", flush=True)
    except Exception as exc:  # noqa: BLE001
        failures.append(f"aborted: {exc}")
        raise
    finally:
        timings["suite_total"] = time.time() - suite_t0
        write_report(profile, timings, failures, extra)
        log(f"REPORT {project_display_path(out)}", flush=True)
        _stop_job(proc)

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
    log(f"hotspots={len(hotspot_tables(profile))}")
    log(f"indices={len({index_for(t) for t in profile.tables})}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="go-cdc Elasticsearch scale suite")
    parser.add_argument("profile", nargs="?", default=None, help="profile name (normal|heavy)")
    parser.add_argument(
        "--print-plan",
        dest="print_plan",
        metavar="PROFILE",
        help="print the topology and exit (no MySQL/Elasticsearch needed)",
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
        _print_plan(load_profile(args.print_plan, total_rows, workers))
        return 0
    if args.profile:
        return run_profile(load_profile(args.profile, total_rows, workers), args.until)
    parser.print_help()
    return 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        _stop_job(_proc)
