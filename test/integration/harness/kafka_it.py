#!/usr/bin/env python3
"""Kafka sink integration suite (TP-K01..K08).

Functional (K01–K04): route / projection / filter / CRUD keys / DDL switch / restart.
Load-parity (K05–K08): snapshot volume, peak catch-up, typed spot-check, mega fields.
Requires KAFKA_BROKERS (compose kafka via scripts/kafka-up.sh).
"""
from __future__ import annotations

import base64
import json
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import pymysql
from kafka import KafkaConsumer, TopicPartition

from . import config as cfg
from . import load_it as load
from .logutil import log

SRC = cfg.conn_kwargs(cfg.SRC_USER, cfg.SRC_PASSWORD, cfg.FUNC_SRC_DB)
WORKDIR = cfg.WORKDIR
BINARY = cfg.BINARY
CK = Path(cfg.CHECKPOINT_KAFKA_PATH)
CK_LOAD = Path(cfg.CHECKPOINT_KAFKA_LOAD_PATH)
YAML = WORKDIR / "pipeline.kafka.yaml"
YAML_LOAD = WORKDIR / "pipeline.kafka.load.yaml"
LOG = WORKDIR / "cdc-kafka.log"
LOG_LOAD = WORKDIR / "cdc-kafka-load.log"
REPORT = cfg.REPORT_KAFKA

TOPIC_USERS = "kafka.tpk01_users"
TOPIC_ORDERS = "kafka.tpk01_orders"
TOPIC_DEFAULT = cfg.KAFKA_DEFAULT_TOPIC
TOPIC_T1 = "kafka.tpl01_t1"
TOPIC_TYPED = "kafka.tpl01_typed"
TOPIC_LARGE = "kafka.tpl01_large"

BROKERS = [b.strip() for b in cfg.KAFKA_BROKERS.split(",") if b.strip()]

results: list["TPResult"] = []
proc: subprocess.Popen | None = None


@dataclass
class TPResult:
    id: str
    title: str
    action: str
    expected_desc: str
    passed: bool = False
    detail_md: str = ""


def exec_sql(sql: str, args: tuple | None = None) -> None:
    c = pymysql.connect(**SRC, autocommit=True)
    try:
        with c.cursor() as cur:
            cur.execute(sql, args or ())
    finally:
        c.close()


def _row_id(env: dict) -> str | None:
    after = env.get("after") or {}
    before = env.get("before") or {}
    for side in (after, before):
        if "id" in side and side["id"] is not None:
            return str(side["id"])
    return None


def wait_cdc_alive(
    log_path: Path,
    *,
    timeout_s: float = 20.0,
    ready_after_s: float = 4.0,
    interval: float = 0.25,
    desc: str = "go-cdc",
) -> None:
    """Poll until go-cdc stays running for ready_after_s; fail fast if it exits.

    Replaces fixed sleep after Popen with the same readiness budget (~4s) plus
    early-exit detection. This is process liveness only — not proof that MySQL
    snapshot/binlog or Kafka produce has begun; callers must still use
    consume_until / wait_ckpt with their own timeouts.
    """
    deadline = time.time() + timeout_s
    ready_at = time.time() + ready_after_s
    while time.time() < deadline:
        if proc is None:
            raise RuntimeError(f"{desc}: process not started")
        code = proc.poll()
        if code is not None:
            text = log_path.read_text(errors="replace")[-2000:] if log_path.exists() else ""
            raise RuntimeError(f"{desc} exited early ({code}):\n{text}")
        if time.time() >= ready_at:
            return
        time.sleep(interval)
    if proc is not None and proc.poll() is None:
        return
    text = log_path.read_text(errors="replace")[-2000:] if log_path.exists() else ""
    raise TimeoutError(f"timeout waiting for {desc} to stay alive:\n{text}")


def start_cdc(*, emit_ddl: str = "false", reset_ckpt: bool = True) -> None:
    global proc
    stop_cdc()
    if reset_ckpt and CK.exists():
        CK.unlink()
    CK.parent.mkdir(parents=True, exist_ok=True)
    cfg.render_template(
        "pipeline.kafka.yaml.tpl",
        YAML,
        extra={"KAFKA_EMIT_DDL": emit_ddl},
    )
    log_path = LOG.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        [BINARY, str(YAML)],
        stdout=log_path,
        stderr=subprocess.STDOUT,
        text=True,
    )
    wait_cdc_alive(LOG, desc="go-cdc kafka")


def start_load_cdc() -> None:
    global proc
    stop_cdc()
    if CK_LOAD.exists():
        CK_LOAD.unlink()
    CK_LOAD.parent.mkdir(parents=True, exist_ok=True)
    cfg.render_template(
        "pipeline.kafka.load.yaml.tpl",
        YAML_LOAD,
        extra={"LOAD_PARALLELISM": cfg.LOAD_PARALLELISM},
    )
    log_path = LOG_LOAD.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        [BINARY, str(YAML_LOAD)],
        stdout=log_path,
        stderr=subprocess.STDOUT,
        text=True,
    )
    wait_cdc_alive(LOG_LOAD, desc="go-cdc kafka load")


def stop_cdc() -> None:
    global proc
    if proc is None:
        return
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    proc = None


def _consumer(topics: list[str] | None = None, *, group: str | None = None) -> KafkaConsumer:
    kwargs = dict(
        bootstrap_servers=BROKERS,
        group_id=group or f"gocdc-kafka-it-{time.time_ns()}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
        value_deserializer=lambda b: b,
        key_deserializer=lambda b: b,
        max_partition_fetch_bytes=16 * 1024 * 1024,
        fetch_max_bytes=64 * 1024 * 1024,
    )
    if topics:
        return KafkaConsumer(*topics, **kwargs)
    return KafkaConsumer(**kwargs)


def topic_end_sum(topic: str) -> int:
    c = _consumer(group=f"ends-{topic}-{time.time_ns()}")
    try:
        parts = c.partitions_for_topic(topic)
        if not parts:
            return 0
        tps = [TopicPartition(topic, p) for p in parts]
        return int(sum(c.end_offsets(tps).values()))
    finally:
        c.close()


def topic_catchup_done(end: int, *, baseline: int, min_increase: int) -> bool:
    """True when end - baseline covers the expected increase (ignores prior history)."""
    return (end - baseline) >= min_increase


def peak_msg_min_increase(src_before: int, src_after: int) -> int:
    """Kafka offset growth to wait for after a peak write (99% of new source rows).

    Uses only this operation's row delta — never mixes cumulative source rows with
    cumulative topic end offsets (historical Kafka data can exceed source counts).
    """
    return max(0, int((src_after - src_before) * 0.99))


def consume_until(
    topics: list[str],
    *,
    min_msgs: int,
    timeout_s: float = 60.0,
    group: str | None = None,
    predicate=None,
) -> list[dict]:
    """Poll until min_msgs (or predicate) satisfied, or timeout."""
    consumer = _consumer(topics, group=group)
    out: list[dict] = []
    deadline = time.time() + timeout_s
    try:
        while time.time() < deadline:
            if predicate is not None and predicate(out):
                break
            if predicate is None and len(out) >= min_msgs:
                break
            polled = consumer.poll(timeout_ms=1000)
            for records in polled.values():
                for rec in records:
                    try:
                        env = json.loads(rec.value.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue
                    env["_topic"] = rec.topic
                    env["_key"] = rec.key.decode("utf-8") if rec.key else ""
                    out.append(env)
    finally:
        consumer.close()
    return out


def wait_topic_msgs(
    topic: str,
    *,
    baseline: int,
    min_increase: int,
    timeout_s: float = 3600.0,
    interval: float = 10.0,
    desc: str = "kafka catch-up",
) -> float:
    """Wait until topic end-offset sum grows by at least min_increase since baseline.

    Callers must capture baseline before the operation under test starts producing.
    """
    t0 = time.time()
    deadline = t0 + timeout_s
    last_log = 0.0
    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"go-cdc exited during {desc}: {proc.returncode}")
        n = topic_end_sum(topic)
        if topic_catchup_done(n, baseline=baseline, min_increase=min_increase):
            elapsed = time.time() - t0
            log(
                f"TIMING  wait [{desc}] done in {elapsed:.1f}s "
                f"(end={n} baseline={baseline} +{n - baseline} need>={min_increase})"
            )
            return elapsed
        now = time.time()
        if now - last_log >= 15.0:
            log(
                f"    {desc} {now - t0:.0f}s: {topic} end={n} "
                f"baseline={baseline} delta={n - baseline} need>={min_increase}"
            )
            last_log = now
        time.sleep(interval)
    raise TimeoutError(
        f"timeout waiting for {desc}: {topic} baseline={baseline} need_increase>={min_increase}"
    )


def wait_ckpt(path: Path, timeout_s: float = 30.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if path.exists() and path.stat().st_size > 0:
            return True
        time.sleep(0.5)
    return path.exists()


def record(tp: TPResult) -> None:
    results.append(tp)
    mark = "PASS" if tp.passed else "FAIL"
    log(tp.id, mark, tp.title)


def tp_k01_snapshot_route_proj_filter() -> None:
    """Snapshot: routed topics, default topic, projection, filter, before=null."""
    start_cdc(emit_ddl="false", reset_ckpt=True)
    msgs = consume_until(
        [TOPIC_USERS, TOPIC_ORDERS, TOPIC_DEFAULT],
        min_msgs=4,
        timeout_s=120,
        group=f"k01-{time.time_ns()}",
        predicate=lambda out: (
            sum(1 for m in out if m.get("_topic") == TOPIC_USERS and m.get("op") == "r") >= 1
            and sum(1 for m in out if m.get("_topic") == TOPIC_ORDERS and m.get("op") == "r") >= 2
            and sum(1 for m in out if m.get("_topic") == TOPIC_DEFAULT and m.get("op") == "r") >= 1
        ),
    )
    users_r = [m for m in msgs if m.get("_topic") == TOPIC_USERS and m.get("op") == "r"]
    orders_r = [m for m in msgs if m.get("_topic") == TOPIC_ORDERS and m.get("op") == "r"]
    misc_r = [
        m
        for m in msgs
        if m.get("_topic") == TOPIC_DEFAULT
        and m.get("op") == "r"
        and _row_id(m) == "100"
    ]
    user_ids = {_row_id(m) for m in users_r}
    # filter city='hz' → only alice; bob (sh) must not appear
    filter_ok = "1" in user_ids and "2" not in user_ids
    sample = next((m for m in users_r if _row_id(m) == "1"), None)
    before_null = sample is not None and sample.get("before") is None
    after = (sample or {}).get("after") or {}
    proj_ok = set(after.keys()) <= {"id", "name", "city"} and "secret" not in after and after.get("city") == "hz"
    src = (sample or {}).get("source") or {}
    src_ok = (
        src.get("connector") == "mysql"
        and src.get("db") == cfg.FUNC_SRC_DB
        and src.get("table") == "tpk01_users"
        and src.get("name") == cfg.PIPELINE_KAFKA_NAME
        and "ts_ms" in src  # may be null JSON → key present as None
    )
    ts_ms_ok = isinstance((sample or {}).get("ts_ms"), int)
    orders_ok = len(orders_r) >= 2
    misc_ok = len(misc_r) >= 1
    ok = filter_ok and before_null and proj_ok and src_ok and ts_ms_ok and orders_ok and misc_ok
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| users op=r ids (hz only) | {sorted(user_ids)} | {'✅' if filter_ok else '❌'} |\n"
        f"| bob id=2 filtered out | {'2' not in user_ids} | {'✅' if '2' not in user_ids else '❌'} |\n"
        f"| before=null on op=r | {before_null} | {'✅' if before_null else '❌'} |\n"
        f"| projection (no secret) | {sorted(after.keys())} | {'✅' if proj_ok else '❌'} |\n"
        f"| source meta | connector={src.get('connector')} db={src.get('db')} | "
        f"{'✅' if src_ok else '❌'} |\n"
        f"| envelope ts_ms int | {ts_ms_ok} | {'✅' if ts_ms_ok else '❌'} |\n"
        f"| orders op=r ≥2 | {len(orders_r)} | {'✅' if orders_ok else '❌'} |\n"
        f"| misc → default topic | {len(misc_r)} | {'✅' if misc_ok else '❌'} |\n"
    )
    record(
        TPResult(
            id="TP-K01",
            title="snapshot: route + default topic + projection + filter",
            action="seed users/orders/misc; consume routed + cdc_default",
            expected_desc="hz-only users; orders routed; misc on default; before=null; no secret",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_k02_crud_and_keys() -> None:
    """Incremental c/u/d with keys; filtered insert must not appear."""
    group = f"k02-{time.time_ns()}"
    exec_sql(
        "INSERT INTO tpk01_users (id, name, city, secret) VALUES (%s, %s, %s, %s)",
        (3, "cara", "hz", "s3"),
    )
    exec_sql(
        "INSERT INTO tpk01_users (id, name, city, secret) VALUES (%s, %s, %s, %s)",
        (4, "dan", "sh", "s4"),
    )
    exec_sql("UPDATE tpk01_users SET name=%s WHERE id=%s", ("alice2", 1))
    exec_sql("DELETE FROM tpk01_orders WHERE id=%s", (11,))

    def done(out: list[dict]) -> bool:
        has_c = any(m.get("op") == "c" and _row_id(m) == "3" for m in out)
        has_u = any(m.get("op") == "u" and _row_id(m) == "1" for m in out)
        has_d = any(m.get("op") == "d" and _row_id(m) == "11" for m in out)
        return has_c and has_u and has_d

    msgs = consume_until(
        [TOPIC_USERS, TOPIC_ORDERS],
        min_msgs=3,
        timeout_s=90,
        group=group,
        predicate=done,
    )
    creates = [m for m in msgs if m.get("op") == "c" and _row_id(m) == "3"]
    updates = [m for m in msgs if m.get("op") == "u" and _row_id(m) == "1"]
    deletes = [m for m in msgs if m.get("op") == "d" and _row_id(m) == "11"]
    filtered = [m for m in msgs if _row_id(m) == "4"]

    c_ok = (
        len(creates) >= 1
        and creates[0].get("before") is None
        and creates[0].get("_key") == "[3]"
        and "secret" not in (creates[0].get("after") or {})
    )
    u = updates[0] if updates else {}
    u_ok = (
        len(updates) >= 1
        and isinstance(u.get("before"), dict)
        and isinstance(u.get("after"), dict)
        and (u.get("after") or {}).get("name") == "alice2"
        and u.get("_key") == "[1]"
    )
    d = deletes[0] if deletes else {}
    d_ok = (
        len(deletes) >= 1
        and d.get("after") is None
        and isinstance(d.get("before"), dict)
        and d.get("_key") == "[11]"
    )
    filt_ok = len(filtered) == 0
    ok = c_ok and u_ok and d_ok and filt_ok
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| op=c id=3 before=null key=3 | {c_ok} | {'✅' if c_ok else '❌'} |\n"
        f"| op=u id=1 before+after key=1 | {u_ok} | {'✅' if u_ok else '❌'} |\n"
        f"| op=d id=11 after=null key=11 | {d_ok} | {'✅' if d_ok else '❌'} |\n"
        f"| city=sh insert id=4 absent | {len(filtered)} | {'✅' if filt_ok else '❌'} |\n"
    )
    record(
        TPResult(
            id="TP-K02",
            title="incremental c/u/d + keys + filter",
            action="INSERT hz/sh; UPDATE name; DELETE order",
            expected_desc="c/u/d envelopes + keys; sh insert filtered",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_k03_ddl_emit_switch() -> None:
    """emit=false → no ddl; emit=true → ddl on routed users topic (same as DML)."""
    # --- off: poll a window; any ddl is failure (no fixed long sleep) ---
    group_off = f"k03-off-{time.time_ns()}"
    consume_until([TOPIC_DEFAULT, TOPIC_USERS], min_msgs=0, timeout_s=2, group=group_off + "-drain")
    exec_sql("ALTER TABLE tpk01_users ADD COLUMN tag VARCHAR(16) NULL")
    ddls_off: list[dict] = []
    deadline = time.time() + 12.0
    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"go-cdc exited during K03 off: {proc.returncode}")
        batch = consume_until(
            [TOPIC_DEFAULT, TOPIC_USERS, TOPIC_ORDERS],
            min_msgs=0,
            timeout_s=2,
            group=f"{group_off}-{int(time.time())}",
        )
        ddls_off.extend(m for m in batch if m.get("op") == "ddl")
        if ddls_off:
            break
    off_ok = len(ddls_off) == 0

    # --- on (resume from checkpoint); DDL must land on routed users topic ---
    stop_cdc()
    start_cdc(emit_ddl="true", reset_ckpt=False)
    group_on = f"k03-on-{time.time_ns()}"

    def ddl_hit(out: list[dict]) -> bool:
        return any(
            m.get("op") == "ddl"
            and "tag2" in (m.get("ddl") or "").lower()
            for m in out
        )

    exec_sql("ALTER TABLE tpk01_users ADD COLUMN tag2 VARCHAR(16) NULL")
    msgs_on = consume_until(
        [TOPIC_DEFAULT, TOPIC_USERS],
        min_msgs=1,
        timeout_s=60,
        group=group_on,
        predicate=ddl_hit,
    )
    ddls_on = [m for m in msgs_on if m.get("op") == "ddl" and "tag2" in (m.get("ddl") or "").lower()]
    on_sample = ddls_on[0] if ddls_on else {}
    src = on_sample.get("source") or {}
    on_ok = (
        len(ddls_on) >= 1
        and on_sample.get("before") is None
        and on_sample.get("after") is None
        and bool(on_sample.get("ddl"))
        and on_sample.get("_topic") == TOPIC_USERS
        and src.get("table") == "tpk01_users"
    )
    ok = off_ok and on_ok
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| emit=false → 0 ddl | {len(ddls_off)} | {'✅' if off_ok else '❌'} |\n"
        f"| emit=true → ddl with tag2 | {len(ddls_on)} | {'✅' if ddls_on else '❌'} |\n"
        f"| ddl topic=routed users | {on_sample.get('_topic')} | "
        f"{'✅' if on_sample.get('_topic') == TOPIC_USERS else '❌'} |\n"
        f"| source.table=tpk01_users | {src.get('table')} | "
        f"{'✅' if src.get('table') == 'tpk01_users' else '❌'} |\n"
        f"| ddl before/after null | {on_sample.get('before')}/{on_sample.get('after')} | "
        f"{'✅' if on_ok else '❌'} |\n"
    )
    record(
        TPResult(
            id="TP-K03",
            title="DDL emit switch off + on (routed topic)",
            action="ALTER with emit=false; restart emit=true; ALTER tag2",
            expected_desc="no ddl when off; ddl on kafka.tpk01_users when on (same as DML)",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_k04_restart_at_least_once() -> None:
    """Keep checkpoint across restart; new row arrives; prior id still in topic."""
    # ensure emit back to false for clean incremental
    stop_cdc()
    start_cdc(emit_ddl="false", reset_ckpt=False)
    exec_sql(
        "INSERT INTO tpk01_orders (id, user_id, amt) VALUES (%s, %s, %s)",
        (12, 1, 1.25),
    )
    pre = consume_until(
        [TOPIC_ORDERS],
        min_msgs=1,
        timeout_s=60,
        group=f"k04-pre-{time.time_ns()}",
        predicate=lambda out: any(_row_id(m) == "12" for m in out),
    )
    pre_hits = [m for m in pre if _row_id(m) == "12"]
    ckpt_ok = wait_ckpt(CK, timeout_s=20)

    stop_cdc()
    start_cdc(emit_ddl="false", reset_ckpt=False)  # keep checkpoint → at-least-once resume
    exec_sql(
        "INSERT INTO tpk01_orders (id, user_id, amt) VALUES (%s, %s, %s)",
        (13, 1, 2.50),
    )
    post = consume_until(
        [TOPIC_ORDERS],
        min_msgs=1,
        timeout_s=90,
        group=f"k04-post-{time.time_ns()}",
        predicate=lambda out: (
            any(_row_id(m) == "13" for m in out) and any(_row_id(m) == "12" for m in out)
        ),
    )
    hits_12 = [m for m in post if _row_id(m) == "12"]
    hits_13 = [m for m in post if _row_id(m) == "13" and m.get("op") in ("c", "r")]
    ok = len(pre_hits) >= 1 and ckpt_ok and len(hits_13) >= 1 and len(hits_12) >= 1
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| id=12 before restart | {len(pre_hits)} | {'✅' if pre_hits else '❌'} |\n"
        f"| checkpoint file present | {ckpt_ok} | {'✅' if ckpt_ok else '❌'} |\n"
        f"| id=13 after restart | {len(hits_13)} | {'✅' if hits_13 else '❌'} |\n"
        f"| id=12 still in topic (ALO) | {len(hits_12)} | {'✅' if hits_12 else '❌'} |\n"
        "| duplicates | allowed | ✅ |\n"
    )
    record(
        TPResult(
            id="TP-K04",
            title="restart with checkpoint (resume continuity; ALO allows dups)",
            action="INSERT 12; wait ckpt; SIGTERM; restart keep ckpt; INSERT 13",
            expected_desc="resume continuity: 13 arrives; 12 still readable (not dup-injection)",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_k05_snapshot_load() -> None:
    """Load-parity snapshot: end offsets ≥ 99% of src rows for t1/typed (+ large)."""
    stop_cdc()
    load.ensure_schema()
    log(
        f"==> kafka load seed {load.ROWS} rows × (t1+typed) + "
        f"{load.LARGE_ROWS} large × {load.LARGE_BYTES}B"
    )
    load.seed_baseline()
    # drop unused biz tables' data cost: seed_baseline seeds all TABLES including t2-t4;
    # pipeline only watches t1|typed|large — OK, extra tables just sit in MySQL.
    # Baseline before CDC starts so historical topic data cannot satisfy the wait.
    base_t1 = topic_end_sum(TOPIC_T1)
    base_ty = topic_end_sum(TOPIC_TYPED)
    base_lg = topic_end_sum(TOPIC_LARGE)
    start_load_cdc()
    need_t1 = int(load.ROWS * 0.99)
    need_typed = int(load.ROWS * 0.99)
    need_large = int(load.LARGE_ROWS * 0.99)
    t_t1 = wait_topic_msgs(
        TOPIC_T1, baseline=base_t1, min_increase=need_t1, timeout_s=3600, desc="k05 t1 snapshot"
    )
    t_ty = wait_topic_msgs(
        TOPIC_TYPED, baseline=base_ty, min_increase=need_typed, timeout_s=3600, desc="k05 typed snapshot"
    )
    t_lg = wait_topic_msgs(
        TOPIC_LARGE, baseline=base_lg, min_increase=need_large, timeout_s=3600, desc="k05 large snapshot"
    )
    end_t1 = topic_end_sum(TOPIC_T1)
    end_ty = topic_end_sum(TOPIC_TYPED)
    end_lg = topic_end_sum(TOPIC_LARGE)
    src_t1 = load.count_rows(load.SRC, load.SRC_DB, "tpl01_t1")
    src_ty = load.count_rows(load.SRC, load.SRC_DB, load.TYPED_TABLE)
    src_lg = load.count_rows(load.SRC, load.SRC_DB, load.LARGE_TABLE)
    # sample one snapshot envelope
    sample = consume_until(
        [TOPIC_T1],
        min_msgs=1,
        timeout_s=30,
        group=f"k05-sample-{time.time_ns()}",
        predicate=lambda out: any(m.get("op") == "r" for m in out),
    )
    sample_r = next((m for m in sample if m.get("op") == "r"), None)
    shape_ok = (
        sample_r is not None
        and sample_r.get("before") is None
        and isinstance(sample_r.get("after"), dict)
        and "id" in (sample_r.get("after") or {})
    )
    lag_t1 = abs(src_t1 - end_t1) / max(src_t1, 1)
    # ALO: end may exceed src; fail only if lag behind src > 1%
    behind_t1 = max(0, src_t1 - end_t1) / max(src_t1, 1) <= 0.01
    behind_ty = max(0, src_ty - end_ty) / max(src_ty, 1) <= 0.01
    behind_lg = max(0, src_lg - end_lg) / max(src_lg, 1) <= 0.01
    ok = behind_t1 and behind_ty and behind_lg and shape_ok
    detail = (
        "| Topic | Src rows | Kafka end | Behind | Verdict |\n"
        "| --- | ---: | ---: | ---: | --- |\n"
        f"| {TOPIC_T1} | {src_t1} | {end_t1} | {max(0, src_t1 - end_t1) / max(src_t1, 1):.2%} | "
        f"{'✅' if behind_t1 else '❌'} |\n"
        f"| {TOPIC_TYPED} | {src_ty} | {end_ty} | {max(0, src_ty - end_ty) / max(src_ty, 1):.2%} | "
        f"{'✅' if behind_ty else '❌'} |\n"
        f"| {TOPIC_LARGE} | {src_lg} | {end_lg} | {max(0, src_lg - end_lg) / max(src_lg, 1):.2%} | "
        f"{'✅' if behind_lg else '❌'} |\n"
        f"| sample op=r shape | {shape_ok} | | | {'✅' if shape_ok else '❌'} |\n"
        f"| timings | t1={t_t1:.1f}s typed={t_ty:.1f}s large={t_lg:.1f}s | | | |\n"
        f"| note | end≥src OK (ALO dups); fail if behind>1% | lag_t1_abs={lag_t1:.2%} | | |\n"
    )
    record(
        TPResult(
            id="TP-K05",
            title="load snapshot volume (t1 + typed + large)",
            action=f"seed {load.ROWS}+{load.LARGE_ROWS}; wait kafka end offsets",
            expected_desc="topic end ≥ 99% src rows; sample op=r shape OK",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_k07_typed_spotcheck() -> None:
    """Spot-check typed column JSON for sample ids (after snapshot, before peak)."""
    sample_ids = {1, max(1, load.ROWS // 2), load.ROWS}
    # pull enough messages to cover high ids (may be slow on full profile — capped scan)
    need = min(load.ROWS, max(sample_ids) + 100)
    msgs = consume_until(
        [TOPIC_TYPED],
        min_msgs=need,
        timeout_s=600,
        group=f"k07-{time.time_ns()}",
        predicate=lambda out: sample_ids.issubset(
            {_row_id(m) for m in out if m.get("op") == "r" and _row_id(m)}
        ),
    )
    by_id: dict[str, dict] = {}
    for m in msgs:
        if m.get("op") != "r":
            continue
        rid = _row_id(m)
        if rid and rid not in by_id:
            by_id[rid] = m
    checks = []
    ok = True
    for i in sorted(sample_ids):
        m = by_id.get(str(i))
        after = (m or {}).get("after") or {}
        expect = load._typed_row(i)
        # compare a subset that JSON preserves stably as strings/numbers
        hit = (
            m is not None
            and str(after.get("id")) == str(i)
            and str(after.get("c_varchar")) == expect[7]
            and str(after.get("c_enum")) == expect[12]
            and after.get("c_json") is not None
        )
        ok = ok and hit
        checks.append(f"| id={i} | found={m is not None} varchar={after.get('c_varchar')} | "
                      f"{'✅' if hit else '❌'} |")
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n" + "\n".join(checks) + "\n"
    )
    record(
        TPResult(
            id="TP-K07",
            title="typed column spot-check on Kafka JSON",
            action=f"consume typed op=r for ids {sorted(sample_ids)}",
            expected_desc="varchar/enum/json present and match seed",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_k08_large_fields() -> None:
    """Mega LONGBLOB/LONGTEXT: JPEG magic + approximate decoded length."""
    msgs = consume_until(
        [TOPIC_LARGE],
        min_msgs=min(3, load.LARGE_ROWS),
        timeout_s=600,
        group=f"k08-{time.time_ns()}",
        predicate=lambda out: any(m.get("op") == "r" and (m.get("after") or {}).get("c_img") for m in out),
    )
    samples = [m for m in msgs if m.get("op") == "r" and (m.get("after") or {}).get("c_img")][:3]
    ok = len(samples) >= 1
    rows = []
    for m in samples:
        after = m.get("after") or {}
        img = after.get("c_img")
        text = after.get("c_text")
        raw = b""
        if isinstance(img, str):
            try:
                raw = base64.b64decode(img)
            except Exception:  # noqa: BLE001
                raw = b""
        elif isinstance(img, (bytes, bytearray)):
            raw = bytes(img)
        magic = raw[:2] == b"\xff\xd8" and raw[-2:] == b"\xff\xd9"
        # allow ±1% on wire size after base64 round-trip
        len_ok = abs(len(raw) - load.LARGE_BYTES) / max(load.LARGE_BYTES, 1) <= 0.01
        text_ok = isinstance(text, str) and abs(len(text) - load.LARGE_BYTES) / max(load.LARGE_BYTES, 1) <= 0.05
        hit = magic and len_ok and text_ok
        ok = ok and hit
        rows.append(
            f"| id={after.get('id')} | img_len={len(raw)} magic={magic} text_len="
            f"{len(text) if isinstance(text, str) else 'n/a'} | {'✅' if hit else '❌'} |"
        )
    if not samples:
        rows.append("| no large msgs | 0 | ❌ |")
        ok = False
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n" + "\n".join(rows) + "\n"
        f"| expect field bytes | {load.LARGE_BYTES} | |\n"
    )
    record(
        TPResult(
            id="TP-K08",
            title="mega LONGBLOB/LONGTEXT on Kafka",
            action=f"consume {TOPIC_LARGE}; check JPEG magic + lengths",
            expected_desc=f"decoded img ≈ {load.LARGE_BYTES}B with SOI/EOI; text ≈ size",
            passed=ok,
            detail_md=detail,
        )
    )


def tp_k06_peak_catchup() -> None:
    """Peak writers on MySQL; Kafka end offsets catch up within 1% behind."""
    src_before = load.count_rows(load.SRC, load.SRC_DB, "tpl01_t1")
    src_ty_before = load.count_rows(load.SRC, load.SRC_DB, load.TYPED_TABLE)
    end_before = topic_end_sum(TOPIC_T1)
    base_ty = topic_end_sum(TOPIC_TYPED)
    stop_at = time.time() + load.PEAK_SEC
    counter = [0]
    lock = threading.Lock()
    log(f"==> kafka peak {load.PEAK_SEC}s × {load.WORKERS} workers")
    with ThreadPoolExecutor(max_workers=load.WORKERS) as pool:
        futs = [
            pool.submit(load.peak_writer, i, stop_at, counter, lock)
            for i in range(load.WORKERS)
        ]
        written = sum(f.result() for f in futs)
    src_after = load.count_rows(load.SRC, load.SRC_DB, "tpl01_t1")
    src_ty = load.count_rows(load.SRC, load.SRC_DB, load.TYPED_TABLE)
    # Wait for this peak's CDC volume only (not cumulative src vs cumulative end).
    need_inc = peak_msg_min_increase(src_before, src_after)
    elapsed = wait_topic_msgs(
        TOPIC_T1,
        baseline=end_before,
        min_increase=need_inc,
        timeout_s=3600,
        desc="k06 peak catch-up",
    )
    need_inc_ty = peak_msg_min_increase(src_ty_before, src_ty)
    wait_topic_msgs(
        TOPIC_TYPED,
        baseline=base_ty,
        min_increase=need_inc_ty,
        timeout_s=3600,
        desc="k06 typed peak catch-up",
    )
    end_after = topic_end_sum(TOPIC_T1)
    src_inc = src_after - src_before
    kafka_inc = end_after - end_before
    behind = max(0, src_inc - kafka_inc) / max(src_inc, 1)
    grew = kafka_inc > 0 and src_inc > 0
    ok = behind <= 0.01 and grew and written > 0
    detail = (
        "| Check | Actual | Verdict |\n| --- | --- | --- |\n"
        f"| peak written (per-worker sum) | {written} | {'✅' if written > 0 else '❌'} |\n"
        f"| src t1 before→after | {src_before}→{src_after} (Δ{src_inc}) | "
        f"{'✅' if src_inc > 0 else '❌'} |\n"
        f"| kafka end before→after | {end_before}→{end_after} (Δ{kafka_inc}, need≥{need_inc}) | "
        f"{'✅' if grew else '❌'} |\n"
        f"| behind this peak | {behind:.2%} | {'✅' if behind <= 0.01 else '❌'} |\n"
        f"| catch-up time | {elapsed:.1f}s | |\n"
        "| lag metric | ALO: kafka Δ ≥ 0.99×src Δ for this peak | ✅ |\n"
    )
    record(
        TPResult(
            id="TP-K06",
            title="peak catch-up to Kafka",
            action=f"peak {load.PEAK_SEC}s×{load.WORKERS}; wait end offsets",
            expected_desc="kafka end within 1% behind src; volume grew",
            passed=ok,
            detail_md=detail,
        )
    )


def write_report() -> Path:
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    profile = cfg.LOAD_PROFILE or "(unset)"
    scale_note = (
        "Local/CI load scale is whatever knobs were set for this run — "
        "**not** proof of the other tier. CI full uses `LOAD_ROWS_PER_TABLE` "
        "30000 (normal) / 100000 (full) with `LOAD_LARGE_FIELD_BYTES=2097152`; "
        "a `local-smoke` / reduced-ROWS run only validates that smaller matrix."
    )
    lines = [
        "# MySQL → Kafka integration report",
        "",
        f"- Brokers: `{cfg.KAFKA_BROKERS}`",
        f"- Default topic: `{cfg.KAFKA_DEFAULT_TOPIC}`",
        f"- Functional pipeline: `{cfg.PIPELINE_KAFKA_NAME}`",
        f"- Load pipeline: `{cfg.PIPELINE_KAFKA_LOAD_NAME}`",
        f"- Load knobs: ROWS={load.ROWS} LARGE_ROWS={load.LARGE_ROWS} "
        f"LARGE_BYTES={load.LARGE_BYTES} PEAK={load.PEAK_SEC}s×{load.WORKERS} "
        f"PARALLELISM={cfg.LOAD_PARALLELISM} PROFILE={profile}",
        f"- Scale caveat: {scale_note}",
        "",
    ]
    failed = 0
    for r in results:
        mark = "✅ PASS" if r.passed else "❌ FAIL"
        if not r.passed:
            failed += 1
        lines += [
            f"## {r.id} — {r.title} ({mark})",
            "",
            f"- Action: {r.action}",
            f"- Expected: {r.expected_desc}",
            "",
            r.detail_md,
            "",
        ]
    lines.append(f"**Summary:** {len(results) - failed}/{len(results)} passed")
    text = cfg.sanitize_report_text("\n".join(lines) + "\n")
    REPORT.write_text(text, encoding="utf-8")
    return REPORT


def main() -> int:
    WORKDIR.mkdir(parents=True, exist_ok=True)
    try:
        # Functional boundaries first (dedicated server-id / checkpoint).
        tp_k01_snapshot_route_proj_filter()
        tp_k02_crud_and_keys()
        tp_k03_ddl_emit_switch()
        tp_k04_restart_at_least_once()
        stop_cdc()

        # Load-parity (separate pipeline / topics / server-id).
        tp_k05_snapshot_load()
        tp_k07_typed_spotcheck()
        tp_k08_large_fields()
        tp_k06_peak_catchup()
    finally:
        stop_cdc()
    path = write_report()
    log("REPORT", cfg.project_display_path(path))
    failed = [r for r in results if not r.passed]
    log("PASS", sum(1 for r in results if r.passed), "FAIL", len(failed))
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
