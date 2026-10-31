"""Environment-driven connection settings for integration tests.

Naming:
  - Users/passwords are anonymous fixed tokens (not production-like).
  - Databases are suite-scoped: db_tp_{func,types,load}_{src,snk}.
  - Tables are test-point scoped: tp01_*, tpd01_*, tpl01_*.
  - PostgreSQL suites keep MySQL ``db_tp_pg_*`` sources; the sink side is one
    database (``cdc_it``) with per-suite schemas (``tp_pg_*``), because the
    PostgreSQL sink accepts exactly one target database.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


ROOT = Path(__file__).resolve().parents[3]  # go-cdc/
IT_ROOT = Path(__file__).resolve().parents[1]  # test/integration/
WORKDIR = IT_ROOT / "workdir"
FIXTURES = IT_ROOT / "fixtures"
REPORT_DIR = ROOT / "docs" / "test" / "reports"
REPORT_FUNC = REPORT_DIR / "mysql-mysql-func.md"
REPORT_TYPES = REPORT_DIR / "mysql-mysql-types.md"
REPORT_LOAD = REPORT_DIR / "mysql-mysql-load.md"
REPORT_KAFKA = REPORT_DIR / "mysql-kafka.md"
REPORT_DORIS = REPORT_DIR / "mysql-doris.md"
REPORT_ES = Path(env("REPORT_ES", str(REPORT_DIR / "mysql-elasticsearch.md")))
REPORT_ES_LOAD = REPORT_DIR / "mysql-elasticsearch-load.md"
REPORT_ES_SCALE_TMPL = "mysql-elasticsearch-scale-{profile}.md"
# Overridable so the compatibility matrix can keep one E2E report per server
# version (`mysql-postgresql-func-pg14.md`, …) instead of overwriting the
# single-version report. The default path is unchanged.
REPORT_PG_FUNC = Path(env("REPORT_PG_FUNC", str(REPORT_DIR / "mysql-postgresql-func.md")))
REPORT_PG_LOAD = REPORT_DIR / "mysql-postgresql-load.md"
REPORT_PG_SCALE_TMPL = "mysql-postgresql-scale-{profile}.md"
REPORT_PG_COMPAT = REPORT_DIR / "mysql-postgresql-compat.md"
# Overridable so a benchmark rerun on another machine keeps both reports.
REPORT_PG_BENCH = Path(
    env("REPORT_PG_BENCH", str(REPORT_DIR / "mysql-postgresql-benchmark.md"))
)


MYSQL_HOST = env("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(env("MYSQL_PORT", "13306"))

# Anonymous principals (override via env if needed).
SRC_USER = env("SRC_USER", "u_a7k9m2x4")
SRC_PASSWORD = env("SRC_PASSWORD", "P_w8q3n5v1r6")
SINK_USER = env("SINK_USER", "u_b3j6c8y1")
SINK_PASSWORD = env("SINK_PASSWORD", "P_z2h5t9k4m7")

# Suite databases (test-point / suite based).
FUNC_SRC_DB = env("FUNC_SRC_DB", "db_tp_func_src")
FUNC_SNK_DB = env("FUNC_SNK_DB", "db_tp_func_snk")
TYPES_SRC_DB = env("TYPES_SRC_DB", "db_tp_types_src")
TYPES_SNK_DB = env("TYPES_SNK_DB", "db_tp_types_snk")
LOAD_SRC_DB = env("LOAD_SRC_DB", "db_tp_load_src")
LOAD_SNK_DB = env("LOAD_SNK_DB", "db_tp_load_snk")
# Elasticsearch suites: source DB is MySQL, the ``snk`` DB is only a logical
# namespace - it becomes the index name (``snk_db.table`` normalized).
ES_SRC_DB = env("ES_SRC_DB", "db_tp_es_src")
ES_SNK_DB = env("ES_SNK_DB", "db_tp_es_snk")
ES_LOAD_SNK_DB = env("ES_LOAD_SNK_DB", "db_tp_es_load_snk")

# Back-compat aliases used by functional harness.
SRC_DB = env("SRC_DB", FUNC_SRC_DB)
SINK_DB = env("SINK_DB", FUNC_SNK_DB)

CDC_SERVER_ID = env("CDC_SERVER_ID", "5613")
CDC_SERVER_ID_STDOUT = env("CDC_SERVER_ID_STDOUT", "5612")
CDC_SERVER_ID_TYPES = env("CDC_SERVER_ID_TYPES", "5614")
CDC_SERVER_ID_LOAD = env("CDC_SERVER_ID_LOAD", "5615")
CDC_SERVER_ID_KAFKA = env("CDC_SERVER_ID_KAFKA", "5616")
CDC_SERVER_ID_KAFKA_LOAD = env("CDC_SERVER_ID_KAFKA_LOAD", "5617")
CDC_SERVER_ID_DORIS = env("CDC_SERVER_ID_DORIS", "5618")
CDC_SERVER_ID_DORIS_LOAD = env("CDC_SERVER_ID_DORIS_LOAD", "5619")
CDC_SERVER_ID_DORIS_SCALE = env("CDC_SERVER_ID_DORIS_SCALE", "5620")
# Elasticsearch suites keep their own ids so a Doris and an Elasticsearch job can
# run side by side without a duplicate server-id.
CDC_SERVER_ID_ES = env("CDC_SERVER_ID_ES", "5621")
CDC_SERVER_ID_ES_RESTART = env("CDC_SERVER_ID_ES_RESTART", "5622")
CDC_SERVER_ID_ES_LOAD = env("CDC_SERVER_ID_ES_LOAD", "5623")
CDC_SERVER_ID_ES_SCALE = env("CDC_SERVER_ID_ES_SCALE", "5624")
# PostgreSQL suites.
CDC_SERVER_ID_PG = env("CDC_SERVER_ID_PG", "5625")
CDC_SERVER_ID_PG_RESTART = env("CDC_SERVER_ID_PG_RESTART", "5626")
CDC_SERVER_ID_PG_LOAD = env("CDC_SERVER_ID_PG_LOAD", "5627")
CDC_SERVER_ID_PG_SCALE = env("CDC_SERVER_ID_PG_SCALE", "5628")
CDC_SERVER_ID_PG_SCALE_HEAVY = env("CDC_SERVER_ID_PG_SCALE_HEAVY", "5629")
# Benchmark: its own id so it never collides with a running E2E/load/scale job.
CDC_SERVER_ID_PG_BENCH = env("CDC_SERVER_ID_PG_BENCH", "5630")

KAFKA_BROKERS = env("KAFKA_BROKERS", "127.0.0.1:19092")
KAFKA_DEFAULT_TOPIC = env("KAFKA_DEFAULT_TOPIC", "cdc_default")
KAFKA_EMIT_DDL = env("KAFKA_EMIT_DDL", "false")

# Doris FE endpoints (compose uses host networking; defaults match doris-up.sh).
DORIS_HOST = env("DORIS_HOST", "127.0.0.1")
DORIS_HTTP_PORT = int(env("DORIS_HTTP_PORT", "8030"))
DORIS_SQL_PORT = int(env("DORIS_SQL_PORT", "9030"))
DORIS_USER = env("DORIS_USER", "root")
DORIS_PASSWORD = env("DORIS_PASSWORD", "")
DORIS_REPLICATION_NUM = env("DORIS_REPLICATION_NUM", "1")
DORIS_SNK_DB = env("DORIS_SNK_DB", "db_tp_doris_snk")

# Elasticsearch 8 (compose service ``elasticsearch``; security disabled).
ES_HOST_PORT = env("ES_HOST_PORT", "19200")
ES_URL = env("ES_URL", f"http://127.0.0.1:{ES_HOST_PORT}")
ES_USERNAME = env("ES_USERNAME", "")
ES_PASSWORD = env("ES_PASSWORD", "")
# Index cleanup pattern: derived indices are ``<snk_db>.<table>[-hash]``, so the
# suite namespace doubles as the isolation prefix.
ES_INDEX_PREFIX = env("ES_INDEX_PREFIX", ES_SNK_DB)
ES_LOAD_INDEX_PREFIX = env("ES_LOAD_INDEX_PREFIX", ES_LOAD_SNK_DB)
ES_REQUEST_TIMEOUT = float(env("ES_REQUEST_TIMEOUT", "30"))
# sink.version rendered into the functional E2E fixtures. The ES8 suite renders
# 8 (the sink default, so its behaviour is unchanged); the ES7 E2E run renders
# 7, so the v7 adapter is exercised on purpose instead of relying on a probe.
ES_SINK_VERSION = env("ES_SINK_VERSION", "8")
# Guard for the E2E harness: refuse to seed a cluster of another major version
# (empty disables the check). ES7 runs use "7"; ES8 runs leave it unset.
ES_EXPECT_MAJOR = env("ES_EXPECT_MAJOR", "")
# Scale suite: every final table lives under ``db_tp_scale_snk_<nn>``, which is
# both the index-name namespace and the cleanup prefix.
ES_SCALE_INDEX_PREFIX = env("ES_SCALE_INDEX_PREFIX", "db_tp_scale_snk_*")

# Elasticsearch load suite: CDC bulk size / interval sweep (one bulk per Write).
ES_LOAD_BATCH_SIZE = env("ES_LOAD_BATCH_SIZE", "1000")
ES_LOAD_BATCH_INTERVAL = env("ES_LOAD_BATCH_INTERVAL", "500ms")
ES_LOAD_SMALL_BATCH = env("ES_LOAD_SMALL_BATCH", "100")
ES_LOAD_SMALL_INTERVAL = env("ES_LOAD_SMALL_INTERVAL", "100ms")

# PostgreSQL sink (compose service ``postgres``; MySQL stays the source).
PG_HOST = env("PG_HOST", "127.0.0.1")
PG_PORT = int(env("PG_PORT", "15432"))
PG_USER = env("PG_USER", "postgres")
PG_PASSWORD = env("PG_PASSWORD", "postgres")
# ``sink.database`` is required and single-valued, so suites are isolated by
# schema: ``sink.schema`` for a whole pipeline, or the route ``db`` segment.
PG_DB = env("PG_DB", "cdc_it")
PG_SSLMODE = env("PG_SSLMODE", "prefer")
# Guard for the E2E harness: refuse to seed a server of another major version
# (empty disables the check; the compatibility suite pins it per run).
PG_EXPECT_MAJOR = env("PG_EXPECT_MAJOR", "")
# Optional container-level restart (spec PG-206). Off by default so a plain PR
# run never needs docker; ``PG_RESTART_CMD`` defaults to restarting the compose
# service via scripts/run-pg-e2e.sh's helper.
PG_RESTART_ENABLED = env("PG_RESTART_ENABLED", "0")
PG_RESTART_CMD = env("PG_RESTART_CMD", "")

# PostgreSQL E2E suites: two source databases so the same table name lives in
# two MySQL databases and every source DB routes to its own target schema.
PG_SRC_DB_A = env("PG_SRC_DB_A", "db_tp_pg_src_a")
PG_SRC_DB_B = env("PG_SRC_DB_B", "db_tp_pg_src_b")
PG_SNK_SCHEMA_A = env("PG_SNK_SCHEMA_A", "tp_pg_a")
PG_SNK_SCHEMA_B = env("PG_SNK_SCHEMA_B", "tp_pg_b")
# The PostgreSQL load suite reuses the MySQL load source database
# (``LOAD_SRC_DB``) so ``load_it.ensure_schema()``/``seed_baseline()`` are used
# verbatim, exactly like the Elasticsearch load suite. Only the target is
# PostgreSQL-specific: one schema, tables suffixed ``_out``.
PG_LOAD_SNK_SCHEMA = env("PG_LOAD_SNK_SCHEMA", "tp_pg_load")

# PostgreSQL load suite: CDC write batch sweep (mirrors the Elasticsearch load).
PG_LOAD_BATCH_SIZE = env("PG_LOAD_BATCH_SIZE", "1000")
PG_LOAD_BATCH_INTERVAL = env("PG_LOAD_BATCH_INTERVAL", "500ms")
PG_LOAD_SMALL_BATCH = env("PG_LOAD_SMALL_BATCH", "100")
PG_LOAD_SMALL_INTERVAL = env("PG_LOAD_SMALL_INTERVAL", "100ms")
# Bound on snapshot / peak catch-up before a load case is failed (never a pass).
PG_LOAD_CATCHUP_BUDGET_S = float(env("PG_LOAD_CATCHUP_BUDGET_S", "3600"))
# Primary-key window used by the chunked reconciliation (bounds memory and still
# names the ids/columns that differ).
PG_LOAD_VERIFY_CHUNK = int(env("PG_LOAD_VERIFY_CHUNK", "5000"))

# PostgreSQL benchmark (spec PG-210, plan T3.21..T3.24). A benchmark run owns one
# source database and one target schema of its own: it must stay independent of
# the E2E / load / scale suites so a leftover table or checkpoint cannot change a
# measured number. ``PG_BENCH_BATCHES`` is the matrix dimension; the workload is
# the second one (``insert`` = pure INSERT/UPSERT, ``mixed`` = UPDATE/DELETE).
PG_BENCH_SRC_DB = env("PG_BENCH_SRC_DB", "db_tp_pg_bench_src")
PG_BENCH_SNK_SCHEMA = env("PG_BENCH_SNK_SCHEMA", "tp_pg_bench")
PG_BENCH_BATCHES = env("PG_BENCH_BATCHES", "100,1000,5000")
PG_BENCH_WORKLOADS = env("PG_BENCH_WORKLOADS", "insert,mixed")
# Rows per matrix cell (the primary rate), and an optional wall-clock cap so a
# cell can be time-boxed instead (0 disables the cap).
PG_BENCH_ROWS = int(env("PG_BENCH_ROWS", "20000"))
PG_BENCH_SECONDS = float(env("PG_BENCH_SECONDS", "0"))
# Writers per cell. The required matrix is over batch size, so this is a knob that
# is recorded in the report rather than a second matrix dimension.
PG_BENCH_WORKERS = int(env("PG_BENCH_WORKERS", "1"))
# Fixed seed for the generated dataset: same seed -> same ids and payloads.
PG_BENCH_SEED = int(env("PG_BENCH_SEED", "20261030"))
# sink.write-batch-interval for every cell; only the batch size varies.
PG_BENCH_BATCH_INTERVAL = env("PG_BENCH_BATCH_INTERVAL", "200ms")
# pipeline.parallelism for every cell (1 keeps a table's event stream serialized,
# which is what makes the probe latency a sound end-to-end measurement).
PG_BENCH_PARALLELISM = int(env("PG_BENCH_PARALLELISM", "1"))
# Probe channel: one single-row transaction every interval, timed from the MySQL
# commit to the row being visible on the target. 0 disables the probe.
PG_BENCH_PROBE_INTERVAL_S = float(env("PG_BENCH_PROBE_INTERVAL_S", "2"))
PG_BENCH_PROBE_TIMEOUT_S = float(env("PG_BENCH_PROBE_TIMEOUT_S", "30"))
# Poll resolution of the probe (and of the catch-up wait).
PG_BENCH_POLL_S = float(env("PG_BENCH_POLL_S", "0.02"))
PG_BENCH_CATCHUP_BUDGET_S = float(env("PG_BENCH_CATCHUP_BUDGET_S", "600"))
PG_BENCH_VERIFY_CHUNK = int(env("PG_BENCH_VERIFY_CHUNK", "5000"))


def it_rel_path(path: str | Path) -> str:
    """Paths relative to test/integration for YAML/reports (absolute IT paths rewritten)."""
    p = Path(path)
    if not p.is_absolute():
        return str(p)
    try:
        return str(p.resolve().relative_to(IT_ROOT))
    except ValueError:
        return str(p)


def _abs_under_it(path: str) -> str:
    """Make path absolute under IT_ROOT when relative (portable across machines)."""
    p = Path(path)
    return str(p if p.is_absolute() else IT_ROOT / p)


# Runtime binary/checkpoints: absolute under WORKDIR (or IT_ROOT if override is relative).
# YAML uses it_rel_path() so rendered file-path stays portable.
BINARY = _abs_under_it(env("GO_CDC", env("CDC_CLI", str(WORKDIR / "go-cdc"))))
CHECKPOINT_DIR = _abs_under_it(env("CHECKPOINT_DIR", str(WORKDIR / "ckpts")))
CHECKPOINT_STDOUT_DIR = _abs_under_it(
    env("CHECKPOINT_STDOUT_DIR", str(WORKDIR / "ckpts-stdout"))
)
CHECKPOINT_TYPES_DIR = _abs_under_it(
    env("CHECKPOINT_TYPES_DIR", str(WORKDIR / "ckpts-types"))
)
CHECKPOINT_LOAD_DIR = _abs_under_it(env("CHECKPOINT_LOAD_DIR", str(WORKDIR / "ckpts-load")))
CHECKPOINT_KAFKA_DIR = _abs_under_it(env("CHECKPOINT_KAFKA_DIR", str(WORKDIR / "ckpts-kafka")))
CHECKPOINT_KAFKA_LOAD_DIR = _abs_under_it(
    env("CHECKPOINT_KAFKA_LOAD_DIR", str(WORKDIR / "ckpts-kafka-load"))
)
CHECKPOINT_DORIS_DIR = _abs_under_it(
    env("CHECKPOINT_DORIS_DIR", str(WORKDIR / "ckpts-doris"))
)
CHECKPOINT_DORIS_LOAD_DIR = _abs_under_it(
    env("CHECKPOINT_DORIS_LOAD_DIR", str(WORKDIR / "ckpts-doris-load"))
)
CHECKPOINT_DORIS_SCALE_DIR = _abs_under_it(
    env("CHECKPOINT_DORIS_SCALE_DIR", str(WORKDIR / "ckpts-doris-scale"))
)
CHECKPOINT_ES_DIR = _abs_under_it(env("CHECKPOINT_ES_DIR", str(WORKDIR / "ckpts-es")))
CHECKPOINT_ES_RESTART_DIR = _abs_under_it(
    env("CHECKPOINT_ES_RESTART_DIR", str(WORKDIR / "ckpts-es-restart"))
)
CHECKPOINT_ES_LOAD_DIR = _abs_under_it(
    env("CHECKPOINT_ES_LOAD_DIR", str(WORKDIR / "ckpts-es-load"))
)
CHECKPOINT_PG_DIR = _abs_under_it(env("CHECKPOINT_PG_DIR", str(WORKDIR / "ckpts-pg")))
CHECKPOINT_PG_RESTART_DIR = _abs_under_it(
    env("CHECKPOINT_PG_RESTART_DIR", str(WORKDIR / "ckpts-pg-restart"))
)
CHECKPOINT_PG_LOAD_DIR = _abs_under_it(
    env("CHECKPOINT_PG_LOAD_DIR", str(WORKDIR / "ckpts-pg-load"))
)
CHECKPOINT_PG_SCALE_DIR = _abs_under_it(
    env("CHECKPOINT_PG_SCALE_DIR", str(WORKDIR / "ckpts-pg-scale"))
)
CHECKPOINT_PG_BENCH_DIR = _abs_under_it(
    env("CHECKPOINT_PG_BENCH_DIR", str(WORKDIR / "ckpts-pg-bench"))
)

PIPELINE_NAME = "tp_func_pipeline"
PIPELINE_STDOUT_NAME = "tp_func_stdout"
PIPELINE_TYPES_NAME = "tp_types_pipeline"
PIPELINE_LOAD_NAME = "tp_load_pipeline"
PIPELINE_KAFKA_NAME = "tp_kafka_pipeline"
PIPELINE_KAFKA_LOAD_NAME = "tp_kafka_load_pipeline"
PIPELINE_DORIS_NAME = "tp_doris_pipeline"
PIPELINE_DORIS_LOAD_NAME = "tp_doris_load_pipeline"
PIPELINE_ES_NAME = "tp_es_pipeline"
PIPELINE_ES_RESTART_NAME = "tp_es_restart"
PIPELINE_ES_LOAD_NAME = "tp_es_load_pipeline"
PIPELINE_PG_NAME = "tp_pg_pipeline"
PIPELINE_PG_RESTART_NAME = "tp_pg_restart"
PIPELINE_PG_LOAD_NAME = "tp_pg_load_pipeline"
PIPELINE_PG_BENCH_NAME = "tp_pg_bench_pipeline"

ASSERT_TIME_ZONE = env("ASSERT_TIME_ZONE", "+08:00")

# Load suite knobs.
# Normal tables (t1–t4 + typed): 50k rows/table by default.
LOAD_PROFILE = env("LOAD_PROFILE", "")  # optional label: full | normal | …
LOAD_ROWS_PER_TABLE = int(env("LOAD_ROWS_PER_TABLE", "50000"))
LOAD_PEAK_SECONDS = int(env("LOAD_PEAK_SECONDS", "60"))
LOAD_PEAK_WORKERS = int(env("LOAD_PEAK_WORKERS", "4"))
LOAD_PEAK_BATCH = int(env("LOAD_PEAK_BATCH", "200"))
# CDC pipeline.parallelism for load suite (defaults to peak workers).
LOAD_PARALLELISM = int(env("LOAD_PARALLELISM", str(LOAD_PEAK_WORKERS)))
# Mega-field tables: few rows × 1–5MB LONGBLOB (image-like) + LONGTEXT.
# (50k × 5MB is not practical; volume is rows × bytes, reported in the load report.)
LOAD_LARGE_ROWS = int(env("LOAD_LARGE_ROWS", "50"))
_raw_large = int(env("LOAD_LARGE_FIELD_BYTES", str(2 * 1024 * 1024)))  # default 2MiB
LOAD_LARGE_FIELD_BYTES = max(1024 * 1024, min(5 * 1024 * 1024, _raw_large))


def conn_kwargs(user: str, password: str, database: str) -> dict:
    return dict(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
    )


def project_display_path(path: str | Path) -> str:
    """Rewrite absolute / IT-relative repo paths as ${PROJECT_ROOT}/... for reports."""
    text = str(path)
    root = str(ROOT)
    if text == root or text.startswith(root + "/"):
        return "${PROJECT_ROOT}" + text[len(root) :]
    it = str(IT_ROOT)
    if text == it or text.startswith(it + "/"):
        return "${PROJECT_ROOT}/test/integration" + text[len(it) :]
    # Already relative to test/integration (e.g. workdir/go-cdc).
    if text.startswith("workdir/") or text == "workdir":
        return "${PROJECT_ROOT}/test/integration/" + text
    return text


def md_cell(value, *, code: bool = False) -> str:
    """Single-line Markdown table cell (newlines/pipes would break GFM tables)."""
    if value is None:
        text = "NULL"
    else:
        text = str(value)
    text = (
        text.replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("\n", "<br>")
        .replace("|", "\\|")
    )
    if code:
        text = text.replace("`", "'")
        return f"`{text}`"
    return text


def sanitize_report_text(text: str) -> str:
    """Strip absolute repo roots from report bodies (belt-and-suspenders)."""
    return text.replace(str(ROOT), "${PROJECT_ROOT}")


SRC = conn_kwargs(SRC_USER, SRC_PASSWORD, SRC_DB)
SNK = conn_kwargs(SINK_USER, SINK_PASSWORD, SINK_DB)


def checkpoint_file(base_dir: str, pipeline_name: str) -> Path:
    digest = hashlib.md5(pipeline_name.encode("utf-8")).hexdigest()
    return Path(base_dir) / pipeline_name / f"{digest}.ckpt"


def pg_pipeline_scale_name(profile: str) -> str:
    """Per-profile pipeline name; keeps normal/heavy scale checkpoints apart."""
    return f"tp_pg_scale_{profile}"


CHECKPOINT_PATH = checkpoint_file(CHECKPOINT_DIR, PIPELINE_NAME)
CHECKPOINT_STDOUT_PATH = checkpoint_file(CHECKPOINT_STDOUT_DIR, PIPELINE_STDOUT_NAME)
CHECKPOINT_TYPES_PATH = checkpoint_file(CHECKPOINT_TYPES_DIR, PIPELINE_TYPES_NAME)
CHECKPOINT_LOAD_PATH = checkpoint_file(CHECKPOINT_LOAD_DIR, PIPELINE_LOAD_NAME)
CHECKPOINT_KAFKA_PATH = checkpoint_file(CHECKPOINT_KAFKA_DIR, PIPELINE_KAFKA_NAME)
CHECKPOINT_KAFKA_LOAD_PATH = checkpoint_file(CHECKPOINT_KAFKA_LOAD_DIR, PIPELINE_KAFKA_LOAD_NAME)
CHECKPOINT_DORIS_PATH = checkpoint_file(CHECKPOINT_DORIS_DIR, PIPELINE_DORIS_NAME)
CHECKPOINT_DORIS_LOAD_PATH = checkpoint_file(
    CHECKPOINT_DORIS_LOAD_DIR, PIPELINE_DORIS_LOAD_NAME
)
CHECKPOINT_ES_PATH = checkpoint_file(CHECKPOINT_ES_DIR, PIPELINE_ES_NAME)
CHECKPOINT_ES_RESTART_PATH = checkpoint_file(CHECKPOINT_ES_RESTART_DIR, PIPELINE_ES_RESTART_NAME)
CHECKPOINT_ES_LOAD_PATH = checkpoint_file(CHECKPOINT_ES_LOAD_DIR, PIPELINE_ES_LOAD_NAME)
CHECKPOINT_PG_PATH = checkpoint_file(CHECKPOINT_PG_DIR, PIPELINE_PG_NAME)
CHECKPOINT_PG_RESTART_PATH = checkpoint_file(
    CHECKPOINT_PG_RESTART_DIR, PIPELINE_PG_RESTART_NAME
)
CHECKPOINT_PG_LOAD_PATH = checkpoint_file(CHECKPOINT_PG_LOAD_DIR, PIPELINE_PG_LOAD_NAME)
CHECKPOINT_PG_BENCH_PATH = checkpoint_file(CHECKPOINT_PG_BENCH_DIR, PIPELINE_PG_BENCH_NAME)


def render_template(tpl_name: str, out_path: Path, extra: dict | None = None) -> Path:
    """Replace ${VAR} placeholders from os.environ + extra."""
    text = (FIXTURES / tpl_name).read_text(encoding="utf-8")
    mapping = {
        "MYSQL_HOST": MYSQL_HOST,
        "MYSQL_PORT": str(MYSQL_PORT),
        "SRC_USER": SRC_USER,
        "SRC_PASSWORD": SRC_PASSWORD,
        "SINK_USER": SINK_USER,
        "SINK_PASSWORD": SINK_PASSWORD,
        "SRC_DB": SRC_DB,
        "SINK_DB": SINK_DB,
        "FUNC_SRC_DB": FUNC_SRC_DB,
        "FUNC_SNK_DB": FUNC_SNK_DB,
        "TYPES_SRC_DB": TYPES_SRC_DB,
        "TYPES_SNK_DB": TYPES_SNK_DB,
        "LOAD_SRC_DB": LOAD_SRC_DB,
        "LOAD_SNK_DB": LOAD_SNK_DB,
        "CDC_SERVER_ID": CDC_SERVER_ID,
        "CDC_SERVER_ID_STDOUT": CDC_SERVER_ID_STDOUT,
        "CDC_SERVER_ID_TYPES": CDC_SERVER_ID_TYPES,
        "CDC_SERVER_ID_LOAD": CDC_SERVER_ID_LOAD,
        "CDC_SERVER_ID_KAFKA": CDC_SERVER_ID_KAFKA,
        "CDC_SERVER_ID_KAFKA_LOAD": CDC_SERVER_ID_KAFKA_LOAD,
        "CDC_SERVER_ID_DORIS": CDC_SERVER_ID_DORIS,
        "CDC_SERVER_ID_DORIS_LOAD": CDC_SERVER_ID_DORIS_LOAD,
        "CDC_SERVER_ID_ES": CDC_SERVER_ID_ES,
        "CDC_SERVER_ID_ES_RESTART": CDC_SERVER_ID_ES_RESTART,
        "CDC_SERVER_ID_ES_LOAD": CDC_SERVER_ID_ES_LOAD,
        "ES_URL": ES_URL,
        "ES_SINK_VERSION": ES_SINK_VERSION,
        "ES_SRC_DB": ES_SRC_DB,
        "ES_SNK_DB": ES_SNK_DB,
        "ES_LOAD_SNK_DB": ES_LOAD_SNK_DB,
        "ES_INDEX_PREFIX": ES_INDEX_PREFIX,
        "ES_LOAD_INDEX_PREFIX": ES_LOAD_INDEX_PREFIX,
        "ES_SCALE_INDEX_PREFIX": ES_SCALE_INDEX_PREFIX,
        "ES_LOAD_BATCH_SIZE": ES_LOAD_BATCH_SIZE,
        "ES_LOAD_BATCH_INTERVAL": ES_LOAD_BATCH_INTERVAL,
        "ES_LOAD_SMALL_BATCH": ES_LOAD_SMALL_BATCH,
        "ES_LOAD_SMALL_INTERVAL": ES_LOAD_SMALL_INTERVAL,
        "CHECKPOINT_ES_DIR": it_rel_path(CHECKPOINT_ES_DIR),
        "CHECKPOINT_ES_RESTART_DIR": it_rel_path(CHECKPOINT_ES_RESTART_DIR),
        "CHECKPOINT_ES_LOAD_DIR": it_rel_path(CHECKPOINT_ES_LOAD_DIR),
        "PIPELINE_ES_NAME": PIPELINE_ES_NAME,
        "PIPELINE_ES_RESTART_NAME": PIPELINE_ES_RESTART_NAME,
        "PIPELINE_ES_LOAD_NAME": PIPELINE_ES_LOAD_NAME,
        "CHECKPOINT_DIR": it_rel_path(CHECKPOINT_DIR),
        "CHECKPOINT_STDOUT_DIR": it_rel_path(CHECKPOINT_STDOUT_DIR),
        "CHECKPOINT_TYPES_DIR": it_rel_path(CHECKPOINT_TYPES_DIR),
        "CHECKPOINT_LOAD_DIR": it_rel_path(CHECKPOINT_LOAD_DIR),
        "CHECKPOINT_KAFKA_DIR": it_rel_path(CHECKPOINT_KAFKA_DIR),
        "CHECKPOINT_KAFKA_LOAD_DIR": it_rel_path(CHECKPOINT_KAFKA_LOAD_DIR),
        "CHECKPOINT_DORIS_DIR": it_rel_path(CHECKPOINT_DORIS_DIR),
        "CHECKPOINT_DORIS_LOAD_DIR": it_rel_path(CHECKPOINT_DORIS_LOAD_DIR),
        "PIPELINE_NAME": PIPELINE_NAME,
        "PIPELINE_STDOUT_NAME": PIPELINE_STDOUT_NAME,
        "PIPELINE_TYPES_NAME": PIPELINE_TYPES_NAME,
        "PIPELINE_LOAD_NAME": PIPELINE_LOAD_NAME,
        "PIPELINE_KAFKA_NAME": PIPELINE_KAFKA_NAME,
        "PIPELINE_KAFKA_LOAD_NAME": PIPELINE_KAFKA_LOAD_NAME,
        "PIPELINE_DORIS_NAME": PIPELINE_DORIS_NAME,
        "PIPELINE_DORIS_LOAD_NAME": PIPELINE_DORIS_LOAD_NAME,
        "KAFKA_BROKERS": KAFKA_BROKERS,
        "KAFKA_DEFAULT_TOPIC": KAFKA_DEFAULT_TOPIC,
        "KAFKA_EMIT_DDL": KAFKA_EMIT_DDL,
        "DORIS_HOST": DORIS_HOST,
        "DORIS_HTTP_PORT": str(DORIS_HTTP_PORT),
        "DORIS_SQL_PORT": str(DORIS_SQL_PORT),
        "DORIS_USER": DORIS_USER,
        "DORIS_PASSWORD": DORIS_PASSWORD,
        "DORIS_REPLICATION_NUM": DORIS_REPLICATION_NUM,
        "DORIS_SNK_DB": DORIS_SNK_DB,
        "LOAD_PARALLELISM": str(LOAD_PARALLELISM),
    }
    if extra:
        mapping.update({k: str(v) for k, v in extra.items()})
    for key, value in mapping.items():
        text = text.replace("${" + key + "}", value)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")
    return out_path
