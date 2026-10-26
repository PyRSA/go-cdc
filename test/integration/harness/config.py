"""Environment-driven connection settings for integration tests.

Naming:
  - Users/passwords are anonymous fixed tokens (not production-like).
  - Databases are suite-scoped: db_tp_{func,types,load}_{src,snk}.
  - Tables are test-point scoped: tp01_*, tpd01_*, tpl01_*.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

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
REPORT_ES = REPORT_DIR / "mysql-elasticsearch.md"
REPORT_ES_LOAD = REPORT_DIR / "mysql-elasticsearch-load.md"
REPORT_ES_SCALE_TMPL = "mysql-elasticsearch-scale-{profile}.md"


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


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
# Scale suite: every final table lives under ``db_tp_scale_snk_<nn>``, which is
# both the index-name namespace and the cleanup prefix.
ES_SCALE_INDEX_PREFIX = env("ES_SCALE_INDEX_PREFIX", "db_tp_scale_snk_*")

# Elasticsearch load suite: CDC bulk size / interval sweep (one bulk per Write).
ES_LOAD_BATCH_SIZE = env("ES_LOAD_BATCH_SIZE", "1000")
ES_LOAD_BATCH_INTERVAL = env("ES_LOAD_BATCH_INTERVAL", "500ms")
ES_LOAD_SMALL_BATCH = env("ES_LOAD_SMALL_BATCH", "100")
ES_LOAD_SMALL_INTERVAL = env("ES_LOAD_SMALL_INTERVAL", "100ms")


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
