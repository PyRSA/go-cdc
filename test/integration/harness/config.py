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

# Back-compat aliases used by functional harness.
SRC_DB = env("SRC_DB", FUNC_SRC_DB)
SINK_DB = env("SINK_DB", FUNC_SNK_DB)

CDC_SERVER_ID = env("CDC_SERVER_ID", "5613")
CDC_SERVER_ID_STDOUT = env("CDC_SERVER_ID_STDOUT", "5612")
CDC_SERVER_ID_TYPES = env("CDC_SERVER_ID_TYPES", "5614")
CDC_SERVER_ID_LOAD = env("CDC_SERVER_ID_LOAD", "5615")

# Binary artifact name matches the product: go-cdc (CDC_CLI kept as alias).
BINARY = env("GO_CDC", env("CDC_CLI", str(WORKDIR / "go-cdc")))

CHECKPOINT_DIR = env("CHECKPOINT_DIR", str(WORKDIR / "ckpts"))
CHECKPOINT_STDOUT_DIR = env("CHECKPOINT_STDOUT_DIR", str(WORKDIR / "ckpts-stdout"))
CHECKPOINT_TYPES_DIR = env("CHECKPOINT_TYPES_DIR", str(WORKDIR / "ckpts-types"))
CHECKPOINT_LOAD_DIR = env("CHECKPOINT_LOAD_DIR", str(WORKDIR / "ckpts-load"))

PIPELINE_NAME = "tp_func_pipeline"
PIPELINE_STDOUT_NAME = "tp_func_stdout"
PIPELINE_TYPES_NAME = "tp_types_pipeline"
PIPELINE_LOAD_NAME = "tp_load_pipeline"

ASSERT_TIME_ZONE = env("ASSERT_TIME_ZONE", "+08:00")

# Load suite knobs.
# Normal tables (t1–t4 + typed): 50k rows/table by default.
LOAD_PROFILE = env("LOAD_PROFILE", "")  # optional label: full | normal | …
LOAD_ROWS_PER_TABLE = int(env("LOAD_ROWS_PER_TABLE", "50000"))
LOAD_PEAK_SECONDS = int(env("LOAD_PEAK_SECONDS", "90"))
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


SRC = conn_kwargs(SRC_USER, SRC_PASSWORD, SRC_DB)
SNK = conn_kwargs(SINK_USER, SINK_PASSWORD, SINK_DB)


def checkpoint_file(base_dir: str, pipeline_name: str) -> Path:
    digest = hashlib.md5(pipeline_name.encode("utf-8")).hexdigest()
    return Path(base_dir) / pipeline_name / f"{digest}.ckpt"


CHECKPOINT_PATH = checkpoint_file(CHECKPOINT_DIR, PIPELINE_NAME)
CHECKPOINT_STDOUT_PATH = checkpoint_file(CHECKPOINT_STDOUT_DIR, PIPELINE_STDOUT_NAME)
CHECKPOINT_TYPES_PATH = checkpoint_file(CHECKPOINT_TYPES_DIR, PIPELINE_TYPES_NAME)
CHECKPOINT_LOAD_PATH = checkpoint_file(CHECKPOINT_LOAD_DIR, PIPELINE_LOAD_NAME)


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
        "CHECKPOINT_DIR": CHECKPOINT_DIR,
        "CHECKPOINT_STDOUT_DIR": CHECKPOINT_STDOUT_DIR,
        "CHECKPOINT_TYPES_DIR": CHECKPOINT_TYPES_DIR,
        "CHECKPOINT_LOAD_DIR": CHECKPOINT_LOAD_DIR,
        "PIPELINE_NAME": PIPELINE_NAME,
        "PIPELINE_STDOUT_NAME": PIPELINE_STDOUT_NAME,
        "PIPELINE_TYPES_NAME": PIPELINE_TYPES_NAME,
        "PIPELINE_LOAD_NAME": PIPELINE_LOAD_NAME,
    }
    if extra:
        mapping.update({k: str(v) for k, v in extra.items()})
    for key, value in mapping.items():
        text = text.replace("${" + key + "}", value)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")
    return out_path
