"""Doris-safe scale table templates.

Mirrors ``scale.schemas`` but restricts every column to a type the Doris sink
can map in v1 (see ``connector/sink/doris/types.go``): no BLOB/VARBINARY, no
TIME, no ENUM/SET/BIT/spatial. Template *names* and per-template row weights
stay identical to the MySQL→MySQL suite so profiles allocate the same row
counts and the two reports stay comparable.

Two deliberate substitutions:
  - ``wide`` drops ``c_time TIME`` (no lossless Doris mapping in v1).
  - ``large`` replaces ``binary_data BLOB`` with ``payload LONGTEXT`` of the
    same byte size, keeping the template about large-field throughput.

Checksums come in two strengths because MySQL and Doris CRC32 equivalence is
not assumed: ``crc32`` (cross-engine sum) and ``len`` (character-length sum,
spelled CHAR_LENGTH on MySQL and LENGTH on Doris, both character-based).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from decimal import Decimal

from common.profiles import TEMPLATES

CHUNK_SIZE = 500
_LARGE_CONTENT_BYTES = 2048
_LARGE_PAYLOAD_BYTES = 1024

# Stable string column per template, used for the content-level checksum.
_CHECKSUM_COLUMN = {
    "basic": "name",
    "wide": "c_label",
    "indexed": "external_id",
    "large": "name",
}

CHECKSUM_MODES = ("crc32", "len")
CHECKSUM_DIALECTS = ("mysql", "doris")


def large_payload_sizes() -> tuple[int, int]:
    """Return (content character length, payload character length) for large rows."""
    return (_LARGE_CONTENT_BYTES, _LARGE_PAYLOAD_BYTES)


def create_table_sql(template: str, table: str) -> str:
    if template not in TEMPLATES:
        raise ValueError(f"unknown template: {template!r}")
    return _DDL[template].format(table=table)


def checksum_sql(
    template: str,
    db: str,
    table: str,
    *,
    dialect: str = "mysql",
    mode: str = "crc32",
) -> str:
    """Return a three-part aggregate query labelled cnt / sum_id / name_crc.

    ``mode="len"`` is the degraded content check used when Doris and MySQL do
    not agree on CRC32; the alias is kept so callers compare tuples uniformly.
    """
    if template not in TEMPLATES:
        raise ValueError(f"unknown template: {template!r}")
    if dialect not in CHECKSUM_DIALECTS:
        raise ValueError(f"unknown dialect: {dialect!r}")
    if mode not in CHECKSUM_MODES:
        raise ValueError(f"unknown checksum mode: {mode!r}")
    col = _CHECKSUM_COLUMN[template]
    if mode == "crc32":
        content = f"COALESCE(SUM(CRC32(`{col}`)), 0)"
    elif dialect == "mysql":
        content = f"COALESCE(SUM(CHAR_LENGTH(`{col}`)), 0)"
    else:
        # Doris LENGTH() counts characters, matching MySQL CHAR_LENGTH().
        content = f"COALESCE(SUM(LENGTH(`{col}`)), 0)"
    return (
        f"SELECT COUNT(*) AS cnt, COALESCE(SUM(`id`), 0) AS sum_id, "
        f"{content} AS name_crc FROM `{db}`.`{table}`"
    )


def insert_batch(
    conn,
    db: str,
    table: str,
    template: str,
    id_start: int,
    id_end: int,
    db_index: int,
    ordinal: int,
) -> None:
    if template not in TEMPLATES:
        raise ValueError(f"unknown template: {template!r}")
    if id_end < id_start:
        return
    insert_sql = _INSERT_SQL[template].format(db=db, table=table)
    row_id = id_start
    while row_id <= id_end:
        chunk_end = min(row_id + CHUNK_SIZE - 1, id_end)
        rows = [_row(template, i, db_index, ordinal) for i in range(row_id, chunk_end + 1)]
        with conn.cursor() as cur:
            cur.executemany(insert_sql, rows)
        row_id = chunk_end + 1
    conn.commit()


def _seed_key(db_index: int, ordinal: int, row_id: int) -> int:
    return db_index * 1_000_000 + ordinal * 10_000 + row_id


def _name(db_index: int, ordinal: int, row_id: int) -> str:
    return f"dscale-d{db_index:02d}-t{ordinal:03d}-id{row_id}"


def _timestamps(db_index: int, ordinal: int, row_id: int) -> tuple[str, str]:
    base = datetime(2024, 1, 1, 8, 0, 0) + timedelta(
        seconds=_seed_key(db_index, ordinal, row_id) % 86_400,
        microseconds=(row_id * 17 + ordinal) % 1_000_000,
    )
    ts = base.strftime("%Y-%m-%d %H:%M:%S.%f")
    return ts, ts


def _filler(prefix: str, db_index: int, ordinal: int, row_id: int, size: int) -> str:
    unit = f"{prefix}{db_index:02d}T{ordinal:03d}I{row_id}|"
    repeat = (size // len(unit)) + 2
    return (unit * repeat)[:size]


def _row(template: str, row_id: int, db_index: int, ordinal: int) -> tuple:
    sk = _seed_key(db_index, ordinal, row_id)
    created_at, updated_at = _timestamps(db_index, ordinal, row_id)
    if template == "basic":
        amount = Decimal(f"{(sk % 10_000) / 100:.4f}")
        description = None if row_id % 5 == 0 else f"desc-{db_index}-{ordinal}-{row_id}"
        return (
            row_id,
            (db_index * 100 + ordinal + row_id) % 10_000 + 1,
            _name(db_index, ordinal, row_id),
            (row_id + db_index + ordinal) % 128,
            amount,
            description,
            created_at,
            updated_at,
        )
    if template == "wide":
        # No c_time: Doris v1 rejects TIME.
        return (
            row_id,
            row_id + db_index,
            (sk % 2_000_000_000) - 1_000_000_000,
            row_id % 128 if row_id % 7 else None,
            Decimal(f"{(sk % 50_000) / 100:.4f}") if row_id % 3 else None,
            f"v-{db_index}-{ordinal}-{row_id}" if row_id % 4 else None,
            f"text-{sk}" if row_id % 6 else None,
            f"2024-{(sk % 12) + 1:02d}-{(sk % 28) + 1:02d}",
            created_at,
            created_at,
            json.dumps({"db": db_index, "ord": ordinal, "id": row_id}) if row_id % 2 else None,
            row_id % 2,
            row_id + ordinal if row_id % 5 else None,
            f"code-{sk % 10000:04d}",
            None if row_id % 8 == 0 else f"note-{sk}",
            Decimal(f"{(sk % 9999) / 100:.2f}"),
            sk % 1000,
            float((sk % 1000) / 10.0),
            json.dumps({"k": sk % 100}) if row_id % 3 == 0 else None,
            f"lbl-{ordinal}-{row_id % 1000}",
            created_at,
            updated_at,
        )
    if template == "indexed":
        return (
            row_id,
            db_index * 1000 + ordinal,
            row_id + ordinal * 1_000_000,
            f"ext-d{db_index:02d}-t{ordinal:03d}-id{row_id}",
            (row_id + db_index) % 16,
            Decimal(f"{(sk % 20_000) / 100:.4f}") if row_id % 4 else None,
            created_at,
            updated_at,
        )
    if template == "large":
        # payload LONGTEXT stands in for the MySQL suite's BLOB column.
        return (
            row_id,
            _name(db_index, ordinal, row_id),
            _filler("C", db_index, ordinal, row_id, _LARGE_CONTENT_BYTES),
            json.dumps({"db": db_index, "ord": ordinal, "id": row_id}),
            _filler("P", db_index, ordinal, row_id, _LARGE_PAYLOAD_BYTES),
            created_at,
            updated_at,
        )
    raise ValueError(f"unknown template: {template!r}")


_DDL: dict[str, str] = {
    "basic": """CREATE TABLE `{table}` (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `tenant_id` INT NOT NULL,
    `name` VARCHAR(128) NOT NULL,
    `status` TINYINT NOT NULL,
    `amount` DECIMAL(18, 4) NULL,
    `description` VARCHAR(512) NULL,
    `created_at` DATETIME(6) NOT NULL,
    `updated_at` DATETIME(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    "wide": """CREATE TABLE `{table}` (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `c_bigint` BIGINT NOT NULL,
    `c_int` INT NOT NULL,
    `c_tinyint` TINYINT NULL,
    `c_decimal` DECIMAL(18, 4) NULL,
    `c_varchar` VARCHAR(256) NULL,
    `c_text` TEXT NULL,
    `c_date` DATE NULL,
    `c_datetime` DATETIME(6) NULL,
    `c_timestamp` TIMESTAMP NULL,
    `c_json` JSON NULL,
    `c_flag` TINYINT NOT NULL,
    `c_ref_id` BIGINT NULL,
    `c_code` VARCHAR(64) NULL,
    `c_note` TEXT NULL,
    `c_amount` DECIMAL(10, 2) NULL,
    `c_qty` INT NULL,
    `c_score` DOUBLE NULL,
    `c_meta` JSON NULL,
    `c_label` VARCHAR(128) NOT NULL,
    `created_at` DATETIME(6) NOT NULL,
    `updated_at` DATETIME(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    "indexed": """CREATE TABLE `{table}` (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `tenant_id` BIGINT NOT NULL,
    `user_id` BIGINT NOT NULL,
    `external_id` VARCHAR(128) NULL,
    `status` TINYINT NOT NULL,
    `amount` DECIMAL(18, 4) NULL,
    `created_at` DATETIME(6) NOT NULL,
    `updated_at` DATETIME(6) NOT NULL,
    UNIQUE KEY `uk_external_id` (`external_id`),
    KEY `idx_tenant_id` (`tenant_id`),
    KEY `idx_user_id` (`user_id`),
    KEY `idx_status_updated` (`status`, `updated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    "large": """CREATE TABLE `{table}` (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `name` VARCHAR(128) NOT NULL,
    `content` TEXT NULL,
    `json_data` JSON NULL,
    `payload` LONGTEXT NULL,
    `created_at` DATETIME(6) NOT NULL,
    `updated_at` DATETIME(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
}

_INSERT_SQL: dict[str, str] = {
    "basic": (
        "INSERT INTO `{db}`.`{table}` "
        "(`id`, `tenant_id`, `name`, `status`, `amount`, `description`, `created_at`, `updated_at`) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    ),
    "wide": (
        "INSERT INTO `{db}`.`{table}` "
        "(`id`, `c_bigint`, `c_int`, `c_tinyint`, `c_decimal`, `c_varchar`, `c_text`, "
        "`c_date`, `c_datetime`, `c_timestamp`, `c_json`, `c_flag`, `c_ref_id`, "
        "`c_code`, `c_note`, `c_amount`, `c_qty`, `c_score`, `c_meta`, `c_label`, "
        "`created_at`, `updated_at`) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    ),
    "indexed": (
        "INSERT INTO `{db}`.`{table}` "
        "(`id`, `tenant_id`, `user_id`, `external_id`, `status`, `amount`, `created_at`, `updated_at`) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    ),
    "large": (
        "INSERT INTO `{db}`.`{table}` "
        "(`id`, `name`, `content`, `json_data`, `payload`, `created_at`, `updated_at`) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    ),
}
