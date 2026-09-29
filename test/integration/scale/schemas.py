from __future__ import annotations

import json
from datetime import datetime, timedelta
from decimal import Decimal

from scale.profiles import TEMPLATES

CHUNK_SIZE = 500
_LARGE_CONTENT_BYTES = 2048
_LARGE_BINARY_BYTES = 1024

# Stable string column used for CRC32 checksum per template (alias remains name_crc).
_CHECKSUM_CRC_COLUMN = {
    "basic": "name",
    "wide": "c_label",
    "indexed": "external_id",
    "large": "name",
}


def large_payload_sizes() -> tuple[int, int]:
    """Return (content character length, binary byte length) for large template rows."""
    return (_LARGE_CONTENT_BYTES, _LARGE_BINARY_BYTES)


def create_table_sql(template: str, table: str) -> str:
    if template not in TEMPLATES:
        raise ValueError(f"unknown template: {template!r}")
    ddl = _DDL[template]
    return ddl.format(table=table)


def checksum_sql(template: str, db: str, table: str) -> str:
    if template not in TEMPLATES:
        raise ValueError(f"unknown template: {template!r}")
    col = _CHECKSUM_CRC_COLUMN[template]
    name_crc = f"COALESCE(SUM(CRC32(`{col}`)), 0)"
    return (
        f"SELECT COUNT(*) AS cnt, COALESCE(SUM(`id`), 0) AS sum_id, "
        f"{name_crc} AS name_crc FROM `{db}`.`{table}`"
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
    return f"scale-d{db_index:02d}-t{ordinal:03d}-id{row_id}"


def _timestamps(db_index: int, ordinal: int, row_id: int) -> tuple[str, str]:
    base = datetime(2024, 1, 1, 8, 0, 0) + timedelta(
        seconds=_seed_key(db_index, ordinal, row_id) % 86_400,
        microseconds=(row_id * 17 + ordinal) % 1_000_000,
    )
    ts = base.strftime("%Y-%m-%d %H:%M:%S.%f")
    return ts, ts


def _large_content(db_index: int, ordinal: int, row_id: int) -> str:
    unit = f"C{db_index:02d}T{ordinal:03d}I{row_id}|"
    repeat = (_LARGE_CONTENT_BYTES // len(unit)) + 2
    return (unit * repeat)[:_LARGE_CONTENT_BYTES]


def _large_binary(db_index: int, ordinal: int, row_id: int) -> bytes:
    head = bytes([db_index & 0xFF, ordinal & 0xFF, row_id & 0xFF])
    unit = head + f"{row_id:012d}".encode("ascii")
    repeat = (_LARGE_BINARY_BYTES // len(unit)) + 2
    return (unit * repeat)[:_LARGE_BINARY_BYTES]


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
        return (
            row_id,
            row_id + db_index,
            (sk % 2_000_000_000) - 1_000_000_000,
            row_id % 128 if row_id % 7 else None,
            Decimal(f"{(sk % 50_000) / 100:.4f}") if row_id % 3 else None,
            f"v-{db_index}-{ordinal}-{row_id}" if row_id % 4 else None,
            f"text-{sk}" if row_id % 6 else None,
            f"2024-{(sk % 12) + 1:02d}-{(sk % 28) + 1:02d}",
            f"{(sk % 24):02d}:{(sk % 60):02d}:{(sk % 60):02d}",
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
        return (
            row_id,
            _name(db_index, ordinal, row_id),
            _large_content(db_index, ordinal, row_id),
            json.dumps({"db": db_index, "ord": ordinal, "id": row_id}),
            _large_binary(db_index, ordinal, row_id),
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
    `c_time` TIME NULL,
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
    `binary_data` BLOB NULL,
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
        "`c_date`, `c_time`, `c_datetime`, `c_timestamp`, `c_json`, `c_flag`, `c_ref_id`, "
        "`c_code`, `c_note`, `c_amount`, `c_qty`, `c_score`, `c_meta`, `c_label`, "
        "`created_at`, `updated_at`) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    ),
    "indexed": (
        "INSERT INTO `{db}`.`{table}` "
        "(`id`, `tenant_id`, `user_id`, `external_id`, `status`, `amount`, `created_at`, `updated_at`) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    ),
    "large": (
        "INSERT INTO `{db}`.`{table}` "
        "(`id`, `name`, `content`, `json_data`, `binary_data`, `created_at`, `updated_at`) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    ),
}
