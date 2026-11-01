"""Offline checks for the Doris scale templates.

Guard the two things that would otherwise only surface mid-run against a live
Doris: a rejected column type reaching the sink, and a checksum query that
changes shape between dialects.
"""
import re

from ..scale.doris_schemas import (
    checksum_sql,
    create_table_sql,
    insert_batch,
    large_payload_sizes,
)
from common.profiles import TEMPLATES

# Base type tokens connector/sink/doris/types.go rejects in v1. Matched on the
# parsed column type, never on the raw DDL text: CHARSET=utf8mb4 contains "SET"
# and DATETIME contains "TIME".
FORBIDDEN_TYPES = {
    "BLOB",
    "TINYBLOB",
    "MEDIUMBLOB",
    "LONGBLOB",
    "BINARY",
    "VARBINARY",
    "TIME",
    "ENUM",
    "SET",
    "BIT",
    "GEOMETRY",
}

_COLUMN_RE = re.compile(r"^\s*`(\w+)`\s+([A-Za-z]+)")


def _column_types(sql: str) -> dict[str, str]:
    """Map column name → upper-cased base type for every backticked column."""
    out: dict[str, str] = {}
    for line in sql.splitlines():
        match = _COLUMN_RE.match(line)
        if match:
            out[match.group(1)] = match.group(2).upper()
    return out


def test_no_template_uses_doris_rejected_types():
    for tpl in TEMPLATES:
        types = _column_types(create_table_sql(tpl, "tp_scale_001"))
        bad = {name: t for name, t in types.items() if t in FORBIDDEN_TYPES}
        assert not bad, f"{tpl} has Doris-rejected columns: {bad}"


def test_wide_drops_time_but_keeps_datetime():
    types = _column_types(create_table_sql("wide", "tp_scale_002"))
    assert "c_time" not in types
    assert types["c_datetime"] == "DATETIME"
    assert types["c_timestamp"] == "TIMESTAMP"


def test_large_uses_longtext_not_blob():
    types = _column_types(create_table_sql("large", "tp_scale_004"))
    assert types["payload"] == "LONGTEXT"
    assert types["content"] == "TEXT"
    assert "binary_data" not in types
    assert "BLOB" not in set(types.values())


def test_basic_and_indexed_match_mysql_suite_columns():
    # These two templates are reused as-is; a silent column rename here would
    # make the Doris report incomparable with the MySQL→MySQL one.
    assert "tenant_id" in create_table_sql("basic", "t")
    assert "uk_external_id" in create_table_sql("indexed", "t")


def test_checksum_crc32_shape():
    sql = checksum_sql("basic", "db_tp_dscale_src_01", "tp_scale_001")
    compact = sql.replace(" ", "").upper()
    assert "COUNT(*)" in compact
    assert "CRC32(`NAME`)" in compact
    assert "name_crc" in sql.lower()


def test_checksum_len_mode_uses_dialect_proper_function():
    mysql_sql = checksum_sql("wide", "db_tp_dscale_src_01", "tp_scale_002",
                             dialect="mysql", mode="len")
    doris_sql = checksum_sql("wide", "db_tp_dscale_snk_01", "tp_scale_002_out",
                             dialect="doris", mode="len")
    # MySQL LENGTH() is bytes, Doris LENGTH() is characters: they must not match.
    assert "CHAR_LENGTH(`c_label`)" in mysql_sql
    assert "CHAR_LENGTH" not in doris_sql
    assert "LENGTH(`c_label`)" in doris_sql
    assert "CRC32" not in doris_sql


def test_checksum_crc32_mode_is_dialect_agnostic():
    mysql_sql = checksum_sql("wide", "db_tp_dscale_src_01", "tp_scale_002", dialect="mysql")
    doris_sql = checksum_sql("wide", "db_tp_dscale_snk_01", "tp_scale_002_out", dialect="doris")
    assert "CRC32(`c_label`)" in mysql_sql and "CRC32(`c_label`)" in doris_sql


def test_checksum_rejects_unknown_mode_and_dialect():
    for kwargs in ({"mode": "md5"}, {"dialect": "sqlite"}):
        try:
            checksum_sql("basic", "db", "t", **kwargs)
        except ValueError:
            continue
        raise AssertionError(f"expected ValueError for {kwargs}")


def test_row_width_matches_insert_placeholders():
    for tpl in TEMPLATES:
        sql = insert_batch_sql_for(tpl)
        placeholders = sql.split("VALUES")[1].count("%s")
        columns = sql.split("(")[1].split(")")[0].count(",") + 1
        assert placeholders == columns, f"{tpl}: {placeholders} vs {columns}"


def insert_batch_sql_for(template: str) -> str:
    from ..scale import doris_schemas

    return doris_schemas._INSERT_SQL[template].format(db="d", table="t")  # noqa: SLF001


def test_insert_batch_chunks_and_matches_row_arity():
    seen: list[list[tuple]] = []

    class FakeCursor:
        def executemany(self, _sql, rows):
            seen.append(list(rows))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class FakeConn:
        committed = False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            self.committed = True

    conn = FakeConn()
    insert_batch(conn, "db_tp_dscale_src_01", "tp_scale_001", "basic", 1, 1200, 1, 1)
    assert [len(chunk) for chunk in seen] == [500, 500, 200]
    assert all(len(row) == 8 for row in seen[0])
    assert conn.committed


def test_large_payload_sizes_are_unchanged_volume():
    content_len, payload_len = large_payload_sizes()
    assert 1500 <= content_len <= 2500
    assert 500 <= payload_len <= 1500
