"""Offline checks for the PostgreSQL scale templates.

The PostgreSQL suite reuses ``scale.schemas`` (source is MySQL), so the only
thing that may differ is the one column ``sink.postgresql`` refuses. These tests
pin that difference: it is derived, it is asserted, it never reaches the source
DDL, and the row generator / aggregate / projection stay in step with the
MySQL->MySQL suite. Everything here runs without MySQL or PostgreSQL.
"""
import re

from ..scale import pg_schemas
from common import schemas as base
from common.profiles import TEMPLATES

_COLUMN_RE = re.compile(r"^\s*`(?P<name>[^`]+)`\s+(?P<type>[A-Za-z]+(?:\([^)]*\))?)")


def _column_types(sql: str) -> dict[str, str]:
    """Map column name -> upper-cased base type for every backticked column."""
    out: dict[str, str] = {}
    for line in sql.splitlines():
        match = _COLUMN_RE.match(line)
        if match:
            out[match.group("name")] = match.group("type").upper()
    return out


def test_only_wide_drops_a_column():
    assert pg_schemas.SINK_DROPPED_COLUMNS == {"wide": ("c_time",)}
    for tpl in TEMPLATES:
        expected = () if tpl != "wide" else ("c_time",)
        assert pg_schemas.dropped_columns(tpl) == expected


def test_source_ddl_matches_mysql_suite_except_the_dropped_column():
    for tpl in TEMPLATES:
        ours = pg_schemas.create_table_sql(tpl, "tp_scale_001")
        theirs = base.create_table_sql(tpl, "tp_scale_001")
        if tpl != "wide":
            # Byte-for-byte: the PostgreSQL run consumes the same source tables.
            assert ours == theirs
            continue
        assert len(ours.splitlines()) == len(theirs.splitlines()) - 1
        assert "`c_time`" not in ours
        assert "`c_time`" in theirs


def test_wide_drops_time_but_keeps_datetime_and_timestamp():
    types = _column_types(pg_schemas.create_table_sql("wide", "tp_scale_002"))
    assert "c_time" not in types
    assert types["c_datetime"] == "DATETIME(6)"
    assert types["c_timestamp"] == "TIMESTAMP"
    # The trailing column of the template must survive the comma trim.
    assert types["updated_at"] == "DATETIME(6)"
    assert pg_schemas.create_table_sql("wide", "t").rstrip().endswith(
        "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    )


def test_create_table_sql_fails_when_the_dropped_column_is_gone(monkeypatch):
    # An upstream template change must fail loudly, not silently ship a column
    # the PostgreSQL sink would refuse at run time.
    monkeypatch.setitem(pg_schemas.SINK_DROPPED_COLUMNS, "wide", ("c_time", "c_not_there"))
    try:
        pg_schemas.create_table_sql("wide", "tp_scale_002")
    except ValueError as exc:
        assert "c_not_there" in str(exc)
    else:
        raise AssertionError("expected ValueError when the column to drop is absent")


def test_columns_of_excludes_only_the_dropped_column():
    for tpl in TEMPLATES:
        source = tuple(base.table_columns(tpl))
        dropped = set(pg_schemas.dropped_columns(tpl))
        assert pg_schemas.columns_of(tpl) == tuple(c for c in source if c not in dropped)


def test_kinds_and_sink_types_cover_exactly_the_projected_columns():
    for tpl in TEMPLATES:
        columns = pg_schemas.columns_of(tpl)
        kinds = pg_schemas.kinds_of(tpl)
        sink_types = pg_schemas.sink_types(tpl)
        assert set(kinds) == set(columns)
        assert set(sink_types) == set(columns)
        assert list(kinds) == list(columns)
        assert list(sink_types) == list(columns)
        assert "c_time" not in sink_types
        # Every kind must be one harness.pg_verify.canon understands.
        assert set(kinds.values()) <= {
            "int", "decimal", "float", "text", "char", "bytes",
            "date", "datetime", "json", "bool", "raw",
        }


def test_sink_types_follow_the_documented_mapping():
    wide = pg_schemas.sink_types("wide")
    assert wide["c_tinyint"] == "smallint"
    assert wide["c_int"] == "integer"
    assert wide["c_bigint"] == "bigint"
    assert wide["c_decimal"] == "numeric(18,4)"
    assert wide["c_timestamp"] == "timestamp without time zone"
    assert wide["c_datetime"] == "timestamp(6) without time zone"
    assert wide["c_score"] == "double precision"
    assert wide["c_json"] == "json"
    assert pg_schemas.sink_types("large")["binary_data"] == "bytea"
    assert pg_schemas.sink_types("basic")["description"] == "character varying(512)"


def test_row_values_match_the_shared_generator():
    for tpl in TEMPLATES:
        source_columns = list(base.table_columns(tpl))
        dropped = set(pg_schemas.dropped_columns(tpl))
        for row_id in (1, 2, 7, 99):
            full = base._row(tpl, row_id, 3, 5)  # noqa: SLF001 - generator is the model
            want = tuple(v for c, v in zip(source_columns, full) if c not in dropped)
            assert pg_schemas.row_values(tpl, row_id, 3, 5) == want


def test_row_values_rejects_a_generator_arity_change(monkeypatch):
    monkeypatch.setattr(base, "_row", lambda *_args, **_kwargs: (1, 2))
    try:
        pg_schemas.row_values("basic", 1, 1, 1)
    except ValueError as exc:
        assert "basic" in str(exc)
    else:
        raise AssertionError("expected ValueError for a short generator row")


def test_insert_sql_placeholder_count_matches_columns():
    for tpl in TEMPLATES:
        sql = pg_schemas.insert_sql(tpl, "db", "t")
        columns = pg_schemas.columns_of(tpl)
        assert sql.count("%s") == len(columns)
        named = sql.split("(", 1)[1].split(")", 1)[0]
        assert named.count("`") == 2 * len(columns)


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
    pg_schemas.insert_batch(conn, "db_tp_pg_scale_src_01", "tp_scale_002", "wide", 1, 1200, 1, 2)
    assert [len(chunk) for chunk in seen] == [500, 500, 200]
    assert all(len(row) == len(pg_schemas.columns_of("wide")) for chunk in seen for row in chunk)
    assert conn.committed


def test_checksum_sql_uses_a_character_length_per_dialect():
    mysql_sql = pg_schemas.checksum_sql("wide", "db_tp_pg_scale_src_01", "tp_scale_002")
    pg_sql = pg_schemas.checksum_sql(
        "wide", "tp_pg_scale_normal_01", "tp_scale_002_out", dialect="pg"
    )
    # CHAR_LENGTH (MySQL) and length (PostgreSQL) both count characters, so the
    # triple is comparable across engines; a byte-length would not be.
    assert "CHAR_LENGTH(`c_label`)" in mysql_sql
    assert 'length("c_label")' in pg_sql
    assert "CRC32" not in mysql_sql
    for sql in (mysql_sql, pg_sql):
        assert "COUNT(*)" in sql
        assert "SUM(" in sql
        assert "content_len" in sql
        assert "sum_id" in sql


def test_checksum_rejects_an_unknown_dialect():
    try:
        pg_schemas.checksum_sql("basic", "db", "t", dialect="sqlite")
    except ValueError as exc:
        assert "sqlite" in str(exc)
    else:
        raise AssertionError("expected ValueError for an unknown dialect")


def test_select_rows_sql_projects_every_column_and_quotes_per_dialect():
    for tpl in TEMPLATES:
        columns = pg_schemas.columns_of(tpl)
        mysql_sql = pg_schemas.select_rows_sql(tpl, "db", "t")
        pg_sql = pg_schemas.select_rows_sql(tpl, "db", "t", dialect="pg")
        # Every column plus the two schema/table names is quoted per dialect.
        assert mysql_sql.count("`") == 2 * len(columns) + 4, tpl
        assert pg_sql.count('"') == 2 * len(columns) + 4, tpl
        # ``c_timestamp`` contains ``c_time`` as a substring, so match the quoted name.
        assert '"c_time"' not in pg_sql
        assert "WHERE" not in mysql_sql.upper()


def test_mutation_and_checksum_columns_exist_and_match_the_mysql_suite():
    # scale_it mutates `name`, `c_label` and `status`; `indexed.status` is the
    # mutated column while `external_id` stays the character-length marker.
    assert pg_schemas.update_column("indexed") == "status"
    assert pg_schemas.marker_column("indexed") == "external_id"
    for tpl in TEMPLATES:
        source = base.table_columns(tpl)
        for col in (pg_schemas.update_column(tpl), pg_schemas.marker_column(tpl)):
            assert col in source, f"{tpl}: {col} is not a source column"
        # The mutation must target a column the PostgreSQL target also receives.
        assert pg_schemas.update_column(tpl) in pg_schemas.columns_of(tpl)


def test_report_deviations_names_every_recorded_difference():
    text = "\n".join(pg_schemas.report_deviations())
    assert "c_time TIME" in text
    assert "refuses TIME" in text
    assert "bytea" in text
    assert "timestamp(6) without time zone" in text
    assert "json (not jsonb)" in text
    # One markdown row per recorded difference, plus the two header lines.
    assert len(pg_schemas.report_deviations()) == len(pg_schemas.TYPE_DIFFERENCES) + 2


def test_unknown_template_is_rejected_everywhere():
    for call in (
        lambda: pg_schemas.dropped_columns("nope"),
        lambda: pg_schemas.columns_of("nope"),
        lambda: pg_schemas.kinds_of("nope"),
        lambda: pg_schemas.sink_types("nope"),
        lambda: pg_schemas.marker_column("nope"),
        lambda: pg_schemas.update_column("nope"),
        lambda: pg_schemas.create_table_sql("nope", "t"),
        lambda: pg_schemas.row_values("nope", 1, 1, 1),
    ):
        try:
            call()
        except ValueError:
            continue
        raise AssertionError(f"expected ValueError from {call}")
