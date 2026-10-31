"""PostgreSQL-safe scale table templates and cross-engine aggregates.

Mirrors ``scale.schemas`` - the source is still MySQL, so the DDL, the row
generator and the insert batching stay the ones the MySQL->MySQL suite uses.
Exactly one column is not carried into a PostgreSQL run:

    ``wide.c_time TIME`` is dropped, because ``sink.postgresql`` refuses
    ``TIME`` (MySQL time spans negative and over-24-hour values that
    PostgreSQL ``time`` cannot hold).

The removal is derived from ``scale.schemas`` instead of a copied template, and
it is asserted: ``create_table_sql`` fails when the column it expects to drop is
gone (an upstream template change must not slip through as a silent difference).
Template names and per-template row weights are untouched, so ``profiles.py``
allocates exactly the same rows per table as the MySQL->MySQL suite.

Aggregates are limited to what both engines compute identically on this data -
``COUNT(*)``, ``SUM(id)`` and a character-length sum of a stable string column.
No content hash is compared across engines (MySQL ``CRC32`` has no PostgreSQL
equivalent, and a concatenation hash would depend on each engine's text
rendering). Content is decided by the row-level oracle in ``harness.pg_verify``.
"""
from __future__ import annotations

from common import schemas as base
from common.profiles import TEMPLATES

CHUNK_SIZE = base.CHUNK_SIZE

# template -> columns the PostgreSQL sink cannot receive
SINK_DROPPED_COLUMNS: dict[str, tuple[str, ...]] = {"wide": ("c_time",)}

# Why each dropped column is dropped, rendered into every scale report.
DROPPED_COLUMN_REASONS: dict[str, str] = {
    "c_time": "sink.postgresql refuses TIME (MySQL time spans negative and "
              "over-24-hour values PostgreSQL time cannot hold)",
}

# Documented mapping differences this suite exercises (docs/connectors/
# postgresql-sink.md "Column Type Mapping"), listed in every report so a
# type difference is never silent.
TYPE_DIFFERENCES: tuple[tuple[str, str, str, str], ...] = (
    ("common/schemas.py wide", "c_time TIME", "dropped", DROPPED_COLUMN_REASONS["c_time"]),
    ("all templates", "BIGINT", "bigint", "same range, integer compare"),
    ("wide/indexed", "TINYINT", "smallint", "no boolean is implied"),
    ("basic/indexed", "DECIMAL(p,s)", "numeric(p,s)", "numeric compare, same digits"),
    ("wide", "DOUBLE", "double precision", "float compare within 1e-12"),
    ("basic/indexed/wide/large", "DATETIME(6)", "timestamp(6) without time zone",
     "naive wall clock of source.server-time-zone"),
    ("wide", "TIMESTAMP (fsp 0)", "timestamp without time zone",
     "MySQL truncates to the second before the binlog carries it"),
    ("large", "BLOB", "bytea", "bytes compare, no hex round-trip"),
    ("wide/large", "JSON", "json (not jsonb)", "the document text is stored verbatim"),
)

# Stable string column per template, used by the character-length aggregate.
CHECKSUM_COLUMN = {
    "basic": "name",
    "wide": "c_label",
    "indexed": "external_id",
    "large": "name",
}

# Column the mutation phase rewrites, identical to ``scale.scale_it._update_rows``
# so a PostgreSQL run mutates the same column with the same values as the
# MySQL->MySQL run. ``indexed`` deliberately differs from CHECKSUM_COLUMN
# (``status`` is mutated, ``external_id`` stays the length marker).
UPDATE_COLUMN = {
    "basic": "name",
    "wide": "c_label",
    "indexed": "status",
    "large": "name",
}

# Canonical kinds for harness.pg_verify.diff_rows (column -> kind).
_KINDS: dict[str, dict[str, str]] = {
    "basic": {
        "id": "int", "tenant_id": "int", "name": "text", "status": "int",
        "amount": "decimal", "description": "text",
        "created_at": "datetime", "updated_at": "datetime",
    },
    "wide": {
        "id": "int", "c_bigint": "int", "c_int": "int", "c_tinyint": "int",
        "c_decimal": "decimal", "c_varchar": "text", "c_text": "text",
        "c_date": "date", "c_datetime": "datetime", "c_timestamp": "datetime",
        "c_json": "json", "c_flag": "int", "c_ref_id": "int", "c_code": "text",
        "c_note": "text", "c_amount": "decimal", "c_qty": "int",
        "c_score": "float", "c_meta": "json", "c_label": "text",
        "created_at": "datetime", "updated_at": "datetime",
    },
    "indexed": {
        "id": "int", "tenant_id": "int", "user_id": "int", "external_id": "text",
        "status": "int", "amount": "decimal",
        "created_at": "datetime", "updated_at": "datetime",
    },
    "large": {
        "id": "int", "name": "text", "content": "text", "json_data": "json",
        "binary_data": "bytes", "created_at": "datetime", "updated_at": "datetime",
    },
}

# Expected pg_catalog format_type per column, asserted after auto-create-table.
_SINK_TYPES: dict[str, dict[str, str]] = {
    "basic": {
        "id": "bigint", "tenant_id": "integer",
        "name": "character varying(128)", "status": "smallint",
        "amount": "numeric(18,4)", "description": "character varying(512)",
        "created_at": "timestamp(6) without time zone",
        "updated_at": "timestamp(6) without time zone",
    },
    "wide": {
        "id": "bigint", "c_bigint": "bigint", "c_int": "integer",
        "c_tinyint": "smallint", "c_decimal": "numeric(18,4)",
        "c_varchar": "character varying(256)", "c_text": "text",
        "c_date": "date", "c_datetime": "timestamp(6) without time zone",
        "c_timestamp": "timestamp without time zone", "c_json": "json",
        "c_flag": "smallint", "c_ref_id": "bigint",
        "c_code": "character varying(64)", "c_note": "text",
        "c_amount": "numeric(10,2)", "c_qty": "integer",
        "c_score": "double precision", "c_meta": "json",
        "c_label": "character varying(128)",
        "created_at": "timestamp(6) without time zone",
        "updated_at": "timestamp(6) without time zone",
    },
    "indexed": {
        "id": "bigint", "tenant_id": "bigint", "user_id": "bigint",
        "external_id": "character varying(128)", "status": "smallint",
        "amount": "numeric(18,4)",
        "created_at": "timestamp(6) without time zone",
        "updated_at": "timestamp(6) without time zone",
    },
    "large": {
        "id": "bigint", "name": "character varying(128)", "content": "text",
        "json_data": "json", "binary_data": "bytea",
        "created_at": "timestamp(6) without time zone",
        "updated_at": "timestamp(6) without time zone",
    },
}


def _check_template(template: str) -> None:
    if template not in TEMPLATES:
        raise ValueError(f"unknown template: {template!r}")


def dropped_columns(template: str) -> tuple[str, ...]:
    _check_template(template)
    return SINK_DROPPED_COLUMNS.get(template, ())


def columns_of(template: str) -> tuple[str, ...]:
    """Source columns the PostgreSQL target receives, in source order."""
    _check_template(template)
    dropped = dropped_columns(template)
    return tuple(c for c in base.table_columns(template) if c not in dropped)


def kinds_of(template: str) -> dict[str, str]:
    """Column -> canonical kind, in the order the target stores them."""
    _check_template(template)
    kinds = _KINDS[template]
    return {c: kinds[c] for c in columns_of(template)}


def sink_types(template: str) -> dict[str, str]:
    """Column -> expected pg_catalog format_type, in target column order."""
    _check_template(template)
    expected = _SINK_TYPES[template]
    return {c: expected[c] for c in columns_of(template)}


def marker_column(template: str) -> str:
    """Column whose character length the aggregate sums."""
    _check_template(template)
    return CHECKSUM_COLUMN[template]


def update_column(template: str) -> str:
    """Column the mutation phase rewrites (``scale.scale_it`` parity)."""
    _check_template(template)
    return UPDATE_COLUMN[template]


def create_table_sql(template: str, table: str) -> str:
    """Source DDL with the columns the PostgreSQL sink refuses removed."""
    _check_template(template)
    ddl = base.create_table_sql(template, table)
    dropped = dropped_columns(template)
    if not dropped:
        return ddl
    lines: list[str] = []
    removed: list[str] = []
    for line in ddl.splitlines():
        stripped = line.strip()
        if stripped.startswith("`"):
            name = stripped[1:stripped.index("`", 1)]
            if name in dropped:
                removed.append(name)
                continue
        lines.append(line)
    if sorted(removed) != sorted(dropped):
        raise ValueError(
            f"template {template!r}: expected to drop {list(dropped)} from the source "
            f"DDL but found {removed}; common/schemas.py changed"
        )
    return "\n".join(_trim_trailing_comma(lines))


def _trim_trailing_comma(lines: list[str]) -> list[str]:
    """Drop a trailing comma left on the last column definition by a removal."""
    for index, line in enumerate(lines):
        if line.strip().startswith(")"):
            previous = index - 1
            while previous >= 0 and not lines[previous].strip():
                previous -= 1
            if previous >= 0 and lines[previous].rstrip().endswith(","):
                lines[previous] = lines[previous].rstrip()[:-1]
            break
    return lines


def row_values(template: str, row_id: int, db_index: int, ordinal: int) -> tuple:
    """The shared generator's row, minus the columns the sink refuses.

    ``scale.schemas`` stays the single source of truth for the generated values;
    the private accessor is pinned by ``test_pg_schemas.py`` so a rename fails
    loudly instead of silently diverging from the MySQL->MySQL data model.
    """
    _check_template(template)
    full = base._row(template, row_id, db_index, ordinal)  # noqa: SLF001 - see above
    all_columns = list(base.table_columns(template))
    if len(full) != len(all_columns):
        raise ValueError(
            f"template {template!r}: generator produced {len(full)} values for "
            f"{len(all_columns)} columns; common/schemas.py changed"
        )
    dropped = set(dropped_columns(template))
    return tuple(v for c, v in zip(all_columns, full) if c not in dropped)


def insert_sql(template: str, db: str, table: str) -> str:
    columns = columns_of(template)
    names = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    return f"INSERT INTO `{db}`.`{table}` ({names}) VALUES ({placeholders})"


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
    """Insert ids [id_start, id_end] in chunks of CHUNK_SIZE (MySQL source side)."""
    _check_template(template)
    if id_end < id_start:
        return
    sql = insert_sql(template, db, table)
    row_id = id_start
    while row_id <= id_end:
        chunk_end = min(row_id + CHUNK_SIZE - 1, id_end)
        rows = [
            row_values(template, i, db_index, ordinal)
            for i in range(row_id, chunk_end + 1)
        ]
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        row_id = chunk_end + 1
    conn.commit()


def checksum_sql(template: str, db: str, table: str, *, dialect: str = "mysql") -> str:
    """Three-part aggregate labelled cnt / sum_id / content_len.

    ``CHAR_LENGTH`` (MySQL, characters) and ``length`` (PostgreSQL, characters)
    agree on the generated data because both count characters, not bytes.
    """
    _check_template(template)
    col = marker_column(template)
    if dialect == "mysql":
        return (
            f"SELECT COUNT(*) AS cnt, COALESCE(SUM(`id`), 0) AS sum_id, "
            f"COALESCE(SUM(CHAR_LENGTH(`{col}`)), 0) AS content_len "
            f"FROM `{db}`.`{table}`"
        )
    if dialect == "pg":
        return (
            f'SELECT COUNT(*) AS cnt, COALESCE(SUM("id"), 0) AS sum_id, '
            f'COALESCE(SUM(length("{col}")), 0) AS content_len '
            f'FROM "{db}"."{table}"'
        )
    raise ValueError(f"unknown dialect: {dialect!r}")


def select_rows_sql(template: str, db: str, table: str, *, dialect: str = "mysql") -> str:
    """Row projection used by the chunked row-level comparison (no WHERE)."""
    _check_template(template)
    columns = columns_of(template)
    if dialect == "mysql":
        names = ", ".join(f"`{c}`" for c in columns)
        return f"SELECT {names} FROM `{db}`.`{table}`"
    if dialect == "pg":
        names = ", ".join(f'"{c}"' for c in columns)
        return f'SELECT {names} FROM "{db}"."{table}"'
    raise ValueError(f"unknown dialect: {dialect!r}")


def report_deviations() -> list[str]:
    """Markdown lines describing every template/type difference."""
    lines = [
        "| Scope | MySQL source | PostgreSQL target | Why |",
        "| --- | --- | --- | --- |",
    ]
    for scope, mysql_side, pg_side, reason in TYPE_DIFFERENCES:
        lines.append(f"| `{scope}` | {mysql_side} | {pg_side} | {reason} |")
    return lines
