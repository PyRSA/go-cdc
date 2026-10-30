"""Thin psycopg2 wrapper for the PostgreSQL sink suites.

psycopg2 is imported lazily so the offline parts of the suite (canonicalization
tests, YAML rendering, report writing) still run on a machine without the
driver. ``DSN`` honours a full ``PG_DSN`` override for CI and for the version
matrix, otherwise it is built from the ``PG_*`` settings in ``config``.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

from . import config as cfg


class DriverMissing(RuntimeError):
    """psycopg2 is not installed; integration runs cannot start."""


def _driver():
    try:
        import psycopg2  # noqa: PLC0415 - lazy on purpose
        import psycopg2.extras  # noqa: F401,PLC0415
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise DriverMissing(
            "psycopg2 is not installed; run "
            "`python3 -m pip install -r test/integration/harness/requirements.txt`"
        ) from exc
    return psycopg2


def dsn(database: str | None = None) -> str:
    """libpq DSN for the target database (sink database unless overridden)."""
    override = cfg.env("PG_DSN", "")
    if override and database is None:
        return override
    parts = [
        f"host={cfg.PG_HOST}",
        f"port={cfg.PG_PORT}",
        f"user={cfg.PG_USER}",
        f"password={cfg.PG_PASSWORD}",
        f"dbname={database or cfg.PG_DB}",
        f"sslmode={cfg.PG_SSLMODE}",
        "connect_timeout=10",
    ]
    return " ".join(parts)


def connect(database: str | None = None, autocommit: bool = True):
    psycopg2 = _driver()
    conn = psycopg2.connect(dsn(database))
    conn.autocommit = autocommit
    return conn


def q(sql: str, args: Sequence[Any] | None = None) -> list[dict]:
    """Run ``sql`` and return rows as dicts (RealDictCursor)."""
    psycopg2 = _driver()
    conn = psycopg2.connect(dsn())
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args or ())
            rows = list(cur.fetchall()) if cur.description else []
        return [dict(r) for r in rows]
    finally:
        conn.close()


def exec_sql(sql: str, args: Sequence[Any] | None = None) -> None:
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args or ())
    finally:
        conn.close()


def ping() -> bool:
    try:
        q("SELECT 1 AS ok")
        return True
    except Exception:  # noqa: BLE001 - any failure means "not reachable"
        return False


def server_version() -> str:
    row = q("SHOW server_version")[0]
    return str(row["server_version"])


def server_version_num() -> int:
    row = q("SHOW server_version_num")[0]
    return int(row["server_version_num"])


def major_version() -> int:
    return server_version_num() // 10000


def ensure_database() -> None:
    """Create the target database when the server already exists without it.

    The compose service creates it through POSTGRES_DB; a hand-managed server
    may not. Never used to create anything the sink itself must create.
    """
    psycopg2 = _driver()
    try:
        conn = psycopg2.connect(dsn(cfg.PG_DB))
        conn.close()
        return
    except psycopg2.OperationalError as exc:
        if "does not exist" not in str(exc):
            raise
    admin = connect(database="postgres")
    try:
        with admin.cursor() as cur:
            cur.execute(f'CREATE DATABASE "{cfg.PG_DB}"')
    finally:
        admin.close()


def create_schema(schema: str) -> None:
    exec_sql(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')


def drop_schema(schema: str) -> None:
    exec_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def drop_schemas(schemas: Iterable[str]) -> None:
    for name in schemas:
        drop_schema(name)


def table_exists(schema: str, table: str) -> bool:
    rows = q(
        "SELECT 1 AS ok FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return bool(rows)


def table_columns(schema: str, table: str) -> dict[str, str]:
    """Ordered ``{column: format_type}`` of a target table (empty when missing)."""
    rows = q(
        "SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type "
        "FROM pg_catalog.pg_attribute a "
        "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped "
        "ORDER BY a.attnum",
        (schema, table),
    )
    return {str(r["name"]): str(r["type"]) for r in rows}


def primary_keys(schema: str, table: str) -> list[str]:
    """Primary-key columns of a target table ([] when it has none)."""
    rows = q(
        "SELECT a.attname AS name FROM pg_catalog.pg_index i "
        "JOIN pg_catalog.pg_class c ON c.oid = i.indrelid "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey) "
        "WHERE n.nspname = %s AND c.relname = %s AND i.indisprimary "
        "ORDER BY array_position(i.indkey, a.attnum)",
        (schema, table),
    )
    return [str(r["name"]) for r in rows]


def dump_table(schema: str, table: str, order: str) -> list[dict]:
    """All rows of ``schema.table`` ordered by ``order`` (empty when missing)."""
    if not table_exists(schema, table):
        return []
    return q(f'SELECT * FROM "{schema}"."{table}" ORDER BY {order}')


def row_count(schema: str, table: str) -> int:
    if not table_exists(schema, table):
        return 0
    return int(q(f'SELECT count(*) AS n FROM "{schema}"."{table}"')[0]["n"])


def schemas() -> list[str]:
    rows = q("SELECT nspname AS name FROM pg_catalog.pg_namespace ORDER BY 1")
    return [str(r["name"]) for r in rows]


def tables(schema: str) -> list[str]:
    rows = q(
        "SELECT table_name AS name FROM information_schema.tables "
        "WHERE table_schema = %s ORDER BY 1",
        (schema,),
    )
    return [str(r["name"]) for r in rows]


def columns_of_rows(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    """Column order of a result set (first row); used for report headers."""
    return list(rows[0].keys()) if rows else []


# ------------------------------------------------------- fault injection tools
#
# Everything below only ever touches the disposable integration cluster, runs
# as the superuser the compose service hands out, and is expected to be undone
# by the caller in a `finally` block. They exist so the E2E suite can produce
# the SQLSTATEs docs/connectors/postgresql-sink.md names retryable (57P01,
# 57P03, 08xxx) and permanent (57014, 28000) without stopping a container.


def client_backends(user: str | None = None, exclude: Iterable[int] = ()) -> list[int]:
    """PIDs of client backends on the target database, excluding this session."""
    sql = (
        "SELECT pid FROM pg_catalog.pg_stat_activity "
        "WHERE backend_type = 'client backend' AND datname = %s "
        "AND pid <> pg_backend_pid()"
    )
    args: list[Any] = [cfg.PG_DB]
    if user:
        sql += " AND usename = %s"
        args.append(user)
    skip = set(exclude)
    return [int(r["pid"]) for r in q(sql, tuple(args)) if int(r["pid"]) not in skip]


def terminate_backends(user: str | None = None, exclude: Iterable[int] = ()) -> int:
    """Kill the sink's backends: the next statement fails with 57P01."""
    pids = client_backends(user, exclude)
    for pid in pids:
        q("SELECT pg_terminate_backend(%s)", (pid,))
    return len(pids)


def cancel_backends(user: str | None = None, exclude: Iterable[int] = ()) -> int:
    """Cancel the running statement of the sink's backends: 57014."""
    pids = client_backends(user, exclude)
    cancelled = 0
    for pid in pids:
        row = q("SELECT pg_cancel_backend(%s) AS ok", (pid,))[0]
        cancelled += 1 if row["ok"] else 0
    return cancelled


def backend_pid(conn) -> int:
    """Session PID of an open connection (so a fault injector can spare it)."""
    with conn.cursor() as cur:
        cur.execute("SELECT pg_backend_pid() AS pid")
        return int(cur.fetchone()[0])


def set_database_statement_timeout(value: str) -> None:
    """Database-wide ``statement_timeout``; applies to connections opened after.

    The sink keeps one pooled connection for its lifetime, so a caller that
    wants the new limit to take effect must also terminate that backend.
    """
    exec_sql(f'ALTER DATABASE "{cfg.PG_DB}" SET statement_timeout = %s', (value,))


def reset_database_statement_timeout() -> None:
    exec_sql(f'ALTER DATABASE "{cfg.PG_DB}" RESET statement_timeout')


def create_login_role(role: str, password: str) -> None:
    """Non-superuser sink role: no bypass of CONNECT privileges."""
    rows = q("SELECT 1 AS ok FROM pg_catalog.pg_roles WHERE rolname = %s", (role,))
    if rows:
        exec_sql(f'ALTER ROLE "{role}" WITH LOGIN PASSWORD %s', (password,))
        return
    exec_sql(f'CREATE ROLE "{role}" WITH LOGIN PASSWORD %s', (password,))


def set_role_login(role: str, enabled: bool) -> None:
    """Refuse or allow authentication for a role: an unreachable-target fault.

    `REVOKE CONNECT` cannot do this on a stock cluster, because the PUBLIC grant
    on every database keeps CONNECT available to every role.
    """
    exec_sql(f'ALTER ROLE "{role}" {"LOGIN" if enabled else "NOLOGIN"}')


def drop_login_role(role: str) -> None:
    exec_sql(f'DROP ROLE IF EXISTS "{role}"')


def grant_database(role: str, privileges: str = "CONNECT, CREATE") -> None:
    exec_sql(f'GRANT {privileges} ON DATABASE "{cfg.PG_DB}" TO "{role}"')


def revoke_database(role: str, privileges: str = "CONNECT, CREATE") -> None:
    exec_sql(f'REVOKE {privileges} ON DATABASE "{cfg.PG_DB}" FROM "{role}"')


def statement_timeout_of(database: str) -> str:
    rows = q(
        "SELECT COALESCE(s.setconfig, '{}') AS cfg FROM pg_catalog.pg_db_role_setting s "
        "JOIN pg_catalog.pg_database d ON d.oid = s.setdatabase "
        "WHERE d.datname = %s AND s.setrole = 0",
        (database,),
    )
    return str(rows[0]["cfg"]) if rows else ""


def error_sqlstate(exc: BaseException) -> str:
    """SQLSTATE of a driver exception ('' when the server supplied none)."""
    return str(getattr(exc, "pgcode", "") or "")
