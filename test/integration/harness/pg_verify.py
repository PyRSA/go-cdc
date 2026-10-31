"""PostgreSQL sink oracle: canonical values and row/PK comparison.

Deliberately stdlib-only. The MySQL and PostgreSQL drivers return different
Python shapes for the same logical value (``Decimal`` vs ``int``, ``str`` vs
``dict`` for ``JSON``, ``bytes`` vs ``memoryview``, padded ``CHAR``), so a raw
``==`` would produce false failures and hide real ones. Everything that decides
pass/fail goes through this module, and every rule is unit-tested in
``harness/test_pg_verify.py`` without a database.

Column kinds used by the suites:

    int | decimal | float | text | char | bytes | date | datetime | json | bool | raw
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence

# The suites assert in the pipeline's source time zone; an aware timestamp is
# converted here before its offset is dropped.
ASSERT_TZ = timezone(timedelta(hours=8))
# Relative tolerance for REAL/DOUBLE PRECISION. Both sides store IEEE floats, so
# the default is tight enough to catch a wrong column and loose enough to survive
# a different text-to-float path.
FLOAT_REL_TOL = 1e-12


class CompareError(ValueError):
    """A value cannot be canonicalized under the declared kind."""


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        dt = datetime.fromisoformat(value.strip())
    else:
        raise CompareError(f"not a datetime: {value!r}")
    if dt.tzinfo is not None:
        dt = dt.astimezone(ASSERT_TZ).replace(tzinfo=None)
    return dt


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value.strip())
    raise CompareError(f"not a date: {value!r}")


def _as_json_text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8")
    if isinstance(value, str):
        value = json.loads(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def canon(value: Any, kind: str) -> Any:
    """Canonical form of ``value`` under ``kind``; ``None`` stays ``None``."""
    if value is None:
        return None
    k = (kind or "raw").lower()
    if k in ("int", "bigint", "smallint", "integer"):
        if isinstance(value, Decimal):
            if value != value.to_integral_value():
                raise CompareError(f"{value!r} is not an integer")
            return int(value)
        if isinstance(value, float):
            if not value.is_integer():
                raise CompareError(f"{value!r} is not an integer")
            return int(value)
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, str):
            return int(value.strip())
        return int(value)
    if k in ("decimal", "numeric"):
        # Decimal equality is numeric, so NUMERIC(12,4) 1.5000 == 1.5.
        return Decimal(str(value).strip())
    if k in ("float", "real", "double"):
        return float(value)
    if k == "char":
        # PostgreSQL pads bpchar, MySQL strips CHAR on retrieval.
        return str(value).rstrip()
    if k == "text":
        return str(value)
    if k in ("bytes", "bytea", "blob"):
        if isinstance(value, memoryview):
            return bytes(value)
        if isinstance(value, bytearray):
            return bytes(value)
        if isinstance(value, str):
            return value.encode("utf-8")
        return bytes(value)
    if k == "date":
        return _as_date(value)
    if k in ("datetime", "timestamp"):
        return _as_datetime(value)
    if k == "json":
        return _as_json_text(value)
    if k in ("bool", "boolean"):
        if isinstance(value, str):
            return value.strip().lower() in ("1", "t", "true", "y", "yes")
        return bool(value)
    return value


def _equal(kind: str, expected: Any, actual: Any) -> bool:
    if expected is None or actual is None:
        return expected is actual
    k = (kind or "raw").lower()
    if k in ("float", "real", "double"):
        e, a = float(expected), float(actual)
        if e == a:
            return True
        return abs(e - a) <= FLOAT_REL_TOL * max(1.0, abs(e), abs(a))
    return expected == actual


def key_of(row: Mapping[str, Any], key_columns: Sequence[str], kinds: Mapping[str, str]) -> tuple:
    """Canonical primary-key tuple of ``row`` (raises when a key is missing)."""
    out = []
    for col in key_columns:
        if col not in row:
            raise CompareError(f"key column {col!r} missing from row {sorted(row)}")
        kind = kinds.get(col, "text")
        out.append(canon(row[col], kind))
    return tuple(out)


def pk_set(
    rows: Iterable[Mapping[str, Any]], key_columns: Sequence[str], kinds: Mapping[str, str]
) -> set[tuple]:
    return {key_of(r, key_columns, kinds) for r in rows}


def format_key(key: tuple, key_columns: Sequence[str]) -> str:
    parts = [f"{col}={value!r}" for col, value in zip(key_columns, key)]
    return "(" + ", ".join(parts) + ")"


def diff_rows(
    src_rows: Iterable[Mapping[str, Any]],
    snk_rows: Iterable[Mapping[str, Any]],
    columns: Sequence[str],
    kinds: Mapping[str, str],
    key_columns: Sequence[str],
) -> list[str]:
    """Row-set comparison by business key; returns human-readable failures.

    Order does not matter: rows are indexed by canonical key, so a duplicate key
    on the target (the classic replayed-INSERT symptom) is reported as well.
    """
    failures: list[str] = []
    src_by_key: dict[tuple, Mapping[str, Any]] = {}
    snk_by_key: dict[tuple, Mapping[str, Any]] = {}
    dup_snk: list[tuple] = []
    for row in src_rows:
        src_by_key[key_of(row, key_columns, kinds)] = row
    for row in snk_rows:
        key = key_of(row, key_columns, kinds)
        if key in snk_by_key:
            dup_snk.append(key)
        snk_by_key[key] = row

    missing = sorted(src_by_key.keys() - snk_by_key.keys(), key=repr)
    extra = sorted(snk_by_key.keys() - src_by_key.keys(), key=repr)
    for key in missing[:20]:
        failures.append(
            f"missing on target: {format_key(key, key_columns)} "
            f"src={ {c: src_by_key[key].get(c) for c in columns} }"
        )
    for key in extra[:20]:
        failures.append(
            f"extra on target: {format_key(key, key_columns)} "
            f"sink={ {c: snk_by_key[key].get(c) for c in columns} }"
        )
    for key in dup_snk[:20]:
        failures.append(f"duplicate key on target: {format_key(key, key_columns)}")
    if len(missing) > 20 or len(extra) > 20:
        failures.append(
            f"... {len(missing)} missing / {len(extra)} extra rows in total"
        )

    for key in sorted(src_by_key.keys() & snk_by_key.keys(), key=repr):
        src, snk = src_by_key[key], snk_by_key[key]
        for col in columns:
            if col not in snk:
                failures.append(
                    f"missing column {col!r} on target for {format_key(key, key_columns)}"
                )
                continue
            kind = kinds.get(col, "text")
            want = canon(src.get(col), kind)
            got = canon(snk.get(col), kind)
            if not _equal(kind, want, got):
                failures.append(
                    f"{format_key(key, key_columns)} column {col}: "
                    f"src={want!r} sink={got!r}"
                )
    return failures


def summarize(failures: Sequence[str], limit: int = 5) -> str:
    """One-line digest for logs, deterministic for the same failure set."""
    if not failures:
        return "ok"
    head = "; ".join(failures[:limit])
    more = f" (+{len(failures) - limit} more)" if len(failures) > limit else ""
    return head + more
