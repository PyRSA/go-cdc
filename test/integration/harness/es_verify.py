"""Independent re-implementation of the Elasticsearch sink v1 write contract.

Every constant and rule here mirrors `connector/sink/elasticsearch`
(document.go / index.go / mapping.go). The tests need an independent oracle:
computing an expectation with the production code would make a wrong `_id`
look correct.

Mirrored rules:
  * index name: explicit `sink.index-name`, else `database.table` normalized
    (lowercase, invalid chars -> `_`, `-<12 hex of sha256(raw)>` when changed).
  * `_id`: sha256 over the routed `database.table` plus, for each final schema
    primary key in order, `uint32be(len(name)) name uint32be(len(value)) value`.
  * values: canonical text per MySQL column type (decimal keeps its scale,
    datetime is fixed-width, binary is base64, enum/set use labels).
  * mapping: the derived Elasticsearch field type per MySQL column type.
"""
from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import struct
import time
import zlib
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Iterable

INDEX_HASH_BYTES = 6  # 12 hex characters, same as Go
MAX_INDEX_NAME_BYTES = 255
DATE_FORMAT = "yyyy-MM-dd"
DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"
YEAR_FORMAT = "yyyy"
KEYWORD_MAX_CHARS = 512


# --------------------------------------------------------------------- types
@dataclass
class ColumnType:
    raw: str
    base: str
    unsigned: bool = False
    length: int = -1
    scale: int = -1
    fsp: int = 0
    values: list[str] = field(default_factory=list)


def parse_column_type(raw: str) -> ColumnType:
    """Parse information_schema.COLUMN_TYPE text exactly like parseColumnType."""
    out = ColumnType(raw=raw, base="", length=-1, scale=-1)
    trimmed = raw.strip()
    if not trimmed:
        raise ValueError("empty column type")
    open_at = trimmed.find("(")
    close_at = trimmed.rfind(")")
    if open_at < 0 and close_at < 0:
        fields = trimmed.split()
        out.base = fields[0].lower()
        for extra in fields[1:]:
            token = extra.lower()
            if token == "unsigned":
                out.unsigned = True
            elif token == "zerofill":
                continue
            else:
                raise ValueError(f"unsupported type modifier {extra!r} in {raw!r}")
        return out
    if open_at < 0 or close_at < open_at:
        raise ValueError(f"malformed type {raw!r}")
    out.base = trimmed[:open_at].strip().lower()
    for extra in trimmed[close_at + 1 :].split():
        token = extra.lower()
        if token == "unsigned":
            out.unsigned = True
        elif token == "zerofill":
            continue
        else:
            raise ValueError(f"unsupported type modifier {extra!r} in {raw!r}")
    params = trimmed[open_at + 1 : close_at]
    if out.base in ("enum", "set"):
        out.values = _parse_labels(params)
    elif out.base in ("datetime", "timestamp", "time"):
        if params.strip():
            out.fsp = int(params.strip())
    else:
        parts = params.split(",")
        try:
            out.length = int(parts[0].strip())
        except ValueError:
            out.length = -1
        if len(parts) > 1:
            try:
                out.scale = int(parts[1].strip())
            except ValueError:
                out.scale = -1
    return out


def _parse_labels(params: str) -> list[str]:
    labels: list[str] = []
    i = 0
    while i < len(params):
        while i < len(params) and params[i] in " ,":
            i += 1
        if i >= len(params):
            break
        if params[i] != "'":
            raise ValueError("malformed enum/set labels")
        i += 1
        buf: list[str] = []
        closed = False
        while i < len(params):
            ch = params[i]
            if ch == "\\":
                if i + 1 >= len(params):
                    raise ValueError("malformed enum/set escape")
                buf.append(params[i + 1])
                i += 2
            elif ch == "'":
                if i + 1 < len(params) and params[i + 1] == "'":
                    buf.append("'")
                    i += 2
                    continue
                i += 1
                closed = True
            else:
                buf.append(ch)
                i += 1
            if closed:
                break
        if not closed:
            raise ValueError("unterminated enum/set label")
        labels.append("".join(buf))
    if not labels:
        raise ValueError("empty enum/set definition")
    return labels


def value_kind(ct: ColumnType) -> str:
    base = ct.base
    if base in ("tinyint", "smallint", "mediumint", "int", "integer", "bigint", "bool", "boolean"):
        return "integer"
    if base in ("float", "double", "real"):
        return "float"
    if base in ("decimal", "numeric", "dec", "fixed"):
        return "decimal"
    if base in ("char", "varchar", "tinytext", "text", "mediumtext", "longtext"):
        return "string"
    if base in ("date", "datetime", "timestamp", "year"):
        return "date"
    if base == "time":
        return "time"
    if base == "json":
        return "json"
    if base in ("binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob"):
        return "binary"
    if base in ("enum", "set"):
        return "enum_set"
    return "string"


# ------------------------------------------------------------------- mapping
def derived_field(column_type: str) -> dict:
    """The derived mapping entry for one MySQL column type (fieldForColumn)."""
    ct = parse_column_type(column_type)
    base = ct.base
    if base in ("bool", "boolean"):
        return {"type": "byte"}
    if base == "tinyint":
        if ct.length == 1:
            return {"type": "byte"}
        return {"type": "short"} if ct.unsigned else {"type": "byte"}
    if base == "smallint":
        return {"type": "integer"} if ct.unsigned else {"type": "short"}
    if base == "mediumint":
        return {"type": "integer"}
    if base in ("int", "integer"):
        return {"type": "long"} if ct.unsigned else {"type": "integer"}
    if base == "bigint":
        return {"type": "unsigned_long"} if ct.unsigned else {"type": "long"}
    if base == "float":
        return {"type": "float"}
    if base in ("double", "real"):
        return {"type": "double"}
    if base in ("decimal", "numeric", "dec", "fixed"):
        return {"type": "keyword"}
    if base == "date":
        return {"type": "date", "format": DATE_FORMAT}
    if base in ("datetime", "timestamp"):
        return {"type": "date", "format": DATETIME_FORMAT}
    if base == "year":
        return {"type": "date", "format": YEAR_FORMAT}
    if base == "time":
        return {"type": "keyword"}
    if base in ("char", "varchar"):
        if 0 <= ct.length <= KEYWORD_MAX_CHARS:
            return {"type": "keyword"}
        return {"type": "text"}
    if base in ("tinytext", "text", "mediumtext", "longtext"):
        return {"type": "text"}
    if base == "json":
        return {"type": "object", "dynamic": True}
    if base in ("binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob"):
        return {"type": "binary"}
    if base in ("enum", "set"):
        return {"type": "keyword"}
    raise ValueError(f"column type {column_type!r} is not supported by the sink")


# --------------------------------------------------------------- index names
def _short_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[: INDEX_HASH_BYTES * 2]


def normalize_index_name(raw: str) -> str:
    """normalizeIndexName: byte-wise, so length limits match the Go code."""
    cleaned = bytearray()
    for byte in raw.encode("utf-8"):
        if 0x41 <= byte <= 0x5A:  # A-Z
            cleaned.append(byte - 0x41 + 0x61)
        elif 0x61 <= byte <= 0x7A or 0x30 <= byte <= 0x39 or byte in (0x2E, 0x5F, 0x2D):
            cleaned.append(byte)
        else:
            cleaned.append(0x5F)
    name = cleaned.decode("utf-8")
    if name == "":
        name = "cdc"
    if name[0] in ("_", "-", "+"):
        name = "cdc" + name
    if name != raw and raw != "":
        name += "-" + _short_hash(raw)
    if len(name.encode("utf-8")) > MAX_INDEX_NAME_BYTES:
        suffix = "-" + _short_hash(raw)
        keep = MAX_INDEX_NAME_BYTES - len(suffix)
        name = name.encode("utf-8")[:keep].decode("utf-8", "ignore") + suffix
    return name


def index_name_for(explicit: str, database: str, table: str) -> str:
    if explicit:
        return explicit
    return normalize_index_name(f"{database}.{table}")


# ------------------------------------------------------------------ id / docs
def _write_part(digest: "hashlib._Hash", value: str) -> None:
    raw = value.encode("utf-8")
    digest.update(struct.pack(">I", len(raw)))
    digest.update(raw)


def doc_id(scope: str, keys: list[tuple[str, str]]) -> str:
    """sha256 hex over the routed scope plus ordered (name, canonical) pairs."""
    digest = hashlib.sha256()
    _write_part(digest, scope)
    for name, value in keys:
        _write_part(digest, name)
        _write_part(digest, value)
    return digest.hexdigest()


def expected_doc_id(database: str, table: str, primary_keys: dict[str, str], row: dict) -> str:
    """Compute the sink `_id` for one MySQL row (case-insensitive lookup).

    `primary_keys` maps the final schema primary key names (in order) to their
    MySQL column types, because the canonical form depends on the type.
    """
    pairs: list[tuple[str, str]] = []
    for key, column_type in primary_keys.items():
        value = lookup_case(row, key)
        if value is None:
            raise ValueError(f"{database}.{table}: primary key {key!r} missing or NULL")
        pairs.append((key, canonical_text(column_type, value)))
    return doc_id(f"{database}.{table}", pairs)


def lookup_case(row: dict, name: str) -> Any:
    if name in row:
        return row[name]
    lowered = name.lower()
    for key, value in row.items():
        if str(key).lower() == lowered:
            return value
    return None


def canonical_text(column_type: str, value: Any) -> str:
    """Canonical text form used for both the document value and the _id hash.

    `column_type` is the MySQL COLUMN_TYPE of that column (the sink parses the
    same string). Values arrive from pymysql as int/Decimal/date/datetime/str/bytes.
    """
    if value is None:
        raise ValueError("NULL has no canonical text form")
    ct = parse_column_type(column_type)
    kind = value_kind(ct)
    if kind == "integer":
        return str(int(value))
    if kind == "decimal":
        return canonical_decimal(str(value), ct.scale)
    if kind == "binary":
        raw = value if isinstance(value, (bytes, bytearray)) else str(value).encode("utf-8")
        return base64.b64encode(bytes(raw)).decode("ascii")
    if kind == "date":
        return canonical_date(ct, value)
    if kind == "time":
        return canonical_time(value)
    if kind == "enum_set":
        return canonical_enum_set(ct, str(value))
    if kind == "float":
        return _canonical_float(value, 32 if ct.base == "float" else 64)
    if kind == "json":
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    return str(value)


def canonical_decimal(text: str, scale: int) -> str:
    value = text.strip()
    if not value:
        raise ValueError("empty decimal value")
    negative = value.startswith("-")
    if negative or value.startswith("+"):
        value = value[1:]
    if "." in value:
        int_part, frac_part = value.split(".", 1)
        has_fraction = True
    else:
        int_part, frac_part, has_fraction = value, "", False
    if not int_part:
        int_part = "0"
    if int_part.strip("0123456789") or frac_part.strip("0123456789"):
        raise ValueError(f"{text!r} is not a decimal number")
    int_part = int_part.lstrip("0") or "0"
    if scale >= 0:
        if len(frac_part) > scale:
            if frac_part[scale:].strip("0"):
                raise ValueError(f"{text!r} has more than {scale} fractional digits")
            frac_part = frac_part[:scale]
        frac_part += "0" * (scale - len(frac_part))
    else:
        frac_part = frac_part.rstrip("0")
    if int_part == "0" and not frac_part.strip("0"):
        negative = False
    out = int_part + ("." + frac_part if has_fraction or frac_part else "")
    return ("-" + out) if negative else out


def canonical_date(ct: ColumnType, value: Any) -> str:
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, dt.date):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    if ct.base == "date":
        return dt.date.fromisoformat(text[:10]).strftime("%Y-%m-%d")
    if ct.base == "year":
        return text[:4]
    parsed = dt.datetime.fromisoformat(text.replace("T", " ").replace("Z", ""))
    return parsed.strftime("%Y-%m-%d %H:%M:%S.%f")


def canonical_time(value: Any) -> str:
    """Mirror document.go canonicalTime: ``[-]H:MM:SS.ffffff`` (hours unpadded).

    pymysql returns MySQL TIME as a ``timedelta`` and Python normalizes negative
    values as ``-1 day + positive seconds + microseconds``: -01:02:03.000001 is
    ``days=-1, seconds=82676, microseconds=999999``. The sign and the fractional
    part therefore have to come from the same total; reading ``value.microseconds``
    directly yields 999999 for a value whose sub-second part is .000001.
    """
    if isinstance(value, dt.timedelta):
        total_us = (value.days * 86400 + value.seconds) * 1_000_000 + value.microseconds
        sign = "-" if total_us < 0 else ""
        total_seconds, micros = divmod(abs(total_us), 1_000_000)
        hours, rem = divmod(total_seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        if hours == 0 and minutes == 0 and seconds == 0 and micros == 0:
            sign = ""
        return f"{sign}{hours}:{minutes:02d}:{seconds:02d}.{micros:06d}"
    text = str(value).strip()
    sign = "-" if text.startswith("-") else ""
    if sign:
        text = text[1:]
    whole, _, frac = text.partition(".")
    hours, minutes, seconds = (int(part) for part in whole.split(":"))
    frac = (frac[:6] + "0" * 6)[:6]
    if hours == 0 and minutes == 0 and seconds == 0 and not frac.strip("0"):
        sign = ""
    return f"{sign}{hours}:{minutes:02d}:{seconds:02d}.{frac}"


def canonical_enum_set(ct: ColumnType, text: str) -> str:
    value = text.strip()
    if ct.base == "enum":
        try:
            index = int(value)
        except ValueError:
            if value in ct.values:
                return value
            raise ValueError(f"{text!r} is not a label of {ct.raw}") from None
        if index < 1 or index > len(ct.values):
            raise ValueError(f"enum index {index} is out of range for {ct.raw}")
        return ct.values[index - 1]
    if not value:
        return ""
    if "," in value:
        for label in value.split(","):
            if label not in ct.values:
                raise ValueError(f"{label!r} is not a member of {ct.raw}")
        return value
    if value in ct.values:
        return value
    mask = int(value)
    labels = [label for i, label in enumerate(ct.values) if mask & (1 << i)]
    return ",".join(labels)


def _shortest_float32(value: float) -> str:
    """Shortest decimal text that round-trips to the same float32.

    Mirrors Go's strconv.FormatFloat(f, 'g', -1, 32): Python's repr() is only
    shortest for float64, so the float32 case is searched digit by digit.
    """
    for digits in range(1, 10):
        text = f"%.{digits}g" % value
        if struct.unpack(">f", struct.pack(">f", float(text)))[0] == value:
            return text
    return repr(value)


def _canonical_float(value: Any, bits: int) -> str:
    number = float(value)
    if number != number or number in (float("inf"), float("-inf")):
        raise ValueError(f"{value!r} cannot be represented in JSON")
    if bits == 32:
        number = struct.unpack(">f", struct.pack(">f", number))[0]
        return _shortest_float32(number)
    text = repr(number)
    if text.endswith(".0"):
        text = text[:-2]
    return text


# ------------------------------------------------------------------ checksums
def stream_checksum(
    docs: Iterable[Any], *, id_field: str = "id", crc_field: str | None = None
) -> tuple[int, int, int]:
    """(count, sum(id), sum(crc32(crc_field))) over a stream of documents.

    `docs` is any iterable of objects exposing `.source` (an `es_client.Doc`) or
    plain `dict` sources. Streaming keeps deep-pagination checksums bounded:
    a 300k-document index is never materialised in memory.
    """
    count = 0
    total_ids = 0
    total_crc = 0
    for doc in docs:
        source = getattr(doc, "source", None)
        if source is None:
            source = doc
        count += 1
        value = lookup_case(source, id_field)
        if value is not None:
            total_ids += int(value)
        if crc_field:
            raw = lookup_case(source, crc_field)
            total_crc += zlib.crc32(("" if raw is None else str(raw)).encode("utf-8"))
    return (count, total_ids, total_crc)


def doc_checksum(docs: dict[str, dict], *, id_field: str = "id", crc_field: str | None = None) -> tuple[int, int, int]:
    """(count, sum(id), sum(crc32(crc_field))) over the scanned documents.

    The MySQL side computes the same triple with
    `COUNT(*), SUM(id), SUM(CRC32(col))`, so the comparison is content-based,
    not count-based.
    """
    return stream_checksum(docs.values(), id_field=id_field, crc_field=crc_field)


# --------------------------------------------------------------------- waits
def wait_for(
    predicate: Callable[[], bool],
    *,
    timeout: float,
    interval: float = 1.0,
    desc: str = "condition",
    on_poll: Callable[[float], None] | None = None,
    on_error: Callable[[BaseException], None] | None = None,
) -> float:
    """Bounded poll. Returns elapsed seconds; raises TimeoutError otherwise.

    Refresh must be driven by the caller inside `predicate` (or by ES itself),
    because only the caller knows which indices are involved.
    """
    started = time.time()
    deadline = started + timeout
    last_error: BaseException | None = None
    while time.time() < deadline:
        try:
            if predicate():
                return time.time() - started
        except Exception as exc:  # noqa: BLE001 - harness keeps polling until the deadline
            last_error = exc
            if on_error is not None:
                on_error(exc)
        if on_poll is not None:
            on_poll(time.time() - started)
        time.sleep(interval)
    detail = f": {last_error}" if last_error else ""
    raise TimeoutError(f"timeout waiting for {desc} after {time.time() - started:.1f}s{detail}")


# ------------------------------------------------- MySQL vs Elasticsearch
def _json_canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compare_value(column_type: str, mysql_value: Any, es_value: Any) -> bool:
    """Compare one MySQL column value with the value Elasticsearch returned.

    Elasticsearch hands back JSON scalars (numbers, strings, objects), while
    MySQL hands back pymysql types (int, Decimal, date, timedelta, str, bytes).
    Both sides are reduced to the sink's canonical form before comparing.
    """
    ct = parse_column_type(column_type)
    kind = value_kind(ct)
    if mysql_value is None or es_value is None:
        return mysql_value is None and es_value is None
    try:
        if kind == "integer":
            return int(mysql_value) == int(es_value)
        if kind == "decimal":
            return canonical_decimal(str(mysql_value), ct.scale) == str(es_value)
        if kind == "float":
            bits = 32 if ct.base == "float" else 64
            return _canonical_float(mysql_value, bits) == _canonical_float(es_value, bits)
        if kind == "binary":
            return canonical_text(column_type, mysql_value) == es_value
        if kind == "json":
            expected = mysql_value
            if isinstance(expected, (str, bytes, bytearray)):
                expected = json.loads(expected)
            return _json_canonical(expected) == _json_canonical(es_value)
        return canonical_text(column_type, mysql_value) == str(es_value)
    except (TypeError, ValueError):
        return False


def compare_row(columns: dict[str, str], mysql_row: dict, es_doc: dict) -> list[str]:
    """Mismatch descriptions for one projected row (empty list == equal)."""
    problems: list[str] = []
    extra = sorted(set(es_doc) - set(columns))
    if extra:
        problems.append(f"unexpected fields in _source: {extra}")
    for name, column_type in columns.items():
        if name not in es_doc:
            problems.append(f"{name}: missing from _source (mysql={lookup_case(mysql_row, name)!r})")
            continue
        mysql_value = lookup_case(mysql_row, name)
        if not compare_value(column_type, mysql_value, es_doc[name]):
            problems.append(f"{name}: mysql={mysql_value!r} es={es_doc[name]!r}")
    return problems


def mapping_properties(mapping: dict) -> dict:
    props = mapping.get("properties") or {}
    return props if isinstance(props, dict) else {}


def field_mapping(mapping: dict, name: str) -> dict:
    props = mapping_properties(mapping)
    if name in props:
        return props[name]
    for key, value in props.items():
        if str(key).lower() == name.lower():
            return value
    return {}


def mapping_problems(columns: dict[str, str], mapping: dict, *, allow_extra: bool = True) -> list[str]:
    """Mismatches between a stored index mapping and the derived field contract.

    Used by the load/scale suites to prove the index was created (or validated)
    with the types the sink must write, without trusting the sink's own check.
    """
    problems: list[str] = []
    if not mapping:
        return ["index has no mapping"]
    props = mapping_properties(mapping)
    if not props:
        return ["index mapping has no properties"]
    for name, column_type in columns.items():
        field = field_mapping(mapping, name)
        if not field:
            problems.append(f"{name}: missing from mapping")
            continue
        want = derived_field(column_type)
        got_type = str(field.get("type", ""))
        if got_type != want["type"]:
            problems.append(f"{name}: mapping type {got_type!r} != {want['type']!r}")
            continue
        if "format" in want and str(field.get("format", "")) != want["format"]:
            problems.append(
                f"{name}: mapping format {field.get('format')!r} != {want['format']!r}"
            )
    if not allow_extra:
        extra = sorted(set(props) - {name.lower() for name in columns} - set(columns))
        if extra:
            problems.append(f"unexpected mapping fields: {extra}")
    return problems
