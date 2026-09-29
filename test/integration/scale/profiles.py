from __future__ import annotations

from dataclasses import dataclass

TEMPLATES = ("basic", "wide", "indexed", "large")
NORMAL_DB_TABLES = (8, 5, 3, 2, 2)
HEAVY_DB_TABLES = (15, 10, 7, 6, 4, 3, 2, 1, 1, 1)

_EVENT_GROUPS = ("insert", "update", "delete", "insert_update", "idle")
_TEMPLATE_WEIGHT = {"basic": 4, "wide": 2, "indexed": 2, "large": 1}

_PROFILE_DEFAULTS = {
    "normal": {"total_rows": 50_000, "workers": 4, "db_tables": NORMAL_DB_TABLES},
    "heavy": {"total_rows": 300_000, "workers": 8, "db_tables": HEAVY_DB_TABLES},
}


@dataclass
class TableSpec:
    ordinal: int
    db_index: int
    src_db: str
    snk_db: str
    name: str
    sink_name: str
    template: str
    rows: int
    event_group: str


@dataclass
class Profile:
    name: str
    workers: int
    total_rows: int
    tables: list[TableSpec]


def src_db_name(i: int) -> str:
    return f"db_tp_scale_src_{i:02d}"


def snk_db_name(i: int) -> str:
    return f"db_tp_scale_snk_{i:02d}"


def _allocate_rows(templates: list[str], total_rows: int) -> list[int]:
    n = len(templates)
    if total_rows < n:
        raise ValueError(
            f"total_rows ({total_rows}) must be >= number of tables ({n})"
        )
    raw = [_TEMPLATE_WEIGHT[t] for t in templates]
    total_raw = sum(raw)
    rows = [max(1, round(w / total_raw * total_rows)) for w in raw]
    diff = total_rows - sum(rows)
    if diff != 0:
        last_basic = max(i for i, t in enumerate(templates) if t == "basic")
        rows[last_basic] += diff
    # Remainder adjustment can drive one table below 1; borrow from surplus.
    for i, count in enumerate(rows):
        if count >= 1:
            continue
        need = 1 - count
        rows[i] = 1
        for j, donor in enumerate(rows):
            if j == i or donor <= 1:
                continue
            take = min(need, donor - 1)
            rows[j] -= take
            need -= take
            if need == 0:
                break
        if need > 0:
            raise ValueError(
                f"cannot keep every table >= 1 row when allocating "
                f"{total_rows} rows across {n} tables"
            )
    return rows


def _build_tables(db_tables: tuple[int, ...], total_rows: int) -> list[TableSpec]:
    meta: list[tuple[int, int, str, str]] = []
    ordinal = 0
    for db_index, count in enumerate(db_tables, start=1):
        for _ in range(count):
            ordinal += 1
            template = TEMPLATES[(ordinal - 1) % len(TEMPLATES)]
            event_group = _EVENT_GROUPS[(ordinal - 1) % len(_EVENT_GROUPS)]
            meta.append((ordinal, db_index, template, event_group))

    templates = [m[2] for m in meta]
    row_counts = _allocate_rows(templates, total_rows)

    tables: list[TableSpec] = []
    for (ordinal, db_index, template, event_group), rows in zip(meta, row_counts):
        name = f"tp_scale_{ordinal:03d}"
        tables.append(
            TableSpec(
                ordinal=ordinal,
                db_index=db_index,
                src_db=src_db_name(db_index),
                snk_db=snk_db_name(db_index),
                name=name,
                sink_name=f"{name}_out",
                template=template,
                rows=rows,
                event_group=event_group,
            )
        )
    return tables


def load_profile(
    name: str,
    total_rows_override: int | None = None,
    workers_override: int | None = None,
) -> Profile:
    if name not in _PROFILE_DEFAULTS:
        raise ValueError(f"unknown profile: {name!r}")
    defaults = _PROFILE_DEFAULTS[name]
    total_rows = (
        total_rows_override if total_rows_override is not None else defaults["total_rows"]
    )
    workers = workers_override if workers_override is not None else defaults["workers"]
    tables = _build_tables(defaults["db_tables"], total_rows)
    return Profile(name=name, workers=workers, total_rows=total_rows, tables=tables)
