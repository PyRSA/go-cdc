# test/integration/scale/test_profiles.py
import pytest

from scale.profiles import load_profile, NORMAL_DB_TABLES, HEAVY_DB_TABLES


def test_normal_topology():
    p = load_profile("normal")
    assert p.workers == 4
    assert len(p.tables) == 20
    assert sum(NORMAL_DB_TABLES) == 20
    assert abs(p.total_rows - 50000) <= 20  # allow tiny rounding
    assert sum(t.rows for t in p.tables) == p.total_rows
    assert all(t.rows >= 1 for t in p.tables)
    assert p.tables[0].name == "tp_scale_001"
    assert p.tables[0].src_db == "db_tp_scale_src_01"
    assert p.tables[0].sink_name == "tp_scale_001_out"


def test_heavy_defaults():
    p = load_profile("heavy")
    assert p.workers == 8
    assert len(p.tables) == 50
    assert sum(HEAVY_DB_TABLES) == 50
    assert p.total_rows == 300000
    assert sum(t.rows for t in p.tables) == 300000
    assert all(t.rows >= 1 for t in p.tables)


def test_heavy_override_rows_and_workers():
    p = load_profile("heavy", total_rows_override=200000, workers_override=6)
    assert p.total_rows == 200000
    assert p.workers == 6
    assert sum(t.rows for t in p.tables) == 200000
    assert all(t.rows >= 1 for t in p.tables)


def test_tiny_total_rows_override_raises():
    # normal profile has 20 tables; fewer rows than tables must fail clearly
    with pytest.raises(ValueError, match=r"total_rows \(19\) must be >= number of tables \(20\)"):
        load_profile("normal", total_rows_override=19)


def test_min_total_rows_keeps_every_table_at_least_one():
    p = load_profile("normal", total_rows_override=20)
    assert sum(t.rows for t in p.tables) == 20
    assert all(t.rows >= 1 for t in p.tables)


def test_template_rotation_and_event_groups():
    p = load_profile("normal")
    assert p.tables[0].template == "basic"
    assert p.tables[1].template == "wide"
    assert p.tables[2].template == "indexed"
    assert p.tables[3].template == "large"
    groups = {t.event_group for t in p.tables}
    assert groups == {"insert", "update", "delete", "insert_update", "idle"}
