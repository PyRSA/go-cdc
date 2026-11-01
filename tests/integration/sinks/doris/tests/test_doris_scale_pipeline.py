"""Offline checks for the Doris scale pipeline rendering (no MySQL / Doris needed).

These guard the sink block: without replication-num the Doris FE applies its own
default of 3 replicas, which a single-BE CI cluster rejects at table creation.
"""

from ..scale import doris_scale_it as dsi
from common.profiles import load_profile


def _render(tmp_path, name: str = "normal") -> str:
    profile = dsi.doris_profile(load_profile(name))
    path = tmp_path / f"pipeline.doris-scale-{profile.name}.yaml"
    dsi.render_pipeline_yaml(profile, path)
    return path.read_text(encoding="utf-8")


def test_sink_block_carries_replication_num(tmp_path):
    text = _render(tmp_path)
    assert f"replication-num: {dsi.DORIS_REPLICATION_NUM}" in text
    assert "auto-create-table: true" in text


def test_sink_block_keeps_batch_knobs(tmp_path):
    text = _render(tmp_path)
    for line in (
        "  write-batch-size: 1000",
        "  write-batch-interval: 500ms",
        "  max-retries: 3",
        "  request-timeout: 60s",
    ):
        assert line in text


def test_doris_profile_remaps_delete_groups():
    profile = dsi.doris_profile(load_profile("heavy"))
    groups = {t.event_group for t in profile.tables}
    assert "delete" not in groups
    assert len(profile.tables) == 50


def test_route_uses_doris_scale_databases(tmp_path):
    text = _render(tmp_path)
    assert "sink-table: db_tp_dscale_snk_01.tp_scale_001_out" in text
    assert "- source-table: 'db_tp_dscale_src_01\\.tp_scale_001'" in text
    # Doris scale never shares databases with the MySQL→MySQL scale suite.
    assert "db_tp_scale_src" not in text
