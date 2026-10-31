"""Offline checks for the PostgreSQL scale orchestrator (no MySQL, no PostgreSQL).

Two requirements are pinned here because a live run reports them only after the
fact: the normal and heavy profiles must not share a target schema, a checkpoint
directory, a pipeline name or a MySQL server id (B3), and the rendered route /
sink block must address only this suite's own databases. The load model
(50 000-row burst, 45 s peak, batch 200, 8 writers) is asserted unchanged so a
PostgreSQL port can never quietly re-tune the historical numbers.
"""
from common import config as cfg
from ..scale import pg_scale_it as psi
from common import schemas as base
from common.profiles import load_profile


def _render(tmp_path, name: str = "normal") -> tuple[str, object]:
    profile = psi.pg_scale_profile(load_profile(name))
    path = tmp_path / f"pipeline.pg-scale-{profile.name}.yaml"
    psi.render_pipeline_yaml(profile, path)
    return path.read_text(encoding="utf-8"), profile


# ------------------------------------------------------------------ route/sink
def test_route_addresses_the_postgresql_scale_databases(tmp_path):
    text, profile = _render(tmp_path)
    assert "  - source-table: 'db_tp_pg_scale_src_01\\.tp_scale_001'" in text
    assert "    sink-table: tp_pg_scale_normal_01.tp_scale_001_out" in text
    # Never the MySQL->MySQL / Doris / Elasticsearch scale databases.
    for foreign in ("db_tp_scale_src", "db_tp_scale_snk", "db_tp_dscale_src", "db_tp_es"):
        assert foreign not in text, foreign
    # One route per table, and every target schema belongs to this profile.
    assert text.count("    sink-table: tp_pg_scale_normal_") == len(profile.tables)
    for spec in profile.tables:
        assert f"    sink-table: {spec.snk_db}.{spec.sink_name}" in text
        assert spec.snk_db.startswith("tp_pg_scale_normal_")


def test_sink_schema_is_the_first_source_database_schema(tmp_path):
    text, profile = _render(tmp_path)
    assert "  schema: tp_pg_scale_normal_01" in text
    assert psi.sink_schema("normal", 1) == "tp_pg_scale_normal_01"
    first = min(profile.tables, key=lambda t: t.ordinal)
    assert f"    sink-table: {first.snk_db}.{first.sink_name}" in text
    # sslmode / database are the ones the functional suite proved.
    assert f"  database: {cfg.PG_DB}" in text
    assert f"  sslmode: {cfg.PG_SSLMODE}" in text


def test_sink_and_source_knobs_are_the_ones_the_scale_model_uses(tmp_path):
    text, profile = _render(tmp_path)
    for line in (
        "  auto-create-table: true",
        "  write-batch-size: 1000",
        "  write-batch-interval: 500ms",
        "  max-retries: 3",
        "  connect-timeout: 30s",
        "  schema.change.enabled: false",
        "  schema.change.drop-truncate.enabled: false",
        "  scan.startup.mode: initial",
        "  server-time-zone: Asia/Shanghai",
        "  schema-change.enabled: false",
    ):
        assert line in text, line
    assert f"  parallelism: {profile.workers}" in text


def test_transform_regex_covers_every_source_table(tmp_path):
    import re

    text, profile = _render(tmp_path)
    # One entry, Go regexp fullMatch, as in the MySQL->MySQL scale suite.
    assert "  - source-table: 'db_tp_pg_scale_src_\\d+\\.tp_scale_\\d+'" in text
    assert "    projection: '*" in text
    assert "    primary-keys: id" in text
    go_equivalent = rf"{psi.SRC_DB_PREFIX}_\d+\.tp_scale_\d+"
    for spec in profile.tables:
        assert re.fullmatch(go_equivalent, f"{spec.src_db}.{spec.name}"), spec.name


# -------------------------------------------------------------------- B3 guard
def test_profile_remap_keeps_the_table_model():
    for name in ("normal", "heavy"):
        source = load_profile(name)
        remapped = psi.pg_scale_profile(source)
        assert remapped.name == source.name
        assert remapped.workers == source.workers
        assert remapped.total_rows == source.total_rows
        assert len(remapped.tables) == len(source.tables)
        for before, after in zip(source.tables, remapped.tables):
            assert after.ordinal == before.ordinal
            assert after.template == before.template
            assert after.rows == before.rows
            assert after.event_group == before.event_group
            assert after.sink_name == before.sink_name
            assert after.src_db == f"{psi.SRC_DB_PREFIX}_{before.db_index:02d}"
            assert after.snk_db == psi.sink_schema(name, before.db_index)


def test_normal_and_heavy_share_no_schema_checkpoint_or_server_id():
    normal = psi.pg_scale_profile(load_profile("normal"))
    heavy = psi.pg_scale_profile(load_profile("heavy"))
    normal_schemas = {t.snk_db for t in normal.tables}
    heavy_schemas = {t.snk_db for t in heavy.tables}
    assert normal_schemas.isdisjoint(heavy_schemas)
    assert all(s.startswith("tp_pg_scale_normal_") for s in normal_schemas)
    assert all(s.startswith("tp_pg_scale_heavy_") for s in heavy_schemas)

    assert psi._ckpt_dir(normal) != psi._ckpt_dir(heavy)  # noqa: SLF001
    assert psi._ckpt_file(normal) != psi._ckpt_file(heavy)  # noqa: SLF001
    assert psi._ckpt_file(normal).is_relative_to(psi._ckpt_dir(normal))  # noqa: SLF001
    assert psi._ckpt_dir(normal).parts[-1] == "normal"  # noqa: SLF001
    assert psi._ckpt_dir(heavy).parts[-1] == "heavy"  # noqa: SLF001
    # Every rendered target schema is one the cleanup is allowed to drop.
    for name, schemas in (("normal", normal_schemas), ("heavy", heavy_schemas)):
        for schema in schemas:
            match = psi._SCHEMA_RE.match(schema)  # noqa: SLF001
            assert match is not None, schema
            assert match.group("profile") == name, schema

    assert cfg.pg_pipeline_scale_name("normal") != cfg.pg_pipeline_scale_name("heavy")
    assert psi._profile_server_id("normal") != psi._profile_server_id("heavy")  # noqa: SLF001
    # A scale job must never collide with another suite's server id either.
    ours = {psi._profile_server_id("normal"), psi._profile_server_id("heavy")}  # noqa: SLF001
    others = {
        cfg.CDC_SERVER_ID, cfg.CDC_SERVER_ID_PG, cfg.CDC_SERVER_ID_PG_RESTART,
        cfg.CDC_SERVER_ID_PG_LOAD, cfg.CDC_SERVER_ID_DORIS_SCALE,
        cfg.CDC_SERVER_ID_ES_SCALE,
    }
    assert ours.isdisjoint(others)


def test_rendered_checkpoint_and_pipeline_name_are_per_profile(tmp_path):
    from pathlib import Path

    for name in ("normal", "heavy"):
        text, profile = _render(tmp_path, name)
        assert f"  name: {cfg.pg_pipeline_scale_name(name)}" in text
        assert cfg.pg_pipeline_scale_name(name) == f"tp_pg_scale_{name}"
        assert f"  file-path: {cfg.it_rel_path(psi._ckpt_dir(profile))}" in text  # noqa: SLF001
        assert Path(cfg.it_rel_path(psi._ckpt_dir(profile))).name == name  # noqa: SLF001
        assert f"  server-id: {psi._profile_server_id(name)}" in text  # noqa: SLF001
        # B2: the file the restart reuses lives under the rendered `file-path`
        # directory, so a restart reads the stopped job's checkpoint.
        assert Path(cfg.it_rel_path(psi._ckpt_file(profile))).parent == (  # noqa: SLF001
            Path(cfg.it_rel_path(psi._ckpt_dir(profile)))  # noqa: SLF001
            / cfg.pg_pipeline_scale_name(name)
        )


def test_schema_pattern_never_matches_foreign_objects():
    match = psi._SCHEMA_RE.match  # noqa: SLF001
    assert match("tp_pg_scale_normal_01")
    assert match("tp_pg_scale_heavy_10")
    for foreign in (
        "public",
        "tp_pg_a",
        "tp_pg_load_schema",
        "tp_pg_scale_normal_1",
        "tp_pg_scale_normal_001",
        "db_tp_scale_snk_01",
        "db_tp_pg_scale_src_01",
        "tp_pg_scale_",
    ):
        assert match(foreign) is None, foreign


def test_stale_schemas_filters_by_profile(monkeypatch):
    monkeypatch.setattr(
        psi.pg_client,
        "schemas",
        lambda: [
            "public",
            "tp_pg_a",
            "db_tp_scale_snk_01",
            "tp_pg_scale_normal_01",
            "tp_pg_scale_normal_09",
            "tp_pg_scale_heavy_01",
        ],
    )
    normal = psi.pg_scale_profile(load_profile("normal"))
    heavy = psi.pg_scale_profile(load_profile("heavy"))
    assert psi.stale_schemas(normal) == ["tp_pg_scale_normal_01", "tp_pg_scale_normal_09"]
    assert psi.stale_schemas(heavy) == ["tp_pg_scale_heavy_01"]


def test_clean_target_schemas_drops_only_its_own_schemas(monkeypatch):
    dropped: list[str] = []
    monkeypatch.setattr(psi, "stale_schemas", lambda _profile: ["tp_pg_scale_normal_01"])
    monkeypatch.setattr(psi.pg_client, "drop_schema", dropped.append)
    profile = psi.pg_scale_profile(load_profile("normal"))
    assert psi.clean_target_schemas(profile) == ["tp_pg_scale_normal_01"]
    assert dropped == ["tp_pg_scale_normal_01"]


def test_clean_target_schemas_refuses_a_foreign_schema(monkeypatch):
    monkeypatch.setattr(psi, "stale_schemas", lambda _profile: ["public"])
    monkeypatch.setattr(psi.pg_client, "drop_schema", lambda _name: None)
    profile = psi.pg_scale_profile(load_profile("normal"))
    try:
        psi.clean_target_schemas(profile)
    except RuntimeError as exc:
        assert "public" in str(exc)
    else:
        raise AssertionError("expected RuntimeError before dropping a foreign schema")


def test_clean_target_schemas_refuses_the_other_profile(monkeypatch):
    # A bug in stale_schemas must not be able to delete the heavy run's data
    # while a normal run is starting: the name has to carry *this* profile.
    monkeypatch.setattr(psi, "stale_schemas", lambda _profile: ["tp_pg_scale_heavy_01"])
    dropped: list[str] = []
    monkeypatch.setattr(psi.pg_client, "drop_schema", dropped.append)
    profile = psi.pg_scale_profile(load_profile("normal"))
    try:
        psi.clean_target_schemas(profile)
    except RuntimeError as exc:
        assert "normal" in str(exc)
    else:
        raise AssertionError("expected RuntimeError before dropping another profile's schema")
    assert dropped == []


def test_clear_checkpoint_only_touches_its_own_profile(tmp_path, monkeypatch):
    monkeypatch.setattr(psi, "CHECKPOINT_PG_SCALE_DIR", str(tmp_path))
    for name in ("normal", "heavy"):
        nested = tmp_path / name / f"tp_pg_scale_{name}"
        nested.mkdir(parents=True)
        (nested / "old.ckpt").write_text("x", encoding="utf-8")
    profile = psi.pg_scale_profile(load_profile("normal"))
    psi.clear_checkpoint(profile)
    assert (tmp_path / "normal").is_dir()
    assert not any((tmp_path / "normal").iterdir())
    assert (tmp_path / "heavy" / "tp_pg_scale_heavy" / "old.ckpt").is_file()


# --------------------------------------------------------------- B2 + load model
def test_checkpoint_file_is_what_a_restart_reuses():
    profile = psi.pg_scale_profile(load_profile("normal"))
    expected = cfg.checkpoint_file(
        str(psi._ckpt_dir(profile)),  # noqa: SLF001
        cfg.pg_pipeline_scale_name(profile.name),
    )
    assert psi._ckpt_file(profile) == expected  # noqa: SLF001
    # The restart phase re-renders nothing, so the path must be a pure function
    # of the profile: same profile, same file, before and after the restart.
    assert psi._ckpt_file(psi.pg_scale_profile(load_profile("normal"))) == expected  # noqa: SLF001


def test_restart_phase_writes_to_the_source_while_the_job_is_down():
    assert psi.RESTART_ROWS >= 1
    assert psi.RESTART_ROWS <= 1000


def _restart_stub(monkeypatch, tmp_path, *, checkpoint_present, gate=None, rows=None):
    """Wire ``restart_recovery`` to fakes so the phase can be observed offline."""
    events: list[object] = []
    profile = psi.pg_scale_profile(load_profile("normal"))
    ckpt = tmp_path / "normal" / "tp_pg_scale_normal.ckpt"
    ckpt.parent.mkdir(parents=True, exist_ok=True)
    if checkpoint_present:
        ckpt.write_text("state", encoding="utf-8")

    class _Conn:
        def close(self):
            events.append("close")

    def fake_insert(_conn, _db, _table, _template, start, end, _di, _ord):
        events.append(("insert", start, end))

    monkeypatch.setattr(psi, "_ckpt_file", lambda _profile: ckpt)
    monkeypatch.setattr(psi, "stop_job", lambda _proc: events.append("stop"))
    monkeypatch.setattr(psi, "_max_id", lambda _spec: 40)
    monkeypatch.setattr(psi, "_src_conn", lambda _db: _Conn())
    monkeypatch.setattr(psi.pg_schemas, "insert_batch", fake_insert)
    monkeypatch.setattr(psi, "start_job", lambda _p, append_log=False: events.append("start") or object())
    monkeypatch.setattr(
        psi, "verify_pair_eventual", lambda _p, _d, proc=None, phase="": list(gate or [])
    )
    monkeypatch.setattr(
        psi, "verify_rows_eventual", lambda _p, _d, _phase, proc=None: list(rows or [])
    )
    return profile, events


def test_restart_stops_then_writes_then_resumes_on_the_same_checkpoint(monkeypatch, tmp_path):
    profile, events = _restart_stub(monkeypatch, tmp_path, checkpoint_present=True)
    _, info = psi.restart_recovery(profile, object(), deadline=0.0)
    # Stop first, write at the source while the job is down, then start again.
    assert events[0] == "stop"
    assert events[1][0] == "insert"
    assert events[-1] == "start"
    # Rows are appended above the current max id, exactly RESTART_ROWS of them.
    assert events[1] == ("insert", 41, 41 + psi.RESTART_ROWS - 1)
    assert info["checkpoint_present"] is True
    assert info["rows_written_while_down"] == psi.RESTART_ROWS
    assert info["ok"] is True
    assert info["failures"] == []


def test_restart_without_a_checkpoint_fails_even_when_rows_match(monkeypatch, tmp_path):
    # A restart that finds no checkpoint re-snapshots from scratch, so identical
    # rows prove nothing about the resume path: the phase must not report ok.
    profile, _ = _restart_stub(monkeypatch, tmp_path, checkpoint_present=False)
    _, info = psi.restart_recovery(profile, object(), deadline=0.0)
    assert info["checkpoint_present"] is False
    assert info["ok"] is False
    assert any("no checkpoint file" in msg for msg in info["failures"])


def test_restart_gate_failure_keeps_the_checkpoint_reason(monkeypatch, tmp_path):
    profile, _ = _restart_stub(
        monkeypatch, tmp_path, checkpoint_present=False, gate=["count mismatch"]
    )
    _, info = psi.restart_recovery(profile, object(), deadline=0.0)
    assert info["gate_failures"] == ["count mismatch"]
    assert info["ok"] is False
    assert info["seconds"] is not None
    assert any("no checkpoint file" in msg for msg in info["failures"])


def test_historical_load_model_is_unchanged():
    assert psi.SCALE_BURST_ROWS_PER_HOT == 50_000
    assert psi.SCALE_PEAK_SECONDS == 45
    assert psi.SCALE_PEAK_BATCH == 200
    assert psi.PEAK_CATCHUP_BUDGET_S == 3600.0
    assert 1000 <= psi.VERIFY_CHUNK <= 100_000
    normal = load_profile("normal")
    heavy = load_profile("heavy")
    assert (len(normal.tables), normal.total_rows, normal.workers) == (20, 50_000, 4)
    assert (len(heavy.tables), heavy.total_rows, heavy.workers) == (50, 300_000, 8)


def test_hotspot_count_matches_the_source_database_count():
    assert len(psi.hotspot_tables(load_profile("normal"))) == 5
    assert len(psi.hotspot_tables(load_profile("heavy"))) == 10


def test_seeded_schema_drops_only_the_refused_column():
    # The whole suite depends on the source tables never carrying c_time: the
    # PostgreSQL sink refuses TIME, so a template that kept it would fail at
    # table creation instead of measuring throughput.
    for profile_name in ("normal", "heavy"):
        profile = psi.pg_scale_profile(load_profile(profile_name))
        for spec in profile.tables:
            ddl = psi.pg_schemas.create_table_sql(spec.template, spec.name)
            assert "`c_time`" not in ddl
            if spec.template != "wide":
                assert ddl == base.create_table_sql(spec.template, spec.name)


def test_print_plan_runs_without_a_database(capsys):
    assert psi.main(["--print-plan", "normal"]) == 0
    assert psi.main(["--print-plan", "heavy"]) == 0


# ------------------------------------------------------- verification evidence

def _mutated_value(column: str, row_id: int):
    """Same shapes the mutation phase writes (status is an int, the rest text)."""
    return 7 if column == "status" else f"mut-{row_id}"


def test_update_marker_check_compares_the_mutated_column(monkeypatch):
    profile = psi.pg_scale_profile(load_profile("normal"))
    seen: list[str] = []

    def fake_src(_spec, column, row_id):
        seen.append(column)
        return _mutated_value(column, row_id)

    monkeypatch.setattr(psi, "_src_value", fake_src)
    monkeypatch.setattr(
        psi, "_snk_value", lambda _spec, column, row_id: _mutated_value(column, row_id)
    )
    assert psi.verify_update_markers(profile, "catchup") == []
    assert seen, "no mutated table was sampled"
    # scale_it parity: name / c_label / status are mutated; external_id never is.
    assert set(seen) <= {"name", "c_label", "status"}
    assert "external_id" not in seen


def test_update_marker_check_reports_a_difference(monkeypatch):
    profile = psi.pg_scale_profile(load_profile("normal"))
    monkeypatch.setattr(
        psi, "_src_value", lambda _spec, column, row_id: _mutated_value(column, row_id)
    )
    monkeypatch.setattr(
        psi, "_snk_value", lambda _spec, column, row_id: 8 if column == "status" else "stale"
    )
    failures = psi.verify_update_markers(profile, "catchup")
    assert failures
    assert "marker mismatch" in failures[0]


def test_report_states_isolation_type_differences_and_the_restart(tmp_path, monkeypatch):
    monkeypatch.setattr(psi, "REPORT_DIR", tmp_path)
    profile = psi.pg_scale_profile(load_profile("normal"))
    info = {
        "table": "db_tp_pg_scale_src_01.tp_scale_001",
        "rows_written_while_down": psi.RESTART_ROWS,
        "checkpoint": "workdir/ckpts-pg-scale/normal",
        "checkpoint_present": True,
        "checkpoint_bytes": 42,
        "seconds": 12.5,
        "ok": True,
    }
    out = psi.write_report(
        profile,
        {"seed_baseline": 1.0, "cdc_catchup": 2.0, "restart": 12.5},
        [],
        {
            "until": "all",
            "dropped_schemas": ["tp_pg_scale_normal_09"],
            "hotspots": ["db_tp_pg_scale_src_01.tp_scale_001"],
            "peak_rows": 100,
            "peak_workers": 8,
            "restart": info,
        },
    )
    text = out.read_text(encoding="utf-8")
    assert "Result: ✅ PASS" in text
    assert "Stopped after phase: `all`" in text
    # B3: what was dropped, and what this run is allowed to own.
    assert "tp_pg_scale_normal_09" in text
    assert "Isolation:" in text
    # B2: the restart is its own reported phase, not a footnote.
    assert "## Checkpoint recovery (PG-205)" in text
    assert f"Rows written while down: `{psi.RESTART_ROWS}`" in text
    assert "Outcome: ✅ equal" in text
    # Type differences are recorded, never silent.
    assert "## Template and type differences" in text
    assert "refuses TIME" in text
    assert "json (not jsonb)" in text
    # The spec's historical 1800 s ceiling is recorded instead of re-timed.
    assert "1800 s" in text
    assert "the snapshot, the mutations, the heavy hotspot burst + peak, the checkpoint restart" in text


def test_report_never_reports_pass_with_failures(tmp_path, monkeypatch):
    monkeypatch.setattr(psi, "REPORT_DIR", tmp_path)
    profile = psi.pg_scale_profile(load_profile("normal"))
    out = psi.write_report(profile, {"seed_baseline": 1.0}, ["row-level difference"], {"until": "snapshot"})
    text = out.read_text(encoding="utf-8")
    assert "Result: ❌ FAIL" in text
    assert "## Failures" in text
    assert "row-level difference" in text
    assert "Stopped after phase: `snapshot`" in text
    assert "after the snapshot, the mutations" not in text
