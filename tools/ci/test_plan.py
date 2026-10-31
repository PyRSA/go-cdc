#!/usr/bin/env python3
"""Unit tests for tools/ci/plan.py (the diff -> matrix contract).

Run offline, no git repository needed:
    python3 -m pytest -q tools/ci/test_plan.py
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import plan as planner  # noqa: E402


# ------------------------------------------------------------------ path owners
def test_docs_only_is_ignored() -> None:
    assert planner.owner_of("README.md") == "ignore"
    assert planner.owner_of("docs/superpowers/spesc/CI-Workflow-v1.md") == "ignore"
    # A config that is not a sink's demo is not a reason to run anything.
    assert planner.owner_of("configs/notes.yaml") == "ignore"


def test_sink_own_workflow_and_demo_config_are_sink_local() -> None:
    # A sink's reusable workflow and its demo config belong to that sink alone.
    for sink in planner.SINKS:
        assert planner.owner_of(f".github/workflows/ci-{sink}.yml") == f"sink:{sink}"
        assert planner.owner_of(f"configs/demo-{sink}.yaml") == f"sink:{sink}"
    result = planner.plan(
        [
            "tests/integration/sinks/kafka/harness/kafka_it.py",
            ".github/workflows/ci-kafka.yml",
        ]
    )
    assert result["sinks"] == ["kafka"]
    assert result["matrix"] == {"include": [{"sink": "kafka", "variant": ""}]}
    assert planner.plan(["configs/demo-kafka.yaml"])["sinks"] == ["kafka"]


def test_standalone_workflows_are_not_routed() -> None:
    # release.yml / repo-checks.yml are separate workflows; they must not pull
    # the sink matrix in.
    for path in (
        ".github/workflows/release.yml",
        ".github/workflows/repo-checks.yml",
    ):
        assert planner.owner_of(path) == "ignore", path
    assert planner.plan([".github/workflows/repo-checks.yml"])["count"] == 0


def test_common_paths_run_every_sink() -> None:
    for path in (
        ".github/workflows/ci.yml",
        "tools/ci/plan.py",
        "connector/source/mysql/reader.go",
        "common/event/event.go",
        "tests/integration/scripts/run.sh",
        "tests/integration/common/config.py",
        "tests/integration/sources/mysql/init/01-databases.sql",
    ):
        assert planner.owner_of(path) == "shared", path
    rows = planner.plan([".github/workflows/ci.yml"])["matrix"]["include"]
    assert {row["sink"] for row in rows} == set(planner.SINKS)
    assert len(rows) > len(planner.SINKS)  # mysql and ES contribute >1 variant


def test_sink_paths_are_owned() -> None:
    assert planner.owner_of("connector/sink/kafka/sink.go") == "sink:kafka"
    assert planner.owner_of("tests/integration/sinks/postgresql/harness/pg_it.py") == (
        "sink:postgresql"
    )
    assert planner.owner_of("./tests/integration/sinks/doris/tests/test_ddls.py") == (
        "sink:doris"
    )


def test_shared_and_unknown_paths_run_everything() -> None:
    for path in (
        "runtime/checkpoint/store.go",
        "composer/runner.go",
        "connector/sink/sink.go",
        "connector/source/mysql/reader.go",
        "tests/integration/scripts/run.sh",
        "go.mod",
        ".github/workflows/ci.yml",
        "some/new/directory/file.go",
    ):
        assert planner.owner_of(path) == "shared", path


def test_dot_prefixed_shared_path_is_not_swallowed() -> None:
    # A naive lstrip("./") turns ".github/..." into "github/..." and falls back
    # to "shared" by accident; the explicit rule must still say shared *and*
    # keep the suffix rules intact.
    assert planner.owner_of(".golangci.yml") == "shared"
    assert planner.owner_of("./go.sum") == "shared"


# ------------------------------------------------------------------ matrix rows
def test_affected_sinks_keeps_canonical_order() -> None:
    paths = ["connector/sink/postgresql/ddl.go", "connector/sink/mysql/sql.go"]
    assert planner.affected_sinks(paths) == ["mysql", "postgresql"]


def test_shared_change_affects_every_sink() -> None:
    assert planner.affected_sinks(["runtime/route/route.go"]) == list(planner.SINKS)


def test_docs_only_plan_has_no_rows() -> None:
    # The workflow feeds this straight into strategy.matrix, which requires an
    # object: "nothing to run" is an empty include, never a bare array. The
    # result job must treat that as success.
    assert planner.plan(["README.md", "docs/pipeline.md"])["matrix"] == {"include": []}
    assert planner.plan(["README.md"])["count"] == 0


def test_mysql_matrix_covers_the_version_matrix() -> None:
    result = planner.plan(["connector/sink/mysql/sink.go"], "normal")
    assert result["matrix"] == {
        "include": [
            {"sink": "mysql", "variant": "57"},
            {"sink": "mysql", "variant": "80"},
        ]
    }


def test_elasticsearch_matrix_keeps_all_three_variants() -> None:
    variants = [
        row["variant"]
        for row in planner.plan(["connector/sink/elasticsearch/sink.go"])["matrix"]["include"]
    ]
    assert variants == ["es8", "es7", "es9"]


def test_postgresql_variant_is_a_major_version() -> None:
    rows = planner.plan(["tests/integration/sinks/postgresql/tests/test_pg_verify.py"])
    assert rows["matrix"] == {"include": [{"sink": "postgresql", "variant": "pg16"}]}


def test_per_sink_matrices_are_filtered() -> None:
    result = planner.plan(["connector/sink/kafka/sink.go", "connector/sink/doris/ddl.go"])
    assert set(result["per_sink"]) == {"kafka", "doris"}
    assert result["per_sink"]["kafka"] == {"include": [{"sink": "kafka", "variant": ""}]}
    # A sink that is not affected has no matrix at all: its job never starts.
    assert "mysql" not in result["per_sink"]


def test_heavy_profile_keeps_the_same_variants() -> None:
    normal = planner.plan(["common/event/event.go"], "normal")["matrix"]["include"]
    heavy = planner.plan(["common/event/event.go"], "heavy")["matrix"]["include"]
    assert normal == heavy


# ----------------------------------------------------------------------- docker
def test_changed_files_returns_empty_without_a_base() -> None:
    # An unknown base is not "nothing changed": it must lead to a full run.
    assert planner.changed_files("") == []
    assert planner.changed_files("0" * 40) == []
    assert planner.plan(planner.changed_files(""))["count"] > 0


def test_cli_writes_step_outputs(tmp_path: Path) -> None:
    out = tmp_path / "github_output"
    out.write_text("", encoding="utf-8")
    code = planner.main(
        [
            "--paths",
            "connector/sink/kafka/sink.go",
            "--profile",
            "normal",
            "--github-output",
            str(out),
        ]
    )
    assert code == 0
    lines = dict(line.split("=", 1) for line in out.read_text().splitlines())
    assert json.loads(lines["matrix"]) == {"include": [{"sink": "kafka", "variant": ""}]}
    assert lines["count"] == "1"
    assert lines["sinks"] == "kafka"
    assert json.loads(lines["kafka"]) == {"include": [{"sink": "kafka", "variant": ""}]}


def test_cli_prints_empty_include_for_docs(tmp_path: Path) -> None:
    out = tmp_path / "github_output"
    out.write_text("", encoding="utf-8")
    result = subprocess.run(
        [
            sys.executable,
            str(Path(planner.__file__)),
            "--paths",
            "README.md",
            "--github-output",
            str(out),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == '{"include":[]}'
    lines = dict(line.split("=", 1) for line in out.read_text().splitlines())
    assert lines["matrix"] == '{"include":[]}'
    assert lines["count"] == "0"


@pytest.mark.parametrize("profile", ["normal", "heavy"])
def test_every_sink_has_a_variant_list(profile: str) -> None:
    for sink in planner.SINKS:
        assert planner.VARIANTS[profile][sink], f"{profile}/{sink}"
