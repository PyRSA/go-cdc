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


def test_sink_path_templates_resolve_to_real_files() -> None:
    # SINK_PATHS is the single routing table owner_of consults. A typo there
    # has a fail-close failure mode: e.g. `configs/demo-postgressql.yaml`
    # would not match the sink table, IGNORE_PREFIXES `configs/` would
    # swallow the real file, and the sink would never run on its own demo
    # config. This test is the backstop: walk every sink's entries and assert
    # each one resolves on disk. Entries ending in `/` are directories, the
    # rest are files - one assertion each, collected across every sink before
    # failing, so a broken table reports all typos in one run.
    root = Path(".")
    missing: list[tuple[str, str]] = []
    for sink, entries in planner.SINK_PATHS.items():
        for entry in entries:
            target = root / entry
            if entry.endswith("/"):
                if not target.is_dir():
                    missing.append((sink, entry))
            elif not target.is_file():
                missing.append((sink, entry))
    assert not missing, (
        "SINK_PATHS entries that do not resolve on disk: "
        + ", ".join(f"{sink}={entry!r}" for sink, entry in missing)
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


def test_heavy_matches_normal_except_the_pg_compat_matrix() -> None:
    # The tier still never changes which suites run, so every sink keeps the
    # same variants in both tiers EXCEPT PostgreSQL, whose compat majors move
    # from a single in-process loop to parallel heavy matrix rows.
    normal = planner.plan(["common/event/event.go"], "normal")["matrix"]["include"]
    heavy = planner.plan(["common/event/event.go"], "heavy")["matrix"]["include"]
    without_pg = lambda rows: [r for r in rows if r["sink"] != "postgresql"]
    assert without_pg(normal) == without_pg(heavy)
    # normal pins the one default major; heavy fans the five majors out.
    assert [r["variant"] for r in normal if r["sink"] == "postgresql"] == ["pg16"]
    assert [r["variant"] for r in heavy if r["sink"] == "postgresql"] == [
        "pg14",
        "pg15",
        "pg16",
        "pg17",
        "pg18",
    ]


# --------------------------------------------------------------- changed files
BASE = "a" * 40
FORK = "b" * 40
DROPPED = "c" * 40  # a force-pushed-away tip: a fresh clone cannot resolve it


class GitStub:
    """Answers ``git -C <repo> ...`` from a ``{(args...): stdout | None}`` map.

    ``None`` makes the call fail the way git does on a bad revision, so the
    fallback chain is exercised without a repository and without depending on
    whether the checkout running the suite happens to have commits.
    """

    def __init__(self, answers: dict[tuple[str, ...], str | None]) -> None:
        self.answers = answers
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, cmd, **kwargs) -> subprocess.CompletedProcess:
        assert cmd[:2] == ["git", "-C"], cmd
        args = tuple(cmd[3:])
        self.calls.append(args)
        if args not in self.answers:
            raise AssertionError(f"unexpected git call: {args}")
        stdout = self.answers[args]
        if stdout is None:
            raise subprocess.CalledProcessError(128, cmd, "", "fatal: bad object")
        return subprocess.CompletedProcess(cmd, 0, stdout, "")


def stub_git(
    monkeypatch: pytest.MonkeyPatch,
    answers: dict[tuple[str, ...], str | None],
    event: str | None = None,
) -> GitStub:
    """Install the git stub and pin ``GITHUB_EVENT_NAME``.

    Pinning matters in both directions: a suite that runs under a manual
    ``workflow_dispatch`` would otherwise pick up the real event name and take
    the full-run shortcut inside ``changed_files``, so these tests would stop
    exercising the diff logic at all. ``event=None`` means "not on Actions".
    """
    if event is None:
        monkeypatch.delenv("GITHUB_EVENT_NAME", raising=False)
    else:
        monkeypatch.setenv("GITHUB_EVENT_NAME", event)
    stub = GitStub(answers)
    monkeypatch.setattr(planner.subprocess, "run", stub)
    return stub


def diff_args(base: str, head: str = "HEAD") -> tuple[str, ...]:
    """The exact `git diff` command plan.py is expected to issue.

    ``-c core.quotePath=false`` is part of the contract: without it git quotes
    non-ASCII path bytes and the routing table sees garbage (see
    ``test_chinese_paths_are_not_quoted_by_git``).
    """
    return (
        "-c",
        "core.quotePath=false",
        "diff",
        "--name-only",
        "--no-renames",
        base,
        head,
    )


def merge_base_args(candidate: str, head: str = "HEAD") -> tuple[str, ...]:
    """The probe `_fork_point` is expected to issue for one trunk candidate."""
    return ("merge-base", candidate, head)


def test_changed_files_returns_empty_without_a_base(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An unknown base is not "nothing changed": it must lead to a full run.
    # No real git here on purpose. ``tmp_path`` is *not* safe: pytest's basetemp
    # can land inside this repository (``.tmpsandbox/``), where ``git -C``
    # resolves upward to the real checkout - the assertion would then ride on
    # whatever the current branch happens to contain, i.e. green before the fix
    # is committed and red after it.
    stub = stub_git(
        monkeypatch,
        {merge_base_args("origin/master"): None, merge_base_args("origin/main"): None},
    )
    assert planner.changed_files("") == []
    assert stub.calls == [
        merge_base_args("origin/master"),
        merge_base_args("origin/main"),
    ]
    stub.calls.clear()
    assert planner.changed_files("0" * 40) == []
    assert stub.calls == [
        merge_base_args("origin/master"),
        merge_base_args("origin/main"),
    ]
    assert planner.plan([])["count"] > 0


def test_workflow_dispatch_forces_full_run(monkeypatch: pytest.MonkeyPatch) -> None:
    # A dispatch carries BASE="" but still sits on a real branch, so the
    # merge-base fallback would answer it with a *selective* plan - a hand-started
    # promotion could then silently skip sinks. It is a request for a full
    # verdict, so git is never consulted: the stub has no answers at all, and any
    # call would fail the test.
    stub = stub_git(monkeypatch, {}, event="workflow_dispatch")
    paths = planner.changed_files(BASE)
    assert paths == []
    assert stub.calls == []
    assert planner.plan(paths)["count"] == len(
        planner.rows_for(list(planner.SINKS), "normal")
    )

    # The same base under a normal event still asks git: the guard is about the
    # trigger, not about the base.
    pull = stub_git(
        monkeypatch, {diff_args(BASE): "composer/runner.go\n"}, event="pull_request"
    )
    assert planner.changed_files(BASE) == ["composer/runner.go"]
    assert pull.calls == [diff_args(BASE)]


def test_usable_base_asks_git_exactly_one_question(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The normal path stays a single diff: no merge-base probe (a wasted process
    # per run) and no chance of widening the change set behind the caller's back.
    stub = stub_git(monkeypatch, {diff_args(BASE): "composer/runner.go\n"})
    assert planner.changed_files(BASE) == ["composer/runner.go"]
    assert stub.calls == [diff_args(BASE)]


def test_new_branch_plans_every_pushed_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # before = 40 zeros (a brand-new branch). ``HEAD^`` would plan only the last
    # commit and silently miss whatever the earlier ones touched; the fork point
    # against the default branch covers the whole push.
    stub = stub_git(
        monkeypatch,
        {
            merge_base_args("origin/master"): FORK + "\n",
            diff_args(FORK): (
                "connector/sink/kafka/sink.go\n"
                "connector/sink/doris/ddl.go\n"
                "README.md\n"
            ),
        },
    )
    paths = planner.changed_files("0" * 40)
    assert paths == [
        "connector/sink/kafka/sink.go",
        "connector/sink/doris/ddl.go",
        "README.md",
    ]
    assert stub.calls == [merge_base_args("origin/master"), diff_args(FORK)]
    # No single-parent shortcut anywhere: that is the silent-miss bug.
    assert not any(
        "HEAD^" in arg or "HEAD~" in arg for call in stub.calls for arg in call
    )
    # Two sinks plus a docs path: selective, not the whole matrix.
    assert planner.plan(paths)["sinks"] == ["kafka", "doris"]


def test_unresolvable_base_falls_back_to_the_fork_point(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A force push leaves the old tip dangling: git cannot resolve it, so the
    # first diff raises and the fallback has to carry the run.
    stub = stub_git(
        monkeypatch,
        {
            diff_args(DROPPED): None,
            merge_base_args("origin/master"): FORK,
            diff_args(FORK): "connector/sink/mysql/sink.go\n",
        },
    )
    assert planner.changed_files(DROPPED) == ["connector/sink/mysql/sink.go"]
    assert stub.calls == [
        diff_args(DROPPED),
        merge_base_args("origin/master"),
        diff_args(FORK),
    ]


def test_no_base_and_no_fork_point_still_runs_everything(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Neither trunk candidate resolves (no remote yet, or a shallow clone): the
    # contract is unchanged - an empty list means "unknown history, run all".
    stub = stub_git(
        monkeypatch,
        {merge_base_args("origin/master"): None, merge_base_args("origin/main"): None},
    )
    paths = planner.changed_files("0" * 40)
    assert paths == []
    assert stub.calls == [
        merge_base_args("origin/master"),
        merge_base_args("origin/main"),
    ]
    assert planner.plan(paths)["count"] == len(
        planner.rows_for(list(planner.SINKS), "normal")
    )


def test_repo_on_main_uses_the_second_trunk_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stub = stub_git(
        monkeypatch,
        {
            merge_base_args("origin/master"): None,
            merge_base_args("origin/main"): FORK,
            diff_args(FORK): "configs/demo-kafka.yaml\n",
        },
    )
    paths = planner.changed_files("0" * 40)
    assert paths == ["configs/demo-kafka.yaml"]
    # origin/master is probed first and fails before origin/main answers: the
    # candidate order is part of the contract, not an implementation detail.
    assert stub.calls == [
        merge_base_args("origin/master"),
        merge_base_args("origin/main"),
        diff_args(FORK),
    ]
    assert planner.plan(paths)["sinks"] == ["kafka"]


def test_head_is_passed_through_to_merge_base(monkeypatch: pytest.MonkeyPatch) -> None:
    # --head has to reach the probe: a literal "HEAD" there would diff against
    # the wrong commit whenever the planner is pointed at another ref.
    head = "d" * 40
    stub = stub_git(
        monkeypatch,
        {
            merge_base_args("origin/master", head): FORK,
            diff_args(FORK, head): "connector/sink/doris/ddl.go\n",
        },
    )
    assert planner.changed_files("0" * 40, head=head) == ["connector/sink/doris/ddl.go"]
    assert stub.calls == [merge_base_args("origin/master", head), diff_args(FORK, head)]


def test_chinese_paths_are_not_quoted_by_git(monkeypatch: pytest.MonkeyPatch) -> None:
    # git's default core.quotePath turns docs/需求.md into
    # '"docs/\351\234\200.md"': the ``.md`` suffix rule misses the trailing quote,
    # owner_of falls through to "shared" and every sink runs. The switch has to
    # be on the command line and the router has to see the raw path.
    stub = stub_git(
        monkeypatch,
        {diff_args(BASE): "docs/需求.md\nconnector/sink/kafka/sink.go\n"},
    )
    paths = planner.changed_files(BASE)
    assert paths == ["docs/需求.md", "connector/sink/kafka/sink.go"]
    assert stub.calls == [diff_args(BASE)]
    assert stub.calls[0][:2] == ("-c", "core.quotePath=false")
    # Prose is ignored, the sink is not: a selective plan, not the whole matrix.
    assert planner.owner_of(paths[0]) == "ignore"
    assert planner.plan(paths)["sinks"] == ["kafka"]


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
