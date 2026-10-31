"""Offline checks for the PostgreSQL 14-18 compatibility matrix (no docker, no DB).

    cd test/integration && python3 -m unittest harness.test_pg_compat

Nothing here starts a container or connects to a database. The tests pin the
parts that decide whether a matrix row may ever say PASS: one shared core suite
per version, per-version image / ``PG_EXPECT_MAJOR`` / report / checkpoint
paths, the exit-code to status mapping, and the report's honesty rules
(``NOT RUN`` is never a pass, PG-209 is only complete when five versions pass).
"""
from __future__ import annotations

import contextlib
import io
import os
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from . import config as cfg
from . import pg_compat as pc


def _runner(*, up: int = 0, suite: int = 0, log_text: str = ""):
    """Fake step runner: records argv/env, writes the requested log body."""
    calls: list[dict] = []

    def run(argv, env, log_path, append):
        calls.append(
            {"argv": list(argv), "env": dict(env), "append": append, "log": str(log_path)}
        )
        Path(log_path).write_text(log_text, encoding="utf-8")
        return up if any(str(a).endswith("pg-up.sh") for a in argv) else suite

    run.calls = calls  # type: ignore[attr-defined]
    return run


class VersionPlanTest(unittest.TestCase):
    def test_the_matrix_is_exactly_pg14_to_18(self) -> None:
        self.assertEqual(pc.VERSIONS, ("14", "15", "16", "17", "18"))
        self.assertEqual(pc.normalize_versions(pc.VERSIONS), list(pc.VERSIONS))

    def test_aliases_and_duplicates_normalise(self) -> None:
        self.assertEqual(pc.normalize_versions(["PG16", "pg16", "16.4", " 15 "]), ["16", "15"])

    def test_unknown_major_is_rejected(self) -> None:
        for bad in ("13", "19", "pg", "sixteen"):
            with self.assertRaises(ValueError):
                pc.normalize_versions([bad])

    def test_image_defaults_to_the_major_tag(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            for version in pc.VERSIONS:
                os.environ.pop(f"PG_COMPAT_IMAGE_{version}", None)
                self.assertEqual(pc.image_for(version), f"postgres:{version}")

    def test_a_minor_tag_can_be_pinned_per_version(self) -> None:
        with mock.patch.dict(os.environ, {"PG_COMPAT_IMAGE_16": "postgres:16.4"}):
            self.assertEqual(pc.image_for("16"), "postgres:16.4")
            self.assertEqual(pc.image_for("17"), "postgres:17")


class EnvPinningTest(unittest.TestCase):
    def test_each_row_pins_its_own_major_report_and_checkpoints(self) -> None:
        reports, dirs, restarts = set(), set(), set()
        for version in pc.VERSIONS:
            env = pc.compat_env(version, base={})
            self.assertEqual(env["PG_EXPECT_MAJOR"], version)
            self.assertEqual(env["PG_IMAGE"], pc.image_for(version))
            reports.add(env["REPORT_PG_FUNC"])
            dirs.add(env["CHECKPOINT_PG_DIR"])
            restarts.add(env["CHECKPOINT_PG_RESTART_DIR"])
            self.assertIn(f"pg{version}", env["REPORT_PG_FUNC"])
            self.assertIn(version, env["CHECKPOINT_PG_DIR"])
        # Five versions, five artefacts: a later version never reads an earlier
        # version's report or checkpoint.
        for group in (reports, dirs, restarts):
            self.assertEqual(len(group), len(pc.VERSIONS))

    def test_rows_never_collide_with_the_single_version_pg_suites(self) -> None:
        taken = {
            str(cfg.CHECKPOINT_PG_DIR),
            str(cfg.CHECKPOINT_PG_RESTART_DIR),
            str(cfg.CHECKPOINT_PG_LOAD_DIR),
            str(cfg.CHECKPOINT_PG_SCALE_DIR),
            str(cfg.REPORT_PG_FUNC),
        }
        for version in pc.VERSIONS:
            env = pc.compat_env(version, base={})
            self.assertNotIn(env["CHECKPOINT_PG_DIR"], taken)
            self.assertNotIn(env["CHECKPOINT_PG_RESTART_DIR"], taken)
            self.assertNotIn(env["REPORT_PG_FUNC"], taken)


class SuiteReuseTest(unittest.TestCase):
    def test_every_version_runs_the_same_core_suite(self) -> None:
        self.assertEqual(pc.SUITE_MODULE, "harness.pg_it")
        source = (cfg.IT_ROOT / "harness" / "pg_it.py").read_text(encoding="utf-8")
        ids = re.findall(r'\(\s*"(TP-P\d\d)"\s*,\s*tp_p\d\d\s*\)', source)
        self.assertEqual(len(ids), 20)
        self.assertEqual(ids, [f"TP-P{i:02d}" for i in range(1, 21)])
        # No per-version copy of the suite exists: the matrix only parameterises.
        copies = sorted(
            p.name for p in (cfg.IT_ROOT / "harness").glob("pg*it*.py")
            if "compat" in p.name and p.name != "pg_compat.py"
        )
        self.assertEqual(copies, [])

    def test_the_shared_suite_argv_is_identical_across_versions(self) -> None:
        argv = {tuple(pc.suite_argv()) for _ in pc.VERSIONS}
        self.assertEqual(len(argv), 1)
        self.assertEqual(pc.suite_argv()[1:], ["-m", "harness.pg_it"])

    def test_case_selection_is_forwarded_to_the_shared_suite(self) -> None:
        self.assertEqual(pc.case_args(["tp-p01", "TP-P16"]), ["TP-P01", "TP-P16"])
        self.assertEqual(pc.suite_argv(["TP-P01"])[-1], "TP-P01")
        self.assertEqual(pc.case_args([]), [])


class RunVersionTest(unittest.TestCase):
    def _report(self, path: Path, failing: tuple[str, ...] = ()) -> Path:
        rows = ["| TP | Name | Status |", "| --- | --- | --- |"]
        for tp_id in ("TP-P01", "TP-P07"):
            status = "❌ FAIL" if tp_id in failing else "✅ PASS"
            rows.append(f"| {tp_id} | some case | {status} |")
        path.write_text("\n".join(rows), encoding="utf-8")
        return path

    def test_no_docker_is_env_blocked_and_never_runs_a_suite(self) -> None:
        runner = _runner()
        result = pc.run_version("16", docker=False, runner=runner)
        self.assertEqual(result.status, pc.STATUS_BLOCKED)
        self.assertIsNone(result.exit_code)
        self.assertIn("docker", result.reason)
        self.assertEqual(runner.calls, [])  # type: ignore[attr-defined]

    def test_zero_exit_is_a_pass(self) -> None:
        runner = _runner(up=0, suite=0)
        with tempfile.TemporaryDirectory() as tmp:
            result = pc.run_version("16", docker=True, runner=runner, log_dir=Path(tmp))
        self.assertEqual(result.status, pc.STATUS_PASS)
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(len(runner.calls), 2)  # type: ignore[attr-defined]
        self.assertTrue(any(a.endswith("pg-up.sh") for a in runner.calls[0]["argv"]))  # type: ignore[attr-defined]
        self.assertIn("harness.pg_it", runner.calls[1]["argv"])  # type: ignore[attr-defined]
        # The up step truncates the log, the suite run appends to it.
        self.assertFalse(runner.calls[0]["append"])  # type: ignore[attr-defined]
        self.assertTrue(runner.calls[1]["append"])  # type: ignore[attr-defined]

    def test_a_failing_suite_names_the_failing_test_points(self) -> None:
        runner = _runner(up=0, suite=1)
        with tempfile.TemporaryDirectory() as tmp:
            report = self._report(Path(tmp) / "func.md", failing=("TP-P07",))
            with mock.patch.object(pc, "report_path", lambda _v: report):
                result = pc.run_version("17", docker=True, runner=runner, log_dir=Path(tmp))
        self.assertEqual(result.status, pc.STATUS_FAIL)
        self.assertEqual(result.failed, ["TP-P07"])
        self.assertIn("TP-P07", result.reason)

    def test_a_blocked_preflight_keeps_its_reason(self) -> None:
        runner = _runner(
            up=0,
            suite=pc.SUITE_ENV_BLOCKED,
            log_text="ENV BLOCKED: PostgreSQL is not reachable at 127.0.0.1:15432\n",
        )
        with tempfile.TemporaryDirectory() as tmp:
            result = pc.run_version("18", docker=True, runner=runner, log_dir=Path(tmp))
        self.assertEqual(result.status, pc.STATUS_BLOCKED)
        self.assertIn("not reachable", result.reason)

    def test_a_failed_pg_up_is_env_blocked_not_a_test_failure(self) -> None:
        runner = _runner(up=1, suite=0, log_text="docker daemon is not available.\n")
        with tempfile.TemporaryDirectory() as tmp:
            result = pc.run_version("14", docker=True, runner=runner, log_dir=Path(tmp))
        self.assertEqual(result.status, pc.STATUS_BLOCKED)
        self.assertEqual(result.exit_code, 1)
        self.assertEqual(len(runner.calls), 1)  # type: ignore[attr-defined]


class ReportTest(unittest.TestCase):
    def _results(self, statuses: dict[str, str]) -> list[pc.Result]:
        return [
            pc.Result(version=version, image=f"postgres:{version}", status=status)
            for version, status in statuses.items()
        ]

    def test_a_row_that_did_not_run_is_never_a_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = pc.write_report(
                self._results({v: pc.STATUS_NOT_RUN for v in pc.VERSIONS}),
                meta={"modes": ["dry-run"], "docker": False},
                path=Path(tmp) / "compat.md",
            )
            text = out.read_text(encoding="utf-8")
        self.assertIn("NOT RUN", text)
        self.assertIn("PG-209: INCOMPLETE", text)
        self.assertIn("PASS 0 / FAIL 0 / ENV BLOCKED 0 / NOT RUN 5 of 5", text)
        self.assertNotIn("PG-209: COMPLETE", text)
        for version in pc.VERSIONS:
            self.assertIn(f"| {version} |", text)

    def test_pg209_is_complete_only_when_all_five_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            all_pass = pc.write_report(
                self._results({v: pc.STATUS_PASS for v in pc.VERSIONS}),
                path=Path(tmp) / "pass.md",
            ).read_text(encoding="utf-8")
            partial = pc.write_report(
                self._results(
                    {**{v: pc.STATUS_PASS for v in ("14", "15", "16", "17")}, "18": pc.STATUS_FAIL}
                ),
                path=Path(tmp) / "partial.md",
            ).read_text(encoding="utf-8")
            subset = pc.write_report(
                self._results({"16": pc.STATUS_PASS}),
                path=Path(tmp) / "subset.md",
            ).read_text(encoding="utf-8")
        self.assertIn("PG-209: COMPLETE", all_pass)
        self.assertIn("PG-209: INCOMPLETE", partial)
        self.assertIn("PG-209: INCOMPLETE", subset)

    def test_the_report_records_toolchain_and_ci_image(self) -> None:
        deps = pc.go_mod_versions()
        self.assertRegex(deps["go"], r"^\d+\.\d+")
        self.assertRegex(deps["pgx"], r"^v5\.")
        self.assertRegex(pc.ci_pg_image(), r"^postgres:\d+")
        with tempfile.TemporaryDirectory() as tmp:
            text = pc.write_report(
                self._results({v: pc.STATUS_NOT_RUN for v in pc.VERSIONS}),
                path=Path(tmp) / "deps.md",
            ).read_text(encoding="utf-8")
        self.assertIn(f"`{deps['go']}`", text)
        self.assertIn(f"`{deps['pgx']}`", text)
        self.assertIn(pc.ci_pg_image(), text)

    def test_no_version_difference_is_recorded_without_evidence(self) -> None:
        self.assertEqual(pc.RECORDED_DIFFERENCES, ())
        with tempfile.TemporaryDirectory() as tmp:
            text = pc.write_report(
                self._results({v: pc.STATUS_NOT_RUN for v in pc.VERSIONS}),
                path=Path(tmp) / "diff.md",
            ).read_text(encoding="utf-8")
        self.assertIn("## 4. Version differences (T3.20)", text)
        self.assertIn("No version-specific behaviour difference has been proven", text)

    def test_the_skipped_cases_are_stated_not_assumed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            text = pc.write_report(
                self._results({v: pc.STATUS_NOT_RUN for v in pc.VERSIONS}),
                path=Path(tmp) / "limits.md",
            ).read_text(encoding="utf-8")
        self.assertIn("PG_RESTART_ENABLED=1", text)
        self.assertIn("never as passed", text)
        # TP-P18 has no injector at all, so a PASS row must not be read as covering it.
        self.assertIn("TP-P17", text)
        self.assertIn("TP-P18", text)
        self.assertIn("TP-P01..TP-P16, TP-P19", text)

    def test_an_empty_artefact_never_renders_an_empty_cell(self) -> None:
        # A Result built without a report/log (as in a unit test or a dry run of a
        # single version) must not leave `` `` `` in the matrix table.
        with tempfile.TemporaryDirectory() as tmp:
            text = pc.write_report(
                [pc.Result(version="16", image="postgres:16", status=pc.STATUS_NOT_RUN)],
                path=Path(tmp) / "empty.md",
            ).read_text(encoding="utf-8")
        self.assertNotIn("| `` |", text)
        self.assertNotIn("- E2E report: ``", text)
        self.assertIn("| 16 | `postgres:16` | `NOT RUN` |", text)


class CliTest(unittest.TestCase):
    def test_list_prints_the_plan_without_a_database(self) -> None:
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            self.assertEqual(pc.main(["--list"]), pc.EXIT_OK)
        printed = buffer.getvalue()
        for version in pc.VERSIONS:
            self.assertIn(f"PG{version}:", printed)
            self.assertIn(f"expected_major={version}", printed)
        self.assertIn("harness.pg_it", printed)

    def test_dry_run_writes_a_not_run_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "compat.md"
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                code = pc.main(["--dry-run", "--report", str(out)])
            self.assertEqual(code, pc.EXIT_OK)
            text = out.read_text(encoding="utf-8")
        self.assertIn(pc.STATUS_NOT_RUN, text)
        self.assertNotIn("`PASS`", buffer.getvalue())
        self.assertIn("PG-209: INCOMPLETE", text)

    def test_an_unknown_version_is_a_usage_error(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                pc.main(["13"])


if __name__ == "__main__":
    unittest.main()
