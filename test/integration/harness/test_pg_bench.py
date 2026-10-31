"""Offline checks for the PostgreSQL sink benchmark (no Docker, no database).

    cd test/integration && python3 -m unittest harness.test_pg_bench

Nothing here starts a container or connects to a server. The tests pin the parts
that decide whether a benchmark number may be believed: the matrix, the
deterministic batch planner, the metric arithmetic (percentiles included), the
rendered pipeline, the CLI, and the report's honesty rules (a cell that did not
run carries ``NOT RUN`` and a dash, never a fabricated number, and the retry
column stays ``n/a`` because the sink does not log retries).
"""
from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from . import config as cfg
from . import pg_bench_it as pb


def _cell(workload: str = "insert", batch: int = 100) -> pb.Cell:
    return pb.Cell(workload=workload, batch=batch)


class MatrixTest(unittest.TestCase):
    def test_the_matrix_is_two_workloads_by_three_batch_sizes(self) -> None:
        self.assertEqual(pb.WORKLOADS, ("insert", "mixed"))
        self.assertEqual(pb.parse_batches(cfg.PG_BENCH_BATCHES), (100, 1000, 5000))
        matrix = pb.build_matrix(pb.WORKLOADS, pb.parse_batches(cfg.PG_BENCH_BATCHES))
        self.assertEqual(len(matrix), 6)
        self.assertEqual([c.label for c in matrix], [
            "insert/batch-100", "insert/batch-1000", "insert/batch-5000",
            "mixed/batch-100", "mixed/batch-1000", "mixed/batch-5000",
        ])
        self.assertTrue(all(c.status == pb.STATUS_NOT_RUN for c in matrix))

    def test_batch_sizes_are_sorted_and_deduplicated(self) -> None:
        self.assertEqual(pb.parse_batches("5000,100,1000,100"), (100, 1000, 5000))
        self.assertEqual(pb.parse_batches(" 1000 "), (1000,))

    def test_a_batch_size_must_be_a_positive_integer(self) -> None:
        for bad in ("0", "-1", "abc", "", "100,x"):
            with self.assertRaises(ValueError):
                pb.parse_batches(bad)

    def test_workloads_are_normalised_and_ordered(self) -> None:
        self.assertEqual(pb.parse_workloads("MIXED, insert"), ("insert", "mixed"))
        self.assertEqual(pb.parse_workloads("mixed"), ("mixed",))

    def test_an_unknown_workload_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            pb.parse_workloads("delete")

    def test_unselected_cells_stay_in_the_matrix(self) -> None:
        # A rerun of one column must still show the other cells as NOT RUN, so a
        # partial run can never look like a full matrix.
        full = pb.build_matrix(pb.WORKLOADS, (100, 1000, 5000))
        chosen = {("insert", 100)}
        not_run = [c for c in full if (c.workload, c.batch) not in chosen]
        self.assertEqual(len(not_run), 5)
        self.assertTrue(all(c.status == pb.STATUS_NOT_RUN for c in not_run))


class PlannerTest(unittest.TestCase):
    def test_insert_batches_are_contiguous_and_deterministic(self) -> None:
        first = pb.BatchPlanner("insert", 100, seed=7, base_id=1_000_000, pool=1000)
        second = pb.BatchPlanner("insert", 100, seed=7, base_id=1_000_000, pool=1000)
        a_inserts, a_upserts, a_deletes = first.next()
        b_inserts, b_upserts, b_deletes = second.next()
        self.assertEqual(a_inserts, b_inserts)
        self.assertEqual((a_upserts, a_deletes), ([], []))
        self.assertEqual((b_upserts, b_deletes), ([], []))
        self.assertEqual(len(a_inserts), 100)
        self.assertEqual([row[0] for row in a_inserts], list(range(1_000_000, 1_000_100)))

    def test_a_later_batch_continues_the_id_range(self) -> None:
        planner = pb.BatchPlanner("insert", 10, seed=1, base_id=500, pool=1000)
        first, _, _ = planner.next()
        second, _, _ = planner.next()
        self.assertEqual(first[-1][0] + 1, second[0][0])

    def test_payloads_depend_on_seed_and_id_only(self) -> None:
        self.assertEqual(pb.payload(3, 42), pb.payload(3, 42))
        self.assertNotEqual(pb.payload(3, 42), pb.payload(3, 43))
        self.assertNotEqual(pb.payload(3, 42), pb.payload(4, 42))
        self.assertEqual(len(pb.payload(3, 42)), 32)

    def test_the_mixed_batch_holds_inserts_upserts_and_deletes(self) -> None:
        planner = pb.BatchPlanner("mixed", 100, seed=11, base_id=2_000_000, pool=400)
        planner.next()  # the first batch has nothing to delete yet
        inserts, upserts, deletes = planner.next()
        self.assertEqual(len(inserts) + len(upserts) + len(deletes), 100)
        self.assertTrue(inserts and upserts and deletes)

    def test_mixed_upserts_only_touch_the_seeded_pool(self) -> None:
        planner = pb.BatchPlanner("mixed", 200, seed=11, base_id=2_000_000, pool=400)
        for _ in range(5):
            _, upserts, _ = planner.next()
            for row_id, _, _ in upserts:
                self.assertGreaterEqual(row_id, 1)
                self.assertLessEqual(row_id, 400)

    def test_a_delete_never_repeats_an_id_and_only_removes_own_inserts(self) -> None:
        planner = pb.BatchPlanner("mixed", 100, seed=5, base_id=3_000_000, pool=400)
        inserted: set[int] = set()
        deleted: list[int] = []
        for _ in range(10):
            inserts, _, deletes = planner.next()
            inserted.update(row[0] for row in inserts)
            deleted.extend(deletes)
        self.assertEqual(len(deleted), len(set(deleted)), "an id was deleted twice")
        self.assertTrue(set(deleted) <= inserted, "a delete hit a row the writer never inserted")

    def test_an_invalid_planner_request_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            pb.BatchPlanner("truncate", 10, seed=1, base_id=1, pool=10)
        with self.assertRaises(ValueError):
            pb.BatchPlanner("insert", 0, seed=1, base_id=1, pool=10)

    def test_a_mixed_batch_is_row_count_neutral_after_the_warm_up(self) -> None:
        # Why the catch-up barrier cannot be "the counts are equal": once the delete
        # queue is warm, a mixed batch inserts exactly as many rows as it deletes,
        # so the source row count stops growing and an equal-count sample can also
        # be taken while the sink is still applying a backlog.
        for batch in pb.parse_batches(cfg.PG_BENCH_BATCHES):
            planner = pb.BatchPlanner("mixed", batch, seed=13, base_id=4_000_000, pool=4000)
            planner.next()  # warms the delete queue: nothing to delete yet
            net = 0
            for _ in range(10):
                inserts, _, deletes = planner.next()
                self.assertEqual(len(inserts), len(deletes), f"batch {batch} is not neutral")
                net += len(inserts) - len(deletes)
            self.assertEqual(net, 0, f"batch {batch} changed the source row count")


class CatchUpTest(unittest.TestCase):
    """The catch-up barrier: an equal row count is not proof of a drained sink."""

    def test_equal_counts_end_the_wait_when_no_verdict_is_asked_for(self) -> None:
        with mock.patch.object(pb, "src_count", return_value=10), \
                mock.patch.object(pb, "snk_count", return_value=10):
            elapsed = pb.wait_catchup(5, "snapshot")
        self.assertGreaterEqual(elapsed, 0.0)

    def test_equal_counts_do_not_end_the_wait_while_rows_still_differ(self) -> None:
        verdicts = [["missing on target: id 7"], []]
        calls: list[int] = []

        def verify() -> list[str]:
            calls.append(len(calls))
            return verdicts.pop(0)

        with mock.patch.object(pb, "src_count", return_value=100), \
                mock.patch.object(pb, "snk_count", return_value=100), \
                mock.patch.object(pb.time, "sleep", lambda _s: None):
            elapsed = pb.wait_catchup(30, "catch-up", verify=verify)
        self.assertEqual(len(calls), 2, "the wait ended on equal counts, not on a clean diff")
        self.assertGreaterEqual(elapsed, 0.0)


class MetricsTest(unittest.TestCase):
    def test_percentiles_interpolate_linearly(self) -> None:
        self.assertEqual(pb.percentile([4, 1, 3, 2], 50), 2.5)
        self.assertAlmostEqual(float(pb.percentile([1, 2, 3, 4], 95)), 3.85)
        self.assertEqual(pb.percentile([10], 95), 10)
        self.assertEqual(pb.percentile([1, 2, 3, 4], 0), 1)
        self.assertEqual(pb.percentile([1, 2, 3, 4], 100), 4)

    def test_percentile_of_nothing_is_none(self) -> None:
        self.assertIsNone(pb.percentile([], 50))

    def test_an_impossible_percentile_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            pb.percentile([1.0], 101)

    def test_latency_summary_reports_p50_p95_and_count(self) -> None:
        summary = pb.latency_summary([10.0, 20.0, 30.0, 40.0])
        self.assertEqual(summary["count"], 4)
        self.assertEqual(summary["p50"], 25.0)
        self.assertAlmostEqual(float(summary["p95"]), 38.5)
        self.assertEqual(summary["max"], 40.0)
        self.assertEqual(pb.latency_summary([])["p50"], None)

    def test_throughput_needs_a_positive_window(self) -> None:
        self.assertEqual(pb.throughput(1000, 2.0), 500.0)
        self.assertIsNone(pb.throughput(1000, 0))
        self.assertIsNone(pb.throughput(-1, 5))
        self.assertIsNone(pb.throughput(10, -1))

    def test_failure_rate_needs_attempts(self) -> None:
        self.assertEqual(pb.failure_pct(5, 200), 2.5)
        self.assertIsNone(pb.failure_pct(0, 0))

    def test_a_missing_number_renders_as_a_dash(self) -> None:
        self.assertEqual(pb.fmt_num(None), "-")
        self.assertEqual(pb.fmt_num(0), "0")
        self.assertEqual(pb.fmt_num(1234.5678), "1234.6")
        self.assertEqual(pb.fmt_num(1234.5678, 2), "1234.57")
        self.assertEqual(pb.fmt_num(float("inf")), "-")

    def test_the_pool_is_never_smaller_than_a_thousand_rows(self) -> None:
        self.assertEqual(pb.pool_size(100), 1000)
        self.assertEqual(pb.pool_size(20_000), 5000)


class PipelineRenderTest(unittest.TestCase):
    def _render(self, tmp: str, batch: int) -> str:
        with mock.patch.object(pb, "YAML", Path(tmp) / "pipeline.pg.bench.yaml"):
            return pb.render_yaml(batch).read_text(encoding="utf-8")

    def test_only_the_batch_size_changes_between_cells(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            small = self._render(tmp, 100)
            large = self._render(tmp, 5000)
        self.assertIn("write-batch-size: 100\n", small)
        self.assertIn("write-batch-size: 5000\n", large)
        self.assertEqual(
            small.replace("write-batch-size: 100", "write-batch-size: 5000"), large
        )

    def test_the_pipeline_owns_its_schema_checkpoint_and_server_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            text = self._render(tmp, 1000)
        self.assertIn(f"name: {cfg.PIPELINE_PG_BENCH_NAME}", text)
        self.assertIn("parallelism: 1", text)
        self.assertIn("ckpts-pg-bench", text)
        self.assertIn(f"server-id: {cfg.CDC_SERVER_ID_PG_BENCH}", text)
        self.assertIn(f"schema: {pb.SCHEMA}", text)
        # No other suite's namespace may appear in a benchmark pipeline.
        for foreign in (cfg.PIPELINE_PG_NAME, cfg.PIPELINE_PG_LOAD_NAME, "tp_pg_scale"):
            self.assertNotIn(foreign, text)

    def test_both_benchmark_tables_are_routed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            text = self._render(tmp, 100)
        self.assertIn(f"source-table: '{pb.SRC_DB}\\.{pb.DATA}'", text)
        self.assertIn(f"source-table: '{pb.SRC_DB}\\.{pb.PROBE}'", text)
        self.assertIn(f"sink-table: {pb.SCHEMA}.{pb.DATA}", text)
        self.assertIn(f"sink-table: {pb.SCHEMA}.{pb.PROBE}", text)
        self.assertIn(f"tables: '{pb.SRC_DB}\\.({pb.DATA}|{pb.PROBE})'", text)


class CellStatusTest(unittest.TestCase):
    def test_a_cell_without_checks_stays_not_run(self) -> None:
        cell = _cell()
        cell.finish()
        self.assertEqual(cell.status, pb.STATUS_NOT_RUN)

    def test_every_check_must_hold_for_a_pass(self) -> None:
        cell = _cell()
        cell.check("one", True, "ok")
        cell.check("two", True, "ok")
        cell.finish()
        self.assertEqual(cell.status, pb.STATUS_PASS)

    def test_one_failed_check_fails_the_cell(self) -> None:
        cell = _cell()
        cell.check("one", True, "ok")
        cell.check("two", False, "boom")
        cell.finish()
        self.assertEqual(cell.status, pb.STATUS_FAIL)


class ReportTest(unittest.TestCase):
    def _write(self, cells: list[pb.Cell], *, settings: dict[str, str] | None = None) -> str:
        with tempfile.TemporaryDirectory() as tmp:
            path = pb.write_report(
                cells,
                {
                    "generated_at": "2026-01-01 00:00:00",
                    "mode": "benchmark",
                    "command": "python -m harness.pg_bench_it",
                    "platform": "Linux-test",
                    "go": "1.25.0",
                    "pgx": "v5.11.0",
                },
                settings=settings,
                path=Path(tmp) / "bench.md",
            )
            return path.read_text(encoding="utf-8")

    def test_an_unrun_cell_carries_no_number(self) -> None:
        text = self._write(pb.build_matrix(pb.WORKLOADS, (100, 1000, 5000)))
        self.assertIn("**incomplete**", text)
        self.assertIn("pass 0 / fail 0 / not run 6 of 6", text)
        self.assertIn("| insert | 100 | - | - | - |", text)
        self.assertNotIn("✅ PASS", text)

    def test_retries_are_reported_as_not_instrumented(self) -> None:
        text = self._write([_cell()])
        self.assertIn("| n/a |", text)
        self.assertIn("does not emit a retry event", text)

    def test_a_pass_cell_renders_its_measured_numbers(self) -> None:
        cell = _cell()
        cell.metrics = {
            "rows": 20000, "throughput": 1234.5, "p50": 8.0, "p95": 33.0,
            "catchup_s": 12.0, "cpu_max": 55.0, "rss_max": 90.0, "conns_max": 3,
            "failure_pct": 0.0,
        }
        cell.check("every row is canonically equal to the source", True, "20000 rows")
        cell.finish()
        text = self._write([cell])
        self.assertIn("| insert | 100 | 20000 | 1234.5 | 8.0 | 33.0 | 12.0 |", text)
        self.assertIn("✅ PASS", text)
        self.assertIn("**complete**", text)

    def test_a_failed_cell_is_not_hidden(self) -> None:
        cell = _cell("mixed", 5000)
        cell.check("every row is canonically equal to the source", False, "3 rows differ")
        cell.finish()
        text = self._write([cell])
        self.assertIn("❌ FAIL", text)
        self.assertIn("3 rows differ", text)
        self.assertIn("**incomplete**", text)

    def test_the_metadata_section_names_versions_and_command(self) -> None:
        text = self._write([_cell()])
        self.assertIn("1.25.0", text)
        self.assertIn("v5.11.0", text)
        self.assertIn("Linux-test", text)
        self.assertIn("python -m harness.pg_bench_it", text)
        # PostgreSQL settings are marked as not collected when no server ran.
        self.assertIn("not collected", text)

    def test_the_report_defines_every_metric_it_prints(self) -> None:
        text = self._write([_cell()])
        for needle in ("**Throughput**", "**P50 / P95**", "**Catch-up**",
                       "**Connections**", "**Failure**"):
            self.assertIn(needle, text)
        self.assertIn("not an acceptance test", text)
        self.assertIn("no throughput", text)

    def test_a_benchmark_report_never_claims_a_target(self) -> None:
        text = self._write([_cell()])
        for forbidden in ("must reach", "target throughput", "rows/s target"):
            self.assertNotIn(forbidden, text)


class CliTest(unittest.TestCase):
    def test_list_prints_the_matrix_without_a_database(self) -> None:
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = pb.main(["--list"])
        self.assertEqual(code, pb.EXIT_OK)
        lines = [line for line in buffer.getvalue().splitlines() if line.strip()]
        self.assertEqual(len(lines), 6)

    def test_dry_run_writes_a_report_and_touches_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "bench.md"
            code = pb.main(["--dry-run", "--report", str(report)])
            text = report.read_text(encoding="utf-8")
        self.assertEqual(code, pb.EXIT_OK)
        self.assertIn("Mode: **dry run**", text)
        self.assertIn("not run 6 of 6", text)
        self.assertNotIn("✅ PASS", text)

    def test_dry_run_of_one_column_still_lists_the_whole_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "bench.md"
            code = pb.main(["--dry-run", "--batches", "100", "--report", str(report)])
            text = report.read_text(encoding="utf-8")
        self.assertEqual(code, pb.EXIT_OK)
        self.assertIn("not run 6 of 6", text)
        self.assertIn("dry run: not selected", text)

    def test_an_invalid_batch_size_exits_non_zero(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                pb.main(["--dry-run", "--batches", "0"])
        self.assertEqual(raised.exception.code, 2)

    def test_an_unreachable_server_is_env_blocked_not_a_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "bench.md"
            with mock.patch.object(pb, "BINARY", __file__), \
                 mock.patch.object(pb.pg_client, "ping", return_value=False):
                code = pb.main(["--report", str(report)])
            text = report.read_text(encoding="utf-8")
        self.assertEqual(code, pb.EXIT_BLOCKED)
        self.assertIn("ENV BLOCKED", text)
        self.assertNotIn("✅ PASS", text)


if __name__ == "__main__":
    unittest.main()
