"""Offline unit tests for harness.pg_it log_errors (no database).

    cd tests/integration && python3 -m unittest sinks.postgresql.tests.test_pg_it_log_errors
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from ..harness import pg_it


class IsErrorLogLineTest(unittest.TestCase):
    def test_current_logfmt_error_level(self) -> None:
        line = (
            "time=2026-05-01T12:00:00.000+08:00 [ERROR] [pipeline] "
            "Pipeline failed err=\"sink write\""
        )
        self.assertTrue(pg_it._is_error_log_line(line))

    def test_legacy_level_error(self) -> None:
        self.assertTrue(
            pg_it._is_error_log_line(
                'time=2026-05-01T12:00:00.000Z level=ERROR msg="Pipeline failed"'
            )
        )

    def test_info_and_warn_are_not_errors(self) -> None:
        self.assertFalse(
            pg_it._is_error_log_line(
                "time=2026-05-01T12:00:00.000+08:00 [INFO] [pipeline] closing canal"
            )
        )
        self.assertFalse(
            pg_it._is_error_log_line(
                "time=2026-05-01T12:00:00.000+08:00 [WARN] [pipeline] slow batch"
            )
        )

    def test_free_text_error_in_message_is_not_a_level(self) -> None:
        # Business text mentioning ERROR must not count as a fatal log record.
        self.assertFalse(
            pg_it._is_error_log_line(
                "time=2026-05-01T12:00:00.000+08:00 [INFO] [pipeline] "
                "row note=ERROR_CODE_42 accepted"
            )
        )
        self.assertFalse(
            pg_it._is_error_log_line(
                "time=2026-05-01T12:00:00.000+08:00 [INFO] [sink] status=ERROR_RETRY"
            )
        )


class LogErrorsTest(unittest.TestCase):
    def test_prefers_pipeline_failed_over_shutdown_tail(self) -> None:
        body = "\n".join(
            [
                "time=2026-05-01T12:00:00.000+08:00 [INFO] [pipeline] started",
                "time=2026-05-01T12:00:01.000+08:00 [ERROR] [pipeline] Pipeline failed err=\"boom\"",
                "time=2026-05-01T12:00:01.100+08:00 [INFO] [canal] closing canal",
                "time=2026-05-01T12:00:01.200+08:00 [INFO] [canal] syncer is closing...",
                "time=2026-05-01T12:00:01.300+08:00 [INFO] [canal] syncer is closed",
            ]
        )
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "cdc.log"
            log_path.write_text(body, encoding="utf-8")
            with mock.patch.object(pg_it, "active_log", log_path):
                out = pg_it.log_errors()
        self.assertIn("Pipeline failed", out)
        self.assertIn("[ERROR]", out)
        self.assertNotIn("syncer is closed", out)

    def test_legacy_level_error_still_extracted(self) -> None:
        body = "\n".join(
            [
                'time=2026-05-01T12:00:00.000Z level=INFO msg="started"',
                'time=2026-05-01T12:00:01.000Z level=ERROR msg="Pipeline failed"',
                'time=2026-05-01T12:00:01.100Z level=INFO msg="closing canal"',
            ]
        )
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "cdc.log"
            log_path.write_text(body, encoding="utf-8")
            with mock.patch.object(pg_it, "active_log", log_path):
                out = pg_it.log_errors()
        self.assertIn("Pipeline failed", out)
        self.assertIn("level=ERROR", out)

    def test_fallback_names_missing_error_line(self) -> None:
        body = "\n".join(
            [
                "time=2026-05-01T12:00:00.000+08:00 [INFO] [pipeline] started",
                "time=2026-05-01T12:00:01.000+08:00 [INFO] [canal] closing canal",
            ]
        )
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "cdc.log"
            log_path.write_text(body, encoding="utf-8")
            with mock.patch.object(pg_it, "active_log", log_path):
                out = pg_it.log_errors()
        self.assertTrue(out.startswith("no ERROR log line"), out)


if __name__ == "__main__":
    unittest.main()
