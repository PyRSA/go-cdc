"""Offline tests for the e2e target guard (`harness.es_it.assert_target`).

No cluster and no Docker: `assert_target` takes the cluster version as an
argument, so the version-mismatch rules can be checked without one. The fixture
part of the guard runs against the real templates on every call, so the passing
cases cover that too.
"""
from __future__ import annotations

import unittest

from . import config as cfg
from . import es_it


class AssertTargetTest(unittest.TestCase):
    def setUp(self) -> None:
        # The guard reads the same knobs the launcher exports; pin them per test
        # instead of depending on how the suite was started.
        self._saved = (cfg.ES_EXPECT_MAJOR, cfg.ES_SINK_VERSION)
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        cfg.ES_EXPECT_MAJOR, cfg.ES_SINK_VERSION = self._saved

    def _target(self, *, expect: str, sink: str) -> None:
        cfg.ES_EXPECT_MAJOR = expect
        cfg.ES_SINK_VERSION = sink

    def test_es7_run_accepts_a_7_cluster(self) -> None:
        self._target(expect="7", sink="7")
        es_it.assert_target("7.17.24")

    def test_es7_run_rejects_an_8_cluster(self) -> None:
        self._target(expect="7", sink="7")
        with self.assertRaises(RuntimeError) as ctx:
            es_it.assert_target("8.15.3")
        self.assertIn("expected Elasticsearch 7.x", str(ctx.exception))

    def test_es7_run_rejects_the_default_es8_sink_version(self) -> None:
        self._target(expect="7", sink="8")
        with self.assertRaises(RuntimeError) as ctx:
            es_it.assert_target("7.17.24")
        self.assertIn("ES_SINK_VERSION=8", str(ctx.exception))
        self.assertIn("ES_EXPECT_MAJOR=7", str(ctx.exception))

    def test_es8_run_stays_unchecked_without_a_version_expectation(self) -> None:
        self._target(expect="", sink="8")
        es_it.assert_target("8.15.3")

    def test_es9_run_accepts_a_9_cluster(self) -> None:
        self._target(expect="9", sink="9")
        es_it.assert_target("9.5.2")

    def test_es9_run_rejects_an_8_cluster(self) -> None:
        self._target(expect="9", sink="9")
        with self.assertRaises(RuntimeError) as ctx:
            es_it.assert_target("8.15.3")
        self.assertIn("expected Elasticsearch 9.x", str(ctx.exception))

    def test_es9_run_rejects_the_default_es8_sink_version(self) -> None:
        self._target(expect="9", sink="8")
        with self.assertRaises(RuntimeError) as ctx:
            es_it.assert_target("9.5.2")
        self.assertIn("ES_SINK_VERSION=8", str(ctx.exception))
        self.assertIn("ES_EXPECT_MAJOR=9", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
