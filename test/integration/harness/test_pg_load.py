"""Offline checks for the PostgreSQL load suite (``pg_load_it``).

Nothing here touches a database: the tests pin the parts that can silently rot
without one - the typed-table deviation (two refused columns), the oracle column
map, and the rendered pipeline YAML.

``pg_load_it`` imports ``pymysql`` at module level, so on a checkout without the
driver a minimal shim is installed first; its ``connect()`` raises, which keeps an
accidental database call loud instead of silently passing.
"""
from __future__ import annotations

import sys
import tempfile
import types
import unittest
from pathlib import Path

try:  # pragma: no cover - depends on the checkout
    import pymysql  # noqa: F401
except ImportError:  # pragma: no cover - offline checkout
    _stub = types.ModuleType("pymysql")
    _stub.cursors = types.SimpleNamespace(DictCursor=object)

    def _no_connect(**kwargs):
        raise RuntimeError("this test must not open a MySQL connection")

    _stub.connect = _no_connect
    sys.modules["pymysql"] = _stub

from harness import config as cfg  # noqa: E402
from harness import load_it as load  # noqa: E402
from harness import pg_load_it as pg_load  # noqa: E402

REFUSED = ("c_enum", "c_bit")


class TypedTemplateTest(unittest.TestCase):
    def test_refused_columns_are_absent(self):
        for column in REFUSED:
            self.assertNotIn(column, pg_load.TYPED_COLUMNS)

    def test_every_other_column_is_kept_in_order(self):
        expected = tuple(c for c in pg_load.TYPED_SRC_COLUMNS if c not in REFUSED)
        self.assertEqual(pg_load.TYPED_COLUMNS, expected)
        self.assertEqual(len(pg_load.TYPED_COLUMNS), 13)

    def test_typed_row_matches_the_shared_generator(self):
        for i in (1, 7, 1234):
            full = load._typed_row(i)
            trimmed = pg_load.typed_row(i)
            self.assertEqual(len(trimmed), len(pg_load.TYPED_COLUMNS))
            for name, value in zip(pg_load.TYPED_SRC_COLUMNS, full):
                if name in REFUSED:
                    continue
                self.assertEqual(trimmed[pg_load.TYPED_COLUMNS.index(name)], value, f"row {i} {name}")

    def test_dropped_values_are_the_unsupported_types(self):
        row = dict(zip(pg_load.TYPED_SRC_COLUMNS, load._typed_row(1)))
        self.assertIn(row["c_enum"], ("red", "green", "blue"))
        self.assertIsInstance(row["c_bit"], int)

    def test_insert_lists_only_kept_columns(self):
        for column in REFUSED:
            self.assertNotIn(f"{column},", pg_load.INSERT_TYPED)
        self.assertIn("c_bool", pg_load.INSERT_TYPED)
        self.assertEqual(pg_load.INSERT_TYPED.count("%s"), len(pg_load.TYPED_COLUMNS))


class OracleMapTest(unittest.TestCase):
    def test_kinds_match_columns(self):
        for table, columns in pg_load.COLUMNS.items():
            self.assertEqual(tuple(pg_load.KINDS[table]), columns, table)

    def test_every_oracle_table_is_a_load_table(self):
        self.assertEqual(set(pg_load.COLUMNS), set(pg_load.TABLES) - {pg_load.LARGE_TABLE})

    def test_expected_pg_types_cover_the_typed_table(self):
        self.assertEqual(set(pg_load.EXPECTED_PG_TYPES), set(pg_load.TYPED_COLUMNS))


class RenderedYamlTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self._saved = pg_load.YAML
        pg_load.YAML = Path(self.tmp.name) / "pipeline.pg.load.yaml"
        self.addCleanup(self._restore)
        self.path = pg_load.render_yaml()
        self.text = self.path.read_text(encoding="utf-8")

    def _restore(self):
        pg_load.YAML = self._saved

    def test_source_captures_every_table_on_mysql(self):
        self.assertIn("type: mysql", self.text)
        self.assertIn(f"'{cfg.LOAD_SRC_DB}\\.({'|'.join(pg_load.TABLES)})'", self.text)

    def test_sink_targets_one_schema(self):
        self.assertIn("type: postgresql", self.text)
        self.assertIn(f"schema: {cfg.PG_LOAD_SNK_SCHEMA}", self.text)
        self.assertIn(f"write-batch-size: {cfg.PG_LOAD_BATCH_SIZE}", self.text)
        self.assertIn(f"write-batch-interval: {cfg.PG_LOAD_BATCH_INTERVAL}", self.text)

    def test_every_table_has_a_route(self):
        for table in pg_load.TABLES:
            self.assertIn(f"source-table: '{cfg.LOAD_SRC_DB}\\.{table}'", self.text)
            self.assertIn(f"sink-table: {cfg.PG_LOAD_SNK_SCHEMA}.{table}_out", self.text)
        self.assertEqual(self.text.count("- source-table:"), len(pg_load.TABLES))

    def test_no_refused_column_is_routed(self):
        for column in REFUSED:
            self.assertNotIn(column, self.text)

    def test_pipeline_and_checkpoint(self):
        self.assertIn(f"name: {cfg.PIPELINE_PG_LOAD_NAME}", self.text)
        self.assertIn(f"parallelism: {cfg.LOAD_PARALLELISM}", self.text)
        self.assertIn(f"file-path: {cfg.it_rel_path(cfg.CHECKPOINT_PG_LOAD_DIR)}", self.text)
        self.assertIn("scan.startup.mode: initial", self.text)

    def test_peak_shape_is_the_documented_one(self):
        self.assertEqual(pg_load.WORKERS, 4)
        self.assertEqual(pg_load.PEAK_SEC, 60)


class PlanTest(unittest.TestCase):
    def test_plan_ids(self):
        self.assertEqual([tp for tp, _ in pg_load.PLAN],
                         ["TP-PL01", "TP-PL02", "TP-PL03", "TP-PL04"])


if __name__ == "__main__":
    unittest.main()
