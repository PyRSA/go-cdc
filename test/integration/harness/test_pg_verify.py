"""Offline unit tests for harness.pg_verify (no database, no driver).

    cd test/integration && python3 -m unittest harness.test_pg_verify
"""
from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from . import pg_verify as v


class CanonTest(unittest.TestCase):
    def test_int_accepts_decimal_and_bool_mysql_shapes(self) -> None:
        self.assertEqual(v.canon(Decimal("7"), "int"), 7)
        self.assertEqual(v.canon("7", "int"), 7)
        self.assertEqual(v.canon(True, "int"), 1)

    def test_int_rejects_fractional(self) -> None:
        with self.assertRaises(v.CompareError):
            v.canon(Decimal("7.5"), "int")

    def test_decimal_is_numeric_not_textual(self) -> None:
        self.assertEqual(v.canon(Decimal("1.5000"), "decimal"), Decimal("1.5"))
        self.assertEqual(v.canon("1.5000", "decimal"), v.canon(Decimal("1.5"), "decimal"))

    def test_char_drops_bpchar_padding(self) -> None:
        self.assertEqual(v.canon("ab      ", "char"), v.canon("ab", "char"))

    def test_text_keeps_trailing_spaces(self) -> None:
        self.assertNotEqual(v.canon("ab ", "text"), v.canon("ab", "text"))

    def test_bytes_from_memoryview_and_text(self) -> None:
        self.assertEqual(v.canon(memoryview(b"\x00\x01"), "bytes"), b"\x00\x01")
        self.assertEqual(v.canon(b"abc", "bytes"), b"abc")
        self.assertEqual(v.canon("abc", "bytes"), b"abc")

    def test_json_order_and_spacing_do_not_matter(self) -> None:
        a = v.canon('{"b": 1, "a": [1, 2]}', "json")
        b = v.canon({"a": [1, 2], "b": 1}, "json")
        self.assertEqual(a, b)
        self.assertNotEqual(a, v.canon('{"b": 2, "a": [1, 2]}', "json"))

    def test_aware_datetime_is_rebased_on_assert_zone(self) -> None:
        utc = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(v.canon(utc, "datetime"), datetime(2024, 1, 1, 8, 0))
        self.assertEqual(v.canon("2024-01-01T08:00:00", "datetime"), datetime(2024, 1, 1, 8, 0))
        self.assertEqual(v.canon(datetime(2024, 1, 1, 8, 0, tzinfo=v.ASSERT_TZ), "datetime"), datetime(2024, 1, 1, 8, 0))

    def test_date_from_string_and_datetime(self) -> None:
        self.assertEqual(v.canon("2024-05-06", "date"), date(2024, 5, 6))
        self.assertEqual(v.canon(datetime(2024, 5, 6, 12, 0), "date"), date(2024, 5, 6))

    def test_null_is_null(self) -> None:
        self.assertIsNone(v.canon(None, "text"))
        self.assertIsNone(v.canon(None, "int"))

    def test_bool_from_pg_text(self) -> None:
        self.assertTrue(v.canon("t", "bool"))
        self.assertFalse(v.canon("f", "bool"))


class DiffTest(unittest.TestCase):
    kinds = {"id": "int", "amount": "decimal", "note": "text"}

    def test_identical_rows_pass_regardless_of_order(self) -> None:
        src = [{"id": 1, "amount": Decimal("1.50"), "note": "a"}, {"id": 2, "amount": None, "note": None}]
        snk = [{"id": 2, "amount": None, "note": None}, {"id": 1, "amount": Decimal("1.5"), "note": "a"}]
        self.assertEqual(v.diff_rows(src, snk, ["id", "amount", "note"], self.kinds, ["id"]), [])

    def test_missing_and_extra_rows_are_named(self) -> None:
        src = [{"id": 1, "amount": 0, "note": "a"}, {"id": 2, "amount": 0, "note": "b"}]
        snk = [{"id": 1, "amount": 0, "note": "a"}, {"id": 3, "amount": 0, "note": "c"}]
        failures = v.diff_rows(src, snk, ["id", "amount", "note"], self.kinds, ["id"])
        self.assertTrue(any("missing on target" in f and "id=2" in f for f in failures), failures)
        self.assertTrue(any("extra on target" in f and "id=3" in f for f in failures), failures)

    def test_value_mismatch_names_the_column(self) -> None:
        src = [{"id": 1, "amount": Decimal("1.25"), "note": "a"}]
        snk = [{"id": 1, "amount": Decimal("1.26"), "note": "a"}]
        failures = v.diff_rows(src, snk, ["id", "amount", "note"], self.kinds, ["id"])
        self.assertEqual(len(failures), 1, failures)
        self.assertIn("column amount", failures[0])

    def test_duplicate_target_key_is_a_failure(self) -> None:
        src = [{"id": 1, "amount": 0, "note": "a"}]
        snk = [
            {"id": 1, "amount": 0, "note": "a"},
            {"id": 1, "amount": 0, "note": "a"},
        ]
        failures = v.diff_rows(src, snk, ["id", "amount", "note"], self.kinds, ["id"])
        self.assertTrue(any("duplicate key" in f for f in failures), failures)

    def test_composite_key(self) -> None:
        kinds = {"id": "int", "sub": "int", "v": "int"}
        src = [{"id": 1, "sub": 1, "v": 10}, {"id": 1, "sub": 2, "v": 20}]
        snk = [{"id": 1, "sub": 2, "v": 20}]
        failures = v.diff_rows(src, snk, ["id", "sub", "v"], kinds, ["id", "sub"])
        self.assertEqual(len(failures), 1, failures)
        self.assertIn("sub=1", failures[0])

    def test_missing_column_on_target(self) -> None:
        src = [{"id": 1, "amount": 0, "note": "a"}]
        snk = [{"id": 1, "amount": 0}]
        failures = v.diff_rows(src, snk, ["id", "amount", "note"], self.kinds, ["id"])
        self.assertTrue(any("missing column 'note'" in f for f in failures), failures)

    def test_missing_key_column_raises(self) -> None:
        with self.assertRaises(v.CompareError):
            v.diff_rows([{"amount": 1}], [], ["id", "amount"], self.kinds, ["id"])

    def test_float_tolerance_tight_but_not_bitwise(self) -> None:
        kinds = {"id": "int", "f": "float"}
        src = [{"id": 1, "f": 1.0 / 3.0}]
        snk = [{"id": 1, "f": (1.0 / 3.0) + 1e-16}]
        self.assertEqual(v.diff_rows(src, snk, ["id", "f"], kinds, ["id"]), [])
        snk = [{"id": 1, "f": (1.0 / 3.0) + 1e-9}]
        self.assertEqual(len(v.diff_rows(src, snk, ["id", "f"], kinds, ["id"])), 1)

    def test_summarize_is_bounded(self) -> None:
        self.assertEqual(v.summarize([]), "ok")
        text = v.summarize([f"f{i}" for i in range(9)], limit=3)
        self.assertIn("(+6 more)", text)


class KeyTest(unittest.TestCase):
    def test_pk_set_and_format(self) -> None:
        kinds = {"docid": "text", "sub": "int"}
        rows = [{"docid": "a", "sub": 1}, {"docid": "a", "sub": 2}]
        keys = v.pk_set(rows, ["docid", "sub"], kinds)
        self.assertEqual(keys, {("a", 1), ("a", 2)})
        self.assertIn("docid='a'", v.format_key(("a", 1), ["docid", "sub"]))

    def test_datetime_timedelta_guard(self) -> None:
        # A naive value must not be shifted; only aware values are rebased.
        naive = datetime(2024, 3, 1, 0, 0)
        self.assertEqual(v.canon(naive, "datetime"), naive)
        aware = naive.replace(tzinfo=timezone(timedelta(hours=0)))
        self.assertEqual(v.canon(aware, "datetime"), datetime(2024, 3, 1, 8, 0))


if __name__ == "__main__":
    unittest.main()
