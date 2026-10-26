"""Offline unit tests for the Elasticsearch test harness.

These run without Docker, MySQL, pymysql or a Go binary: they pin the parts the
integration suites rely on (index-name derivation, the `_id` contract, canonical
values, the MySQL/ES comparator, checksums, bounded waits and scroll paging).

Run from `test/integration`:

    python -m unittest harness.test_es_verify -v
"""
from __future__ import annotations

import datetime as dt
import hashlib
import struct
import unittest
import zlib
from decimal import Decimal

from . import es_verify as v
from .es_client import Doc, ESClient


def manual_doc_id(scope: str, pairs: list[tuple[str, str]]) -> str:
    """Independent encoding: uint32be(len) + bytes, scope first."""
    digest = hashlib.sha256()
    for part in [scope] + [item for pair in pairs for item in pair]:
        raw = part.encode("utf-8")
        digest.update(struct.pack(">I", len(raw)))
        digest.update(raw)
    return digest.hexdigest()


class TestIndexName(unittest.TestCase):
    def test_untouched_when_already_valid(self):
        self.assertEqual(v.normalize_index_name("db_tp_es_snk.tpe01_users"), "db_tp_es_snk.tpe01_users")

    def test_normalized_name_carries_a_hash(self):
        # Same vector the Go integration test asserts (index_test.go/lifecycle).
        self.assertEqual(v.normalize_index_name("Shop-DB.Order.Items"), "shop-db.order.items-476626c2208d")

    def test_leading_reserved_character_is_prefixed(self):
        name = v.normalize_index_name("_hidden")
        self.assertTrue(name.startswith("cdc_hidden-"), name)
        self.assertEqual(len(name), len("cdc_hidden-") + 12)

    def test_long_name_is_capped_at_255_bytes(self):
        raw = "A" * 300
        name = v.normalize_index_name(raw)
        self.assertLessEqual(len(name.encode("utf-8")), v.MAX_INDEX_NAME_BYTES)
        self.assertTrue(name.endswith("-" + v._short_hash(raw)))

    def test_explicit_name_wins(self):
        self.assertEqual(v.index_name_for("custom-index", "db", "tbl"), "custom-index")
        self.assertEqual(v.index_name_for("", "db", "tbl"), "db.tbl")


class TestDocID(unittest.TestCase):
    def test_matches_the_manual_encoding(self):
        pairs = [("id", "1"), ("tenant", "acme")]
        self.assertEqual(v.doc_id("db.snk", pairs), manual_doc_id("db.snk", pairs))

    def test_length_prefix_prevents_ambiguity(self):
        self.assertNotEqual(v.doc_id("s", [("a", "bc")]), v.doc_id("s", [("ab", "c")]))

    def test_expected_doc_id_uses_column_types(self):
        row = {"tenant_id": "t1", "seq": 5}
        got = v.expected_doc_id("snk", "tbl", {"tenant_id": "varchar(16)", "seq": "int"}, row)
        want = v.doc_id("snk.tbl", [("tenant_id", "t1"), ("seq", "5")])
        self.assertEqual(got, want)

    def test_expected_doc_id_canonicalizes_decimal_keys(self):
        row = {"id": Decimal("1.5")}
        got = v.expected_doc_id("snk", "tbl", {"id": "decimal(10,2)"}, row)
        self.assertEqual(got, v.doc_id("snk.tbl", [("id", "1.50")]))

    def test_null_primary_key_is_rejected(self):
        with self.assertRaises(ValueError):
            v.expected_doc_id("snk", "tbl", {"id": "bigint"}, {"id": None})


class TestCanonicalValues(unittest.TestCase):
    def test_decimal_keeps_its_scale(self):
        self.assertEqual(v.canonical_decimal("1.5", 2), "1.50")
        self.assertEqual(v.canonical_decimal("0001.200", 3), "1.200")
        self.assertEqual(v.canonical_decimal("-0.00", 2), "0.00")
        with self.assertRaises(ValueError):
            v.canonical_decimal("1.234", 2)

    def test_datetime_is_fixed_width(self):
        ct = v.parse_column_type("datetime(6)")
        self.assertEqual(v.canonical_date(ct, "2024-01-02 03:04:05"), "2024-01-02 03:04:05.000000")
        self.assertEqual(
            v.canonical_date(ct, dt.datetime(2024, 1, 2, 3, 4, 5, 123456)),
            "2024-01-02 03:04:05.123456",
        )

    def test_date_and_year(self):
        self.assertEqual(v.canonical_date(v.parse_column_type("date"), dt.date(2024, 1, 2)), "2024-01-02")
        self.assertEqual(v.canonical_date(v.parse_column_type("year"), 2024), "2024")

    def test_time_keeps_mysql_semantics(self):
        self.assertEqual(v.canonical_time("-01:02:03.000001"), "-1:02:03.000001")
        self.assertEqual(v.canonical_time(dt.timedelta(hours=838, minutes=59, seconds=59)), "838:59:59.000000")
        self.assertEqual(v.canonical_time(dt.timedelta(0)), "0:00:00.000000")

    def test_enum_and_set_labels(self):
        enum = v.parse_column_type("enum('new','done')")
        self.assertEqual(v.canonical_enum_set(enum, "2"), "done")
        with self.assertRaises(ValueError):
            v.canonical_enum_set(enum, "9")
        tags = v.parse_column_type("set('a','b','c')")
        self.assertEqual(v.canonical_enum_set(tags, "5"), "a,c")
        self.assertEqual(v.canonical_enum_set(tags, ""), "")

    def test_float32_uses_the_shortest_round_trip(self):
        self.assertEqual(v._canonical_float(1.5, 32), "1.5")
        self.assertEqual(v._canonical_float(1.1, 32), "1.1")
        self.assertEqual(v._canonical_float(2.25, 64), "2.25")

    def test_binary_is_base64(self):
        self.assertEqual(v.canonical_text("varbinary(32)", bytes([0x00, 0xFF, 0x10])), "AP8Q")


class TestMapping(unittest.TestCase):
    def test_derived_matrix(self):
        cases = {
            "tinyint(1)": "byte",
            "tinyint(4)": "byte",
            "tinyint unsigned": "short",
            "smallint": "short",
            "smallint unsigned": "integer",
            "int": "integer",
            "int unsigned": "long",
            "bigint": "long",
            "bigint unsigned": "unsigned_long",
            "float": "float",
            "double": "double",
            "decimal(18,6)": "keyword",
            "varchar(512)": "keyword",
            "varchar(600)": "text",
            "text": "text",
            "json": "object",
            "blob": "binary",
            "date": "date",
            "datetime(6)": "date",
            "year": "date",
            "time(6)": "keyword",
            "enum('a','b')": "keyword",
            "set('a','b')": "keyword",
        }
        for column_type, want in cases.items():
            with self.subTest(column_type=column_type):
                self.assertEqual(v.derived_field(column_type)["type"], want)

    def test_date_formats_are_explicit(self):
        self.assertEqual(v.derived_field("date")["format"], "yyyy-MM-dd")
        self.assertEqual(v.derived_field("datetime(6)")["format"], "yyyy-MM-dd HH:mm:ss.SSSSSS")
        self.assertEqual(v.derived_field("year")["format"], "yyyy")

    def test_bit_is_not_supported(self):
        with self.assertRaises(ValueError):
            v.derived_field("bit(8)")

    def test_field_lookup_is_case_insensitive(self):
        mapping = {"properties": {"ID": {"type": "long"}}}
        self.assertEqual(v.field_mapping(mapping, "id"), {"type": "long"})


class TestCompare(unittest.TestCase):
    def test_scalar_comparisons(self):
        cases = [
            ("bigint", 18446744073709551615, 18446744073709551615, True),
            ("int unsigned", 4294967295, 4294967295, True),
            ("decimal(12,2)", Decimal("10.5"), "10.50", True),
            ("decimal(12,2)", Decimal("10.5"), "10.5", False),
            ("double", 2.25, 2.25, True),
            ("float", 1.5, 1.5, True),
            ("varchar(16)", "x", "x", True),
            ("varchar(16)", None, None, True),
            ("varchar(16)", None, "x", False),
            ("varbinary(32)", b"\x00\xff\x10", "AP8Q", True),
            ("json", '{"b": 2, "a": 1}', {"a": 1, "b": 2}, True),
            ("json", '{"a": 1}', {"a": 2}, False),
            ("datetime(6)", dt.datetime(2024, 1, 2, 3, 4, 5, 123456), "2024-01-02 03:04:05.123456", True),
            ("date", dt.date(2024, 1, 2), "2024-01-02", True),
            ("time(6)", dt.timedelta(hours=12, minutes=34, seconds=56, microseconds=123456), "12:34:56.123456", True),
            ("enum('new','done')", "done", "done", True),
            ("set('a','b','c')", "a,c", "a,c", True),
            ("year", 2024, "2024", True),
        ]
        for column_type, mysql_value, es_value, want in cases:
            with self.subTest(column_type=column_type, mysql=mysql_value, es=es_value):
                self.assertIs(v.compare_value(column_type, mysql_value, es_value), want)

    def test_row_comparison_reports_every_problem(self):
        columns = {"id": "bigint", "name": "varchar(64)"}
        row = {"id": 1, "name": "x"}
        self.assertEqual(v.compare_row(columns, row, {"id": 1, "name": "x"}), [])

        problems = v.compare_row(columns, row, {"id": 2, "secret": "leak"})
        self.assertEqual(len(problems), 3, problems)
        self.assertTrue(any("secret" in item for item in problems))
        self.assertTrue(any("name" in item for item in problems))


class TestChecksum(unittest.TestCase):
    def test_triple_is_content_based(self):
        docs = {"a": {"id": 3, "v": "x"}, "b": {"id": 4, "v": "y"}}
        count, total, crc = v.doc_checksum(docs, crc_field="v")
        self.assertEqual(count, 2)
        self.assertEqual(total, 7)
        self.assertEqual(crc, zlib.crc32(b"x") + zlib.crc32(b"y"))

    def test_nulls_are_stable(self):
        docs = {"a": {"id": None, "v": None}}
        count, total, crc = v.doc_checksum(docs, crc_field="v")
        self.assertEqual((count, total, crc), (1, 0, zlib.crc32(b"")))

    def test_stream_checksum_matches_the_dict_version(self):
        docs = [{"id": 1, "v": "x"}, {"id": 2, "v": None}]
        self.assertEqual(
            v.stream_checksum(docs, crc_field="v"),
            v.doc_checksum({str(i): d for i, d in enumerate(docs)}, crc_field="v"),
        )

    def test_stream_checksum_accepts_doc_objects(self):
        docs = [Doc(id="5", index="idx", source={"id": 5, "name": "n5"})]
        self.assertEqual(v.stream_checksum(docs, crc_field="name"), (1, 5, zlib.crc32(b"n5")))


class TestWait(unittest.TestCase):
    def test_returns_immediately_when_true(self):
        self.assertGreaterEqual(v.wait_for(lambda: True, timeout=1.0, interval=0.01), 0.0)

    def test_times_out(self):
        with self.assertRaises(TimeoutError):
            v.wait_for(lambda: False, timeout=0.2, interval=0.05, desc="never")

    def test_retries_after_an_exception(self):
        calls = {"n": 0}

        def flaky() -> bool:
            calls["n"] += 1
            if calls["n"] < 3:
                raise RuntimeError("not yet")
            return True

        v.wait_for(flaky, timeout=2.0, interval=0.01)
        self.assertEqual(calls["n"], 3)


class _ScrollClient(ESClient):
    """Stub cluster: one hit, then an empty scroll page."""

    def __init__(self):
        super().__init__("http://es.invalid:9200")
        self.calls: list[tuple[str, str, object]] = []

    def request(self, method, path, body=None, *, ndjson=False, ok=(200, 201), allow=(), timeout=None):
        self.calls.append((method, path, body))
        if method == "POST" and path.endswith("/_search?scroll=2m"):
            return 200, {
                "_scroll_id": "s1",
                "hits": {"hits": [{"_id": "a", "_index": "idx", "_source": {}}]},
            }
        if method == "POST" and path == "_search/scroll":
            return 200, {"_scroll_id": "s1", "hits": {"hits": []}}
        if method == "DELETE" and path == "_search/scroll":
            return 200, {}
        raise AssertionError(f"unexpected {method} {path}")


class TestScroll(unittest.TestCase):
    def test_scan_pages_and_clears_the_scroll(self):
        client = _ScrollClient()
        docs = list(client.scan("idx"))
        self.assertEqual([doc.id for doc in docs], ["a"])
        self.assertTrue(any(method == "DELETE" and path == "_search/scroll" for method, path, _ in client.calls))


class _MGetClient(ESClient):
    def __init__(self):
        super().__init__("http://es.invalid:9200")
        self.chunks: list[list[str]] = []

    def request(self, method, path, body=None, *, ndjson=False, ok=(200, 201), allow=(), timeout=None):
        self.chunks.append(list(body["ids"]))
        return 200, {"docs": [{"_id": i, "found": True, "_source": {"v": i}} for i in body["ids"]]}


class _DeleteClient(ESClient):
    def __init__(self):
        super().__init__("http://es.invalid:9200")
        self.deleted: list[str] = []

    def list_indices(self, pattern="*"):
        return [".security-7", "db.t1", "db.t2"]

    def delete_index(self, index, *, ignore_missing=True):
        self.deleted.append(index)
        return True


class TestClients(unittest.TestCase):
    def test_get_docs_chunks_and_drops_missing(self):
        client = _MGetClient()
        docs = client.get_docs("idx", [str(i) for i in range(5)], chunk=2)
        self.assertEqual([len(chunk) for chunk in client.chunks], [2, 2, 1])
        self.assertEqual(sorted(docs), ["0", "1", "2", "3", "4"])

    def test_delete_indices_skips_system_indices(self):
        client = _DeleteClient()
        self.assertEqual(client.delete_indices("db.*"), ["db.t1", "db.t2"])


if __name__ == "__main__":
    unittest.main()
