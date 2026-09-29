from scale.schemas import checksum_sql, create_table_sql, insert_batch, large_payload_sizes


def test_create_basic_has_pk():
    sql = create_table_sql("basic", "tp_scale_001")
    assert "tp_scale_001" in sql
    assert "PRIMARY KEY" in sql.upper()
    assert "tenant_id" in sql


def test_create_indexed_has_unique():
    sql = create_table_sql("indexed", "tp_scale_003")
    assert "UNIQUE" in sql.upper()
    assert "uk_external_id" in sql or "external_id" in sql


def test_checksum_mentions_crc():
    sql = checksum_sql("basic", "db_tp_scale_src_01", "tp_scale_001")
    assert "COUNT(*)" in sql.upper()
    assert "SUM(`id`)" in sql.replace(" ", "") or "SUM(id)" in sql.upper()


def test_create_wide_covers_common_types():
    sql = create_table_sql("wide", "tp_scale_002")
    upper = sql.upper()
    for token in ("BIGINT", "DECIMAL", "VARCHAR", "TEXT", "DATE", "TIME", "DATETIME", "TIMESTAMP", "JSON"):
        assert token in upper


def test_create_large_has_blob_and_text():
    sql = create_table_sql("large", "tp_scale_004")
    upper = sql.upper()
    assert "TEXT" in upper
    assert "BLOB" in upper


def test_checksum_wide_uses_c_label_crc():
    sql = checksum_sql("wide", "db_tp_scale_src_01", "tp_scale_002")
    compact = sql.replace(" ", "").upper()
    assert "CRC32(`C_LABEL`)" in compact or "CRC32(C_LABEL)" in compact
    assert "name_crc" in sql.lower()


def test_checksum_indexed_uses_external_id_crc():
    sql = checksum_sql("indexed", "db_tp_scale_src_01", "tp_scale_003")
    compact = sql.replace(" ", "").upper()
    assert "CRC32(`EXTERNAL_ID`)" in compact or "CRC32(EXTERNAL_ID)" in compact
    assert "name_crc" in sql.lower()
    assert " 0 AS name_crc" not in sql and "0 AS name_crc" not in sql


def test_large_payload_sizes_kb_not_mib():
    content_len, binary_len = large_payload_sizes()
    assert 1500 <= content_len <= 2500
    assert 500 <= binary_len <= 1500


def test_insert_batch_chunks_executemany():
    calls: list[int] = []

    class FakeCursor:
        def executemany(self, _sql, rows):
            calls.append(len(rows))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class FakeConn:
        committed = False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            self.committed = True

    conn = FakeConn()
    insert_batch(conn, "db_tp_scale_src_01", "tp_scale_001", "basic", 1, 1200, 1, 1)
    assert calls == [500, 500, 200]
    assert conn.committed

