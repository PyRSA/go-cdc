package mysql

import (
	"database/sql"
	"testing"
)

// An empty MySQL binary value is a value, not NULL. Keeping it a non-nil empty
// slice is what stops the PostgreSQL sink from writing NULL (pgx encodes nil as
// NULL, an empty slice as an empty bytea) for a zero-length BINARY or BLOB column.
func TestNormalizeKeepsEmptyBinaryDistinctFromNull(t *testing.T) {
	if got := Normalize(nil); got != nil {
		t.Fatalf("NULL must stay nil, got %#v", got)
	}
	empty, ok := Normalize([]byte{}).([]byte)
	if !ok {
		t.Fatalf("an empty binary value must stay a byte slice")
	}
	if empty == nil {
		t.Fatalf("an empty binary value must not become NULL")
	}
	if len(empty) != 0 {
		t.Fatalf("an empty binary value must stay empty, got %v", empty)
	}
}

func TestNormalizeCopiesBinaryValues(t *testing.T) {
	source := []byte{0x01, 0x02}
	normalized := Normalize(source).([]byte)
	normalized[0] = 0xFF
	if source[0] != 0x01 {
		t.Fatalf("Normalize must not alias the driver buffer, source became %v", source)
	}
}

// TestTypedValueKeepsEmptyBinaryDistinctFromNull is the snapshot twin of
// Normalize: the same empty-versus-NULL rule decides whether a zero-length
// BINARY or BLOB arrives as an empty bytea.
func TestTypedValueKeepsEmptyBinaryDistinctFromNull(t *testing.T) {
	if got := typedValue("blob", nil); got != nil {
		t.Fatalf("NULL must stay nil, got %#v", got)
	}
	empty, ok := typedValue("blob", sql.RawBytes{}).([]byte)
	if !ok || empty == nil || len(empty) != 0 {
		t.Fatalf("an empty blob must stay a non-nil empty slice, got %#v", empty)
	}
	source := sql.RawBytes{0x01, 0x02}
	value := typedValue("varbinary(16)", source).([]byte)
	value[0] = 0xFF
	if source[0] != 0x01 {
		t.Fatalf("typedValue must not alias the driver buffer, source became %v", source)
	}
}

// TestIsBinaryType pins the shared classifier: the six byte-carrying types and
// nothing else. A text, json, bit or spatial column misclassified here would
// change the shape the sink receives, so every neighbour is listed.
func TestIsBinaryType(t *testing.T) {
	binary := []string{
		"binary(4)", "BINARY(16)", "varbinary(16)", "VARBINARY(255)",
		"blob", "tinyblob", "mediumblob", "longblob", " longblob ",
	}
	for _, mysqlType := range binary {
		if !isBinaryType(mysqlType) {
			t.Errorf("%q must classify as binary", mysqlType)
		}
	}
	text := []string{
		"", "text", "longtext", "mediumtext", "tinytext", "varchar(64)",
		"char(8)", "json", "bit(1)", "geometry", "point", "decimal(18,6)",
		"bigint unsigned", "int(11)", "datetime(6)",
	}
	for _, mysqlType := range text {
		if isBinaryType(mysqlType) {
			t.Errorf("%q must not classify as binary", mysqlType)
		}
	}
}

// TestBothPathsClassifyBinaryColumnsAlike pins the invariant the shared
// classifier exists for: a column type the snapshot hands over as []byte must
// also reach the binlog path as []byte, and one the snapshot stringifies must
// stay a string.
func TestBothPathsClassifyBinaryColumnsAlike(t *testing.T) {
	for _, mysqlType := range []string{
		"binary(4)", "VARBINARY(16)", "blob", "tinyblob", "mediumblob", "longblob",
		"text", "longtext", "varchar(64)", "char(8)", "json",
	} {
		snapshot := typedValue(mysqlType, sql.RawBytes("xy"))
		row, err := NormalizeRow([]string{"c"}, []any{"xy"}, []string{mysqlType})
		if err != nil {
			t.Fatalf("%s: %v", mysqlType, err)
		}
		_, snapshotBinary := snapshot.([]byte)
		_, binlogBinary := row["c"].([]byte)
		if snapshotBinary != binlogBinary {
			t.Fatalf("%s: snapshot []byte=%v but binlog []byte=%v", mysqlType, snapshotBinary, binlogBinary)
		}
	}
}

// TestNormalizeRowRejectsTypeLengthMismatch covers the new guard: a type list
// that does not line up with the columns would silently shift every decision by
// one, so it is an error rather than a best effort.
func TestNormalizeRowRejectsTypeLengthMismatch(t *testing.T) {
	_, err := NormalizeRow([]string{"a", "b"}, []any{"1", "2"}, []string{"text"})
	if err == nil {
		t.Fatal("expected an error when the type list is shorter than the columns")
	}
}

// TestNormalizeRowEmptyTypesSliceIsNotNoCatalogue guards the boundary the nil
// check could get wrong: an empty but non-nil slice claims to carry types, so it
// must be rejected instead of quietly disabling the binary fix.
func TestNormalizeRowEmptyTypesSliceIsNotNoCatalogue(t *testing.T) {
	if _, err := NormalizeRow([]string{"a"}, []any{"\xff"}, []string{}); err == nil {
		t.Fatal("an empty non-nil type list must not be treated as no catalogue")
	}
	// Nothing to classify: no columns, no types. Must not panic.
	row, err := NormalizeRow(nil, nil, []string{})
	if err != nil {
		t.Fatalf("no columns and no types must be accepted: %v", err)
	}
	if len(row) != 0 {
		t.Fatalf("got %#v, want an empty row", row)
	}
}
