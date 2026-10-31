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

// typedValue is the snapshot twin of Normalize: the same empty-versus-NULL rule
// decides whether a zero-length BINARY or BLOB arrives as an empty bytea.
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
