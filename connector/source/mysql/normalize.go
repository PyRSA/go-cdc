package mysql

import (
	"fmt"
	"strings"
	"time"
)

// Normalize makes snapshot values and binlog values comparable.
// Integers become int64, unsigned integers become uint64, and decimals and
// times become strings. NULL stays nil. Binary values stay byte slices.
func Normalize(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case int:
		return int64(typed)
	case int8:
		return int64(typed)
	case int16:
		return int64(typed)
	case int32:
		return int64(typed)
	case int64:
		return typed
	case uint:
		return uint64(typed)
	case uint8:
		return uint64(typed)
	case uint16:
		return uint64(typed)
	case uint32:
		return uint64(typed)
	case uint64:
		return typed
	case float32, float64:
		return fmt.Sprint(typed)
	case []byte:
		// Copy, and keep the empty value distinct from NULL: pgx writes a nil
		// slice as SQL NULL and an empty slice as an empty bytea, and MySQL's
		// X'' is a value rather than a missing one.
		if typed == nil {
			return nil
		}
		out := make([]byte, len(typed))
		copy(out, typed)
		return out
	case time.Time:
		// Prefer the location already attached (canal TIMESTAMP with
		// TimestampStringLocation / ParseTime). Falling back to the instant's
		// own zone keeps wall-clock aligned with server-time-zone.
		return typed.Format("2006-01-02 15:04:05.000000")
	default:
		return fmt.Sprint(typed)
	}
}

// NormalizeRow returns a new map with normalized values. mysqlTypes is the
// catalogue type of each column and may be nil; when it is present, a binary
// column is forced back into the byte slice the snapshot path produces.
func NormalizeRow(columns []string, values []any, mysqlTypes []string) (map[string]any, error) {
	if len(columns) != len(values) {
		return nil, fmt.Errorf("mysql: %d columns and %d values", len(columns), len(values))
	}
	if mysqlTypes != nil && len(mysqlTypes) != len(columns) {
		return nil, fmt.Errorf("mysql: %d columns and %d types", len(columns), len(mysqlTypes))
	}
	row := make(map[string]any, len(columns))
	for i, column := range columns {
		value := Normalize(values[i])
		if mysqlTypes != nil && isBinaryType(mysqlTypes[i]) {
			value = binaryValue(value)
		}
		row[column] = value
	}
	return row, nil
}

// isBinaryType reports whether a MySQL column holds raw bytes. It is the single
// source of truth for both capture paths: the snapshot (typedValue) and the
// binlog (NormalizeRow) must classify a column identically, because go-mysql
// hands BINARY(n) and VARBINARY(n) back as Go strings. Callers may pass either
// the raw catalogue value or an already normalized base name.
func isBinaryType(mysqlType string) bool {
	switch baseTypeName(mysqlType) {
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob":
		return true
	}
	return false
}

// baseTypeName normalizes an information_schema COLUMN_TYPE value to its bare
// type name: "VARBINARY(16)" -> "varbinary", "BIGINT UNSIGNED" -> "bigint".
// Both capture paths read the same spelling — the snapshot from
// information_schema, the binlog from canal's TableColumn.RawType — so they have
// to normalize it the same way.
func baseTypeName(mysqlType string) string {
	base := strings.ToLower(mysqlType)
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = base[:i]
	}
	base = strings.TrimSpace(base)
	base = strings.TrimSuffix(base, "unsigned")
	return strings.TrimSpace(base)
}

// binaryValue restores the snapshot shape of a binary column. go-mysql decodes
// BINARY(n) and VARBINARY(n) into a Go string, and pgx sends a string parameter
// as text, so the server's byteain() would read that value as an escape-format
// literal: it stops at the first NUL byte and treats a backslash as an
// introducer. Keeping the bytes as a slice sends them verbatim instead. NULL
// stays NULL and the empty value stays a non-nil empty slice.
func binaryValue(value any) any {
	if text, ok := value.(string); ok {
		out := make([]byte, len(text))
		copy(out, text)
		return out
	}
	return value
}
