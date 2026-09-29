package mysql

import (
	"fmt"
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
		return append([]byte(nil), typed...)
	case time.Time:
		// Prefer the location already attached (canal TIMESTAMP with
		// TimestampStringLocation / ParseTime). Falling back to the instant's
		// own zone keeps wall-clock aligned with server-time-zone.
		return typed.Format("2006-01-02 15:04:05.000000")
	default:
		return fmt.Sprint(typed)
	}
}

// NormalizeRow returns a new map with normalized values.
func NormalizeRow(columns []string, values []any) (map[string]any, error) {
	if len(columns) != len(values) {
		return nil, fmt.Errorf("mysql: %d columns and %d values", len(columns), len(values))
	}
	row := make(map[string]any, len(columns))
	for i, column := range columns {
		row[column] = Normalize(values[i])
	}
	return row, nil
}
