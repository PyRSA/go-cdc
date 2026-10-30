package postgresql

import (
	"fmt"
	"math"
	"strconv"
)

// ConvertValue maps one normalized source value onto a pgx argument.
//
// The source normalizes every MySQL value to nil, int64, uint64, string, or
// []byte. Both capture paths agree on integers, blobs, NULL, DECIMAL, and the
// date/time text, and they differ only for the types this sink refuses at table
// creation (enum, set, bit, whose binlog value is an integer while the snapshot
// value is the label or the packed bytes).
//
// PostgreSQL infers each parameter type from the target column, and pgx sends a
// string parameter in text format and a []byte parameter verbatim to a text or
// json column, so the server parses the text according to the column type.
// Values therefore never pass through float64 here.
func ConvertValue(value any) any {
	switch v := value.(type) {
	case nil:
		return nil
	case int64:
		return v
	case uint64:
		if v <= math.MaxInt64 {
			return int64(v)
		}
		// Keep the exact decimal text. A NUMERIC target accepts it exactly and a
		// BIGINT target rejects it with SQLSTATE 22003 instead of truncating.
		return strconv.FormatUint(v, 10)
	case string:
		return v
	case []byte:
		return v
	default:
		// Defensive: the source only emits the shapes above, and every other
		// shape is already stringified there.
		return fmt.Sprint(v)
	}
}
