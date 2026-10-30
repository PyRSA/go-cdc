package postgresql

import (
	"fmt"
	"strconv"
	"strings"
)

// Limits that decide whether a MySQL column definition can be rendered without
// losing or changing the source meaning.
const (
	// maxCharLength is the longest character(n) PostgreSQL stores (bpchar).
	maxCharLength = 10000
	// maxVarcharLength is the longest character varying(n) PostgreSQL stores.
	// Above it the column becomes text, which keeps every value.
	maxVarcharLength = 10485760
	// maxNumericPrecision and maxNumericScale bound numeric(p,s).
	maxNumericPrecision = 1000
	maxNumericScale     = 1000
	// maxTimePrecision is the fractional-seconds precision PostgreSQL accepts.
	maxTimePrecision = 6
)

// unverifiedValue is the reason given for a type whose arriving value is not
// proven in this release. The snapshot path and the binlog path can render the
// same column differently, so mapping it on the strength of one path would risk
// writing the other path's representation: an enum member index stored as text,
// a bit field widened wrongly, a set bitmask read as a member count. Failing
// at table creation keeps the pipeline honest because the target then has to be
// produced by hand, where the operator chooses the representation.
const unverifiedValue = "the value this column type reaches the sink with is not verified in this release"

// mapColumnType converts one MySQL column type into the PostgreSQL column type
// used by CREATE TABLE. mysqlType is the information_schema COLUMN_TYPE
// spelling (tinyint(4), bigint(20) unsigned, varchar(64), datetime(6)).
// An unmapped type is an error, never a fallback to text: text would accept the
// write and silently lose the source type.
func mapColumnType(mysqlType string) (string, error) {
	base, args, unsigned, err := splitMySQLType(mysqlType)
	if err != nil {
		return "", err
	}
	switch base {
	case "tinyint":
		// tinyint(1) stays SMALLINT. The source option that would make it a
		// boolean is rejected for every sink (parse.go rejectedExact), so no
		// Boolean meaning is implied here.
		return "SMALLINT", nil
	case "smallint":
		return "INTEGER", nil
	case "mediumint", "int", "integer":
		if unsigned {
			return "BIGINT", nil
		}
		return "INTEGER", nil
	case "bigint":
		if unsigned {
			// 18446744073709551615 needs 20 digits, one past bigint's 19.
			return "NUMERIC(20,0)", nil
		}
		return "BIGINT", nil
	case "decimal", "numeric", "dec", "fixed":
		return mapNumeric(args, mysqlType)
	case "float":
		// real is wider than every MySQL float, precision arguments included.
		return "REAL", nil
	case "double", "real", "double precision":
		// MySQL spells real as a synonym of double precision.
		return "DOUBLE PRECISION", nil
	case "char":
		return mapCharacter("CHAR", args, mysqlType, maxCharLength)
	case "varchar", "character varying":
		return mapCharacter("VARCHAR", args, mysqlType, maxVarcharLength)
	case "tinytext", "text", "mediumtext", "longtext":
		return "TEXT", nil
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		// bytea holds every length, including the empty string that MySQL's
		// CHAR(0) and blob types produce.
		return "BYTEA", nil
	case "date":
		return "DATE", nil
	case "datetime", "timestamp":
		return mapTimestamp(args, mysqlType)
	case "year":
		// year 0 is stored as 0, not as the MySQL 0000 sentinel. A target that
		// must distinguish them has to be created by hand.
		return "SMALLINT", nil
	case "enum", "set":
		// The two source paths disagree on the value: the snapshot renders the
		// member label as text, the binlog row event decodes an enum to its
		// member index and a set to its bitmask.
		return "", unsupportedType(mysqlType, unverifiedValue+" (the binlog path carries the member index or the bitmask, the snapshot path the label)")
	case "bit":
		// The same disagreement: the binlog row event gives an integer holding
		// the N bits, the snapshot gives the packed bytes the text protocol sent.
		return "", unsupportedType(mysqlType, unverifiedValue+" (the binlog path carries an integer of N bits, the snapshot path the packed bytes)")
	case "json":
		// The document reaches the sink as JSON text on both paths: the snapshot
		// renders the column as the text the text protocol returned, and the
		// binlog row event decodes the binary document back to that text. pgx
		// sends either a string or a []byte parameter to a json column verbatim,
		// without re-encoding it, so what the target stores is what the source
		// produced. json rather than jsonb is used because jsonb
		// normalises key order, drops duplicate keys, and re-renders numbers,
		// which would store a document that is not the one the source holds.
		return "JSON", nil
	case "time":
		return "", unsupportedType(mysqlType, "MySQL time spans negative and over-24-hour values that PostgreSQL time cannot hold")
	case "geometry", "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon", "geometrycollection":
		return "", unsupportedType(mysqlType, "spatial types need a PostGIS extension this release does not require")
	default:
		return "", unsupportedType(mysqlType, "no PostgreSQL equivalent is mapped yet")
	}
}

// splitMySQLType separates the base type name from its parenthesised arguments
// and reports the unsigned flag. zerofill is dropped: it pads display, not the
// stored value, so widening the numeric type covers it.
func splitMySQLType(mysqlType string) (base, args string, unsigned bool, err error) {
	text := strings.ToLower(strings.TrimSpace(mysqlType))
	if strings.Contains(text, "unsigned") {
		unsigned = true
		text = strings.ReplaceAll(text, "unsigned", "")
	}
	text = strings.ReplaceAll(text, "zerofill", "")
	text = strings.TrimSpace(text)
	open := strings.IndexByte(text, '(')
	if open < 0 {
		return text, "", unsigned, nil
	}
	closing := strings.LastIndexByte(text, ')')
	if closing < open {
		return "", "", false, fmt.Errorf("sink.postgresql: cannot parse MySQL column type %q", mysqlType)
	}
	return strings.TrimSpace(text[:open]), strings.TrimSpace(text[open+1 : closing]), unsigned, nil
}

// mapNumeric renders decimal and numeric. Both keep every digit of the source,
// so an out-of-range precision is an error rather than a silent narrowing.
func mapNumeric(args, mysqlType string) (string, error) {
	if args == "" {
		// MySQL defaults decimal to (10,0); information_schema always spells
		// the pair, so this only covers a hand-built definition.
		return "NUMERIC(10,0)", nil
	}
	parts := strings.Split(args, ",")
	if len(parts) > 2 {
		return "", fmt.Errorf("sink.postgresql: cannot parse MySQL column type %q", mysqlType)
	}
	if len(parts) == 1 {
		parts = append(parts, "0")
	}
	precision, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil || precision < 1 || precision > maxNumericPrecision {
		return "", fmt.Errorf("sink.postgresql: MySQL column type %q has a precision outside 1..%d", mysqlType, maxNumericPrecision)
	}
	scale, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil || scale < 0 || scale > maxNumericScale {
		return "", fmt.Errorf("sink.postgresql: MySQL column type %q has a scale outside 0..%d", mysqlType, maxNumericScale)
	}
	if scale > precision {
		return "", fmt.Errorf("sink.postgresql: MySQL column type %q has a scale larger than its precision", mysqlType)
	}
	if scale == 0 {
		return fmt.Sprintf("NUMERIC(%d)", precision), nil
	}
	return fmt.Sprintf("NUMERIC(%d,%d)", precision, scale), nil
}

// mapCharacter renders char and varchar. A length beyond the PostgreSQL limit
// becomes text, which holds the same values without the bound.
func mapCharacter(keyword, args, mysqlType string, limit int) (string, error) {
	if args == "" {
		// MySQL treats a bare char as char(1) and rejects a bare varchar, so
		// this only covers a hand-built definition.
		if keyword == "CHAR" {
			return "CHAR(1)", nil
		}
		return "TEXT", nil
	}
	length, err := strconv.Atoi(strings.TrimSpace(args))
	if err != nil || length < 1 {
		return "", fmt.Errorf("sink.postgresql: cannot parse MySQL column type %q", mysqlType)
	}
	if length > limit {
		return "TEXT", nil
	}
	return fmt.Sprintf("%s(%d)", keyword, length), nil
}

// mapTimestamp renders datetime and timestamp as timestamp without time zone.
// The source sends wall-clock strings for both, and the session TimeZone is set
// to the source server time zone, so no offset is applied on either side.
func mapTimestamp(args, mysqlType string) (string, error) {
	if args == "" {
		return "TIMESTAMP", nil
	}
	precision, err := strconv.Atoi(strings.TrimSpace(args))
	if err != nil || precision < 0 || precision > maxTimePrecision {
		return "", fmt.Errorf("sink.postgresql: MySQL column type %q has a fractional precision outside 0..%d", mysqlType, maxTimePrecision)
	}
	return fmt.Sprintf("TIMESTAMP(%d)", precision), nil
}

func unsupportedType(mysqlType, reason string) error {
	return fmt.Errorf("sink.postgresql: MySQL type %s is not supported: %s", mysqlType, reason)
}
