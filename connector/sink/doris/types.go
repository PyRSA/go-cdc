package doris

import (
	"fmt"
	"strconv"
	"strings"
)

// columnType is one mapped Doris column type.
type columnType struct {
	// Doris is the full Doris type, e.g. BIGINT or DECIMAL(10,2).
	Doris string
	// Base is the upper-case base type without parameters.
	Base string
	// Precision and Scale apply to DECIMAL.
	Precision int
	Scale     int
	// Length applies to CHAR and VARCHAR.
	Length int
	// DateTimePrecision applies to DATETIME.
	DateTimePrecision int
}

func (t columnType) isJSON() bool { return t.Base == "JSONB" }
func (t columnType) isNumeric() bool {
	return t.Base == "FLOAT" || t.Base == "DOUBLE" || t.Base == "DECIMAL"
}
func (t columnType) isString() bool {
	return t.Base == "STRING" || t.Base == "VARCHAR" || t.Base == "CHAR"
}

// mapColumnType maps a MySQL column_type (for example "varchar(255)" or
// "bigint unsigned") to a Doris type. Unknown or unsafe types are rejected:
// v1 never truncates or silently converts a value.
func mapColumnType(raw string) (columnType, error) {
	base, unsigned, args, err := parseMySQLType(raw)
	if err != nil {
		return columnType{}, err
	}
	switch base {
	case "tinyint":
		// Unsigned source types widen so no value can overflow the target.
		if unsigned {
			return columnType{Doris: "SMALLINT", Base: "SMALLINT"}, nil
		}
		return columnType{Doris: "TINYINT", Base: "TINYINT"}, nil
	case "smallint":
		if unsigned {
			return columnType{Doris: "INT", Base: "INT"}, nil
		}
		return columnType{Doris: "SMALLINT", Base: "SMALLINT"}, nil
	case "mediumint":
		return columnType{Doris: "INT", Base: "INT"}, nil
	case "int", "integer":
		if unsigned {
			return columnType{Doris: "BIGINT", Base: "BIGINT"}, nil
		}
		return columnType{Doris: "INT", Base: "INT"}, nil
	case "bigint":
		if unsigned {
			return columnType{Doris: "LARGEINT", Base: "LARGEINT"}, nil
		}
		return columnType{Doris: "BIGINT", Base: "BIGINT"}, nil
	case "float":
		return columnType{Doris: "FLOAT", Base: "FLOAT"}, nil
	case "double", "real":
		return columnType{Doris: "DOUBLE", Base: "DOUBLE"}, nil
	case "decimal", "numeric":
		if len(args) != 2 {
			return columnType{}, fmt.Errorf("doris: decimal type %q needs precision and scale", raw)
		}
		precision, scale := args[0], args[1]
		if precision < 1 || precision > 38 {
			return columnType{}, fmt.Errorf("doris: decimal precision %d in %q is outside 1..38", precision, raw)
		}
		if scale < 0 || scale > precision {
			return columnType{}, fmt.Errorf("doris: decimal scale %d in %q is invalid", scale, raw)
		}
		return columnType{
			Doris: fmt.Sprintf("DECIMAL(%d,%d)", precision, scale), Base: "DECIMAL",
			Precision: precision, Scale: scale,
		}, nil
	case "char":
		if len(args) != 1 || args[0] < 1 {
			return columnType{}, fmt.Errorf("doris: char type %q needs a positive length", raw)
		}
		return columnType{Doris: fmt.Sprintf("CHAR(%d)", args[0]), Base: "CHAR", Length: args[0]}, nil
	case "varchar":
		if len(args) != 1 || args[0] < 1 {
			return columnType{}, fmt.Errorf("doris: varchar type %q needs a positive length", raw)
		}
		return columnType{Doris: fmt.Sprintf("VARCHAR(%d)", args[0]), Base: "VARCHAR", Length: args[0]}, nil
	case "tinytext", "text", "mediumtext", "longtext":
		return columnType{Doris: "STRING", Base: "STRING"}, nil
	case "date":
		return columnType{Doris: "DATE", Base: "DATE"}, nil
	case "datetime", "timestamp":
		precision := 0
		if len(args) == 1 {
			precision = args[0]
		}
		if precision < 0 || precision > 6 {
			return columnType{}, fmt.Errorf("doris: datetime precision %d in %q is outside 0..6", precision, raw)
		}
		return columnType{
			Doris: fmt.Sprintf("DATETIME(%d)", precision), Base: "DATETIME",
			DateTimePrecision: precision,
		}, nil
	case "year":
		return columnType{Doris: "SMALLINT", Base: "SMALLINT"}, nil
	case "json":
		return columnType{Doris: "JSONB", Base: "JSONB"}, nil
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return columnType{}, fmt.Errorf("doris: binary type %q is not supported in v1", raw)
	case "time":
		return columnType{}, fmt.Errorf("doris: time type %q has no lossless Doris mapping in v1", raw)
	case "enum", "set":
		return columnType{}, fmt.Errorf("doris: %s type %q has inconsistent snapshot/binlog values in v1", base, raw)
	case "bit":
		return columnType{}, fmt.Errorf("doris: bit type %q is not supported in v1", raw)
	case "geometry", "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon", "geometrycollection":
		return columnType{}, fmt.Errorf("doris: spatial type %q is not supported in v1", raw)
	default:
		return columnType{}, fmt.Errorf("doris: column type %q is not supported", raw)
	}
}

// parseMySQLType splits a MySQL column_type into base, unsigned, and
// parenthesised integer arguments. enum/set arguments are intentionally not
// parsed because those types are rejected before arguments are needed.
func parseMySQLType(raw string) (string, bool, []int, error) {
	text := strings.ToLower(strings.TrimSpace(raw))
	if text == "" {
		return "", false, nil, fmt.Errorf("doris: empty column type")
	}
	unsigned := strings.Contains(text, "unsigned")
	base := text
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = base[:i]
	}
	if fields := strings.Fields(base); len(fields) > 0 {
		base = fields[0]
	}
	base = strings.TrimSpace(base)
	var args []int
	if open := strings.IndexByte(text, '('); open >= 0 {
		if close := strings.LastIndexByte(text, ')'); close > open {
			for _, part := range strings.Split(text[open+1:close], ",") {
				part = strings.TrimSpace(part)
				if part == "" {
					continue
				}
				n, err := strconv.Atoi(part)
				if err != nil {
					// enum/set values are quoted strings; they are rejected later.
					return base, unsigned, nil, nil
				}
				args = append(args, n)
			}
		}
	}
	return base, unsigned, args, nil
}

// mapDorisType parses a type returned by Doris DESC / information_schema.
func mapDorisType(raw string) (columnType, error) {
	text := strings.ToUpper(strings.TrimSpace(raw))
	if text == "" {
		return columnType{}, fmt.Errorf("doris: empty target type")
	}
	base := text
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = base[:i]
	}
	base = strings.TrimSpace(base)
	args := parseTypeArgs(text)
	switch base {
	case "TINYINT", "SMALLINT", "INT", "BIGINT", "LARGEINT", "FLOAT", "DOUBLE", "STRING", "DATE", "JSONB", "BOOLEAN":
		return columnType{Doris: base, Base: base}, nil
	case "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
		// Doris CREATE uses STRING; DESC on 2.1.x often reports the alias TEXT.
		return columnType{Doris: "STRING", Base: "STRING"}, nil
	case "DECIMAL":
		if len(args) != 2 {
			return columnType{}, fmt.Errorf("doris: target decimal type %q has no precision", raw)
		}
		return columnType{
			Doris: fmt.Sprintf("DECIMAL(%d,%d)", args[0], args[1]), Base: "DECIMAL",
			Precision: args[0], Scale: args[1],
		}, nil
	case "VARCHAR", "CHAR":
		if len(args) != 1 {
			return columnType{}, fmt.Errorf("doris: target %s type %q has no length", base, raw)
		}
		return columnType{Doris: fmt.Sprintf("%s(%d)", base, args[0]), Base: base, Length: args[0]}, nil
	case "DATETIME":
		precision := 0
		if len(args) == 1 {
			precision = args[0]
		}
		return columnType{Doris: fmt.Sprintf("DATETIME(%d)", precision), Base: "DATETIME", DateTimePrecision: precision}, nil
	case "DATEV2":
		return columnType{Doris: "DATEV2", Base: "DATE"}, nil
	case "JSON":
		// Doris 2.1 treats JSON as an alias of JSONB; DESC may return either.
		return columnType{Doris: "JSONB", Base: "JSONB"}, nil
	case "DATETIMEV2":
		precision := 0
		if len(args) == 1 {
			precision = args[0]
		}
		return columnType{Doris: fmt.Sprintf("DATETIMEV2(%d)", precision), Base: "DATETIME", DateTimePrecision: precision}, nil
	default:
		return columnType{}, fmt.Errorf("doris: target type %q is not supported", raw)
	}
}

func parseTypeArgs(text string) []int {
	open := strings.IndexByte(text, '(')
	if open < 0 {
		return nil
	}
	close := strings.LastIndexByte(text, ')')
	if close <= open {
		return nil
	}
	var args []int
	for _, part := range strings.Split(text[open+1:close], ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			return nil
		}
		args = append(args, n)
	}
	return args
}

// compatible reports whether a source column mapped to src can be written to
// a target column of type target. Widening numeric/string types are accepted;
// narrowing and unrelated types are not.
func compatible(src columnType, target string) bool {
	dst, err := mapDorisType(target)
	if err != nil {
		return false
	}
	if src.Base == dst.Base {
		switch src.Base {
		case "VARCHAR", "CHAR":
			return dst.Length >= src.Length
		case "DECIMAL":
			// Fractional digits and integer digits must both fit.
			return dst.Scale >= src.Scale &&
				dst.Precision-dst.Scale >= src.Precision-src.Scale
		case "DATETIME":
			return dst.DateTimePrecision >= src.DateTimePrecision
		default:
			return true
		}
	}
	if src.Base == "CHAR" && dst.Base == "VARCHAR" {
		return dst.Length >= src.Length
	}
	if dst.Base == "STRING" && src.isString() {
		return true
	}
	if srcRank, ok := integerRank(src.Base); ok {
		if dstRank, ok := integerRank(dst.Base); ok {
			return dstRank >= srcRank
		}
		return false
	}
	if src.Base == "FLOAT" && dst.Base == "DOUBLE" {
		return true
	}
	return false
}

func integerRank(base string) (int, bool) {
	switch base {
	case "TINYINT":
		return 1, true
	case "SMALLINT":
		return 2, true
	case "INT":
		return 3, true
	case "BIGINT":
		return 4, true
	case "LARGEINT":
		return 5, true
	default:
		return 0, false
	}
}
