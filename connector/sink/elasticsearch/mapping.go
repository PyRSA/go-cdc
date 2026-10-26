package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/PyRSA/go-cdc/common/schema"
)

// Canonical date formats written by this sink. The same strings are used as the
// Elasticsearch field format, so a created index always accepts our values.
const (
	dateFormat      = "yyyy-MM-dd"
	datetimeFormat  = "yyyy-MM-dd HH:mm:ss.SSSSSS"
	yearFormat      = "yyyy"
	stringKeyword   = "keyword"
	stringText      = "text"
	keywordMaxChars = 512
)

// esField is one generated mapping property.
type esField struct {
	Type    string `json:"type"`
	Format  string `json:"format,omitempty"`
	Dynamic *bool  `json:"dynamic,omitempty"`
}

// mappingField is the read-only view used for compatibility checks. Extra keys
// (analyzer, fields, ignore_above, …) are ignored on purpose.
type mappingField struct {
	Type       string         `json:"type"`
	Format     string         `json:"format"`
	Dynamic    any            `json:"dynamic"`
	Properties map[string]any `json:"properties"`
}

// columnType is the parsed information_schema.COLUMN_TYPE value.
type columnType struct {
	Raw      string
	Base     string
	Unsigned bool
	Length   int // display width, char length, or bit width; -1 when absent
	Scale    int // decimal scale; -1 when absent
	Fsp      int // fractional seconds precision
	Values   []string
}

// valueKind groups source types by the ES type family that can hold them.
type valueKind int

const (
	kindInteger valueKind = iota
	kindFloat
	kindDecimal
	kindString
	kindDate
	kindTime
	kindJSON
	kindBinary
	kindEnumSet
)

// parseColumnType parses COLUMN_TYPE text such as "bigint(20) unsigned",
// "decimal(10,2)", "enum('a','it”s')", or "datetime(6)".
func parseColumnType(raw string) (columnType, error) {
	out := columnType{Raw: raw, Length: -1, Scale: -1}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return out, errors.New("empty column type")
	}
	open := strings.IndexByte(trimmed, '(')
	closing := strings.LastIndexByte(trimmed, ')')
	if open < 0 && closing < 0 {
		fields := strings.Fields(trimmed)
		out.Base = strings.ToLower(fields[0])
		for _, field := range fields[1:] {
			switch strings.ToLower(field) {
			case "unsigned":
				out.Unsigned = true
			case "zerofill":
			default:
				return out, fmt.Errorf("unsupported type modifier %q in %q", field, raw)
			}
		}
		return out, nil
	}
	if open < 0 || closing < open {
		return out, fmt.Errorf("malformed type %q", raw)
	}
	out.Base = strings.ToLower(strings.TrimSpace(trimmed[:open]))
	for _, field := range strings.Fields(trimmed[closing+1:]) {
		switch strings.ToLower(field) {
		case "unsigned":
			out.Unsigned = true
		case "zerofill":
		default:
			return out, fmt.Errorf("unsupported type modifier %q in %q", field, raw)
		}
	}
	params := trimmed[open+1 : closing]
	switch out.Base {
	case "enum", "set":
		values, err := parseLabels(params)
		if err != nil {
			return out, fmt.Errorf("%v in %q", err, raw)
		}
		out.Values = values
	case "datetime", "timestamp", "time":
		if params != "" {
			n, err := strconv.Atoi(strings.TrimSpace(params))
			if err != nil || n < 0 || n > 6 {
				return out, fmt.Errorf("invalid fractional seconds precision %q in %q", params, raw)
			}
			out.Fsp = n
		}
	default:
		parts := strings.Split(params, ",")
		if n, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
			out.Length = n
		}
		if len(parts) > 1 {
			if n, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
				out.Scale = n
			}
		}
	}
	return out, nil
}

// parseLabels reads the quoted labels of an enum or set definition.
func parseLabels(input string) ([]string, error) {
	var out []string
	for i := 0; i < len(input); {
		for i < len(input) && (input[i] == ' ' || input[i] == ',') {
			i++
		}
		if i >= len(input) {
			break
		}
		if input[i] != '\'' {
			return nil, errors.New("malformed enum/set labels")
		}
		i++
		var label strings.Builder
		closed := false
		for i < len(input) {
			switch input[i] {
			case '\\':
				if i+1 >= len(input) {
					return nil, errors.New("malformed enum/set escape")
				}
				label.WriteByte(input[i+1])
				i += 2
			case '\'':
				if i+1 < len(input) && input[i+1] == '\'' {
					label.WriteByte('\'')
					i += 2
					continue
				}
				i++
				closed = true
			default:
				label.WriteByte(input[i])
				i++
			}
			if closed {
				break
			}
		}
		if !closed {
			return nil, errors.New("unterminated enum/set label")
		}
		out = append(out, label.String())
	}
	if len(out) == 0 {
		return nil, errors.New("empty enum/set definition")
	}
	return out, nil
}

// fieldForColumn maps one source column to its Elasticsearch type. Unsupported
// types fail here; nothing falls back to text.
func fieldForColumn(column schema.Column) (esField, error) {
	ct, err := parseColumnType(column.Type)
	if err != nil {
		return esField{}, fmt.Errorf("column %q has unsupported type %q: %v", column.Name, column.Type, err)
	}
	switch ct.Base {
	case "bool", "boolean":
		return esField{Type: "byte"}, nil
	case "tinyint":
		if ct.Length == 1 {
			return esField{Type: "byte"}, nil
		}
		if ct.Unsigned {
			return esField{Type: "short"}, nil
		}
		return esField{Type: "byte"}, nil
	case "smallint":
		if ct.Unsigned {
			return esField{Type: "integer"}, nil
		}
		return esField{Type: "short"}, nil
	case "mediumint":
		return esField{Type: "integer"}, nil
	case "int", "integer":
		if ct.Unsigned {
			return esField{Type: "long"}, nil
		}
		return esField{Type: "integer"}, nil
	case "bigint":
		if ct.Unsigned {
			return esField{Type: "unsigned_long"}, nil
		}
		return esField{Type: "long"}, nil
	case "float":
		return esField{Type: "float"}, nil
	case "double", "real":
		return esField{Type: "double"}, nil
	case "decimal", "numeric", "dec", "fixed":
		// Decimal values are kept as canonical text: keyword is the only mapping
		// that never loses precision.
		return esField{Type: stringKeyword}, nil
	case "date":
		return esField{Type: "date", Format: dateFormat}, nil
	case "datetime", "timestamp":
		return esField{Type: "date", Format: datetimeFormat}, nil
	case "year":
		return esField{Type: "date", Format: yearFormat}, nil
	case "time":
		return esField{Type: stringKeyword}, nil
	case "char", "varchar":
		if ct.Length >= 0 && ct.Length <= keywordMaxChars {
			return esField{Type: stringKeyword}, nil
		}
		return esField{Type: stringText}, nil
	case "tinytext", "text", "mediumtext", "longtext":
		return esField{Type: stringText}, nil
	case "json":
		dynamic := true
		return esField{Type: "object", Dynamic: &dynamic}, nil
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return esField{Type: "binary"}, nil
	case "enum", "set":
		return esField{Type: stringKeyword}, nil
	case "bit":
		return esField{}, fmt.Errorf(
			"column %q type %q is not supported: bit values are raw bytes and cannot round-trip through JSON",
			column.Name, column.Type)
	default:
		return esField{}, fmt.Errorf("column %q type %q is not supported", column.Name, column.Type)
	}
}

// deriveProperties builds the generated mapping for a table.
func deriveProperties(table schema.Table) (map[string]json.RawMessage, error) {
	props := make(map[string]json.RawMessage, len(table.Columns))
	for _, column := range table.Columns {
		field, err := fieldForColumn(column)
		if err != nil {
			return nil, fmt.Errorf("table %s.%s: %w", table.Database, table.Table, err)
		}
		raw, err := json.Marshal(field)
		if err != nil {
			return nil, fmt.Errorf("table %s.%s: column %q: %w", table.Database, table.Table, column.Name, err)
		}
		props[column.Name] = raw
	}
	return props, nil
}

// checkCompatibility fails unless every column can be written into props
// without changing its value or losing precision. Extra mapping fields are
// allowed; missing fields are not, because v1 never adds mapping fields.
func checkCompatibility(index string, table schema.Table, props map[string]json.RawMessage, rootDynamic string) error {
	for _, column := range table.Columns {
		raw, ok := props[column.Name]
		if !ok {
			return fmt.Errorf(
				"elasticsearch: index %q: column %q (%s) is missing from the mapping; v1 does not add mapping fields",
				index, column.Name, column.Type)
		}
		field, err := decodeMappingField(raw)
		if err != nil {
			return fmt.Errorf("elasticsearch: index %q: column %q: %w", index, column.Name, err)
		}
		if err := compatibleField(index, column, field, rootDynamic); err != nil {
			return err
		}
	}
	return nil
}

func decodeMappingField(raw json.RawMessage) (mappingField, error) {
	var out mappingField
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("invalid mapping: %w", err)
	}
	if out.Type == "" {
		if len(out.Properties) > 0 {
			out.Type = "object"
		} else {
			return out, errors.New("invalid mapping: missing type")
		}
	}
	return out, nil
}

func compatibleField(index string, column schema.Column, field mappingField, rootDynamic string) error {
	ct, err := parseColumnType(column.Type)
	if err != nil {
		return fmt.Errorf("elasticsearch: index %q: column %q: %v", index, column.Name, err)
	}
	target := field.Type
	conflict := func(reason string) error {
		return fmt.Errorf(
			"elasticsearch: index %q: column %q (%s) cannot be written to mapping type %q: %s",
			index, column.Name, column.Type, target, reason)
	}
	switch kindOf(ct) {
	case kindInteger:
		required, ok := requiredIntegerType(ct)
		if !ok {
			return conflict("unsupported integer type")
		}
		rank, isNumeric := integerRank(target)
		if !isNumeric {
			return conflict("expected an integer field")
		}
		if ct.Unsigned && target != "unsigned_long" && rank < required {
			return conflict("value range exceeds the field range")
		}
		if !ct.Unsigned && target == "unsigned_long" {
			return conflict("signed values cannot be stored in unsigned_long")
		}
		if !ct.Unsigned && rank < required {
			return conflict("value range exceeds the field range")
		}
	case kindFloat:
		if ct.Base == "float" {
			if target != "float" && target != "double" {
				return conflict("expected float or double")
			}
			return nil
		}
		if target != "double" {
			return conflict("double values need a double field")
		}
	case kindDecimal:
		if target != stringKeyword {
			return conflict("decimal values are written as text to keep full precision")
		}
	case kindString:
		if target != stringKeyword && target != stringText {
			return conflict("expected keyword or text")
		}
	case kindEnumSet:
		if target != stringKeyword {
			return conflict("enum/set labels are written as keyword")
		}
	case kindTime:
		if target != stringKeyword {
			return conflict("time values are written as keyword")
		}
	case kindDate:
		if target != "date" {
			return conflict("expected a date field")
		}
		want := dateFormat
		if ct.Base == "datetime" || ct.Base == "timestamp" {
			want = datetimeFormat
		}
		if ct.Base == "year" {
			want = yearFormat
		}
		if field.Format != want {
			if field.Format == "" {
				return conflict(fmt.Sprintf("date field has no explicit format; set \"format\": %q", want))
			}
			return conflict(fmt.Sprintf("date format %q cannot parse values written as %q", field.Format, want))
		}
	case kindJSON:
		if target != "object" {
			return conflict("JSON values are written as objects")
		}
		if rootDynamic == "strict" || rootDynamic == "false" {
			if dynamic, ok := field.Dynamic.(bool); !ok || !dynamic {
				return conflict("object field under a strict/false dynamic mapping needs \"dynamic\": true")
			}
		}
	case kindBinary:
		if target != "binary" {
			return conflict("binary values need a binary field")
		}
	}
	return nil
}

func kindOf(ct columnType) valueKind {
	switch ct.Base {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "bool", "boolean":
		return kindInteger
	case "float", "double", "real":
		return kindFloat
	case "decimal", "numeric", "dec", "fixed":
		return kindDecimal
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		return kindString
	case "date", "datetime", "timestamp", "year":
		return kindDate
	case "time":
		return kindTime
	case "json":
		return kindJSON
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return kindBinary
	case "enum", "set":
		return kindEnumSet
	default:
		return kindString
	}
}

// requiredIntegerType returns the narrowest ES integer type that holds the
// whole MySQL column range.
func requiredIntegerType(ct columnType) (int, bool) {
	switch ct.Base {
	case "bool", "boolean":
		return integerRankByte, true
	case "tinyint":
		if ct.Length == 1 || !ct.Unsigned {
			return integerRankByte, true
		}
		return integerRankShort, true
	case "smallint":
		if ct.Unsigned {
			return integerRankInteger, true
		}
		return integerRankShort, true
	case "mediumint":
		return integerRankInteger, true
	case "int", "integer":
		if ct.Unsigned {
			return integerRankLong, true
		}
		return integerRankInteger, true
	case "bigint":
		if ct.Unsigned {
			return integerRankUnsignedLong, true
		}
		return integerRankLong, true
	default:
		return 0, false
	}
}

const (
	integerRankByte = iota + 1
	integerRankShort
	integerRankInteger
	integerRankLong
	integerRankUnsignedLong
)

func integerRank(target string) (int, bool) {
	switch target {
	case "byte":
		return integerRankByte, true
	case "short":
		return integerRankShort, true
	case "integer":
		return integerRankInteger, true
	case "long":
		return integerRankLong, true
	case "unsigned_long":
		return integerRankUnsignedLong, true
	default:
		return 0, false
	}
}
