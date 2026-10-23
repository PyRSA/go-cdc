package doris

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
)

// encodeBatch renders one table's events as JSON Lines. Every event must carry
// a complete After image for its declared column set; partial rows are rejected
// instead of being padded with NULL.
func encodeBatch(batch []event.Event, table schema.Table) ([]byte, error) {
	columns := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		columns[column.Name] = column
	}
	var buf bytes.Buffer
	for _, item := range batch {
		if len(item.Columns) == 0 {
			return nil, fmt.Errorf("sink.doris: %s event has no declared columns", item.SinkID())
		}
		if item.After == nil {
			return nil, fmt.Errorf("sink.doris: %s event has no After image", item.SinkID())
		}
		row := make(map[string]any, len(item.Columns))
		for _, name := range item.Columns {
			value, ok := item.After[name]
			if !ok {
				return nil, fmt.Errorf("sink.doris: %s event is missing column %s", item.SinkID(), name)
			}
			column, known := columns[name]
			if !known {
				return nil, fmt.Errorf("sink.doris: target %s has no column %s", schema.Qualified(table.Database, table.Table), name)
			}
			encoded, err := encodeValue(value, column)
			if err != nil {
				return nil, fmt.Errorf("sink.doris: %s column %s: %w", item.SinkID(), name, err)
			}
			row[name] = encoded
		}
		line, err := json.Marshal(row)
		if err != nil {
			return nil, fmt.Errorf("sink.doris: %s: %w", item.SinkID(), err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

type columnClass int

const (
	classGeneric columnClass = iota
	classJSON
	classNumeric
	classString
)

// resolveColumnType accepts both a MySQL source type (cached from CreateTable)
// and a Doris target type (read back with DESC).
func resolveColumnType(raw string) (columnType, bool) {
	if mapped, err := mapColumnType(raw); err == nil {
		return mapped, true
	}
	if mapped, err := mapDorisType(raw); err == nil {
		return mapped, true
	}
	return columnType{}, false
}

func classifyColumn(raw string) columnClass {
	mapped, ok := resolveColumnType(raw)
	if !ok {
		return classGeneric
	}
	switch {
	case mapped.isJSON():
		return classJSON
	case mapped.isNumeric():
		return classNumeric
	case mapped.isString():
		return classString
	default:
		return classGeneric
	}
}

func encodeValue(value any, column schema.Column) (any, error) {
	if value == nil {
		return nil, nil
	}
	class := classifyColumn(column.Type)
	switch typed := value.(type) {
	case bool:
		return typed, nil
	case int:
		return int64(typed), nil
	case int8:
		return int64(typed), nil
	case int16:
		return int64(typed), nil
	case int32:
		return int64(typed), nil
	case int64:
		return typed, nil
	case uint:
		return uint64(typed), nil
	case uint8:
		return uint64(typed), nil
	case uint16:
		return uint64(typed), nil
	case uint32:
		return uint64(typed), nil
	case uint64:
		return typed, nil
	case float32:
		return float64(typed), nil
	case float64:
		return typed, nil
	case time.Time:
		if mapped, ok := resolveColumnType(column.Type); ok && mapped.Base == "DATE" {
			return typed.Format("2006-01-02"), nil
		}
		return typed.Format("2006-01-02 15:04:05.000000"), nil
	case json.RawMessage:
		if class != classJSON {
			return nil, fmt.Errorf("unexpected JSON value")
		}
		if !json.Valid(typed) {
			return nil, fmt.Errorf("invalid JSON value")
		}
		return typed, nil
	case []byte:
		if class == classJSON {
			if !json.Valid(typed) {
				return nil, fmt.Errorf("invalid JSON value")
			}
			return json.RawMessage(typed), nil
		}
		return nil, fmt.Errorf("binary value is not supported in v1")
	case string:
		switch class {
		case classJSON:
			if !json.Valid([]byte(typed)) {
				return nil, fmt.Errorf("invalid JSON value")
			}
			return json.RawMessage(typed), nil
		case classNumeric:
			if _, err := strconv.ParseFloat(typed, 64); err != nil {
				return nil, fmt.Errorf("invalid numeric value %q", typed)
			}
			return json.Number(typed), nil
		default:
			return typed, nil
		}
	default:
		return nil, fmt.Errorf("unsupported value type %T", value)
	}
}
