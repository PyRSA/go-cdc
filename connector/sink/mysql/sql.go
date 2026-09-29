// Package mysql writes current-state rows with upsert.
// It does not expand one update into -U and +U.
package mysql

import (
	"fmt"
	"strings"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
)

// Statement is one SQL statement and its arguments.
type Statement struct {
	SQL  string
	Args []any
}

// Statements renders c/u/d as current-state writes.
// A key change deletes the old key and upserts the new row.
func Statements(item event.Event) ([]Statement, error) {
	switch item.Op {
	case event.OpCreate, event.OpRead:
		return []Statement{upsert(item, item.After)}, nil
	case event.OpUpdate:
		if keyChanged(item) {
			del, err := deleteByKey(item, item.Before)
			if err != nil {
				return nil, err
			}
			return []Statement{del, upsert(item, item.After)}, nil
		}
		return []Statement{upsert(item, item.After)}, nil
	case event.OpDelete:
		del, err := deleteByKey(item, item.Before)
		if err != nil {
			return nil, err
		}
		return []Statement{del}, nil
	default:
		return nil, fmt.Errorf("mysql: unsupported op %q", item.Op)
	}
}

// CreateTableSQL builds a sink table from the projected columns.
// AUTO_INCREMENT starts at 1. The source counter is not copied.
func CreateTableSQL(table schema.Table) (string, error) {
	if table.Database == "" || table.Table == "" {
		return "", fmt.Errorf("mysql: create table requires database and table")
	}
	if len(table.Columns) == 0 {
		return "", fmt.Errorf("mysql: create table %s has no columns", table.Table)
	}
	var cols []string
	for _, column := range table.Columns {
		nullSQL := "NULL"
		if !column.Nullable {
			nullSQL = "NOT NULL"
		}
		extra := ""
		if column.AutoIncrement {
			extra = " AUTO_INCREMENT"
		}
		cols = append(cols, fmt.Sprintf("%s %s %s%s", schema.Quote(column.Name), column.Type, nullSQL, extra))
	}
	if len(table.PrimaryKeys) > 0 {
		quoted := make([]string, len(table.PrimaryKeys))
		for i, key := range table.PrimaryKeys {
			quoted[i] = schema.Quote(key)
		}
		cols = append(cols, "PRIMARY KEY ("+strings.Join(quoted, ", ")+")")
	}
	return "CREATE TABLE " + schema.Qualified(table.Database, table.Table) + " (" + strings.Join(cols, ", ") + ")", nil
}

func upsert(item event.Event, row map[string]any) Statement {
	columns := item.Columns
	if len(columns) == 0 {
		columns = mapKeys(row)
	}
	quoted := make([]string, len(columns))
	holders := make([]string, len(columns))
	updates := make([]string, len(columns))
	args := make([]any, len(columns))
	for i, column := range columns {
		quoted[i] = schema.Quote(column)
		holders[i] = "?"
		updates[i] = schema.Quote(column) + "=VALUES(" + schema.Quote(column) + ")"
		args[i] = row[column]
	}
	sql := "INSERT INTO " + schema.Qualified(item.Database, item.Table) +
		" (" + strings.Join(quoted, ", ") + ") VALUES (" + strings.Join(holders, ", ") +
		") ON DUPLICATE KEY UPDATE " + strings.Join(updates, ", ")
	return Statement{SQL: sql, Args: args}
}

func deleteByKey(item event.Event, row map[string]any) (Statement, error) {
	if len(item.Keys) == 0 {
		return Statement{}, fmt.Errorf("mysql: %s.%s has no key for delete", item.Database, item.Table)
	}
	var where []string
	args := make([]any, len(item.Keys))
	for i, key := range item.Keys {
		where = append(where, schema.Quote(key)+"=?")
		args[i] = row[key]
	}
	sql := "DELETE FROM " + schema.Qualified(item.Database, item.Table) + " WHERE " + strings.Join(where, " AND ")
	return Statement{SQL: sql, Args: args}, nil
}

func keyChanged(item event.Event) bool {
	if len(item.Keys) == 0 {
		return false
	}
	for _, key := range item.Keys {
		if fmt.Sprint(item.Before[key]) != fmt.Sprint(item.After[key]) {
			return true
		}
	}
	return false
}

func mapKeys(row map[string]any) []string {
	out := make([]string, 0, len(row))
	for key := range row {
		out = append(out, key)
	}
	return out
}
