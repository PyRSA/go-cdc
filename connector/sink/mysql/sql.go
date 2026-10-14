// Package mysql writes current-state rows with upsert.
// It does not expand one update into -U and +U.
package mysql

import (
	"fmt"
	"strconv"
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
		// MySQL requires CHARACTER SET / COLLATE immediately after the type,
		// before NULL / NOT NULL / DEFAULT / AUTO_INCREMENT / COMMENT.
		parts := []string{schema.Quote(column.Name), column.Type}
		if column.Charset != "" {
			parts = append(parts, "CHARACTER SET", column.Charset)
		}
		if column.Collation != "" {
			parts = append(parts, "COLLATE", column.Collation)
		}
		if column.Nullable {
			parts = append(parts, "NULL")
		} else {
			parts = append(parts, "NOT NULL")
		}
		if column.AutoIncrement {
			parts = append(parts, "AUTO_INCREMENT")
		}
		if column.HasDefault {
			parts = append(parts, "DEFAULT", formatDefault(column))
		}
		if column.Comment != "" {
			parts = append(parts, "COMMENT", quoteString(column.Comment))
		}
		cols = append(cols, strings.Join(parts, " "))
	}
	if len(table.PrimaryKeys) > 0 {
		quoted := make([]string, len(table.PrimaryKeys))
		for i, key := range table.PrimaryKeys {
			quoted[i] = schema.Quote(key)
		}
		cols = append(cols, "PRIMARY KEY ("+strings.Join(quoted, ", ")+")")
	}
	for _, index := range table.Indexes {
		if index.Name == "" || len(index.Columns) == 0 {
			continue
		}
		quoted := make([]string, len(index.Columns))
		for i, name := range index.Columns {
			part := schema.Quote(name)
			if i < len(index.Prefixes) && index.Prefixes[i] > 0 {
				part = fmt.Sprintf("%s(%d)", part, index.Prefixes[i])
			}
			quoted[i] = part
		}
		kind := "KEY"
		if index.Unique {
			kind = "UNIQUE KEY"
		}
		// FULLTEXT / SPATIAL need their own keywords; BTREE/HASH stay as KEY/UNIQUE KEY.
		switch strings.ToUpper(index.Type) {
		case "FULLTEXT":
			kind = "FULLTEXT KEY"
		case "SPATIAL":
			kind = "SPATIAL KEY"
		}
		cols = append(cols, fmt.Sprintf("%s %s (%s)", kind, schema.Quote(index.Name), strings.Join(quoted, ", ")))
	}
	sql := "CREATE TABLE " + schema.Qualified(table.Database, table.Table) + " (" + strings.Join(cols, ", ") + ")"
	var opts []string
	if table.Charset != "" {
		opts = append(opts, "DEFAULT CHARSET="+table.Charset)
	}
	if table.Collation != "" {
		opts = append(opts, "COLLATE="+table.Collation)
	}
	if table.Comment != "" {
		opts = append(opts, "COMMENT="+quoteString(table.Comment))
	}
	if len(opts) > 0 {
		sql += " " + strings.Join(opts, " ")
	}
	return sql, nil
}

func formatDefault(column schema.Column) string {
	trim := strings.TrimSpace(column.Default)
	if column.DefaultExpr {
		if trim == "" {
			return "NULL"
		}
		// information_schema may omit outer parentheses for generated defaults.
		if strings.HasPrefix(trim, "(") {
			return trim
		}
		return "(" + trim + ")"
	}
	// Explicit empty-string default (HasDefault + "") must stay DEFAULT ''.
	if column.Default == "" {
		return quoteString("")
	}
	upper := strings.ToUpper(trim)
	switch {
	case upper == "NULL":
		return "NULL"
	case upper == "CURRENT_TIMESTAMP" || strings.HasPrefix(upper, "CURRENT_TIMESTAMP("):
		return trim
	case looksNumeric(trim):
		return trim
	default:
		return quoteString(trim)
	}
}

func looksNumeric(value string) bool {
	if value == "" {
		return false
	}
	_, err := strconv.ParseFloat(value, 64)
	return err == nil
}

func quoteString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
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
