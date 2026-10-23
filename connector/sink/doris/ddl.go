package doris

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/PyRSA/go-cdc/common/schema"
)

// tableOptions carries the DDL attributes that are configurable. C2 in the
// design document is still open, so replication_num is omitted unless set and
// the merge-on-write property is left at the cluster default.
type tableOptions struct {
	Buckets        string
	ReplicationNum int
}

// createTableSQL builds a UNIQUE KEY table. Primary key order follows
// schema.Table.PrimaryKeys, which is the source catalog order.
func createTableSQL(table schema.Table, opts tableOptions) (string, error) {
	if table.Database == "" || table.Table == "" {
		return "", fmt.Errorf("doris: database and table are required")
	}
	if len(table.Columns) == 0 {
		return "", fmt.Errorf("doris: table %s has no columns", schema.Qualified(table.Database, table.Table))
	}
	if len(table.PrimaryKeys) == 0 {
		return "", fmt.Errorf("doris: table %s has no primary key", schema.Qualified(table.Database, table.Table))
	}
	known := make(map[string]struct{}, len(table.Columns))
	for _, column := range table.Columns {
		known[column.Name] = struct{}{}
	}
	for _, key := range table.PrimaryKeys {
		if _, ok := known[key]; !ok {
			return "", fmt.Errorf("doris: primary key %q is not a column of %s", key, schema.Qualified(table.Database, table.Table))
		}
	}

	// Doris requires key columns to be the leading columns, in KEY order.
	// Source ordinal order is not guaranteed to satisfy that.
	ordered := make([]schema.Column, 0, len(table.Columns))
	kept := make(map[string]struct{}, len(table.Columns))
	for _, key := range table.PrimaryKeys {
		column, ok := columnNamed(table.Columns, key)
		if !ok {
			return "", fmt.Errorf("doris: primary key %q is not a column of %s", key, schema.Qualified(table.Database, table.Table))
		}
		mapped, err := mapColumnType(column.Type)
		if err != nil {
			return "", fmt.Errorf("%s.%s: primary key %s: %w", table.Database, table.Table, key, err)
		}
		if !isKeyType(mapped.Base) {
			return "", fmt.Errorf("doris: primary key %q maps to %s, which cannot be a Doris key column", key, mapped.Doris)
		}
		ordered = append(ordered, column)
		kept[column.Name] = struct{}{}
	}
	for _, column := range table.Columns {
		if _, ok := kept[column.Name]; ok {
			continue
		}
		ordered = append(ordered, column)
	}

	var definitions []string
	for _, column := range ordered {
		def, err := columnDefinition(column, isPrimaryKey(column.Name, table.PrimaryKeys))
		if err != nil {
			return "", fmt.Errorf("%s.%s: %w", table.Database, table.Table, err)
		}
		definitions = append(definitions, "  "+def)
	}
	keys := make([]string, len(table.PrimaryKeys))
	for i, key := range table.PrimaryKeys {
		keys[i] = schema.Quote(key)
	}
	buckets := strings.TrimSpace(opts.Buckets)
	if buckets == "" {
		buckets = "auto"
	}
	if buckets != "auto" {
		n, err := strconv.Atoi(buckets)
		if err != nil || n < 1 {
			return "", fmt.Errorf("doris: buckets must be auto or a positive integer")
		}
	}
	var b strings.Builder
	b.WriteString("CREATE TABLE IF NOT EXISTS ")
	b.WriteString(schema.Qualified(table.Database, table.Table))
	b.WriteString(" (\n")
	b.WriteString(strings.Join(definitions, ",\n"))
	b.WriteString("\n) UNIQUE KEY(")
	b.WriteString(strings.Join(keys, ", "))
	b.WriteString(")\nDISTRIBUTED BY HASH(")
	b.WriteString(keys[0])
	b.WriteString(") BUCKETS ")
	b.WriteString(buckets)
	if opts.ReplicationNum > 0 {
		b.WriteString("\nPROPERTIES (\"replication_num\" = \"")
		b.WriteString(strconv.Itoa(opts.ReplicationNum))
		b.WriteString("\")")
	}
	return b.String(), nil
}

// addColumnSQL builds the Doris ALTER used for add.column. MySQL DDL text is
// never executed: it is a different dialect.
func addColumnSQL(database, table string, column schema.Column) (string, error) {
	def, err := columnDefinition(column, false)
	if err != nil {
		return "", err
	}
	return "ALTER TABLE " + schema.Qualified(database, table) + " ADD COLUMN " + def, nil
}

func columnDefinition(column schema.Column, key bool) (string, error) {
	if strings.TrimSpace(column.Name) == "" {
		return "", fmt.Errorf("doris: column name is empty")
	}
	mapped, err := mapColumnType(column.Type)
	if err != nil {
		return "", fmt.Errorf("column %s: %w", column.Name, err)
	}
	var b strings.Builder
	b.WriteString(schema.Quote(column.Name))
	b.WriteByte(' ')
	b.WriteString(mapped.Doris)
	if key || !column.Nullable {
		b.WriteString(" NOT NULL")
	} else {
		b.WriteString(" NULL")
	}
	// Default values are intentionally not copied: the design forbids guessing
	// SQL expressions from metadata, and CDC writes every projected column.
	if column.Comment != "" {
		b.WriteString(" COMMENT '")
		b.WriteString(escapeSQLString(column.Comment))
		b.WriteString("'")
	}
	return b.String(), nil
}

func isPrimaryKey(name string, keys []string) bool {
	for _, key := range keys {
		if key == name {
			return true
		}
	}
	return false
}

func columnNamed(columns []schema.Column, name string) (schema.Column, bool) {
	for _, column := range columns {
		if strings.EqualFold(column.Name, name) {
			return column, true
		}
	}
	return schema.Column{}, false
}

// isKeyType limits UNIQUE KEY columns to the Doris types valid in a key.
// STRING, JSONB, FLOAT and DOUBLE are value-only types.
func isKeyType(base string) bool {
	switch base {
	case "BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "LARGEINT",
		"DATE", "DATETIME", "CHAR", "VARCHAR", "DECIMAL":
		return true
	}
	return false
}

func escapeSQLString(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return strings.ReplaceAll(value, "'", "''")
}
