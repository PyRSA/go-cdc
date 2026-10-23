package doris

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/PyRSA/go-cdc/common/schema"
)

// numericDefaultPattern matches the literal forms MySQL reports for numeric
// defaults. Anything else is treated as an untranslatable expression.
var numericDefaultPattern = regexp.MustCompile(`^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$`)

// dateDefaultPattern matches a MySQL DATE/DATETIME literal. MySQL 5.7 reports
// DEFAULT CURRENT_TIMESTAMP as plain text in COLUMN_DEFAULT, so a value that
// does not look like a date must not be quoted into a Doris default.
var dateDefaultPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2}(\.\d{1,6})?)?$`)

// tableOptions carries configurable CREATE TABLE attributes. Buckets defaults
// to auto; replication_num is omitted unless set; merge-on-write stays at the
// cluster default.
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
// never executed: it is a different dialect. Doris requires a usable default
// for a new NOT NULL value column, so that case fails before touching the FE.
func addColumnSQL(database, table string, column schema.Column) (string, error) {
	mapped, err := mapColumnType(column.Type)
	if err != nil {
		return "", fmt.Errorf("column %s: %w", column.Name, err)
	}
	if !column.Nullable {
		if _, ok, err := defaultClause(column, mapped); err != nil {
			return "", fmt.Errorf("column %s: %w", column.Name, err)
		} else if !ok {
			return "", fmt.Errorf("doris: add column %s is NOT NULL without a usable literal default; Doris requires a default for a new value column", column.Name)
		}
	}
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
	// Key columns cannot carry a Doris default; value columns copy a literal
	// source default so CREATE matches the source and a NOT NULL addition stays
	// valid. Expression defaults are omitted rather than guessed.
	if !key {
		clause, _, err := defaultClause(column, mapped)
		if err != nil {
			return "", fmt.Errorf("column %s: %w", column.Name, err)
		}
		b.WriteString(clause)
	}
	if column.Comment != "" {
		b.WriteString(" COMMENT '")
		b.WriteString(escapeSQLString(column.Comment))
		b.WriteString("'")
	}
	return b.String(), nil
}

// defaultClause renders the DEFAULT for one Doris value column and reports
// whether a clause was produced. MySQL metadata distinguishes a literal
// (COLUMN_DEFAULT) from a SQL expression (DEFAULT_GENERATED in EXTRA); the
// latter is never translated by inspecting its text, so it yields no clause and
// the caller decides whether a missing default is acceptable.
func defaultClause(column schema.Column, mapped columnType) (string, bool, error) {
	if !column.HasDefault || column.DefaultExpr {
		return "", false, nil
	}
	literal := strings.TrimSpace(column.Default)
	switch mapped.Base {
	case "TINYINT", "SMALLINT", "INT", "BIGINT", "LARGEINT", "FLOAT", "DOUBLE", "DECIMAL":
		if !numericDefaultPattern.MatchString(literal) {
			return "", false, fmt.Errorf("default %q is not a numeric literal", literal)
		}
		return " DEFAULT " + literal, true, nil
	case "DATE", "DATETIME":
		if !dateDefaultPattern.MatchString(literal) {
			return "", false, nil
		}
		return " DEFAULT '" + escapeSQLString(literal) + "'", true, nil
	case "CHAR", "VARCHAR", "STRING":
		return " DEFAULT '" + escapeSQLString(literal) + "'", true, nil
	}
	// JSONB and unrecognised bases have no verified Doris default form.
	return "", false, nil
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
