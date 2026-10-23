package doris

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/PyRSA/go-cdc/common/schema"
)

// uniqueKeyPattern extracts the key column list from "UNIQUE KEY(...)" in a
// Doris SHOW CREATE TABLE statement.
var uniqueKeyPattern = regexp.MustCompile(`(?is)\bUNIQUE\s+KEY\s*\(([^)]*)\)`)

// tableModel is the Doris table model recovered from SHOW CREATE TABLE. DESC
// marks UNIQUE, DUPLICATE and AGGREGATE key columns with the same Key=YES, so
// the model cannot be inferred from DESC alone.
type tableModel struct {
	UniqueKey bool
	Keys      []string
}

// parseTableModel classifies the target model. A statement that is neither a
// recognised UNIQUE KEY table nor a recognised unsupported model is rejected:
// an unknown model must not be treated as compatible.
func parseTableModel(createTable string) (tableModel, error) {
	stripped := stripQuoted(createTable)
	if matches := uniqueKeyPattern.FindStringSubmatch(stripped); matches != nil {
		keys := splitKeyColumns(matches[1])
		if len(keys) == 0 {
			return tableModel{}, fmt.Errorf("sink.doris: UNIQUE KEY has no columns")
		}
		return tableModel{UniqueKey: true, Keys: keys}, nil
	}
	upper := strings.ToUpper(stripped)
	switch {
	case strings.Contains(upper, "DUPLICATE KEY"):
		return tableModel{}, fmt.Errorf("sink.doris: target table uses DUPLICATE KEY; a UNIQUE KEY table is required")
	case strings.Contains(upper, "AGGREGATE KEY"):
		return tableModel{}, fmt.Errorf("sink.doris: target table uses AGGREGATE KEY; a UNIQUE KEY table is required")
	}
	return tableModel{}, fmt.Errorf("sink.doris: target table model is not UNIQUE KEY or cannot be determined")
}

// stripQuoted blanks single- and double-quoted text so a column comment that
// happens to mention "UNIQUE KEY(...)" cannot be read as the table model.
// Backticks stay: they delimit the key identifiers being parsed.
func stripQuoted(sql string) string {
	var b strings.Builder
	b.Grow(len(sql))
	var quote byte
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if quote != 0 {
			if c == '\\' && quote == '\'' && i+1 < len(sql) {
				i++
				continue
			}
			if c == quote {
				quote = 0
			}
			b.WriteByte(' ')
			continue
		}
		if c == '\'' || c == '"' {
			quote = c
			b.WriteByte(' ')
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

func splitKeyColumns(list string) []string {
	var out []string
	for _, part := range strings.Split(list, ",") {
		part = strings.Trim(strings.TrimSpace(part), "`")
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

// validateTableModel enforces the design rule that an existing target must
// match the source primary key in both membership and order.
func validateTableModel(source schema.Table, model tableModel) error {
	if !model.UniqueKey {
		return fmt.Errorf("sink.doris: target %s.%s is not a UNIQUE KEY table", source.Database, source.Table)
	}
	if len(model.Keys) != len(source.PrimaryKeys) {
		return fmt.Errorf("sink.doris: target %s.%s UNIQUE KEY (%s) does not match source primary key (%s)",
			source.Database, source.Table, strings.Join(model.Keys, ", "), strings.Join(source.PrimaryKeys, ", "))
	}
	for i, key := range source.PrimaryKeys {
		if !strings.EqualFold(model.Keys[i], key) {
			return fmt.Errorf("sink.doris: target %s.%s UNIQUE KEY order (%s) does not match source primary key order (%s)",
				source.Database, source.Table, strings.Join(model.Keys, ", "), strings.Join(source.PrimaryKeys, ", "))
		}
	}
	return nil
}

// missingColumnsError lists source columns the target does not have yet. It is
// recoverable: the ADD COLUMN that created them is still ahead in the binlog, so
// CreateTable accepts the table and validateEvents rejects any row that actually
// carries an unknown column.
type missingColumnsError struct {
	Database string
	Table    string
	Columns  []string
}

func (e *missingColumnsError) Error() string {
	return fmt.Sprintf("sink.doris: target %s.%s is missing column %s", e.Database, e.Table, strings.Join(e.Columns, ", "))
}

// validateTarget checks the G-05 superset rules against the live Doris table.
// A target may have extra columns only when they are nullable, have a default,
// or are generated; every source column must exist with a compatible type.
// Missing source columns are reported as *missingColumnsError so the caller can
// distinguish a pending ADD COLUMN from a target that can never be written.
func validateTarget(source schema.Table, target []descColumn) error {
	byName := make(map[string]descColumn, len(target))
	for _, column := range target {
		byName[strings.ToLower(column.Name)] = column
	}
	var missing []string
	for _, column := range source.Columns {
		mapped, err := mapColumnType(column.Type)
		if err != nil {
			return fmt.Errorf("sink.doris: %s.%s column %s: %w", source.Database, source.Table, column.Name, err)
		}
		actual, ok := byName[strings.ToLower(column.Name)]
		if !ok {
			missing = append(missing, column.Name)
			continue
		}
		if !compatible(mapped, actual.Type) {
			return fmt.Errorf("sink.doris: target %s.%s column %s has type %s, source needs %s",
				source.Database, source.Table, column.Name, actual.Type, mapped.Doris)
		}
		if column.Nullable && !actual.Nullable {
			return fmt.Errorf("sink.doris: target %s.%s column %s is NOT NULL but the source column is nullable",
				source.Database, source.Table, column.Name)
		}
	}
	sourceKeys := make(map[string]struct{}, len(source.PrimaryKeys))
	for _, key := range source.PrimaryKeys {
		sourceKeys[strings.ToLower(key)] = struct{}{}
		actual, ok := byName[strings.ToLower(key)]
		if !ok {
			return fmt.Errorf("sink.doris: target %s.%s is missing primary key %s", source.Database, source.Table, key)
		}
		if !actual.Key {
			return fmt.Errorf("sink.doris: target %s.%s column %s is not a UNIQUE KEY column", source.Database, source.Table, key)
		}
	}
	if len(sourceKeys) == 0 {
		return fmt.Errorf("sink.doris: source table %s.%s has no primary key", source.Database, source.Table)
	}
	for _, column := range target {
		if !column.Key {
			continue
		}
		if _, ok := sourceKeys[strings.ToLower(column.Name)]; !ok {
			return fmt.Errorf("sink.doris: target %s.%s key column %s is not a source primary key",
				source.Database, source.Table, column.Name)
		}
	}
	for _, column := range target {
		if isSourceColumn(source, column.Name) {
			continue
		}
		if column.Key {
			continue
		}
		if column.Nullable || column.HasDefault || isGenerated(column.Extra) {
			continue
		}
		return fmt.Errorf("sink.doris: target %s.%s extra column %s is NOT NULL without a default",
			source.Database, source.Table, column.Name)
	}
	if len(missing) > 0 {
		return &missingColumnsError{Database: source.Database, Table: source.Table, Columns: missing}
	}
	return nil
}

func isSourceColumn(table schema.Table, name string) bool {
	for _, column := range table.Columns {
		if strings.EqualFold(column.Name, name) {
			return true
		}
	}
	return false
}

func isGenerated(extra string) bool {
	upper := strings.ToUpper(extra)
	return strings.Contains(upper, "GENERATED") || strings.Contains(upper, "VIRTUAL")
}

// validateExistingColumn decides whether a repeated ADD COLUMN is a safe no-op.
func validateExistingColumn(existing descColumn, column schema.Column) error {
	mapped, err := mapColumnType(column.Type)
	if err != nil {
		return err
	}
	if !compatible(mapped, existing.Type) {
		return fmt.Errorf("sink.doris: column %s already exists with type %s, source needs %s",
			column.Name, existing.Type, mapped.Doris)
	}
	if existing.Nullable != column.Nullable {
		return fmt.Errorf("sink.doris: column %s already exists with nullable=%t, source needs %t",
			column.Name, existing.Nullable, column.Nullable)
	}
	return nil
}
