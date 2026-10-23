package doris

import (
	"fmt"
	"strings"

	"github.com/PyRSA/go-cdc/common/schema"
)

// validateTarget checks the G-05 superset rules against the live Doris table.
// A target may have extra columns only when they are nullable, have a default,
// or are generated; every source column must exist with a compatible type.
func validateTarget(source schema.Table, target []descColumn) error {
	byName := make(map[string]descColumn, len(target))
	for _, column := range target {
		byName[strings.ToLower(column.Name)] = column
	}
	for _, column := range source.Columns {
		mapped, err := mapColumnType(column.Type)
		if err != nil {
			return fmt.Errorf("sink.doris: %s.%s column %s: %w", source.Database, source.Table, column.Name, err)
		}
		actual, ok := byName[strings.ToLower(column.Name)]
		if !ok {
			return fmt.Errorf("sink.doris: target %s.%s is missing column %s", source.Database, source.Table, column.Name)
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
