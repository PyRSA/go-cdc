// Package schemaevol decides which DDL reaches MySQL and which is stdout-only.
package schemaevol

import (
	"strings"

	"github.com/PyRSA/go-cdc/common/schema"
)

// Decision is what a sink should do with one DDL statement.
type Decision struct {
	Change  schema.Change
	Execute bool
}

// Decide classifies SQL and applies projection plus the drop/truncate switch.
// CREATE is not executed as raw SQL: the sink builds the table from the projected columns.
// A false Execute still leaves the statement available for stdout.
func Decide(sql string, projection []string, dropTruncate bool) Decision {
	change := schema.Classify(sql)
	decision := Decision{Change: change}
	switch change.Action {
	case schema.ActionCreate:
		decision.Execute = false
	case schema.ActionAddColumn, schema.ActionDropColumn, schema.ActionRenameColumn, schema.ActionModifyColumn:
		decision.Execute = columnVisible(change.Column, projection)
	case schema.ActionDropTable, schema.ActionTruncate:
		decision.Execute = dropTruncate
	default:
		decision.Execute = false
	}
	return decision
}

func columnVisible(column string, projection []string) bool {
	if column == "" || len(projection) == 0 {
		return true
	}
	for _, item := range projection {
		if strings.EqualFold(item, column) {
			return true
		}
	}
	return false
}

// RewriteTable replaces the statement's table reference with the routed sink table.
func RewriteTable(sql, database, table string) string {
	change := schema.Classify(sql)
	if change.Table == "" {
		return sql
	}
	qualified := schema.Qualified(database, table)
	source := change.Table
	if change.Database != "" {
		source = change.Database + "." + change.Table
	}
	rewritten := replaceTableToken(sql, source, strings.Trim(qualified, ""))
	return rewritten
}

func replaceTableToken(sql, source, sink string) string {
	// Prefer a backtick-qualified or plain occurrence of the original reference.
	patterns := []string{
		"`" + strings.ReplaceAll(source, ".", "`.`") + "`",
		source,
	}
	if !strings.Contains(source, ".") {
		patterns = append([]string{"`" + source + "`"}, patterns...)
	}
	for _, pattern := range patterns {
		if i := strings.Index(sql, pattern); i >= 0 {
			return sql[:i] + sink + sql[i+len(pattern):]
		}
	}
	return sql
}
