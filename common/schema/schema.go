// Package schema describes tables and classifies MySQL DDL text.
package schema

// Column is one column of a sink or source table.
type Column struct {
	Name          string
	Type          string
	Nullable      bool
	AutoIncrement bool
}

// Table is a projected table definition used to create the sink table.
type Table struct {
	Database    string
	Table       string
	Columns     []Column
	PrimaryKeys []string
}

// Action is the kind of schema change a DDL statement performs.
type Action string

const (
	ActionCreate       Action = "create"
	ActionAddColumn    Action = "add-column"
	ActionDropColumn   Action = "drop-column"
	ActionRenameColumn Action = "rename-column"
	ActionModifyColumn Action = "modify-column"
	ActionDropTable    Action = "drop-table"
	ActionTruncate     Action = "truncate"
	ActionIndex        Action = "index"
	ActionRenameTable  Action = "rename-table"
	ActionOther        Action = "other"
)

// Change is one parsed DDL statement.
type Change struct {
	Action   Action
	Database string
	Table    string
	Column   string
	NewName  string
	DDL      string
}
