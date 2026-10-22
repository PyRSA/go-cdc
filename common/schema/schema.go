// Package schema describes tables and classifies MySQL DDL text.
package schema

// Column is one column of a sink or source table.
type Column struct {
	Name          string
	Type          string
	Nullable      bool
	AutoIncrement bool
	// Default is the raw default from information_schema (unquoted literal text,
	// or a SQL expression when DefaultExpr is true). Meaningful only when
	// HasDefault is true; "" with HasDefault means an explicit empty-string default.
	Default string
	// HasDefault is true when COLUMN_DEFAULT was non-NULL (including "").
	HasDefault bool
	// DefaultExpr is true when Default is a SQL expression (e.g. MySQL
	// DEFAULT_GENERATED), not a quoted string literal.
	DefaultExpr bool
	Comment     string
	Charset     string
	Collation   string
}

// Index is a secondary or unique index (never the PRIMARY key).
type Index struct {
	Name   string
	Unique bool
	// Columns are index column names in order. Functional-index parts are omitted;
	// if that leaves Columns empty the index is dropped during discovery.
	Columns []string
	// Prefixes parallels Columns; 0 means the full column, >0 is a prefix length.
	Prefixes []int
	// Type is the MySQL index type (BTREE, HASH, FULLTEXT, SPATIAL, …).
	Type string
}

// Table is a projected table definition used to create the sink table.
type Table struct {
	Database    string
	Table       string
	Comment     string
	Charset     string
	Collation   string
	Columns     []Column
	PrimaryKeys []string
	Indexes     []Index
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
	// SourceDatabase / SourceTable are the capturing table when set (stdout/Kafka envelopes).
	// Database / Table remain the routed sink id so Kafka topicFor can match DML routing.
	SourceDatabase string
	SourceTable    string
	// SourceTSMs is the binlog event time in Unix milliseconds when known (same as event.SourceTSMs).
	// Zero means unavailable → JSON source.ts_ms is null. Stdout/Kafka copy it into DDL envelopes.
	SourceTSMs int64
}
