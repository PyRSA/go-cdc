// Package event is the one change record shared by snapshot and binlog.
// An update is a single record: Before and After are siblings, not two events.
package event

// Op is the source operation. Stdout prints CRUD letters; MySQL upserts current state.
type Op string

const (
	// OpRead is a snapshot (full-load) row. Only After is set.
	OpRead Op = "r"
	// OpCreate is an incremental insert. Only After is set.
	OpCreate Op = "c"
	// OpUpdate is one update, with both images filled.
	OpUpdate Op = "u"
	// OpDelete is a delete. Only Before is set.
	OpDelete Op = "d"
	// OpDDL is a schema statement. DDL holds the original SQL.
	OpDDL Op = "ddl"
)

// Event is one source change after capture and before a sink adapts it.
// Database and Table are the sink names after routing. SourceDatabase and
// SourceTable stay on the capturing table.
type Event struct {
	Op             Op
	Database       string
	Table          string
	Before         map[string]any
	After          map[string]any
	DDL            string
	Columns        []string
	Keys           []string
	SourceDatabase string
	SourceTable    string
}

// SourceID returns the capturing table as database.table.
func (e Event) SourceID() string {
	return e.SourceDatabase + "." + e.SourceTable
}

// SinkID returns the routed table as database.table.
func (e Event) SinkID() string {
	return e.Database + "." + e.Table
}

// Clone copies the event so route fan-out cannot share row maps.
func (e Event) Clone() Event {
	e.Before = cloneMap(e.Before)
	e.After = cloneMap(e.After)
	e.Columns = append([]string(nil), e.Columns...)
	e.Keys = append([]string(nil), e.Keys...)
	return e
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
