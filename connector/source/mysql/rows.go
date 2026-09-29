package mysql

import (
	"fmt"

	"github.com/PyRSA/go-cdc/common/event"
)

// RowsToEvents converts one canal row callback into source events.
// An update callback with several pairs becomes several u events, stepping by two.
func RowsToEvents(action, database, table string, columns []string, rows [][]any) ([]event.Event, error) {
	switch action {
	case "insert":
		return oneSided(event.OpCreate, database, table, columns, rows, false)
	case "delete":
		return oneSided(event.OpDelete, database, table, columns, rows, true)
	case "update":
		if len(rows)%2 != 0 {
			return nil, fmt.Errorf("mysql: update rows for %s.%s must be paired, got %d", database, table, len(rows))
		}
		var out []event.Event
		for i := 0; i < len(rows); i += 2 {
			before, err := NormalizeRow(columns, rows[i])
			if err != nil {
				return nil, err
			}
			after, err := NormalizeRow(columns, rows[i+1])
			if err != nil {
				return nil, err
			}
			out = append(out, event.Event{
				Op: event.OpUpdate, Database: database, Table: table,
				SourceDatabase: database, SourceTable: table,
				Before: before, After: after, Columns: append([]string(nil), columns...),
			})
		}
		return out, nil
	default:
		return nil, fmt.Errorf("mysql: unknown action %q", action)
	}
}

func oneSided(op event.Op, database, table string, columns []string, rows [][]any, before bool) ([]event.Event, error) {
	out := make([]event.Event, 0, len(rows))
	for _, row := range rows {
		image, err := NormalizeRow(columns, row)
		if err != nil {
			return nil, err
		}
		item := event.Event{
			Op: op, Database: database, Table: table,
			SourceDatabase: database, SourceTable: table,
			Columns: append([]string(nil), columns...),
		}
		if before {
			item.Before = image
		} else {
			item.After = image
		}
		out = append(out, item)
	}
	return out, nil
}
