package mysql

import (
	"fmt"

	"github.com/PyRSA/go-cdc/common/event"
)

// RowsToEvents converts one canal row callback into source events.
// mysqlTypes is the catalogue type of each column (canal's TableColumn.RawType)
// and is nil only for callers that have no catalogue; it decides whether a
// value is binary, because the binlog decoder cannot express that in the Go
// type alone.
// An update callback with several pairs becomes several u events, stepping by two.
func RowsToEvents(action, database, table string, columns, mysqlTypes []string, rows [][]any) ([]event.Event, error) {
	switch action {
	case "insert":
		return oneSided(event.OpCreate, database, table, columns, mysqlTypes, rows, false)
	case "delete":
		return oneSided(event.OpDelete, database, table, columns, mysqlTypes, rows, true)
	case "update":
		if len(rows)%2 != 0 {
			return nil, fmt.Errorf("mysql: update rows for %s.%s must be paired, got %d", database, table, len(rows))
		}
		var out []event.Event
		for i := 0; i < len(rows); i += 2 {
			before, err := NormalizeRow(columns, rows[i], mysqlTypes)
			if err != nil {
				return nil, err
			}
			after, err := NormalizeRow(columns, rows[i+1], mysqlTypes)
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

func oneSided(op event.Op, database, table string, columns, mysqlTypes []string, rows [][]any, before bool) ([]event.Event, error) {
	out := make([]event.Event, 0, len(rows))
	for _, row := range rows {
		image, err := NormalizeRow(columns, row, mysqlTypes)
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
