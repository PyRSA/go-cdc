package mysql

import (
	"testing"

	"github.com/PyRSA/go-cdc/runtime/checkpoint"
)

func TestNewlyAddedIsConfigMinusCaptured(t *testing.T) {
	state := checkpoint.State{
		CapturedTables: []string{"db.product", "db.user"},
		ExcludedTables: []string{"db.bad"},
		SnapshotTables: map[string]checkpoint.Position{
			"db.custom": {File: "mysql-bin.000001", Pos: 4},
		},
	}
	got := NewlyAdded([]string{"db.product", "db.user", "db.order", "db.custom", "db.bad"}, state)
	if len(got) != 1 || got[0] != "db.order" {
		t.Fatalf("%#v", got)
	}
}
