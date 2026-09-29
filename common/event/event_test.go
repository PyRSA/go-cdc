package event

import "testing"

func TestCloneDoesNotShareRows(t *testing.T) {
	original := Event{
		Op: OpUpdate, Database: "db", Table: "users",
		SourceDatabase: "db", SourceTable: "users",
		Before:  map[string]any{"id": int64(1), "name": "a"},
		After:   map[string]any{"id": int64(1), "name": "b"},
		Columns: []string{"id", "name"},
		Keys:    []string{"id"},
	}
	copied := original.Clone()
	copied.After["name"] = "c"
	copied.Columns[0] = "renamed"
	if original.After["name"] != "b" {
		t.Fatalf("clone shares after map: %#v", original.After)
	}
	if original.Columns[0] != "id" {
		t.Fatalf("clone shares columns: %#v", original.Columns)
	}
	if original.SourceID() != "db.users" || original.SinkID() != "db.users" {
		t.Fatalf("ids: %s %s", original.SourceID(), original.SinkID())
	}
}
