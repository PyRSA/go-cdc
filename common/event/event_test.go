package event

import (
	"testing"

	"github.com/PyRSA/go-cdc/common/schema"
)

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

func TestCloneCopiesSchemaChangeMetadata(t *testing.T) {
	original := Event{
		Op: OpDDL, Database: "db", Table: "users",
		SourceDatabase: "db", SourceTable: "users",
		Change: &schema.Change{
			Action: schema.ActionAddColumn, AddColumns: []string{"note"},
			ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
		},
	}
	copied := original.Clone()
	if copied.Change == nil || original.Change == nil || copied.Change == original.Change {
		t.Fatalf("change pointer not copied: %#v", copied.Change)
	}
	copied.Change.AddColumns[0] = "other"
	copied.Change.ColumnDef.Name = "other"
	if original.Change.AddColumns[0] != "note" {
		t.Fatalf("clone shares AddColumns: %#v", original.Change.AddColumns)
	}
	if original.Change.ColumnDef.Name != "note" {
		t.Fatalf("clone shares ColumnDef: %#v", original.Change.ColumnDef)
	}
}

func TestCloneKeepsNilChange(t *testing.T) {
	copied := (Event{Op: OpCreate}).Clone()
	if copied.Change != nil {
		t.Fatalf("nil change became %#v", copied.Change)
	}
}
