package transform

import (
	"testing"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/composer/definition"
)

func TestProjectionFilterAndFirstRule(t *testing.T) {
	set, err := Compile([]definition.Transform{
		{SourceTable: `tests\.users`, Projection: "id, name", Filter: "id > 1 AND name LIKE 'a%'", PrimaryKeys: "id"},
		{SourceTable: `tests\.users`, Projection: "id"},
	})
	if err != nil {
		t.Fatal(err)
	}
	kept, ok := set.Apply(event.Event{
		Op: event.OpCreate, Database: "tests", Table: "users",
		SourceDatabase: "tests", SourceTable: "users",
		After: map[string]any{"id": int64(2), "name": "alice", "age": int64(3)},
	})
	if !ok || kept.After["age"] != nil && len(kept.Columns) != 2 {
		t.Fatalf("kept %#v ok %v", kept, ok)
	}
	if _, extra := kept.After["age"]; extra {
		t.Fatalf("age leaked: %#v", kept.After)
	}
	if kept.Columns[0] != "id" || kept.Columns[1] != "name" || kept.Keys[0] != "id" {
		t.Fatalf("order %#v keys %#v", kept.Columns, kept.Keys)
	}
	_, ok = set.Apply(event.Event{
		Op: event.OpCreate, Database: "tests", Table: "users",
		SourceDatabase: "tests", SourceTable: "users",
		After: map[string]any{"id": int64(1), "name": "alice"},
	})
	if ok {
		t.Fatal("filter should drop id 1")
	}
}

func TestFilterFallOutRetractsAsDelete(t *testing.T) {
	set, err := Compile([]definition.Transform{{
		SourceTable: `tests\.users`,
		Projection:  "id, name, city",
		Filter:      "city = 'hz'",
	}})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := set.Apply(event.Event{
		Op: event.OpUpdate, Database: "tests", Table: "users",
		SourceDatabase: "tests", SourceTable: "users",
		Before: map[string]any{"id": int64(3), "name": "ann", "city": "hz", "amount": "1"},
		After:  map[string]any{"id": int64(3), "name": "ann", "city": "bj", "amount": "1"},
	})
	if !ok {
		t.Fatal("fall-out should emit a retract, not drop")
	}
	if got.Op != event.OpDelete {
		t.Fatalf("op=%q want delete", got.Op)
	}
	if got.After != nil {
		t.Fatalf("after should be nil: %#v", got.After)
	}
	if got.Before["city"] != "hz" || got.Before["id"] != int64(3) {
		t.Fatalf("before=%#v", got.Before)
	}
	if _, hasAmount := got.Before["amount"]; hasAmount {
		t.Fatalf("projection should drop amount: %#v", got.Before)
	}
}

func TestFilterFallInEmitsCreate(t *testing.T) {
	set, err := Compile([]definition.Transform{{
		SourceTable: `tests\.users`,
		Filter:      "city = 'hz'",
	}})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := set.Apply(event.Event{
		Op: event.OpUpdate, SourceDatabase: "tests", SourceTable: "users",
		Before: map[string]any{"id": int64(2), "city": "sh"},
		After:  map[string]any{"id": int64(2), "city": "hz"},
	})
	if !ok {
		t.Fatal("fall-in should emit create")
	}
	if got.Op != event.OpCreate {
		t.Fatalf("op=%q want create", got.Op)
	}
	if got.Before != nil || got.After["city"] != "hz" {
		t.Fatalf("got %#v", got)
	}
}

func TestFilterStayOutDropsUpdate(t *testing.T) {
	set, err := Compile([]definition.Transform{{
		SourceTable: `tests\.users`,
		Filter:      "city = 'hz'",
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, ok := set.Apply(event.Event{
		Op: event.OpUpdate, SourceDatabase: "tests", SourceTable: "users",
		Before: map[string]any{"id": int64(2), "city": "sh"},
		After:  map[string]any{"id": int64(2), "city": "bj"},
	})
	if ok {
		t.Fatal("stay-out update should drop")
	}
}

func TestFilterForms(t *testing.T) {
	set, err := Compile([]definition.Transform{{
		SourceTable: `t\.t`,
		Filter:      "name = 'a''b' OR age IS NULL OR city IN ('hz', 'sh') OR title LIKE 'go\\_cdc%'",
	}})
	if err != nil {
		t.Fatal(err)
	}
	cases := []map[string]any{
		{"name": "a'b", "age": int64(1), "city": "bj", "title": "x"},
		{"name": "z", "age": nil, "city": "bj", "title": "x"},
		{"name": "z", "age": int64(1), "city": "hz", "title": "x"},
		{"name": "z", "age": int64(1), "city": "bj", "title": "go_cdc-job"},
	}
	for _, row := range cases {
		_, ok := set.Apply(event.Event{
			Op: event.OpCreate, SourceDatabase: "t", SourceTable: "t", After: row,
		})
		if !ok {
			t.Fatalf("dropped %#v", row)
		}
	}
}

func TestUnknownProjectionColumn(t *testing.T) {
	set, err := Compile([]definition.Transform{{SourceTable: `db\.t`, Projection: "id, missing"}})
	if err != nil {
		t.Fatal(err)
	}
	err = set.Validate("db", "t", []string{"id", "name"})
	if err == nil || !contains(err.Error(), "missing") {
		t.Fatal(err)
	}
}

func TestDecimalCompare(t *testing.T) {
	set, err := Compile([]definition.Transform{{SourceTable: `db\.t`, Filter: "amount >= 10"}})
	if err != nil {
		t.Fatal(err)
	}
	_, ok := set.Apply(event.Event{
		Op: event.OpCreate, SourceDatabase: "db", SourceTable: "t",
		After: map[string]any{"amount": "10.50"},
	})
	if !ok {
		t.Fatal("decimal string should compare numerically")
	}
}

func contains(s, part string) bool {
	return len(s) >= len(part) && (s == part || len(part) == 0 || index(s, part) >= 0)
}

func index(s, part string) int {
	for i := 0; i+len(part) <= len(s); i++ {
		if s[i:i+len(part)] == part {
			return i
		}
	}
	return -1
}
