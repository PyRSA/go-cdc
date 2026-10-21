package stdout

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
)

func TestCRUDOneLinePerEvent(t *testing.T) {
	var buf bytes.Buffer
	sink := New(&buf)
	sink.SetSourceName("demo")
	err := sink.Write([]event.Event{{
		Op: event.OpUpdate, Database: "tests", Table: "users",
		SourceDatabase: "tests", SourceTable: "users",
		Before: map[string]any{"id": int64(1), "name": "alice"},
		After:  map[string]any{"id": int64(1), "name": "alice2"},
	}, {
		Op: event.OpRead, Database: "tests", Table: "users",
		SourceDatabase: "tests", SourceTable: "users",
		After: map[string]any{"id": int64(1), "name": "alice"},
	}, {
		Op: event.OpCreate, Database: "tests", Table: "users",
		SourceDatabase: "tests", SourceTable: "users",
		After: map[string]any{"id": int64(2), "name": "bob"},
	}, {
		Op: event.OpDelete, Database: "tests", Table: "users",
		SourceDatabase: "tests", SourceTable: "users",
		Before: map[string]any{"id": int64(1), "name": "alice"},
	}, {
		Op: event.OpDDL, Database: "tests", Table: "users",
		SourceDatabase: "tests", SourceTable: "users",
		DDL: "ALTER TABLE users ADD COLUMN age int",
	}})
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) != 5 {
		t.Fatalf("lines %d\n%s", len(lines), buf.String())
	}
	want := []struct {
		op            string
		before, after bool
	}{
		{"u", true, true},
		{"r", false, true},
		{"c", false, true},
		{"d", true, false},
		{"ddl", false, false},
	}
	for i, raw := range lines {
		var item map[string]any
		if err := json.Unmarshal(raw, &item); err != nil {
			t.Fatal(err)
		}
		if item["op"] != want[i].op {
			t.Fatalf("line %d op=%v want %s", i, item["op"], want[i].op)
		}
		src, _ := item["source"].(map[string]any)
		if src["name"] != "demo" || src["db"] != "tests" || src["table"] != "users" {
			t.Fatalf("line %d source=%v", i, src)
		}
		if want[i].op == "ddl" {
			if item["before"] != nil || item["after"] != nil {
				t.Fatalf("ddl has row images: %s", raw)
			}
			continue
		}
		hasBefore := item["before"] != nil
		hasAfter := item["after"] != nil
		if hasBefore != want[i].before {
			t.Fatalf("line %d before present=%v want %v: %s", i, hasBefore, want[i].before, raw)
		}
		if hasAfter != want[i].after {
			t.Fatalf("line %d after present=%v want %v: %s", i, hasAfter, want[i].after, raw)
		}
	}
}

func TestApplySchemaSourceTSMs(t *testing.T) {
	var buf bytes.Buffer
	sink := New(&buf)
	sink.SetSourceName("demo")
	if err := sink.ApplySchema(schema.Change{
		Database: "tests", Table: "users",
		DDL: "ALTER TABLE users ADD COLUMN age int", SourceTSMs: 42,
	}); err != nil {
		t.Fatal(err)
	}
	var item map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &item); err != nil {
		t.Fatal(err)
	}
	src, _ := item["source"].(map[string]any)
	if item["op"] != "ddl" || src["ts_ms"] != float64(42) {
		t.Fatalf("got %s", buf.String())
	}
}
