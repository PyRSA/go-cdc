package cdcjson

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
)

func TestMarshalDMLAndDDL(t *testing.T) {
	fixed := time.UnixMilli(1_780_000_000_100)
	opt := Options{SourceName: "dbserver1", Now: fixed}

	cases := []struct {
		name string
		ev   event.Event
		op   string
		ddl  bool
	}{
		{"read", event.Event{Op: event.OpRead, SourceDatabase: "demo_db", SourceTable: "users", After: map[string]any{"id": 1}}, "r", false},
		{"create", event.Event{Op: event.OpCreate, SourceDatabase: "demo_db", SourceTable: "users", After: map[string]any{"id": 1}}, "c", false},
		{"update", event.Event{
			Op: event.OpUpdate, SourceDatabase: "demo_db", SourceTable: "users",
			Before: map[string]any{"id": 1, "name": "Alice"}, After: map[string]any{"id": 1, "name": "Alice2"},
			SourceTSMs: 1_780_000_000_000,
		}, "u", false},
		{"delete", event.Event{Op: event.OpDelete, SourceDatabase: "demo_db", SourceTable: "users", Before: map[string]any{"id": 1}}, "d", false},
		{"ddl", event.Event{Op: event.OpDDL, SourceDatabase: "demo_db", SourceTable: "users", DDL: "ALTER TABLE users ADD COLUMN age INT", SourceTSMs: 1_780_000_000_000}, "ddl", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := Marshal(tc.ev, opt)
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]any
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			if got["op"] != tc.op {
				t.Fatalf("op=%v", got["op"])
			}
			if got["ts_ms"] != float64(fixed.UnixMilli()) {
				t.Fatalf("ts_ms=%v", got["ts_ms"])
			}
			src, _ := got["source"].(map[string]any)
			if src["connector"] != "mysql" || src["name"] != "dbserver1" || src["db"] != "demo_db" || src["table"] != "users" {
				t.Fatalf("source=%v", src)
			}
			if _, ok := src["server_id"]; ok {
				t.Fatal("source must not include server_id")
			}
			if tc.ev.SourceTSMs > 0 {
				if src["ts_ms"] != float64(tc.ev.SourceTSMs) {
					t.Fatalf("source.ts_ms=%v", src["ts_ms"])
				}
			} else if src["ts_ms"] != nil {
				t.Fatalf("want null source.ts_ms, got %v", src["ts_ms"])
			}
			if tc.ddl {
				if got["ddl"] != tc.ev.DDL {
					t.Fatalf("ddl=%v", got["ddl"])
				}
				if got["before"] != nil || got["after"] != nil {
					t.Fatalf("ddl before/after must be null: %s", raw)
				}
			}
		})
	}
}

func TestMarshalUnknownOp(t *testing.T) {
	_, err := Marshal(event.Event{Op: "x"}, Options{})
	if err == nil {
		t.Fatal("expected error")
	}
}
