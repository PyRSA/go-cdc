package doris

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
)

func encodeTable() schema.Table {
	return schema.Table{
		Database: "db",
		Table:    "orders",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "name", Type: "varchar(32)", Nullable: true},
			{Name: "amount", Type: "decimal(10,2)", Nullable: true},
			{Name: "doc", Type: "json", Nullable: true},
			{Name: "note", Type: "text", Nullable: true},
			{Name: "active", Type: "tinyint(1)", Nullable: false},
		},
		PrimaryKeys: []string{"id"},
	}
}

func TestEncodeBatch(t *testing.T) {
	table := encodeTable()
	batch := []event.Event{{
		Op: event.OpCreate, Database: "db", Table: "orders",
		Columns: []string{"id", "name", "amount", "doc", "note", "active"},
		After: map[string]any{
			"id": int64(7), "name": `a"b`, "amount": "1.50",
			"doc": `{"k":1}`, "note": nil, "active": int64(1),
		},
	}}
	body, err := encodeBatch(batch, table)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if !strings.HasSuffix(text, "\n") || strings.Count(text, "\n") != 1 {
		t.Fatalf("expected one JSON line: %q", text)
	}
	for _, want := range []string{
		`"id":7`,
		`"name":"a\"b"`,
		`"amount":1.50`,
		`"doc":{"k":1}`,
		`"note":null`,
		`"active":1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in %s", want, text)
		}
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(text)), &decoded); err != nil {
		t.Fatalf("invalid JSON line: %v", err)
	}
}

func TestEncodeBatchMultipleRows(t *testing.T) {
	table := encodeTable()
	batch := []event.Event{
		{Op: event.OpRead, Database: "db", Table: "orders", Columns: []string{"id"}, After: map[string]any{"id": int64(1)}},
		{Op: event.OpUpdate, Database: "db", Table: "orders", Columns: []string{"id"}, After: map[string]any{"id": uint64(18446744073709551615)}},
	}
	body, err := encodeBatch(batch, table)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines=%d %q", len(lines), body)
	}
	if !strings.Contains(lines[1], "18446744073709551615") {
		t.Fatalf("unsigned value: %s", lines[1])
	}
}

func TestEncodeBatchRejectsBadValues(t *testing.T) {
	table := encodeTable()
	cases := []struct {
		name  string
		item  event.Event
		match string
	}{
		{
			name: "missing declared column",
			item: event.Event{Op: event.OpCreate, Database: "db", Table: "orders",
				Columns: []string{"id", "name"}, After: map[string]any{"id": int64(1)}},
			match: "missing column name",
		},
		{
			name: "no declared columns",
			item: event.Event{Op: event.OpCreate, Database: "db", Table: "orders",
				After: map[string]any{"id": int64(1)}},
			match: "no declared columns",
		},
		{
			name: "no after image",
			item: event.Event{Op: event.OpDelete, Database: "db", Table: "orders",
				Columns: []string{"id"}},
			match: "no After image",
		},
		{
			name: "invalid json value",
			item: event.Event{Op: event.OpCreate, Database: "db", Table: "orders",
				Columns: []string{"id", "doc"}, After: map[string]any{"id": int64(1), "doc": "{oops"}},
			match: "invalid JSON value",
		},
		{
			name: "binary value",
			item: event.Event{Op: event.OpCreate, Database: "db", Table: "orders",
				Columns: []string{"id", "note"}, After: map[string]any{"id": int64(1), "note": []byte{0x01}}},
			match: "binary value",
		},
		{
			name: "invalid numeric value",
			item: event.Event{Op: event.OpCreate, Database: "db", Table: "orders",
				Columns: []string{"id", "amount"}, After: map[string]any{"id": int64(1), "amount": "not-a-number"}},
			match: "invalid numeric value",
		},
		{
			name: "unsupported go type",
			item: event.Event{Op: event.OpCreate, Database: "db", Table: "orders",
				Columns: []string{"id"}, After: map[string]any{"id": struct{}{}}},
			match: "unsupported value type",
		},
	}
	for _, tc := range cases {
		if _, err := encodeBatch([]event.Event{tc.item}, table); err == nil || !strings.Contains(err.Error(), tc.match) {
			t.Fatalf("%s: err=%v", tc.name, err)
		}
	}
}

func TestEncodeBatchUnknownColumn(t *testing.T) {
	table := encodeTable()
	item := event.Event{Op: event.OpCreate, Database: "db", Table: "orders",
		Columns: []string{"id", "ghost"}, After: map[string]any{"id": int64(1), "ghost": "x"}}
	if _, err := encodeBatch([]event.Event{item}, table); err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("err=%v", err)
	}
}

func TestEncodeValueDateAndDateTime(t *testing.T) {
	table := schema.Table{Database: "db", Table: "t", Columns: []schema.Column{
		{Name: "d", Type: "date", Nullable: true},
		{Name: "dt", Type: "datetime(6)", Nullable: true},
	}}
	item := event.Event{Op: event.OpCreate, Database: "db", Table: "t",
		Columns: []string{"d", "dt"},
		After: map[string]any{
			"d":  time.Date(2024, 5, 6, 0, 0, 0, 0, time.UTC),
			"dt": time.Date(2024, 5, 6, 7, 8, 9, 123456000, time.UTC),
		}}
	body, err := encodeBatch([]event.Event{item}, table)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if !strings.Contains(text, `"d":"2024-05-06"`) || !strings.Contains(text, `"dt":"2024-05-06 07:08:09.123456"`) {
		t.Fatalf("time encoding: %s", text)
	}
}

func TestEncodeValueJSONFromBytes(t *testing.T) {
	column := schema.Column{Name: "doc", Type: "json"}
	value, err := encodeValue([]byte(`{"a":1}`), column)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := value.(json.RawMessage); !ok {
		t.Fatalf("got %T", value)
	}
	if _, err := encodeValue([]byte(`"plain string"`), column); err != nil {
		t.Fatalf("a JSON string value is valid: %v", err)
	}
	if _, err := encodeValue([]byte(`nope`), column); err == nil {
		t.Fatal("invalid JSON must fail")
	}
}
