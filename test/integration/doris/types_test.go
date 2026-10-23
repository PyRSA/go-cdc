//go:build doris_integration

package doris_it

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
)

func TestIntegrationTypeRoundTrip(t *testing.T) {
	cfg := integrationConfig(t)
	db := openDB(t, cfg)
	database := uniqueName("go_cdc_it")
	createDatabase(t, db, database)
	table := "types_roundtrip"

	source := schema.Table{
		Database: database,
		Table:    table,
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "tiny", Type: "tinyint", Nullable: true},
			{Name: "small", Type: "smallint", Nullable: true},
			{Name: "medium", Type: "mediumint", Nullable: true},
			{Name: "normal", Type: "int", Nullable: true},
			{Name: "big", Type: "bigint", Nullable: true},
			{Name: "ubig", Type: "bigint unsigned", Nullable: true},
			{Name: "f", Type: "float", Nullable: true},
			{Name: "d", Type: "double", Nullable: true},
			{Name: "dec", Type: "decimal(18,4)", Nullable: true},
			{Name: "c", Type: "char(8)", Nullable: true},
			{Name: "vc", Type: "varchar(64)", Nullable: true},
			{Name: "txt", Type: "text", Nullable: true},
			{Name: "day", Type: "date", Nullable: true},
			{Name: "ts", Type: "datetime(6)", Nullable: true},
			{Name: "doc", Type: "json", Nullable: true},
			{Name: "yr", Type: "year", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
	sk := openSink(t, cfg)
	if err := sk.CreateTable(source); err != nil {
		t.Fatalf("create type table: %v", err)
	}

	columns := []string{"id", "tiny", "small", "medium", "normal", "big", "ubig",
		"f", "d", "dec", "c", "vc", "txt", "day", "ts", "doc", "yr"}
	// 40 CJK characters are 120 UTF-8 bytes: wider than the source character
	// count and inside the widened VARCHAR(64*3) the sink must create.
	wide := strings.Repeat("中", 40)
	writeEvents(t, sk, makeEvent(event.OpCreate, database, table, columns, map[string]any{
		"id":     int64(1),
		"tiny":   int8(-8),
		"small":  int16(16),
		"medium": int32(1000),
		"normal": int32(42),
		"big":    int64(1 << 40),
		"ubig":   ^uint64(0),
		"f":      float32(1.5),
		"d":      float64(2.25),
		"dec":    "1234.5678",
		"c":      "abc",
		"vc":     wide,
		"txt":    "long text",
		"day":    "2026-10-22",
		"ts":     "2026-10-22 10:11:12.123456",
		"doc":    `{"k":"v"}`,
		"yr":     int64(2026),
	}))

	row := queryRow(t, db, "SELECT "+joinColumns(columns)+" FROM "+
		schema.Qualified(database, table)+" WHERE id = 1")
	for key, want := range map[string]string{
		"id":     "1",
		"tiny":   "-8",
		"small":  "16",
		"medium": "1000",
		"normal": "42",
		"big":    "1099511627776",
		"ubig":   "18446744073709551615",
		"dec":    "1234.5678",
		"c":      "abc",
		"txt":    "long text",
		"day":    "2026-10-22",
		"yr":     "2026",
	} {
		assertText(t, row, key, want)
	}
	assertText(t, row, "vc", wide)
	assertNumber(t, row, "f", "1.5")
	assertNumber(t, row, "d", "2.25")
	assertPrefix(t, row, "ts", "2026-10-22 10:11:12.123456")
	var doc map[string]any
	if err := json.Unmarshal([]byte(row["doc"]), &doc); err != nil {
		t.Fatalf("doc=%q: %v", row["doc"], err)
	}
	if doc["k"] != "v" {
		t.Fatalf("doc=%v", doc)
	}

	// Binary has no lossless v1 mapping and must fail before DDL/Stream Load.
	bad := schema.Table{
		Database: database,
		Table:    "bad_binary",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "blob_col", Type: "blob", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
	if err := sk.CreateTable(bad); err == nil {
		t.Fatal("blob source column must be rejected")
	}
}

func joinColumns(columns []string) string {
	out := ""
	for i, column := range columns {
		if i > 0 {
			out += ", "
		}
		out += "`" + column + "`"
	}
	return out
}
