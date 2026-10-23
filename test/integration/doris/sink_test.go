//go:build doris_integration

package doris_it

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
)

func ordersTable(database, table string) schema.Table {
	return schema.Table{
		Database: database,
		Table:    table,
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "name", Type: "varchar(64)", Nullable: true},
			{Name: "amount", Type: "decimal(12,2)", Nullable: true},
			{Name: "payload", Type: "json", Nullable: true},
			{Name: "created_at", Type: "datetime(6)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
}

func ordersColumns() []string {
	return []string{"id", "name", "amount", "payload", "created_at"}
}

func orderEvent(op event.Op, database, table string, id int64, name, amount, payload, created string) event.Event {
	return makeEvent(op, database, table, ordersColumns(), map[string]any{
		"id":         id,
		"name":       name,
		"amount":     amount,
		"payload":    payload,
		"created_at": created,
	})
}

func TestIntegrationAutoCreateAndUniqueKeyLifecycle(t *testing.T) {
	cfg := integrationConfig(t)
	db := openDB(t, cfg)
	database := uniqueName("go_cdc_it")
	createDatabase(t, db, database)
	table := ordersTable(database, "orders")

	sk := openSink(t, cfg)
	if err := sk.CreateTable(table); err != nil {
		t.Fatalf("create target table: %v", err)
	}

	writeEvents(t, sk, orderEvent(event.OpRead, database, "orders", 1,
		"snapshot", "12.34", `{"a":1}`, "2026-10-22 10:11:12.123456"))

	row := queryRow(t, db, "SELECT id, name, amount, payload, created_at FROM "+
		schema.Qualified(database, "orders")+" WHERE id = 1")
	assertText(t, row, "id", "1")
	assertText(t, row, "name", "snapshot")
	assertNumber(t, row, "amount", "12.34")
	assertPrefix(t, row, "created_at", "2026-10-22 10:11:12")
	var payload map[string]any
	if err := json.Unmarshal([]byte(row["payload"]), &payload); err != nil {
		t.Fatalf("payload=%q: %v", row["payload"], err)
	}
	if payload["a"] != float64(1) {
		t.Fatalf("payload=%v", payload)
	}

	// A full UPDATE must replace the UNIQUE KEY row, not append.
	writeEvents(t, sk, orderEvent(event.OpUpdate, database, "orders", 1,
		"updated", "99.50", `{"a":2}`, "2026-10-22 10:11:12.123456"))
	row = queryRow(t, db, "SELECT id, name, amount, payload FROM "+
		schema.Qualified(database, "orders")+" WHERE id = 1")
	assertText(t, row, "name", "updated")
	assertNumber(t, row, "amount", "99.50")
	if err := json.Unmarshal([]byte(row["payload"]), &payload); err != nil {
		t.Fatalf("payload=%q: %v", row["payload"], err)
	}
	if payload["a"] != float64(2) {
		t.Fatalf("payload=%v", payload)
	}

	// Duplicate keys in one batch coalesce to the last event before loading.
	writeEvents(t, sk,
		orderEvent(event.OpCreate, database, "orders", 3, "first", "1.00", `{"a":1}`, "2026-10-22 10:11:12.000000"),
		orderEvent(event.OpCreate, database, "orders", 3, "last", "2.00", `{"a":2}`, "2026-10-22 10:11:12.000000"),
	)
	row = queryRow(t, db, "SELECT name, amount FROM "+schema.Qualified(database, "orders")+" WHERE id = 3")
	assertText(t, row, "name", "last")
	assertNumber(t, row, "amount", "2.00")

	// After a restart Runner does not replay Schema; Write must lazily DESC the
	// target and still validate that the event key matches the UNIQUE KEY.
	if err := sk.Close(); err != nil {
		t.Fatalf("close first sink: %v", err)
	}
	restarted := openSink(t, cfg)
	writeEvents(t, restarted, orderEvent(event.OpCreate, database, "orders", 2,
		"after-restart", "3.00", `{"a":3}`, "2026-10-22 11:00:00.000000"))
	row = queryRow(t, db, "SELECT name FROM "+schema.Qualified(database, "orders")+" WHERE id = 2")
	assertText(t, row, "name", "after-restart")
	if got := queryCount(t, db, database, "orders"); got != 3 {
		t.Fatalf("row count=%d want 3", got)
	}
}

func TestIntegrationExtraColumnAndSchemaEvolution(t *testing.T) {
	cfg := integrationConfig(t)
	cfg.AutoCreate = false
	cfg.SchemaChange = true
	cfg.SchemaChangeInclude = []string{"add.column"}

	db := openDB(t, cfg)
	database := uniqueName("go_cdc_it")
	createDatabase(t, db, database)
	target := "evolving"
	// varchar(32) needs 96 bytes in Doris; a VARCHAR(32) target would reject
	// any CJK payload, so the fixture follows the byte-capacity rule.
	execSQL(t, db, "CREATE TABLE "+schema.Qualified(database, target)+" ("+
		"`id` BIGINT NOT NULL, "+
		"`name` VARCHAR(96) NULL, "+
		"`legacy` VARCHAR(16) NULL DEFAULT 'legacy'"+
		") UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 "+
		`PROPERTIES ("replication_num" = "1")`)

	source := schema.Table{
		Database: database,
		Table:    target,
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "name", Type: "varchar(32)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
	sk := openSink(t, cfg)
	if err := sk.CreateTable(source); err != nil {
		t.Fatalf("validate target with extra column: %v", err)
	}

	writeEvents(t, sk, makeEvent(event.OpCreate, database, target, []string{"id", "name"},
		map[string]any{"id": int64(1), "name": "before"}))
	row := queryRow(t, db, "SELECT name, legacy FROM "+schema.Qualified(database, target)+" WHERE id = 1")
	assertText(t, row, "name", "before")
	assertText(t, row, "legacy", "legacy")

	change := schema.Change{
		Action:     schema.ActionAddColumn,
		Database:   database,
		Table:      target,
		AddColumns: []string{"score"},
		ColumnDef:  &schema.Column{Name: "score", Type: "decimal(8,2)", Nullable: true},
	}
	if err := sk.ApplySchema(change); err != nil {
		t.Fatalf("apply add column: %v", err)
	}
	desc := describeColumns(t, db, database, target)
	scoreType, ok := desc["score"]
	if !ok {
		t.Fatalf("score column not visible after ALTER: %v", desc)
	}
	if !strings.HasPrefix(strings.ToUpper(scoreType), "DECIMAL") {
		t.Fatalf("score type=%q", scoreType)
	}

	// Replaying the same ADD COLUMN must be idempotent.
	if err := sk.ApplySchema(change); err != nil {
		t.Fatalf("repeat add column: %v", err)
	}
	bad := change
	bad.ColumnDef = &schema.Column{Name: "score", Type: "varchar(8)", Nullable: true}
	if err := sk.ApplySchema(bad); err == nil {
		t.Fatal("incompatible existing column must fail")
	}

	writeEvents(t, sk, makeEvent(event.OpCreate, database, target, []string{"id", "name", "score"},
		map[string]any{"id": int64(2), "name": "after", "score": "7.25"}))
	row = queryRow(t, db, "SELECT name, legacy, score FROM "+schema.Qualified(database, target)+" WHERE id = 2")
	assertText(t, row, "name", "after")
	assertText(t, row, "legacy", "legacy")
	assertNumber(t, row, "score", "7.25")

	// A live target column narrower than the source must be rejected by DESC
	// validation, not by a later Stream Load failure.
	narrow := "narrow"
	execSQL(t, db, "CREATE TABLE "+schema.Qualified(database, narrow)+" ("+
		"`id` BIGINT NOT NULL, `name` VARCHAR(8) NULL"+
		") UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 "+
		`PROPERTIES ("replication_num" = "1")`)
	narrowSource := schema.Table{
		Database: database,
		Table:    narrow,
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "name", Type: "varchar(32)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
	if err := sk.CreateTable(narrowSource); err == nil {
		t.Fatal("narrow target column must fail CreateTable validation")
	}
}

func TestIntegrationMultiTableStopsAtFirstFailure(t *testing.T) {
	cfg := integrationConfig(t)
	db := openDB(t, cfg)
	database := uniqueName("go_cdc_it")
	createDatabase(t, db, database)

	sk := openSink(t, cfg)
	for _, table := range []string{"a_ok", "c_ok"} {
		if err := sk.CreateTable(ordersTable(database, table)); err != nil {
			t.Fatalf("create %s: %v", table, err)
		}
	}

	// Sorted submission order is a_ok, b_missing, c_ok. a_ok must be written,
	// b_missing must fail the batch, and c_ok must not be attempted.
	err := sk.Write([]event.Event{
		orderEvent(event.OpCreate, database, "a_ok", 1, "a", "1.00", `{"a":1}`, "2026-10-22 10:00:00.000000"),
		orderEvent(event.OpCreate, database, "b_missing", 1, "b", "1.00", `{"a":1}`, "2026-10-22 10:00:00.000000"),
		orderEvent(event.OpCreate, database, "c_ok", 1, "c", "1.00", `{"a":1}`, "2026-10-22 10:00:00.000000"),
	})
	if err == nil {
		t.Fatal("missing table must fail the batch")
	}
	if !strings.Contains(err.Error(), "b_missing") {
		t.Fatalf("error should name b_missing: %v", err)
	}
	if got := queryCount(t, db, database, "a_ok"); got != 1 {
		t.Fatalf("a_ok count=%d want 1 (partial success)", got)
	}
	if got := queryCount(t, db, database, "c_ok"); got != 0 {
		t.Fatalf("c_ok count=%d want 0 (first failure must stop later tables)", got)
	}
}
