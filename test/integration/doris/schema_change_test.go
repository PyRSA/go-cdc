//go:build doris_integration

package doris_it

import (
	"strings"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
)

func schemaChangeConfig(t *testing.T) definition.Sink {
	t.Helper()
	cfg := integrationConfig(t)
	cfg.AutoCreate = false
	cfg.SchemaChange = true
	cfg.SchemaChangeInclude = []string{"add.column"}
	return cfg
}

func visibilityBudget(cfg definition.Sink) time.Duration {
	if cfg.SchemaChangeWait > 0 {
		return cfg.SchemaChangeWait
	}
	return definition.DefaultDorisSchemaChangeWait
}

// TestIntegrationReconcileMissingColumnsOnCreate is the P1 probe for M-2. It
// covers the three states the reconcile path must survive:
//
//  1. a target that already has one source column (partial state), so only the
//     remainder is added;
//  2. the write path right after the ALTER, which proves the column is visible
//     to Stream Load and not just to DESC;
//  3. a restart, where CreateTable must validate the finished table without
//     issuing another ALTER.
func TestIntegrationReconcileMissingColumnsOnCreate(t *testing.T) {
	cfg := schemaChangeConfig(t)
	db := openDB(t, cfg)
	database := uniqueName("go_cdc_it")
	createDatabase(t, db, database)
	const table = "evolve"
	// varchar(32) needs 96 bytes in Doris; a VARCHAR(32) target would be
	// rejected as too narrow before the missing columns are looked at.
	execSQL(t, db, "CREATE TABLE "+schema.Qualified(database, table)+" ("+
		"`id` BIGINT NOT NULL, `a` VARCHAR(96) NULL"+
		") UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 "+
		`PROPERTIES ("replication_num" = "1")`)

	source := schema.Table{
		Database: database,
		Table:    table,
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "a", Type: "varchar(32)", Nullable: true},
			{Name: "b", Type: "varchar(32)", Nullable: true},
			{Name: "c", Type: "decimal(8,2)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
	budget := visibilityBudget(cfg)

	sk := openSink(t, cfg)
	started := time.Now()
	if err := sk.CreateTable(source); err != nil {
		t.Fatalf("CreateTable must add the missing columns: %v", err)
	}
	elapsed := time.Since(started)
	// Each ADD COLUMN gets its own budget (5s by default) and the visibility
	// poll returns as soon as DESC shows the column.
	t.Logf("ADD COLUMN b,c applied and visible in %s (per-column budget %s)", elapsed, budget)

	desc := describeTable(t, db, database, table)
	for _, name := range []string{"a", "b", "c"} {
		if _, ok := desc[name]; !ok {
			t.Fatalf("column %s is not visible after reconcile: %v", name, desc)
		}
	}
	if !strings.EqualFold(strings.TrimSpace(desc["b"]["null"].Text), "Yes") {
		t.Fatalf("reconciled column b null=%q; the source column is nullable", desc["b"]["null"].Text)
	}

	// A column added by the reconcile path must be usable immediately: DESC
	// visibility is not enough if Stream Load still rejects the field.
	writeEvents(t, sk, makeEvent(event.OpCreate, database, table, []string{"id", "b", "c"},
		map[string]any{"id": int64(1), "b": "after-add", "c": "7.25"}))
	row := queryRow(t, db, "SELECT a, b, c FROM "+schema.Qualified(database, table)+" WHERE id = 1")
	assertText(t, row, "b", "after-add")
	assertNumber(t, row, "c", "7.25")

	// Restart: the columns exist, so validation must succeed without another
	// ALTER. A second ALTER on an existing column would fail the call.
	if err := sk.Close(); err != nil {
		t.Fatalf("close sink: %v", err)
	}
	restarted := openSink(t, cfg)
	started = time.Now()
	if err := restarted.CreateTable(source); err != nil {
		t.Fatalf("restart must re-validate the reconciled table without ALTER: %v", err)
	}
	t.Logf("restart re-validation in %s", time.Since(started))
	if got := queryCount(t, db, database, table); got != 1 {
		t.Fatalf("row count=%d want 1 after restart validation", got)
	}
}

// TestIntegrationReconcileRefusesToAlterWhenTheTargetIsIncompatible proves the
// reconcile path has no side effects when validation fails for a reason other
// than a missing column: Doris DDL cannot be rolled back, so the missing
// column must not be added before the table is accepted.
func TestIntegrationReconcileRefusesToAlterWhenTheTargetIsIncompatible(t *testing.T) {
	cfg := schemaChangeConfig(t)
	db := openDB(t, cfg)
	database := uniqueName("go_cdc_it")
	createDatabase(t, db, database)
	const table = "guarded"
	execSQL(t, db, "CREATE TABLE "+schema.Qualified(database, table)+" ("+
		"`id` BIGINT NOT NULL, `a` VARCHAR(8) NULL"+
		") UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 "+
		`PROPERTIES ("replication_num" = "1")`)

	source := schema.Table{
		Database: database,
		Table:    table,
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "a", Type: "varchar(32)", Nullable: true},
			{Name: "b", Type: "varchar(32)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
	sk := openSink(t, cfg)
	err := sk.CreateTable(source)
	if err == nil {
		t.Fatal("a narrower target column must reject the table before any ALTER")
	}
	if !strings.Contains(err.Error(), "column a has type") {
		t.Fatalf("rejection should name column a and its live type: %v", err)
	}
	desc := describeTable(t, db, database, table)
	if _, ok := desc["b"]; ok {
		t.Fatalf("b must not be added when the table is rejected for a: %v", desc)
	}
}

// TestIntegrationMissingColumnWithoutSchemaChangeFailsCleanly checks the
// actionable error that stops the job instead of silently writing a table with
// a missing column.
func TestIntegrationMissingColumnWithoutSchemaChangeFailsCleanly(t *testing.T) {
	cfg := integrationConfig(t)
	cfg.AutoCreate = false
	cfg.SchemaChange = false

	db := openDB(t, cfg)
	database := uniqueName("go_cdc_it")
	createDatabase(t, db, database)
	const table = "no_alter"
	execSQL(t, db, "CREATE TABLE "+schema.Qualified(database, table)+" ("+
		"`id` BIGINT NOT NULL"+
		") UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 "+
		`PROPERTIES ("replication_num" = "1")`)

	source := schema.Table{
		Database: database,
		Table:    table,
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "late", Type: "varchar(32)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
	sk := openSink(t, cfg)
	err := sk.CreateTable(source)
	if err == nil {
		t.Fatal("a missing column without schema.change must fail")
	}
	for _, want := range []string{"schema.change.enabled", "add.column", "late"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error should mention %q: %v", want, err)
		}
	}
	desc := describeTable(t, db, database, table)
	if _, ok := desc["late"]; ok {
		t.Fatalf("late must not be added without add.column permission: %v", desc)
	}
}
