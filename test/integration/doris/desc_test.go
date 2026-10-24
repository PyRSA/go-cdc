//go:build doris_integration

package doris_it

import (
	"strings"
	"testing"

	"github.com/PyRSA/go-cdc/common/schema"
)

// hasNoDefault mirrors client_sql.descDefault: SQL NULL, "NULL" and "\N" mean
// the column has no default, while every other cell (including "") is a real
// default expression.
func hasNoDefault(cell descCell) bool {
	if cell.IsNull {
		return true
	}
	return strings.EqualFold(cell.Text, "NULL") || cell.Text == `\N`
}

// TestIntegrationDescNullabilityAndDefaults is the P1 probe for M-3. It pins
// the two DESC fields the sink parses (Null and Default), then checks that the
// same live structure produces the documented result on both code paths:
// validateTarget (CreateTable) and validateExistingColumn (repeated ADD COLUMN).
func TestIntegrationDescNullabilityAndDefaults(t *testing.T) {
	cfg := integrationConfig(t)
	cfg.AutoCreate = false
	cfg.SchemaChange = true
	cfg.SchemaChangeInclude = []string{"add.column"}

	db := openDB(t, cfg)
	database := uniqueName("go_cdc_it")
	createDatabase(t, db, database)
	const table = "desc_probe"
	execSQL(t, db, "CREATE TABLE "+schema.Qualified(database, table)+" ("+
		"`id` BIGINT NOT NULL, "+
		"`nn` VARCHAR(32) NOT NULL, "+
		"`nn_empty` VARCHAR(32) NOT NULL DEFAULT '', "+
		"`nul` VARCHAR(32) NULL, "+
		"`nul_empty` VARCHAR(32) NULL DEFAULT ''"+
		") UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 "+
		`PROPERTIES ("replication_num" = "1")`)

	desc := describeTable(t, db, database, table)
	for name, row := range desc {
		t.Logf("DESC %s.%s.%s: type=%q null=%q key=%q default=%#v (is_null=%v) extra=%q",
			database, table, name, row["type"].Text, row["null"].Text, row["key"].Text,
			row["default"].Text, row["default"].IsNull, row["extra"].Text)
	}

	// The sink reads nullability from Null only (isYes also accepts true/1).
	for _, want := range []struct {
		column   string
		nullable bool
	}{
		{"id", false},
		{"nn", false},
		{"nn_empty", false},
		{"nul", true},
		{"nul_empty", true},
	} {
		cell := desc[want.column]["null"]
		text := strings.TrimSpace(cell.Text)
		if cell.IsNull || (!strings.EqualFold(text, "Yes") && !strings.EqualFold(text, "No")) {
			t.Fatalf("DESC %s.%s.%s null=%#v; want a literal Yes/No because isYes reads this cell",
				database, table, want.column, cell)
		}
		if got := strings.EqualFold(text, "Yes"); got != want.nullable {
			t.Fatalf("DESC %s.%s.%s null=%q want nullable=%v", database, table, want.column, text, want.nullable)
		}
	}

	// DEFAULT '' must be distinguishable from "no default": descDefault treats
	// NULL / "NULL" / "\N" as none and everything else, including "", as a
	// real default. Mixing the two would let a NOT NULL extra column pass as if
	// it had a default.
	for _, name := range []string{"nn_empty", "nul_empty"} {
		if cell := desc[name]["default"]; hasNoDefault(cell) {
			t.Fatalf("DESC %s.%s.%s default=%#v; DEFAULT '' must not be reported as no default",
				database, table, name, cell)
		}
	}
	for _, name := range []string{"nn", "nul"} {
		if cell := desc[name]["default"]; !hasNoDefault(cell) {
			t.Fatalf("DESC %s.%s.%s default=%#v; a column without DEFAULT must be reported as none, otherwise descDefault reads it as DEFAULT ''",
				database, table, name, cell)
		}
	}

	sk := openSink(t, cfg)

	// nullable source -> NOT NULL target must be rejected.
	nullableSource := schema.Table{
		Database: database,
		Table:    table,
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "nn", Type: "varchar(32)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
	err := sk.CreateTable(nullableSource)
	if err == nil {
		t.Fatal("validateTarget must reject a nullable source column when the target column is NOT NULL")
	}
	if !strings.Contains(err.Error(), "NOT NULL") {
		t.Fatalf("rejection should name the NOT NULL target column: %v", err)
	}

	// NOT NULL source -> nullable target must be accepted, and DEFAULT '' on a
	// NOT NULL source column must validate too.
	wideningSource := schema.Table{
		Database: database,
		Table:    table,
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "nn", Type: "varchar(32)", Nullable: false},
			{Name: "nn_empty", Type: "varchar(32)", Nullable: false},
			{Name: "nul", Type: "varchar(32)", Nullable: false},
			{Name: "nul_empty", Type: "varchar(32)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
	if err := sk.CreateTable(wideningSource); err != nil {
		t.Fatalf("a NOT NULL source column must be accepted by a nullable target: %v", err)
	}

	// validateExistingColumn applies the same one-way rule on the ADD COLUMN
	// path, and it reads the same live DESC output.
	idempotent := schema.Change{
		Action:     schema.ActionAddColumn,
		Database:   database,
		Table:      table,
		AddColumns: []string{"nul"},
		ColumnDef:  &schema.Column{Name: "nul", Type: "varchar(32)", Nullable: false},
	}
	if err := sk.ApplySchema(idempotent); err != nil {
		t.Fatalf("existing nullable column must accept a NOT NULL source column: %v", err)
	}
	incompatible := schema.Change{
		Action:     schema.ActionAddColumn,
		Database:   database,
		Table:      table,
		AddColumns: []string{"nn"},
		ColumnDef:  &schema.Column{Name: "nn", Type: "varchar(32)", Nullable: true},
	}
	err = sk.ApplySchema(incompatible)
	if err == nil {
		t.Fatal("validateExistingColumn must reject a nullable source column when the column is already NOT NULL")
	}
	if !strings.Contains(err.Error(), "NOT NULL") {
		t.Fatalf("rejection should name the NOT NULL existing column: %v", err)
	}
}
