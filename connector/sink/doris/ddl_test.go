package doris

import (
	"strings"
	"testing"

	"github.com/PyRSA/go-cdc/common/schema"
)

func sampleTable() schema.Table {
	return schema.Table{
		Database: "db",
		Table:    "orders",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "name", Type: "varchar(32)", Nullable: true},
			{Name: "amount", Type: "decimal(10,2)", Nullable: true},
			{Name: "created", Type: "datetime(3)", Nullable: false},
		},
		PrimaryKeys: []string{"id"},
	}
}

func TestCreateTableSQL(t *testing.T) {
	statement, err := createTableSQL(sampleTable(), tableOptions{Buckets: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS `db`.`orders` (",
		"`id` BIGINT NOT NULL",
		"`name` VARCHAR(32) NULL",
		"`amount` DECIMAL(10,2) NULL",
		"`created` DATETIME(3) NOT NULL",
		"UNIQUE KEY(`id`)",
		"DISTRIBUTED BY HASH(`id`) BUCKETS auto",
	} {
		if !strings.Contains(statement, want) {
			t.Fatalf("missing %q in:\n%s", want, statement)
		}
	}
	if strings.Contains(statement, "PROPERTIES") {
		t.Fatalf("replication_num must be omitted when unset:\n%s", statement)
	}
	if strings.Contains(statement, "MERGE_ON_WRITE") {
		t.Fatalf("merge-on-write must not be hardcoded:\n%s", statement)
	}
	if strings.Contains(statement, "DEFAULT") {
		t.Fatalf("defaults must not be guessed:\n%s", statement)
	}
}

func TestCreateTableSQLMovesKeysFirst(t *testing.T) {
	table := sampleTable()
	table.Columns = []schema.Column{
		{Name: "name", Type: "varchar(32)", Nullable: true},
		{Name: "id", Type: "bigint", Nullable: false},
	}
	table.PrimaryKeys = []string{"id"}
	statement, err := createTableSQL(table, tableOptions{Buckets: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	idAt := strings.Index(statement, "`id` BIGINT")
	nameAt := strings.Index(statement, "`name` VARCHAR(32)")
	if idAt < 0 || nameAt < 0 || idAt > nameAt {
		t.Fatalf("key column must lead the definition:\n%s", statement)
	}
}

func TestCreateTableSQLCompositeKey(t *testing.T) {
	table := sampleTable()
	table.Columns = []schema.Column{
		{Name: "id", Type: "bigint", Nullable: false},
		{Name: "tenant", Type: "varchar(16)", Nullable: false},
		{Name: "name", Type: "varchar(32)", Nullable: true},
	}
	table.PrimaryKeys = []string{"tenant", "id"}
	statement, err := createTableSQL(table, tableOptions{Buckets: "4"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(statement, "UNIQUE KEY(`tenant`, `id`)") {
		t.Fatalf("key order must follow PrimaryKeys:\n%s", statement)
	}
	if !strings.Contains(statement, "DISTRIBUTED BY HASH(`tenant`) BUCKETS 4") {
		t.Fatalf("buckets:\n%s", statement)
	}
}

func TestCreateTableSQLReplication(t *testing.T) {
	statement, err := createTableSQL(sampleTable(), tableOptions{Buckets: "auto", ReplicationNum: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(statement, `PROPERTIES ("replication_num" = "2")`) {
		t.Fatalf("replication property missing:\n%s", statement)
	}
}

func TestCreateTableSQLRejectsBadTables(t *testing.T) {
	noKey := sampleTable()
	noKey.PrimaryKeys = nil
	if _, err := createTableSQL(noKey, tableOptions{}); err == nil {
		t.Fatal("table without primary key must fail")
	}

	missingColumn := sampleTable()
	missingColumn.PrimaryKeys = []string{"missing"}
	if _, err := createTableSQL(missingColumn, tableOptions{}); err == nil {
		t.Fatal("primary key outside columns must fail")
	}

	textKey := sampleTable()
	textKey.Columns = []schema.Column{{Name: "id", Type: "text", Nullable: false}}
	if _, err := createTableSQL(textKey, tableOptions{}); err == nil || !strings.Contains(err.Error(), "key column") {
		t.Fatalf("STRING key must be rejected, err=%v", err)
	}

	doubleKey := sampleTable()
	doubleKey.Columns = []schema.Column{{Name: "id", Type: "double", Nullable: false}}
	if _, err := createTableSQL(doubleKey, tableOptions{}); err == nil {
		t.Fatal("DOUBLE key must be rejected")
	}
}

func TestCreateTableSQLRejectsUnsupportedColumn(t *testing.T) {
	table := sampleTable()
	table.Columns = append(table.Columns, schema.Column{Name: "payload", Type: "blob", Nullable: true})
	if _, err := createTableSQL(table, tableOptions{}); err == nil || !strings.Contains(err.Error(), "payload") {
		t.Fatalf("err=%v", err)
	}
}

func TestAddColumnSQL(t *testing.T) {
	statement, err := addColumnSQL("db", "orders", schema.Column{Name: "note", Type: "varchar(32)", Nullable: true})
	if err != nil {
		t.Fatal(err)
	}
	want := "ALTER TABLE `db`.`orders` ADD COLUMN `note` VARCHAR(32) NULL"
	if statement != want {
		t.Fatalf("got %q want %q", statement, want)
	}
}

func TestColumnDefinitionEscapesComment(t *testing.T) {
	def, err := columnDefinition(schema.Column{
		Name: "note", Type: "varchar(8)", Nullable: true, Comment: `it's \ ok`,
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(def, `COMMENT 'it''s \\ ok'`) {
		t.Fatalf("comment escaping: %s", def)
	}
}

func TestColumnDefinitionDoesNotCopyDefault(t *testing.T) {
	def, err := columnDefinition(schema.Column{
		Name: "state", Type: "int", Nullable: false, Default: "0", HasDefault: true,
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(def, "DEFAULT") {
		t.Fatalf("defaults must not be copied: %s", def)
	}
}
