package mysql

import (
	"strings"
	"testing"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
)

func TestSameKeyUpdateIsUpsert(t *testing.T) {
	got, err := Statements(event.Event{
		Op: event.OpUpdate, Database: "sink", Table: "users",
		Columns: []string{"id", "name"}, Keys: []string{"id"},
		Before: map[string]any{"id": int64(1), "name": "a"},
		After:  map[string]any{"id": int64(1), "name": "b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !strings.Contains(got[0].SQL, "ON DUPLICATE KEY UPDATE") {
		t.Fatalf("%#v", got)
	}
	if strings.Contains(got[0].SQL, "DELETE") {
		t.Fatal("same-key update must not delete first")
	}
}

func TestKeyChangeDeletesOldKey(t *testing.T) {
	got, err := Statements(event.Event{
		Op: event.OpUpdate, Database: "sink", Table: "users",
		Columns: []string{"id", "name"}, Keys: []string{"docid"},
		Before: map[string]any{"docid": "a", "name": "a"},
		After:  map[string]any{"docid": "b", "name": "b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !strings.HasPrefix(got[0].SQL, "DELETE") || got[0].Args[0] != "a" {
		t.Fatalf("%#v", got)
	}
}

func TestCreateTableStartsAutoIncrementAtOne(t *testing.T) {
	sql, err := CreateTableSQL(schema.Table{
		Database: "sink", Table: "users",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", AutoIncrement: true},
			{Name: "name", Type: "varchar(64)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "AUTO_INCREMENT=") {
		t.Fatal(sql)
	}
	if !strings.Contains(sql, "`id` bigint NOT NULL AUTO_INCREMENT") {
		t.Fatal(sql)
	}
}

func TestCreateTableSQLIncludesCommentCharsetDefaultIndex(t *testing.T) {
	sql, err := CreateTableSQL(schema.Table{
		Database: "sink", Table: "users",
		Comment: "users table", Charset: "utf8mb4", Collation: "utf8mb4_bin",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", AutoIncrement: true, Comment: "pk"},
			{
				Name: "name", Type: "varchar(64)", Nullable: true, Default: "n/a",
				Comment: "display", Charset: "utf8mb4", Collation: "utf8mb4_general_ci",
			},
		},
		PrimaryKeys: []string{"id"},
		Indexes: []schema.Index{
			{Name: "uk_name", Unique: true, Columns: []string{"name"}},
			{Name: "idx_name", Columns: []string{"name"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	need := []string{
		"COMMENT 'pk'",
		"COMMENT 'display'",
		"DEFAULT 'n/a'",
		"`name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT 'n/a' COMMENT 'display'",
		"UNIQUE KEY `uk_name` (`name`)",
		"KEY `idx_name` (`name`)",
		"DEFAULT CHARSET=utf8mb4",
		"COLLATE=utf8mb4_bin",
		"COMMENT='users table'",
	}
	for _, part := range need {
		if !strings.Contains(sql, part) {
			t.Fatalf("missing %q in %s", part, sql)
		}
	}
	if strings.Contains(sql, "FOREIGN KEY") || strings.Contains(sql, "ENGINE=") {
		t.Fatal(sql)
	}
}
