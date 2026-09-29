package schema

import "testing"

func TestClassify(t *testing.T) {
	cases := []struct {
		sql     string
		action  Action
		db      string
		table   string
		column  string
		newName string
	}{
		{"CREATE TABLE `tests`.`users` (id bigint)", ActionCreate, "tests", "users", "", ""},
		{"ALTER TABLE tests.users ADD COLUMN age int", ActionAddColumn, "tests", "users", "age", ""},
		{"ALTER TABLE users DROP COLUMN age", ActionDropColumn, "", "users", "age", ""},
		{"ALTER TABLE users CHANGE COLUMN name full_name varchar(32)", ActionRenameColumn, "", "users", "name", "full_name"},
		{"ALTER TABLE users RENAME COLUMN name TO full_name", ActionRenameColumn, "", "users", "name", "full_name"},
		{"ALTER TABLE users MODIFY COLUMN age bigint", ActionModifyColumn, "", "users", "age", ""},
		{"DROP TABLE IF EXISTS tests.users", ActionDropTable, "tests", "users", "", ""},
		{"TRUNCATE TABLE tests.users", ActionTruncate, "tests", "users", "", ""},
		{"ALTER TABLE users ADD INDEX idx_name (name)", ActionIndex, "", "users", "", ""},
		{"RENAME TABLE tests.users TO tests.people", ActionRenameTable, "tests", "users", "", ""},
	}
	for _, tc := range cases {
		got := Classify(tc.sql)
		if got.Action != tc.action {
			t.Fatalf("%s: action %s", tc.sql, got.Action)
		}
		if tc.db != "" && got.Database != tc.db {
			t.Fatalf("%s: db %s", tc.sql, got.Database)
		}
		if tc.table != "" && got.Table != tc.table {
			t.Fatalf("%s: table %s", tc.sql, got.Table)
		}
		if tc.column != "" && got.Column != tc.column {
			t.Fatalf("%s: column %s", tc.sql, got.Column)
		}
		if tc.newName != "" && got.NewName != tc.newName {
			t.Fatalf("%s: new name %s", tc.sql, got.NewName)
		}
	}
}

func TestClassifyIndexAddDoesNotLookLikeColumn(t *testing.T) {
	got := Classify("ALTER TABLE tests.users ADD INDEX idx_name (name)")
	if got.Action != ActionIndex {
		t.Fatalf("action %s", got.Action)
	}
	if got.Table != "users" || got.Database != "tests" {
		t.Fatalf("table %s.%s", got.Database, got.Table)
	}
}
