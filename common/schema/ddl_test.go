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
		{"ALTER TABLE users ADD FULLTEXT INDEX ft_body (body)", ActionIndex, "", "users", "", ""},
		{"ALTER TABLE users ADD SPATIAL INDEX sp_pt (pt)", ActionIndex, "", "users", "", ""},
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

func TestAddColumns(t *testing.T) {
	cases := []struct {
		sql  string
		want []string
	}{
		{"ALTER TABLE db.t ADD COLUMN note varchar(32)", []string{"note"}},
		{"ALTER TABLE db.t ADD name varchar(32)", []string{"name"}},
		{"ALTER TABLE `db`.`t` ADD COLUMN `note` varchar(32) NULL", []string{"note"}},
		{"ALTER TABLE db.t ADD COLUMN a int, ADD COLUMN b varchar(8)", []string{"a", "b"}},
		{"ALTER TABLE db.t ADD COLUMN a int, ADD INDEX idx_a (a), ADD b int", []string{"a", "b"}},
		{"ALTER TABLE db.t ADD COLUMN amount decimal(10,2) NOT NULL DEFAULT 0", []string{"amount"}},
		{"ALTER TABLE db.t ADD COLUMN keyed int", []string{"keyed"}},
		{"ALTER TABLE db.t ADD INDEX idx_name (name)", nil},
		{"ALTER TABLE db.t ADD UNIQUE KEY uk_name (name)", nil},
		{"ALTER TABLE db.t ADD CONSTRAINT fk FOREIGN KEY (id) REFERENCES p(id)", nil},
		{"CREATE TABLE db.t (id int)", nil},
		{"ALTER TABLE db.t DROP COLUMN note", nil},
	}
	for _, tc := range cases {
		got := AddColumns(tc.sql)
		if len(got) != len(tc.want) {
			t.Fatalf("%s: got %#v want %#v", tc.sql, got, tc.want)
		}
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Fatalf("%s: got %#v want %#v", tc.sql, got, tc.want)
			}
		}
	}
}
