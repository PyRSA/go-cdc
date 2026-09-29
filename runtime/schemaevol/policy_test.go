package schemaevol

import (
	"testing"

	"github.com/PyRSA/go-cdc/common/schema"
)

func TestDecide(t *testing.T) {
	if Decide("ALTER TABLE t ADD COLUMN age int", []string{"id"}, false).Execute {
		t.Fatal("excluded column should not be executed")
	}
	if !Decide("ALTER TABLE t MODIFY COLUMN age bigint", nil, false).Execute {
		t.Fatal("modify should execute when projection is absent")
	}
	drop := Decide("DROP TABLE t", nil, false)
	if drop.Execute || drop.Change.Action != schema.ActionDropTable {
		t.Fatalf("%#v", drop)
	}
	if !Decide("TRUNCATE TABLE t", nil, true).Execute {
		t.Fatal("truncate should execute when enabled")
	}
	if Decide("ALTER TABLE t ADD INDEX i (id)", nil, true).Execute {
		t.Fatal("index is stdout-only")
	}
	if Decide("CREATE TABLE t (id int)", nil, true).Execute {
		t.Fatal("create is built from the projected schema, not the raw statement")
	}
}

func TestRewriteTable(t *testing.T) {
	got := RewriteTable("ALTER TABLE `tests`.`users` ADD COLUMN age int", "sink", "people")
	if got != "ALTER TABLE `sink`.`people` ADD COLUMN age int" {
		t.Fatal(got)
	}
}
