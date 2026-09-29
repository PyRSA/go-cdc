package mysql

import (
	"strings"
	"testing"
)

func TestReadChunkSQLPagesByKey(t *testing.T) {
	statement, args := ReadChunkSQL("db", "t", []string{"id"}, "id", "1", nil, "9", 1024)
	if strings.Contains(statement, "OFFSET") || !strings.Contains(statement, "ORDER BY `id` LIMIT ?") {
		t.Fatal(statement)
	}
	if !strings.Contains(statement, "`id` > ?") {
		t.Fatal(statement)
	}
	if len(args) != 3 || args[0] != "1" || args[1] != "9" || args[2] != 1024 {
		t.Fatalf("args %#v", args)
	}
}

func TestPrimaryKeyNamesFollowKeyOrder(t *testing.T) {
	names := primaryKeyNames([]pkPart{{pos: 2, name: "id"}, {pos: 1, name: "tenant_id"}})
	if len(names) != 2 || names[0] != "tenant_id" || names[1] != "id" {
		t.Fatalf("%#v", names)
	}
}

func TestSafeTimeZoneRejectsInjection(t *testing.T) {
	if !safeTimeZone("+00:00") || !safeTimeZone("Asia/Shanghai") {
		t.Fatal("expected zones were rejected")
	}
	if safeTimeZone("UTC'; DROP TABLE t; --") {
		t.Fatal("unsafe time zone was accepted")
	}
}

func TestSkipEventsThenRows(t *testing.T) {
	h := &handler{skipEvents: 1, skipRows: 1}
	if !h.dropEvent() || h.skipEvents != 0 {
		t.Fatal("event skip did not consume one event")
	}
	if !h.dropRow() || h.dropRow() {
		t.Fatal("row skip did not stop after one row")
	}
}
