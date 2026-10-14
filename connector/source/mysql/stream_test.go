package mysql

import (
	"testing"
	"time"
)

func TestBinlogReadTimeout(t *testing.T) {
	if got := binlogReadTimeout(30 * time.Second); got != 60*time.Second {
		t.Fatalf("got %v", got)
	}
	if got := binlogReadTimeout(0); got != 60*time.Second {
		t.Fatalf("zero heartbeat: got %v", got)
	}
}

func TestBinlogNewlyAddedRequiresTheFlag(t *testing.T) {
	off := &handler{include: compilePatterns(`db\..*`), allow: map[string]struct{}{"db.user": {}}}
	if off.captured("db", "order") {
		t.Fatal("order was captured without the binlog flag and without an allow entry")
	}
	closed := &handler{include: compilePatterns(`db\..*`)}
	if closed.captured("db", "order") {
		t.Fatal("nil allow without binlog flag must not open-ended capture")
	}
	on := &handler{include: compilePatterns(`db\..*`), binlogNewlyAdded: true}
	if !on.captured("db", "order") {
		t.Fatal("binlog flag did not accept a matching new table")
	}
}

func TestNewlyAddedLimitDropsLaterTables(t *testing.T) {
	h := &handler{
		include: compilePatterns(`db\..*`),
		allow:   map[string]struct{}{"db.t": {}},
	}
	if !h.captured("db", "t") || h.captured("db", "new") {
		t.Fatal("known-table limit did not drop the later table")
	}
}

func TestAdmitCreateWithAllowSet(t *testing.T) {
	h := &handler{
		include:          compilePatterns(`db\..*`),
		allow:            map[string]struct{}{"db.user": {}},
		binlogNewlyAdded: true,
	}
	if h.captured("db", "order") {
		t.Fatal("config-diff DML must stay blocked while allow is set")
	}
	if !h.admitCreate("db", "crt") {
		t.Fatal("runtime CREATE should admit when C=true")
	}
	if !h.captured("db", "crt") {
		t.Fatal("admitted CREATE table should pass captured()")
	}
	if h.captured("db", "order") {
		t.Fatal("config-diff table must remain blocked after unrelated CREATE")
	}
}
