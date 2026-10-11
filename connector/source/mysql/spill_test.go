package mysql

import (
	"errors"
	"testing"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
)

func TestSpillReplayKeepsOrderAndPosition(t *testing.T) {
	dir := t.TempDir()
	spill := OpenSpill(dir)
	first := event.Event{Op: event.OpCreate, Database: "db", Table: "order", After: map[string]any{"id": "1"}}
	second := event.Event{Op: event.OpUpdate, Database: "db", Table: "order", After: map[string]any{"id": "1"}}
	if err := spill.AppendBatch("db.order", checkpoint.Position{File: "mysql-bin.000001", Pos: 100}, []event.Event{first}); err != nil {
		t.Fatal(err)
	}
	if err := spill.AppendBatch("db.order", checkpoint.Position{File: "mysql-bin.000001", Pos: 180}, []event.Event{second}); err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := spill.Drain("db.order", func(item event.Event) error {
		got = append(got, string(item.Op))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != string(event.OpCreate) || got[1] != string(event.OpUpdate) {
		t.Fatalf("%#v", got)
	}
	pos, ok, err := spill.LastPosition("db.order")
	if err != nil || !ok || pos.Pos != 180 {
		t.Fatalf("pos %#v ok %v err %v", pos, ok, err)
	}
}

func TestDrainClosesAndRejectsLaterAppend(t *testing.T) {
	spill := OpenSpill(t.TempDir())
	if err := spill.AppendBatch("db.order", checkpoint.Position{File: "mysql-bin.000001", Pos: 100}, []event.Event{
		{Op: event.OpCreate, Database: "db", Table: "order"},
	}); err != nil {
		t.Fatal(err)
	}
	var n int
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = spill.Drain("db.order", func(event.Event) error {
			n++
			if n == 1 {
				_ = spill.AppendBatch("db.order", checkpoint.Position{File: "mysql-bin.000001", Pos: 140}, []event.Event{
					{Op: event.OpUpdate, Database: "db", Table: "order"},
				})
			}
			return nil
		})
	}()
	<-done
	if n != 2 {
		t.Fatalf("drained %d events, want both the original row and the row appended during drain", n)
	}
	err := spill.AppendBatch("db.order", checkpoint.Position{File: "mysql-bin.000001", Pos: 200}, []event.Event{
		{Op: event.OpCreate, Database: "db", Table: "order"},
	})
	if !errors.Is(err, ErrSpillClosed) {
		t.Fatalf("after drain: %v", err)
	}
}

func TestRemoveLeavesTombstoneRejectsAppend(t *testing.T) {
	spill := OpenSpill(t.TempDir())
	if err := spill.AppendBatch("db.order", checkpoint.Position{File: "mysql-bin.000001", Pos: 100}, []event.Event{
		{Op: event.OpCreate, Database: "db", Table: "order"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := spill.Drain("db.order", func(event.Event) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := spill.Remove("db.order"); err != nil {
		t.Fatal(err)
	}
	err := spill.AppendBatch("db.order", checkpoint.Position{File: "mysql-bin.000001", Pos: 200}, []event.Event{
		{Op: event.OpCreate, Database: "db", Table: "order"},
	})
	if !errors.Is(err, ErrSpillClosed) {
		t.Fatalf("after Remove: %v, want ErrSpillClosed", err)
	}
}

func TestXIDAlreadyOnDiskIsSkipped(t *testing.T) {
	last := checkpoint.Position{File: "mysql-bin.000001", Pos: 180}
	if !xidAlreadySpilled(last, true, checkpoint.Position{File: "mysql-bin.000001", Pos: 180}) {
		t.Fatal("same xid must not be appended again")
	}
	if xidAlreadySpilled(last, true, checkpoint.Position{File: "mysql-bin.000002", Pos: 4}) {
		t.Fatal("next file is new")
	}
	if xidAlreadySpilled(last, false, last) {
		t.Fatal("empty spill has nothing to skip")
	}
}

func TestXIDAlreadySpilledGTIDOnly(t *testing.T) {
	last := checkpoint.Position{GTID: "uuid:1-10"}
	if !xidAlreadySpilled(last, true, checkpoint.Position{GTID: "uuid:1-10"}) {
		t.Fatal("equal gtid without file must skip")
	}
	if xidAlreadySpilled(last, true, checkpoint.Position{GTID: "uuid:1-11"}) {
		t.Fatal("different gtid must append")
	}
	mixed := checkpoint.Position{File: "mysql-bin.000001", Pos: 10, GTID: "uuid:1-10"}
	if xidAlreadySpilled(mixed, true, checkpoint.Position{GTID: "uuid:1-10"}) {
		t.Fatal("incomparable file vs gtid-only must not skip")
	}
}
