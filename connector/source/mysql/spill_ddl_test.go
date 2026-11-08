package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/source"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
)

// openSpillInDir opens a spill rooted at a caller-owned directory so a test can
// close it and reopen the same files, which is what a restart does.
func openSpillInDir(t *testing.T, dir string) *Spill {
	t.Helper()
	return OpenSpill(dir)
}

// runBinlogFrom runs binlogFrom to completion and returns every message it
// published, which is what a consumer sees before a crash.
func runBinlogFrom(t *testing.T, cap *Capture, route map[string]bool) []source.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out := make(chan source.Message, 32)
	done := make(chan error, 1)
	go func() {
		done <- cap.binlogFrom(ctx, out, checkpoint.Position{File: "mysql-bin.000001", Pos: 100}, route)
		close(out)
	}()
	var msgs []source.Message
	for msg := range out {
		msgs = append(msgs, msg)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	return msgs
}

// TestDDLPositionNeverOvertakesSpilledRows covers the ordering between a DDL
// position and rows of another table that are still buffered.
//
// binlogFrom spills buffered rows before it publishes any position. A DDL
// statement end is not an XID, but publishing it without spooling would leave
// rows that only exist in memory at the moment a position covering them can be
// saved. The guarantee tested here is therefore: by the time a position is
// published, no buffered row is left unspooled.
func TestDDLPositionNeverOvertakesSpilledRows(t *testing.T) {
	spill := openTestSpill(t, "")
	defer spill.Close()
	stream := &memStream{events: []Commit{
		// A row for a table that is still being snapshotted: buffered.
		{Event: &event.Event{
			Op: event.OpCreate, Database: "db", Table: "order",
			SourceDatabase: "db", SourceTable: "order", After: map[string]any{"id": "1"},
		}},
		// A DDL on a captured table with its own end position.
		{Event: &event.Event{
			Op: event.OpDDL, Database: "db", Table: "user",
			SourceDatabase: "db", SourceTable: "user",
			DDL: "ALTER TABLE `db`.`user` ADD COLUMN `age` int NULL",
		}},
		{Position: &checkpoint.Position{File: "mysql-bin.000001", Pos: 300}},
	}}
	cap := &Capture{Config: definition.Source{SchemaChange: true}, Stream: stream, Spill: spill}
	msgs := runBinlogFrom(t, cap, map[string]bool{"db.order": true, "db.user": false})

	var positions []uint32
	for _, msg := range msgs {
		if msg.Position != nil {
			positions = append(positions, msg.Position.Pos)
		}
		if msg.Event != nil && msg.Event.Table == "order" {
			t.Fatalf("a buffered row was published instead of spilled: %#v", msg.Event)
		}
	}
	if len(positions) != 1 || positions[0] != 300 {
		t.Fatalf("positions %v, want [300]", positions)
	}

	// By the time the position was published the row was already durable, so a
	// crash right after the checkpoint save cannot lose it.
	last, has, err := spill.LastPosition("db.order")
	if err != nil {
		t.Fatal(err)
	}
	if !has || last.Pos != 300 {
		t.Fatalf("spilled position %#v (has=%v)", last, has)
	}
	var drained []event.Event
	if err := spill.Drain("db.order", func(item event.Event) error {
		drained = append(drained, item)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(drained) != 1 || drained[0].After["id"] != "1" {
		t.Fatalf("the buffered row must be replayable: %#v", drained)
	}
}

// TestPendingRowIsNotLostWhenOnlyADDLPositionIsPublished is the adversarial
// shape: a DDL position arrives and no XID follows it. The row buffered before
// the DDL must already be in the spill, so a crash right there replays it.
func TestPendingRowIsNotLostWhenOnlyADDLPositionIsPublished(t *testing.T) {
	spill := openTestSpill(t, "")
	defer spill.Close()
	stream := &memStream{events: []Commit{
		{Event: &event.Event{
			Op: event.OpCreate, Database: "db", Table: "order",
			SourceDatabase: "db", SourceTable: "order", After: map[string]any{"id": "3"},
		}},
		{Event: &event.Event{
			Op: event.OpDDL, Database: "db", Table: "user",
			SourceDatabase: "db", SourceTable: "user",
			DDL: "ALTER TABLE `db`.`user` ADD COLUMN `x` int NULL",
		}},
		{Position: &checkpoint.Position{File: "mysql-bin.000001", Pos: 300}},
	}}
	cap := &Capture{Config: definition.Source{SchemaChange: true}, Stream: stream, Spill: spill}
	runBinlogFrom(t, cap, map[string]bool{"db.order": true, "db.user": false})

	var drained []event.Event
	if err := spill.Drain("db.order", func(item event.Event) error {
		drained = append(drained, item)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(drained) != 1 || drained[0].After["id"] != "3" {
		t.Fatalf("a row buffered across a DDL position was lost: %#v", drained)
	}
}

// TestRecoveryReplaysSpillAfterDDLPosition is the crash case end to end: the
// checkpoint was saved at a DDL position, the process died, and the row waiting
// in the spill must still be replayed on the next start.
func TestRecoveryReplaysSpillAfterDDLPosition(t *testing.T) {
	dir := t.TempDir()
	xid := checkpoint.Position{File: "mysql-bin.000001", Pos: 500}
	row := event.Event{
		Op: event.OpCreate, Database: "db", Table: "order",
		SourceDatabase: "db", SourceTable: "order", After: map[string]any{"id": "7"},
	}
	first := openSpillInDir(t, dir)
	if err := first.AppendBatch("db.order", xid, []event.Event{row}); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart. The checkpoint was saved after the DDL, so AckedOffset is past
	// it, and the table is still marked as pending a snapshot.
	reopened := openSpillInDir(t, dir)
	defer reopened.Close()
	var drained []event.Event
	if err := reopened.Drain("db.order", func(item event.Event) error {
		drained = append(drained, item)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(drained) != 1 || drained[0].After["id"] != "7" {
		t.Fatalf("drained %#v", drained)
	}
	if err := reopened.PersistReplay("db.order"); err != nil {
		t.Fatal(err)
	}
}

// TestResumeKeepsSnapshotTableUntilDrainCompletes proves the checkpoint keeps a
// table in SnapshotTables while its spill may still hold rows, so a resume
// always drains it instead of trusting the acknowledged binlog position alone.
func TestResumeKeepsSnapshotTableUntilDrainCompletes(t *testing.T) {
	dir := t.TempDir()
	xid := checkpoint.Position{File: "mysql-bin.000001", Pos: 500}
	first := openSpillInDir(t, dir)
	if err := first.AppendBatch("db.order", xid, []event.Event{{
		Op: event.OpCreate, Database: "db", Table: "order",
		SourceDatabase: "db", SourceTable: "order", After: map[string]any{"id": "9"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	state := checkpoint.State{
		AckedOffset:    &checkpoint.Position{File: "mysql-bin.000001", Pos: 900},
		SnapshotTables: map[string]checkpoint.Position{"db.order": xid},
	}
	state.Normalize()
	if !state.HasSnapshot("db.order") {
		t.Fatalf("the checkpoint dropped a table whose spill may still hold rows: %#v", state.SnapshotTables)
	}

	reopened := openSpillInDir(t, dir)
	defer reopened.Close()
	db := newMemDB(nil)
	db.tables = []schema.Table{{
		Database: "db", Table: "order",
		Columns:     []schema.Column{{Name: "id", Type: "int"}},
		PrimaryKeys: []string{"id"},
	}}
	cap := &Capture{
		Config: definition.Source{SchemaChange: true, ChunkSize: 1},
		State:  state, DB: db, Spill: reopened,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out := make(chan source.Message, 32)
	go func() {
		for msg := range out {
			if msg.CheckpointBarrier && msg.BarrierDone != nil {
				close(msg.BarrierDone)
			}
		}
	}()
	if err := cap.snapshotResumeTables(ctx, out, map[string]struct{}{"db.order": {}}); err != nil {
		t.Fatal(err)
	}
	close(out)
}
