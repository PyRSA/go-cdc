package composer

import (
	"strings"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
)

func TestCountRowEvents(t *testing.T) {
	batch := []event.Event{
		{Op: event.OpCreate},
		{Op: event.OpUpdate},
		{Op: event.OpDDL},
		{Op: event.OpDelete},
		{Op: event.OpRead},
	}
	if got := countRowEvents(batch); got != 4 {
		t.Fatalf("countRowEvents=%d want 4", got)
	}
}

func TestCheckpointFailureDoesNotUpdateSuccessPosition(t *testing.T) {
	stats := &runStats{}
	pos := &checkpoint.Position{File: "mysql-bin.000001", Pos: 100}
	stats.noteCheckpointSuccess(pos, time.Now(), time.Millisecond)
	stats.noteCheckpointFailure()
	if !stats.checkpointEverSave || stats.checkpointSaveOK {
		t.Fatalf("expected failed status after error, ok=%v ever=%v", stats.checkpointSaveOK, stats.checkpointEverSave)
	}
	if formatPosition(stats.checkpointPos) != "mysql-bin.000001:100" {
		t.Fatalf("failure must keep last success position, got %s", formatPosition(stats.checkpointPos))
	}
	if stats.checkpointErrors != 1 {
		t.Fatalf("checkpointErrors=%d", stats.checkpointErrors)
	}
}

func TestStatusIdleDeltaIsZero(t *testing.T) {
	stats := &runStats{
		startedAt: time.Now().Add(-time.Minute),
		prevAt:    time.Now().Add(-time.Minute),
		events:    10,
		rows:      8,
		sinkRows:  8,
	}
	stats.prevEvents = 10
	stats.prevRows = 8
	stats.prevSinkRows = 8
	msg := stats.statusMessage("demo")
	if !strings.Contains(msg, "Events: total=10 delta=0 rate=0/s") {
		t.Fatalf("events line: %s", msg)
	}
	if !strings.Contains(msg, "Rows: total=8 delta=0 rate=0/s") {
		t.Fatalf("rows line: %s", msg)
	}
	if !strings.Contains(msg, "Rows: total=8 delta=0\n") {
		t.Fatalf("sink rows line: %s", msg)
	}
}

func TestFormatRateUsesElapsed(t *testing.T) {
	if got := formatRate(150, 2*time.Second); got != "75/s" {
		t.Fatalf("got %s", got)
	}
	if got := formatRate(0, time.Second); got != "0/s" {
		t.Fatalf("got %s", got)
	}
}
