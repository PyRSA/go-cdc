package composer

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/source"
	mysqlsrc "github.com/PyRSA/go-cdc/connector/source/mysql"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
)

// TestSpillCrashBeforePersistReplayStillReachesSinkAfterRestart exercises the
// durability boundary required by pr-review/#1: advancing spill .off only after
// CheckpointBarrier (flush+checkpoint). A crash before PersistReplay must not
// lose rows — after restart, all spill events must reach the sink.
func TestSpillCrashBeforePersistReplayStillReachesSinkAfterRestart(t *testing.T) {
	dir := t.TempDir()
	spill := mysqlsrc.OpenSpill(dir)
	tableID := "db.t"
	pos := checkpoint.Position{File: "mysql-bin.000001", Pos: 100}
	ev1 := event.Event{
		Op: event.OpCreate, Database: "db", Table: "t",
		SourceDatabase: "db", SourceTable: "t",
		Columns: []string{"id"}, Keys: []string{"id"},
		After: map[string]any{"id": "1"},
	}
	ev2 := event.Event{
		Op: event.OpCreate, Database: "db", Table: "t",
		SourceDatabase: "db", SourceTable: "t",
		Columns: []string{"id"}, Keys: []string{"id"},
		After: map[string]any{"id": "2"},
	}
	if err := spill.AppendBatch(tableID, pos, []event.Event{ev1, ev2}); err != nil {
		t.Fatal(err)
	}

	sk := &recordingRowSink{}
	ckpt1 := filepath.Join(t.TempDir(), "ckpt1.json")
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "stdout"},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}

	// Phase 1: deliver first spill event through the runner (barrier flushes),
	// then "crash" before PersistReplay — spill .off must not advance.
	msgs1 := make(chan source.Message, 8)
	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- Run(context.Background(), pipe, msgs1, sk, checkpoint.Store{Path: ckpt1}, checkpoint.State{}, nil)
	}()
	var drained int
	drainErr := spill.Drain(tableID, func(item event.Event) error {
		drained++
		copied := item
		msgs1 <- source.Message{Event: &copied}
		if drained == 1 {
			done := make(chan struct{})
			msgs1 <- source.Message{CheckpointBarrier: true, BarrierDone: done}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				return errors.New("barrier timeout")
			}
			return errors.New("inject crash before PersistReplay")
		}
		return nil
	})
	if drainErr == nil {
		t.Fatal("expected injected crash")
	}
	close(msgs1)
	if err := <-errCh1; err != nil {
		t.Fatal(err)
	}
	if !sk.hasID("1") {
		t.Fatalf("phase1 sink missing id=1: %#v", sk.ids())
	}

	// Phase 2: process restart — new Spill handle, no PersistReplay was written.
	spill2 := mysqlsrc.OpenSpill(dir)
	sk2 := &recordingRowSink{}
	ckpt2 := filepath.Join(t.TempDir(), "ckpt2.json")
	msgs2 := make(chan source.Message, 8)
	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- Run(context.Background(), pipe, msgs2, sk2, checkpoint.Store{Path: ckpt2}, checkpoint.State{}, spill2)
	}()
	if err := spill2.Drain(tableID, func(item event.Event) error {
		copied := item
		msgs2 <- source.Message{Event: &copied}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	msgs2 <- source.Message{CheckpointBarrier: true, BarrierDone: done}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("barrier timeout on recovery")
	}
	if err := spill2.PersistReplay(tableID); err != nil {
		t.Fatal(err)
	}
	msgs2 <- source.Message{Captured: tableID}
	close(msgs2)
	if err := <-errCh2; err != nil {
		t.Fatal(err)
	}
	if !sk2.hasID("1") || !sk2.hasID("2") {
		t.Fatalf("after restart sink must contain both rows, got %#v", sk2.ids())
	}
}

// TestIncompleteMultiEventSpillBatchReachesSinkAfterRedelivery covers pr-review/#1:
// a crashed AppendBatch that left multiple complete JSON lines without a fence
// must not advance LastPosition; after binlog redelivery of the full txn, every
// event must reach the sink.
func TestIncompleteMultiEventSpillBatchReachesSinkAfterRedelivery(t *testing.T) {
	dir := t.TempDir()
	spill := mysqlsrc.OpenSpill(dir)
	tableID := "db.t"
	posOK := checkpoint.Position{File: "mysql-bin.000001", Pos: 100}
	ev1 := event.Event{
		Op: event.OpCreate, Database: "db", Table: "t",
		SourceDatabase: "db", SourceTable: "t",
		Columns: []string{"id"}, Keys: []string{"id"},
		After: map[string]any{"id": "1"},
	}
	if err := spill.AppendBatch(tableID, posOK, []event.Event{ev1}); err != nil {
		t.Fatal(err)
	}

	// Simulate crash mid-AppendBatch: two complete event lines, no fence.
	// Path layout matches connector/source/mysql.spillPaths (hex(tableID)+.binlog).
	xid200 := checkpoint.Position{File: "mysql-bin.000001", Pos: 200}
	path := filepath.Join(dir, hex.EncodeToString([]byte(tableID))+".binlog")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	for _, rowID := range []string{"2", "3"} {
		line := struct {
			Pos   checkpoint.Position `json:"pos"`
			Event *event.Event        `json:"event,omitempty"`
		}{
			Pos: xid200,
			Event: &event.Event{
				Op: event.OpCreate, Database: "db", Table: "t",
				SourceDatabase: "db", SourceTable: "t",
				Columns: []string{"id"}, Keys: []string{"id"},
				After: map[string]any{"id": rowID},
			},
		}
		body, err := json.Marshal(line)
		if err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		if _, err := f.Write(append(body, '\n')); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
	}
	_ = f.Close()

	spill2 := mysqlsrc.OpenSpill(dir)
	last, ok, err := spill2.LastPosition(tableID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || last.Pos != 100 {
		t.Fatalf("incomplete multi-event batch must not advance last: %#v ok=%v", last, ok)
	}

	// Binlog redelivery of the full transaction.
	ev2 := event.Event{
		Op: event.OpCreate, Database: "db", Table: "t",
		SourceDatabase: "db", SourceTable: "t",
		Columns: []string{"id"}, Keys: []string{"id"},
		After: map[string]any{"id": "2"},
	}
	ev3 := event.Event{
		Op: event.OpCreate, Database: "db", Table: "t",
		SourceDatabase: "db", SourceTable: "t",
		Columns: []string{"id"}, Keys: []string{"id"},
		After: map[string]any{"id": "3"},
	}
	if err := spill2.AppendBatch(tableID, xid200, []event.Event{ev2, ev3}); err != nil {
		t.Fatal(err)
	}

	sk := &recordingRowSink{}
	ckpt := filepath.Join(t.TempDir(), "ckpt.json")
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "stdout"},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	msgs := make(chan source.Message, 16)
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: ckpt}, checkpoint.State{}, spill2)
	}()
	if err := spill2.Drain(tableID, func(item event.Event) error {
		copied := item
		msgs <- source.Message{Event: &copied}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	msgs <- source.Message{CheckpointBarrier: true, BarrierDone: done}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("barrier timeout")
	}
	if err := spill2.PersistReplay(tableID); err != nil {
		t.Fatal(err)
	}
	close(msgs)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if !sk.hasID("1") || !sk.hasID("2") || !sk.hasID("3") {
		t.Fatalf("sink must contain all txn events after recovery, got %#v", sk.ids())
	}
}

func TestCapturedRemoveErrorFailsRun(t *testing.T) {
	spill := &recordingSpill{err: errors.New("remove failed")}
	msgs := make(chan source.Message, 1)
	msgs <- source.Message{Captured: "db.order"}
	close(msgs)
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "stdout"},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	err := Run(context.Background(), pipe, msgs, &orderSink{}, checkpoint.Store{Path: filepath.Join(t.TempDir(), "c.ckpt")}, checkpoint.State{}, spill)
	if err == nil {
		t.Fatal("expected Remove error to fail Run")
	}
	if len(spill.removed) != 1 {
		t.Fatalf("Remove should have been attempted: %#v", spill.removed)
	}
}

type recordingRowSink struct {
	mu    sync.Mutex
	rows  []event.Event
	flush int
}

func (s *recordingRowSink) Open(context.Context) error { return nil }
func (s *recordingRowSink) CreateTable(schema.Table) error {
	return nil
}
func (s *recordingRowSink) ApplySchema(schema.Change) error { return nil }
func (s *recordingRowSink) Write(batch []event.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append(s.rows, batch...)
	return nil
}
func (s *recordingRowSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flush++
	return nil
}
func (s *recordingRowSink) Close() error { return nil }

func (s *recordingRowSink) hasID(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, row := range s.rows {
		if v, ok := row.After["id"]; ok && v == id {
			return true
		}
	}
	return false
}

func (s *recordingRowSink) ids() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.rows))
	for _, row := range s.rows {
		if v, ok := row.After["id"].(string); ok {
			out = append(out, v)
		}
	}
	return out
}
