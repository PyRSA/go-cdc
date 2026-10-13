package mysql

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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

func TestDrainCrashBeforePersistReplayRedelivers(t *testing.T) {
	dir := t.TempDir()
	spill := OpenSpill(dir)
	ev1 := event.Event{Op: event.OpCreate, Database: "db", Table: "t", After: map[string]any{"id": "1"}}
	ev2 := event.Event{Op: event.OpCreate, Database: "db", Table: "t", After: map[string]any{"id": "2"}}
	if err := spill.AppendBatch("db.t", checkpoint.Position{File: "mysql-bin.000001", Pos: 100}, []event.Event{ev1, ev2}); err != nil {
		t.Fatal(err)
	}
	var first int
	err := spill.Drain("db.t", func(event.Event) error {
		first++
		if first == 1 {
			return errors.New("inject crash after channel handoff")
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected injected error")
	}
	// Simulate process restart: new Spill handle, no PersistReplay was called.
	spill2 := OpenSpill(dir)
	var ids []string
	if err := spill2.Drain("db.t", func(item event.Event) error {
		ids = append(ids, item.After["id"].(string))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "1" || ids[1] != "2" {
		t.Fatalf("after crash before PersistReplay want both events, got %#v", ids)
	}
}

func TestDrainSameInstanceRetryAfterCallbackError(t *testing.T) {
	dir := t.TempDir()
	spill := OpenSpill(dir)
	id := "db.t"
	ev1 := event.Event{Op: event.OpCreate, Database: "db", Table: "t", After: map[string]any{"id": "1"}}
	ev2 := event.Event{Op: event.OpCreate, Database: "db", Table: "t", After: map[string]any{"id": "2"}}
	if err := spill.AppendBatch(id, checkpoint.Position{File: "mysql-bin.000001", Pos: 100}, []event.Event{ev1, ev2}); err != nil {
		t.Fatal(err)
	}
	var n int
	err := spill.Drain(id, func(event.Event) error {
		n++
		if n == 1 {
			return nil // handed off; not yet PersistReplay
		}
		return errors.New("inject fail on second event")
	})
	if err == nil {
		t.Fatal("expected injected error")
	}
	// Same Spill instance must redeliver both events (replayed rolled back).
	var ids []string
	if err := spill.Drain(id, func(item event.Event) error {
		ids = append(ids, item.After["id"].(string))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "1" || ids[1] != "2" {
		t.Fatalf("same-instance retry want [1 2], got %#v", ids)
	}
}

func TestAppendBatchRejectedWhenCorrupt(t *testing.T) {
	dir := t.TempDir()
	spill := OpenSpill(dir)
	id := "db.t"
	if err := spill.AppendBatch(id, checkpoint.Position{File: "mysql-bin.000001", Pos: 100}, []event.Event{
		{Op: event.OpCreate, Database: "db", Table: "t", After: map[string]any{"id": "1"}},
	}); err != nil {
		t.Fatal(err)
	}
	tab, err := spill.table(id)
	if err != nil {
		t.Fatal(err)
	}
	// fail() marks corrupt (not closed) when write/Sync fails and Truncate/Seek
	// rollback also fails; further AppendBatch/Drain/PersistReplay must return
	// ErrSpillCorrupt so dirty bytes are not extended.
	tab.mu.Lock()
	tab.corrupt = true
	if tab.closed {
		t.Fatal("fail() must mark corrupt without setting closed")
	}
	tab.mu.Unlock()

	err = spill.AppendBatch(id, checkpoint.Position{File: "mysql-bin.000001", Pos: 200}, []event.Event{
		{Op: event.OpCreate, Database: "db", Table: "t", After: map[string]any{"id": "2"}},
	})
	if err == nil || !errors.Is(err, ErrSpillCorrupt) {
		t.Fatalf("want ErrSpillCorrupt, got %v", err)
	}
	if err := spill.Drain(id, func(event.Event) error { return nil }); err == nil || !errors.Is(err, ErrSpillCorrupt) {
		t.Fatalf("Drain on corrupt table want ErrSpillCorrupt, got %v", err)
	}
	if err := spill.PersistReplay(id); err == nil || !errors.Is(err, ErrSpillCorrupt) {
		t.Fatalf("PersistReplay on corrupt table want ErrSpillCorrupt, got %v", err)
	}
	tab.mu.Lock()
	corrupt, closed := tab.corrupt, tab.closed
	tab.mu.Unlock()
	if !corrupt || closed {
		t.Fatalf("after reject want corrupt && !closed, got corrupt=%v closed=%v", corrupt, closed)
	}
}

func TestDrainFailsIfCorruptMidFlight(t *testing.T) {
	// Approximates mid-Drain corrupt (as if AppendBatch rollback failed
	// concurrently): corrupt must not look like a successful empty Drain.
	dir := t.TempDir()
	spill := OpenSpill(dir)
	id := "db.t"
	if err := spill.AppendBatch(id, checkpoint.Position{File: "mysql-bin.000001", Pos: 100}, []event.Event{
		{Op: event.OpCreate, Database: "db", Table: "t", After: map[string]any{"id": "1"}},
		{Op: event.OpCreate, Database: "db", Table: "t", After: map[string]any{"id": "2"}},
	}); err != nil {
		t.Fatal(err)
	}
	err := spill.Drain(id, func(event.Event) error {
		tab, err := spill.table(id)
		if err != nil {
			return err
		}
		tab.mu.Lock()
		tab.corrupt = true
		tab.mu.Unlock()
		return nil
	})
	if err == nil || !errors.Is(err, ErrSpillCorrupt) {
		t.Fatalf("Drain must return ErrSpillCorrupt when table turns corrupt mid-flight, got %v", err)
	}
	tab, err := spill.table(id)
	if err != nil {
		t.Fatal(err)
	}
	tab.mu.Lock()
	corrupt, closed := tab.corrupt, tab.closed
	tab.mu.Unlock()
	if !corrupt || closed {
		t.Fatalf("mid-flight corrupt want corrupt && !closed, got corrupt=%v closed=%v", corrupt, closed)
	}
}

func TestPersistReplaySkipsAlreadyDurableEvents(t *testing.T) {
	dir := t.TempDir()
	spill := OpenSpill(dir)
	ev := event.Event{Op: event.OpCreate, Database: "db", Table: "t", After: map[string]any{"id": "1"}}
	if err := spill.AppendBatch("db.t", checkpoint.Position{File: "mysql-bin.000001", Pos: 100}, []event.Event{ev}); err != nil {
		t.Fatal(err)
	}
	if err := spill.Drain("db.t", func(event.Event) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := spill.PersistReplay("db.t"); err != nil {
		t.Fatal(err)
	}
	spill2 := OpenSpill(dir)
	var n int
	if err := spill2.Drain("db.t", func(event.Event) error {
		n++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("after PersistReplay restart drained %d, want 0", n)
	}
}

func TestAppendBatchPartialCrashDoesNotAdvanceLastPosition(t *testing.T) {
	dir := t.TempDir()
	spill := OpenSpill(dir)
	id := "db.t"
	posOK := checkpoint.Position{File: "mysql-bin.000001", Pos: 100}
	if err := spill.AppendBatch(id, posOK, []event.Event{
		{Op: event.OpCreate, Database: "db", Table: "t", After: map[string]any{"id": "1"}},
	}); err != nil {
		t.Fatal(err)
	}
	path, _ := spillPaths(dir, id)
	// Inject multiple complete JSON event lines without a fence (crashed mid-tx
	// after writing more than one row of a multi-event batch).
	xid200 := checkpoint.Position{File: "mysql-bin.000001", Pos: 200}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	for _, rowID := range []string{"2", "3"} {
		partial := spillLine{
			Pos:   xid200,
			Event: &event.Event{Op: event.OpCreate, Database: "db", Table: "t", After: map[string]any{"id": rowID}},
		}
		body, err := json.Marshal(partial)
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

	spill2 := OpenSpill(dir)
	last, ok, err := spill2.LastPosition(id)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || last.Pos != 100 {
		t.Fatalf("last %#v ok %v; incomplete batch must not advance past fenced xid 100", last, ok)
	}
	if xidAlreadySpilled(last, ok, xid200) {
		t.Fatal("incomplete xid 200 must not be treated as already spilled")
	}
	// Binlog redelivery of the full multi-event txn (before Drain closes the queue).
	if err := spill2.AppendBatch(id, xid200, []event.Event{
		{Op: event.OpCreate, Database: "db", Table: "t", After: map[string]any{"id": "2"}},
		{Op: event.OpCreate, Database: "db", Table: "t", After: map[string]any{"id": "3"}},
	}); err != nil {
		t.Fatal(err)
	}
	var ids []string
	if err := spill2.Drain(id, func(item event.Event) error {
		ids = append(ids, item.After["id"].(string))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 || ids[0] != "1" || ids[1] != "2" || ids[2] != "3" {
		t.Fatalf("after redelivery want [1 2 3], got %#v", ids)
	}
}

func TestAppendBatchFirstCrashWithoutFenceDoesNotAdvanceLastPosition(t *testing.T) {
	// Crash on the very first AppendBatch before any fence: hex files must
	// discard unfenced event lines so xid dedup cannot skip binlog redelivery.
	dir := t.TempDir()
	id := "db.t"
	xid := checkpoint.Position{File: "mysql-bin.000001", Pos: 200}
	path, _ := spillPaths(dir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, rowID := range []string{"2", "3"} {
		body, err := json.Marshal(spillLine{
			Pos:   xid,
			Event: &event.Event{Op: event.OpCreate, Database: "db", Table: "t", After: map[string]any{"id": rowID}},
		})
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

	spill := OpenSpill(dir)
	last, ok, err := spill.LastPosition(id)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("unfenced first batch must not set LastPosition, got %#v", last)
	}
	if xidAlreadySpilled(last, ok, xid) {
		t.Fatal("incomplete first batch must not be treated as already spilled")
	}
	if err := spill.AppendBatch(id, xid, []event.Event{
		{Op: event.OpCreate, Database: "db", Table: "t", After: map[string]any{"id": "2"}},
		{Op: event.OpCreate, Database: "db", Table: "t", After: map[string]any{"id": "3"}},
	}); err != nil {
		t.Fatal(err)
	}
	var ids []string
	if err := spill.Drain(id, func(item event.Event) error {
		ids = append(ids, item.After["id"].(string))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "2" || ids[1] != "3" {
		t.Fatalf("after redelivery want [2 3], got %#v", ids)
	}
}

func TestRemoveDeletesHexSpillFiles(t *testing.T) {
	dir := t.TempDir()
	id := "a_b.c"
	spill := OpenSpill(dir)
	if err := spill.AppendBatch(id, checkpoint.Position{File: "mysql-bin.000001", Pos: 1}, []event.Event{
		{Op: event.OpCreate, Database: "a_b", Table: "c", After: map[string]any{"id": "1"}},
	}); err != nil {
		t.Fatal(err)
	}
	hexBin, hexOff := spillPaths(dir, id)
	if err := spill.Remove(id); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(hexBin); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("binlog should be removed: %v", err)
	}
	if _, err := os.Stat(hexOff); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
}

func TestAppendBatchIncompleteTrailingLineTruncated(t *testing.T) {
	dir := t.TempDir()
	spill := OpenSpill(dir)
	id := "db.t"
	if err := spill.AppendBatch(id, checkpoint.Position{File: "mysql-bin.000001", Pos: 100}, []event.Event{
		{Op: event.OpCreate, Database: "db", Table: "t", After: map[string]any{"id": "1"}},
	}); err != nil {
		t.Fatal(err)
	}
	path, _ := spillPaths(dir, id)
	if err := os.WriteFile(path, append(mustRead(t, path), []byte(`{"pos":{"File":"mysql-bin.000001","Pos":200},"event":`)...), 0o644); err != nil {
		t.Fatal(err)
	}
	spill2 := OpenSpill(dir)
	last, ok, err := spill2.LastPosition(id)
	if err != nil || !ok || last.Pos != 100 {
		t.Fatalf("last %#v ok %v err %v", last, ok, err)
	}
	var n int
	if err := spill2.Drain(id, func(event.Event) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("drained %d", n)
	}
}

func TestSpillPathsDistinctForDotUnderscoreCollision(t *testing.T) {
	dir := t.TempDir()
	spill := OpenSpill(dir)
	a := "a_b.c"
	b := "a.b_c"
	if err := spill.AppendBatch(a, checkpoint.Position{File: "mysql-bin.000001", Pos: 10}, []event.Event{
		{Op: event.OpCreate, Database: "a_b", Table: "c", After: map[string]any{"id": "a"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := spill.AppendBatch(b, checkpoint.Position{File: "mysql-bin.000001", Pos: 20}, []event.Event{
		{Op: event.OpCreate, Database: "a", Table: "b_c", After: map[string]any{"id": "b"}},
	}); err != nil {
		t.Fatal(err)
	}
	pathA, _ := spillPaths(dir, a)
	pathB, _ := spillPaths(dir, b)
	if pathA == pathB {
		t.Fatalf("paths collided: %s", pathA)
	}
	var gotA, gotB []string
	if err := spill.Drain(a, func(item event.Event) error {
		gotA = append(gotA, item.After["id"].(string))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := spill.Drain(b, func(item event.Event) error {
		gotB = append(gotB, item.After["id"].(string))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(gotA) != 1 || gotA[0] != "a" || len(gotB) != 1 || gotB[0] != "b" {
		t.Fatalf("a=%#v b=%#v", gotA, gotB)
	}
}

func TestRemoveIdempotentMissingFiles(t *testing.T) {
	spill := OpenSpill(t.TempDir())
	if err := spill.Remove("db.missing"); err != nil {
		t.Fatal(err)
	}
	if err := spill.Remove("db.missing"); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveReportsUnexpectedFailure(t *testing.T) {
	dir := t.TempDir()
	spill := OpenSpill(dir)
	id := "db.t"
	if err := spill.AppendBatch(id, checkpoint.Position{File: "mysql-bin.000001", Pos: 1}, []event.Event{
		{Op: event.OpCreate, Database: "db", Table: "t", After: map[string]any{"id": "1"}},
	}); err != nil {
		t.Fatal(err)
	}
	path, _ := spillPaths(dir, id)
	tab, err := spill.table(id)
	if err != nil {
		t.Fatal(err)
	}
	tab.mu.Lock()
	if tab.f != nil {
		_ = tab.f.Close()
		tab.f = nil
	}
	tab.mu.Unlock()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "blocker"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := spill.Remove(id); err == nil {
		t.Fatal("expected Remove to fail when binlog path is a non-empty directory")
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
