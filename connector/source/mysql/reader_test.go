package mysql

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/source"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
)

func TestInitialSnapshotThenBinlogFromLowWatermark(t *testing.T) {
	db := newMemDB([]string{"1", "2", "3"})
	stream := &memStream{events: []Commit{{
		Event: &event.Event{Op: event.OpCreate, Database: "db", Table: "t", SourceDatabase: "db", SourceTable: "t", After: map[string]any{"id": "9"}},
	}, {
		Position: &checkpoint.Position{File: "mysql-bin.000001", Pos: 200},
	}}}
	cap := &Capture{
		Config:      definition.Source{StartupMode: definition.StartupInitial, ChunkSize: 2, Tables: mustSelector(t)},
		Parallelism: 2,
		DB:          db,
		Stream:      stream,
	}
	msgs := collect(t, cap)
	if msgs[0].Watermark == nil || msgs[0].Watermark.Pos != 100 {
		t.Fatalf("watermark %#v", msgs[0])
	}
	var rows int
	var captured bool
	for _, msg := range msgs {
		if msg.Event != nil && msg.Event.After["id"] != "9" {
			rows++
		}
		if msg.Captured == "db.t" {
			captured = true
		}
	}
	if rows != 3 || !captured {
		t.Fatalf("rows %d captured %v\n%#v", rows, captured, msgs)
	}
	if !db.closed {
		t.Fatal("snapshot connection still open")
	}
	if stream.got.Pos != 100 {
		t.Fatalf("binlog started at %d", stream.got.Pos)
	}
	if db.maxInflight < 2 {
		t.Fatalf("parallelism did not overlap reads: %d", db.maxInflight)
	}
}

func TestResumeSkipsFinishedSplitAndReplaysLowWatermark(t *testing.T) {
	db := newMemDB([]string{"1", "2", "3"})
	high := "2"
	stream := &memStream{}
	cap := &Capture{
		Config:      definition.Source{StartupMode: definition.StartupInitial, ChunkSize: 2, Tables: mustSelector(t)},
		Parallelism: 1,
		State: checkpoint.State{
			LowWatermark:   &checkpoint.Position{File: "mysql-bin.000001", Pos: 100},
			FinishedSplits: map[string][]checkpoint.Split{"db.t": {{Low: "1", High: &high}}},
		},
		DB:     db,
		Stream: stream,
	}
	msgs := collect(t, cap)
	for _, msg := range msgs {
		if msg.Event != nil && msg.Event.After["id"] == "1" {
			t.Fatal("finished split was read again")
		}
	}
	if stream.got.Pos != 100 {
		t.Fatalf("binlog pos %d", stream.got.Pos)
	}
}

func TestBinlogFailureIsDelivered(t *testing.T) {
	stream := &memStream{events: []Commit{{Err: errors.New("binlog down")}}}
	cap := &Capture{
		Config: definition.Source{StartupMode: definition.StartupLatest},
		State:  checkpoint.State{AckedOffset: &checkpoint.Position{File: "mysql-bin.000001", Pos: 10}},
		Stream: stream,
	}
	msgs := collect(t, cap)
	var got error
	for _, msg := range msgs {
		if msg.Err != nil {
			got = msg.Err
		}
	}
	if got == nil || !strings.Contains(got.Error(), "binlog down") {
		t.Fatalf("got %v", got)
	}
}

func TestStartupModesResolveAPosition(t *testing.T) {
	cases := []struct {
		name string
		cfg  definition.Source
		pos  uint32
		file string
	}{
		{"latest", definition.Source{StartupMode: definition.StartupLatest}, 100, "mysql-bin.000001"},
		{"earliest", definition.Source{StartupMode: definition.StartupEarliest}, 4, "mysql-bin.000001"},
		{"specific", definition.Source{StartupMode: definition.StartupSpecificOffset, SpecificFile: "mysql-bin.000008", SpecificPos: 123}, 123, "mysql-bin.000008"},
		{"timestamp", definition.Source{StartupMode: definition.StartupTimestamp, TimestampMillis: 1_700_000_000_000}, 50, "mysql-bin.000001"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream := &memStream{}
			cap := &Capture{Config: tc.cfg, DB: newMemDB(nil), Stream: stream}
			if errMsg := captureErr(t, cap); errMsg != "" {
				t.Fatal(errMsg)
			}
			if stream.got.File != tc.file || stream.got.Pos != tc.pos {
				t.Fatalf("started at %s:%d", stream.got.File, stream.got.Pos)
			}
		})
	}
}

func TestEmptyStartupPositionFails(t *testing.T) {
	db := newMemDB(nil)
	db.emptyMaster = true
	cap := &Capture{
		Config: definition.Source{StartupMode: definition.StartupLatest},
		DB:     db,
		Stream: &memStream{},
	}
	got := captureErr(t, cap)
	if !strings.Contains(got, "binlog position is empty") {
		t.Fatalf("got %s", got)
	}
}

func TestCheckpointSnapshotsUncapturedMatchingTables(t *testing.T) {
	db := newMemDB([]string{"1", "2"})
	stream := &memStream{}
	cap := &Capture{
		Config: definition.Source{StartupMode: definition.StartupInitial, ChunkSize: 2, Tables: mustSelector(t)},
		State:  checkpoint.State{LowWatermark: &checkpoint.Position{File: "mysql-bin.000001", Pos: 100}},
		DB:     db,
		Stream: stream,
	}
	msgs := collect(t, cap)
	var rows int
	var captured bool
	for _, msg := range msgs {
		if msg.Err != nil {
			t.Fatal(msg.Err)
		}
		if msg.Event != nil {
			rows++
		}
		if msg.Captured == "db.t" {
			captured = true
		}
	}
	if rows != 2 || !captured {
		t.Fatalf("expected config-added table snapshot rows=2 captured=true, got rows=%d captured=%v", rows, captured)
	}
	if stream.got.Pos != 100 {
		t.Fatalf("binlog pos %d", stream.got.Pos)
	}
}

func TestEmptyTableStillPublishesCatalog(t *testing.T) {
	db := newMemDB(nil)
	cap := &Capture{
		Config: definition.Source{StartupMode: definition.StartupSnapshot, ChunkSize: 2, Tables: mustSelector(t)},
		DB:     db,
		Stream: &memStream{},
	}
	msgs := collect(t, cap)
	var schemaSeen, captured bool
	for _, msg := range msgs {
		if msg.Err != nil {
			t.Fatal(msg.Err)
		}
		if msg.Schema != nil {
			schemaSeen = true
		}
		if msg.Captured == "db.t" {
			captured = true
		}
		if msg.Event != nil {
			t.Fatalf("empty table emitted a row: %#v", msg.Event)
		}
	}
	if !schemaSeen || !captured {
		t.Fatalf("schema %v captured %v", schemaSeen, captured)
	}
}

func TestChunkKeyIsRejectedBeforeAnyRead(t *testing.T) {
	db := newMemDB([]string{"1"})
	db.tables = []schema.Table{
		{Database: "db", Table: "ok", Columns: []schema.Column{{Name: "id", Type: "int"}}, PrimaryKeys: []string{"id"}},
		{Database: "db", Table: "bad", Columns: []schema.Column{{Name: "id", Type: "float"}}, PrimaryKeys: []string{"id"}},
	}
	sel, err := definition.CompileSelector(`db\..*`, "")
	if err != nil {
		t.Fatal(err)
	}
	cap := &Capture{
		Config: definition.Source{StartupMode: definition.StartupSnapshot, ChunkSize: 2, Tables: sel},
		DB:     db,
		Stream: &memStream{},
	}
	got := captureErr(t, cap)
	if !strings.Contains(got, "db.bad") {
		t.Fatalf("got %s", got)
	}
	if db.reads != 0 {
		t.Fatalf("reads %d", db.reads)
	}
}

func TestSnapshotUsesProjectedColumns(t *testing.T) {
	db := newMemDB([]string{"1"})
	cap := &Capture{
		Config: definition.Source{StartupMode: definition.StartupSnapshot, ChunkSize: 2, Tables: mustSelector(t)},
		DB:     db,
		Stream: &memStream{},
		Project: func(_, _ string, _ []string) ([]string, error) {
			return []string{"name"}, nil
		},
	}
	if errMsg := captureErr(t, cap); errMsg != "" {
		t.Fatal(errMsg)
	}
	if len(db.cols) != 1 || db.cols[0] != "name" {
		t.Fatalf("columns %#v", db.cols)
	}
}

func captureErr(t *testing.T, cap *Capture) string {
	t.Helper()
	for _, msg := range collect(t, cap) {
		if msg.Err != nil {
			return msg.Err.Error()
		}
	}
	return ""
}

func TestChunkColumnRejectsDisallowedAndNullableKeys(t *testing.T) {
	table := schema.Table{
		Database: "db", Table: "t",
		Columns: []schema.Column{
			{Name: "id", Type: "float"},
			{Name: "name", Type: "varchar(32)", Nullable: true},
		},
	}
	if _, err := chunkColumn(table, ""); err == nil || !strings.Contains(err.Error(), "db.t") {
		t.Fatalf("missing pk: %v", err)
	}
	table.PrimaryKeys = []string{"id"}
	if _, err := chunkColumn(table, ""); err == nil || !strings.Contains(err.Error(), "float") {
		t.Fatalf("float pk: %v", err)
	}
	if _, err := chunkColumn(table, "name"); err == nil || !strings.Contains(err.Error(), "NOT NULL") {
		t.Fatalf("nullable key: %v", err)
	}
}

func mustSelector(t *testing.T) *definition.Selector {
	t.Helper()
	sel, err := definition.CompileSelector(`db\.t`, "")
	if err != nil {
		t.Fatal(err)
	}
	return sel
}

func collect(t *testing.T, cap *Capture) []source.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, err := cap.Messages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var msgs []source.Message
	for msg := range ch {
		msgs = append(msgs, msg)
	}
	return msgs
}

type memDB struct {
	keys        []string
	tables      []schema.Table
	closed      bool
	emptyMaster bool
	reads       int
	cols        []string
	earliest    checkpoint.Position
	atTime      checkpoint.Position
	mu          sync.Mutex
	inflight    int
	maxInflight int
}

func newMemDB(keys []string) *memDB { return &memDB{keys: keys} }

func (m *memDB) MasterStatus(context.Context) (checkpoint.Position, error) {
	if m.emptyMaster {
		return checkpoint.Position{}, nil
	}
	return checkpoint.Position{File: "mysql-bin.000001", Pos: 100}, nil
}

func (m *memDB) EarliestBinlog(context.Context) (checkpoint.Position, error) {
	if m.earliest.File != "" || m.earliest.GTID != "" {
		return m.earliest, nil
	}
	return checkpoint.Position{File: "mysql-bin.000001", Pos: 4}, nil
}

func (m *memDB) PositionAt(context.Context, int64) (checkpoint.Position, error) {
	if m.atTime.File != "" || m.atTime.GTID != "" {
		return m.atTime, nil
	}
	return checkpoint.Position{File: "mysql-bin.000001", Pos: 50}, nil
}

func (m *memDB) ListTables(context.Context) ([]schema.Table, error) {
	if m.tables != nil {
		return m.tables, nil
	}
	return []schema.Table{{
		Database: "db", Table: "t",
		Columns:     []schema.Column{{Name: "id", Type: "int"}, {Name: "name", Type: "varchar(32)"}},
		PrimaryKeys: []string{"id"},
	}}, nil
}

func (m *memDB) SplitKeys(_ context.Context, _, _, _ string, low *string, limit int) ([]string, error) {
	start := 0
	if low != nil {
		for start < len(m.keys) && m.keys[start] < *low {
			start++
		}
	}
	end := start + limit
	if end > len(m.keys) {
		end = len(m.keys)
	}
	return append([]string(nil), m.keys[start:end]...), nil
}

func (m *memDB) ReadChunk(_ context.Context, _, _ string, columns []string, _, low string, high *string) ([]map[string]any, error) {
	m.mu.Lock()
	m.reads++
	m.cols = append([]string(nil), columns...)
	m.mu.Unlock()
	m.mu.Lock()
	m.inflight++
	if m.inflight > m.maxInflight {
		m.maxInflight = m.inflight
	}
	m.mu.Unlock()
	time.Sleep(20 * time.Millisecond)
	m.mu.Lock()
	m.inflight--
	m.mu.Unlock()
	var rows []map[string]any
	for _, key := range m.keys {
		if low != "" && key < low {
			continue
		}
		if high != nil && key >= *high {
			continue
		}
		rows = append(rows, map[string]any{"id": key})
	}
	return rows, nil
}

func (m *memDB) Close() error {
	m.closed = true
	return nil
}

type memStream struct {
	got    checkpoint.Position
	events []Commit
}

func (m *memStream) ReadFrom(_ context.Context, pos checkpoint.Position) (<-chan Commit, error) {
	m.got = pos
	ch := make(chan Commit, len(m.events))
	for _, event := range m.events {
		ch <- event
	}
	close(ch)
	return ch, nil
}

func (m *memStream) Close() error { return nil }
