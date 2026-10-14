package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, err := cap.Messages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var msgs []source.Message
	for msg := range ch {
		if msg.CheckpointBarrier && msg.BarrierDone != nil {
			close(msg.BarrierDone)
		}
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
	readTables  []string
	splitTables []string
	cols        []string
	earliest    checkpoint.Position
	atTime      checkpoint.Position
	describeErr error
	mu          sync.Mutex
	inflight    int
	maxInflight int
	blockTable  string
	blockRead   chan struct{}
	scanned     bool
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

func (m *memDB) DescribeTable(_ context.Context, database, table string) (schema.Table, error) {
	if m.describeErr != nil {
		return schema.Table{}, m.describeErr
	}
	tables, err := m.ListTables(context.Background())
	if err != nil {
		return schema.Table{}, err
	}
	for _, item := range tables {
		if item.Database == database && item.Table == table {
			return item, nil
		}
	}
	return schema.Table{}, fmt.Errorf("memdb: %s.%s not found", database, table)
}

func (m *memDB) SplitKeys(_ context.Context, _, table, _ string, low *string, limit int) ([]string, error) {
	m.mu.Lock()
	m.splitTables = append(m.splitTables, table)
	m.mu.Unlock()
	if m.blockTable != "" && table != m.blockTable {
		return nil, nil
	}
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

func (m *memDB) ReadChunk(_ context.Context, _, table string, columns []string, _, low string, high *string) ([]map[string]any, error) {
	if m.blockTable != "" && table == m.blockTable && m.blockRead != nil {
		<-m.blockRead
	}
	if m.blockTable != "" && table != m.blockTable {
		return nil, nil
	}
	m.mu.Lock()
	m.reads++
	m.readTables = append(m.readTables, table)
	m.cols = append([]string(nil), columns...)
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
	got         checkpoint.Position
	events      []Commit
	started     chan struct{}
	startedOnce sync.Once
	limited     []string
}

func (m *memStream) ReadFrom(_ context.Context, pos checkpoint.Position) (<-chan Commit, error) {
	m.got = pos
	if m.started != nil {
		m.startedOnce.Do(func() { close(m.started) })
	}
	ch := make(chan Commit, len(m.events))
	go func() {
		for _, event := range m.events {
			ch <- event
		}
		close(ch)
	}()
	return ch, nil
}

func (m *memStream) LimitTables(ids []string) {
	m.limited = ids
}

func (m *memStream) Close() error { return nil }

func TestBinlogStartsWhileNewTableSnapshotIsBlocked(t *testing.T) {
	db := newMemDB([]string{"1", "2"})
	db.tables = []schema.Table{
		{Database: "db", Table: "user", Columns: []schema.Column{{Name: "id", Type: "int"}}, PrimaryKeys: []string{"id"}},
		{Database: "db", Table: "order", Columns: []schema.Column{{Name: "id", Type: "int"}}, PrimaryKeys: []string{"id"}},
	}
	db.blockTable = "order"
	db.blockRead = make(chan struct{})
	sel, err := definition.CompileSelector(`db\..*`, "")
	if err != nil {
		t.Fatal(err)
	}
	stream := &memStream{
		started: make(chan struct{}),
		events: []Commit{
			{Event: &event.Event{Op: event.OpCreate, Database: "db", Table: "user", SourceDatabase: "db", SourceTable: "user", After: map[string]any{"id": "9"}}},
			{Event: &event.Event{Op: event.OpCreate, Database: "db", Table: "order", SourceDatabase: "db", SourceTable: "order", After: map[string]any{"id": "8"}}},
			{Position: &checkpoint.Position{File: "mysql-bin.000001", Pos: 500}},
		},
	}
	cap := &Capture{
		Config: definition.Source{
			StartupMode: definition.StartupInitial, ChunkSize: 2, Tables: sel,
			NewlyAddedTable: true, NewlyAddedSnapshot: true,
		},
		State: checkpoint.State{
			AckedOffset:    &checkpoint.Position{File: "mysql-bin.000001", Pos: 400},
			CapturedTables: []string{"db.user"},
		},
		DB: db, Stream: stream, Spill: OpenSpill(t.TempDir()),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, err := cap.Messages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var buffered []source.Message
	done := make(chan struct{})
	go func() {
		defer close(done)
		for msg := range ch {
			if msg.CheckpointBarrier && msg.BarrierDone != nil {
				close(msg.BarrierDone)
			}
			mu.Lock()
			buffered = append(buffered, msg)
			mu.Unlock()
		}
	}()
	select {
	case <-stream.started:
	case <-time.After(time.Second):
		t.Fatal("binlog did not start while the new table snapshot was blocked")
	}
	if stream.got.Pos != 400 {
		t.Fatalf("binlog pos %d", stream.got.Pos)
	}
	deadline := time.After(time.Second)
	for {
		mu.Lock()
		userSeen := false
		for _, msg := range buffered {
			if msg.Err != nil {
				mu.Unlock()
				t.Fatal(msg.Err)
			}
			if msg.Event != nil && msg.Event.Table == "user" && msg.Event.After["id"] == "9" {
				userSeen = true
			}
		}
		mu.Unlock()
		if userSeen {
			break
		}
		select {
		case <-deadline:
			t.Fatal("captured table event was not delivered while snapshot was blocked")
		case <-time.After(10 * time.Millisecond):
		}
	}
	close(db.blockRead)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("capture did not finish")
	}
	var sawOrderRow, sawOrderBinlog bool
	for _, msg := range buffered {
		if msg.Err != nil {
			t.Fatal(msg.Err)
		}
		if msg.Event != nil && msg.Event.Table == "order" && msg.Event.After["id"] == "1" {
			sawOrderRow = true
		}
		if msg.Event != nil && msg.Event.Table == "order" && msg.Event.After["id"] == "8" {
			sawOrderBinlog = true
		}
	}
	if !sawOrderRow || !sawOrderBinlog {
		t.Fatalf("snapshot row %v spilled binlog row %v", sawOrderRow, sawOrderBinlog)
	}
	if db.scanned {
		t.Fatal("CREATE scan must not decide newly added tables")
	}
}

func TestNewlyAddedSnapshotsTablesSerially(t *testing.T) {
	db := newMemDB([]string{"1"})
	db.tables = []schema.Table{
		{Database: "db", Table: "user", Columns: []schema.Column{{Name: "id", Type: "int"}}, PrimaryKeys: []string{"id"}},
		{Database: "db", Table: "order", Columns: []schema.Column{{Name: "id", Type: "int"}}, PrimaryKeys: []string{"id"}},
		{Database: "db", Table: "item", Columns: []schema.Column{{Name: "id", Type: "int"}}, PrimaryKeys: []string{"id"}},
	}
	db.blockTable = "order"
	db.blockRead = make(chan struct{})
	sel, err := definition.CompileSelector(`db\..*`, "")
	if err != nil {
		t.Fatal(err)
	}
	stream := &memStream{started: make(chan struct{})}
	cap := &Capture{
		Config: definition.Source{
			StartupMode: definition.StartupInitial, ChunkSize: 2, Tables: sel,
			NewlyAddedTable: true, NewlyAddedSnapshot: true,
		},
		State: checkpoint.State{
			AckedOffset:    &checkpoint.Position{File: "mysql-bin.000001", Pos: 400},
			CapturedTables: []string{"db.user"},
		},
		DB: db, Stream: stream, Spill: OpenSpill(t.TempDir()),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, err := cap.Messages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for msg := range ch {
			if msg.CheckpointBarrier && msg.BarrierDone != nil {
				close(msg.BarrierDone)
			}
		}
	}()
	select {
	case <-stream.started:
	case <-time.After(time.Second):
		t.Fatal("binlog did not start")
	}
	deadline := time.After(time.Second)
	for {
		db.mu.Lock()
		splits := append([]string(nil), db.splitTables...)
		db.mu.Unlock()
		hasOrder, hasItem := false, false
		for _, name := range splits {
			if name == "order" {
				hasOrder = true
			}
			if name == "item" {
				hasItem = true
			}
		}
		if hasOrder && !hasItem {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("want order split before item while blocked; splits %#v", splits)
		case <-time.After(10 * time.Millisecond):
		}
	}
	close(db.blockRead)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	db.mu.Lock()
	splits := append([]string(nil), db.splitTables...)
	db.mu.Unlock()
	orderIdx, itemIdx := -1, -1
	for i, name := range splits {
		if name == "order" && orderIdx < 0 {
			orderIdx = i
		}
		if name == "item" && itemIdx < 0 {
			itemIdx = i
		}
	}
	if orderIdx < 0 || itemIdx < 0 || orderIdx >= itemIdx {
		t.Fatalf("serial order broken: splits %#v", splits)
	}
}

func TestCloneTableCopiesIndexPrefixes(t *testing.T) {
	src := schema.Table{
		Database: "db", Table: "t",
		Columns:     []schema.Column{{Name: "c", Type: "varchar(32)"}},
		PrimaryKeys: []string{"c"},
		Indexes: []schema.Index{{
			Name: "idx_c", Columns: []string{"c"}, Prefixes: []int{8}, Type: "BTREE",
		}},
	}
	cloned := cloneTable(src)
	cloned.Indexes[0].Prefixes[0] = 99
	cloned.Indexes[0].Columns[0] = "mutated"
	if src.Indexes[0].Prefixes[0] != 8 || src.Indexes[0].Columns[0] != "c" {
		t.Fatalf("cloneTable must deep-copy index slices; src=%#v", src.Indexes[0])
	}
}

func TestDescribeTableFailureFallsBackWithWarning(t *testing.T) {
	db := newMemDB([]string{"1"})
	db.tables = []schema.Table{
		{Database: "db", Table: "user", Columns: []schema.Column{{Name: "id", Type: "int"}}, PrimaryKeys: []string{"id"}},
	}
	db.describeErr = errors.New("jdbc down")
	binlogSchema := &schema.Table{
		Database: "db", Table: "crt",
		Columns:     []schema.Column{{Name: "id", Type: "int"}},
		PrimaryKeys: []string{"id"},
	}
	stream := &memStream{events: []Commit{
		{Event: &event.Event{
			Op: event.OpDDL, Database: "db", Table: "crt", SourceDatabase: "db", SourceTable: "crt",
			DDL: "CREATE TABLE `db`.`crt` (id int primary key)",
		}, Schema: binlogSchema},
		{Position: &checkpoint.Position{File: "mysql-bin.000001", Pos: 500}},
	}}
	var warns []string
	sel, err := definition.CompileSelector(`db\..*`, "")
	if err != nil {
		t.Fatal(err)
	}
	cap := &Capture{
		Config: definition.Source{
			StartupMode: definition.StartupInitial, Tables: sel,
			NewlyAddedTable: false, BinlogNewlyAddedTable: true, SchemaChange: true,
		},
		State: checkpoint.State{
			AckedOffset:    &checkpoint.Position{File: "mysql-bin.000001", Pos: 400},
			CapturedTables: []string{"db.user"},
		},
		DB: db, Stream: stream, Spill: OpenSpill(t.TempDir()),
		Warn: func(msg string, _ ...any) { warns = append(warns, msg) },
	}
	msgs := collect(t, cap)
	var sawSchema bool
	for _, msg := range msgs {
		if msg.Err != nil {
			t.Fatal(msg.Err)
		}
		if msg.Schema != nil && msg.Schema.Table == "crt" {
			sawSchema = true
			if len(msg.Schema.Columns) != 1 || msg.Schema.Columns[0].Name != "id" {
				t.Fatalf("fallback schema %#v", msg.Schema)
			}
		}
	}
	if !sawSchema {
		t.Fatal("expected binlog schema fallback")
	}
	if len(warns) != 1 || !strings.Contains(warns[0], "describe table failed") {
		t.Fatalf("warns %#v", warns)
	}
}

func TestDescribeTableSuccessReplacesBinlogSchema(t *testing.T) {
	db := newMemDB([]string{"1"})
	db.tables = []schema.Table{
		{Database: "db", Table: "user", Columns: []schema.Column{{Name: "id", Type: "int"}}, PrimaryKeys: []string{"id"}},
		{
			Database: "db", Table: "crt",
			Columns: []schema.Column{
				{Name: "id", Type: "int"},
				{Name: "name", Type: "varchar(32)", Default: "x"},
			},
			PrimaryKeys: []string{"id"},
			Indexes:     []schema.Index{{Name: "idx_name", Columns: []string{"name"}}},
		},
	}
	stream := &memStream{events: []Commit{
		{Event: &event.Event{
			Op: event.OpDDL, Database: "db", Table: "crt", SourceDatabase: "db", SourceTable: "crt",
			DDL: "CREATE TABLE `db`.`crt` (id int primary key)",
		}, Schema: &schema.Table{
			Database: "db", Table: "crt",
			Columns:     []schema.Column{{Name: "id", Type: "int"}},
			PrimaryKeys: []string{"id"},
		}},
		{Position: &checkpoint.Position{File: "mysql-bin.000001", Pos: 500}},
	}}
	sel, err := definition.CompileSelector(`db\..*`, "")
	if err != nil {
		t.Fatal(err)
	}
	cap := &Capture{
		Config: definition.Source{
			StartupMode: definition.StartupInitial, Tables: sel,
			NewlyAddedTable: false, BinlogNewlyAddedTable: true, SchemaChange: true,
		},
		State: checkpoint.State{
			AckedOffset:    &checkpoint.Position{File: "mysql-bin.000001", Pos: 400},
			CapturedTables: []string{"db.user"},
		},
		DB: db, Stream: stream, Spill: OpenSpill(t.TempDir()),
	}
	msgs := collect(t, cap)
	for _, msg := range msgs {
		if msg.Schema != nil && msg.Schema.Table == "crt" {
			if len(msg.Schema.Columns) != 2 || len(msg.Schema.Indexes) != 1 {
				t.Fatalf("want JDBC schema, got %#v", msg.Schema)
			}
			return
		}
	}
	t.Fatal("missing schema message")
}

func TestNewlyAddedFalseBinlogFlagIgnoresDiffFollowsCreate(t *testing.T) {
	db := newMemDB([]string{"1", "2"})
	db.tables = []schema.Table{
		{Database: "db", Table: "user", Columns: []schema.Column{{Name: "id", Type: "int"}}, PrimaryKeys: []string{"id"}},
		{Database: "db", Table: "order", Columns: []schema.Column{{Name: "id", Type: "int"}}, PrimaryKeys: []string{"id"}},
	}
	sel, err := definition.CompileSelector(`db\..*`, "")
	if err != nil {
		t.Fatal(err)
	}
	stream := &memStream{
		events: []Commit{
			{Event: &event.Event{Op: event.OpCreate, Database: "db", Table: "order", SourceDatabase: "db", SourceTable: "order", After: map[string]any{"id": "8"}}},
			{Event: &event.Event{
				Op: event.OpDDL, Database: "db", Table: "crt", SourceDatabase: "db", SourceTable: "crt",
				DDL: "CREATE TABLE `db`.`crt` (id int primary key)",
			}, Schema: &schema.Table{Database: "db", Table: "crt", Columns: []schema.Column{{Name: "id", Type: "int"}}, PrimaryKeys: []string{"id"}}},
			{Event: &event.Event{Op: event.OpCreate, Database: "db", Table: "crt", SourceDatabase: "db", SourceTable: "crt", After: map[string]any{"id": "1"}}},
			{Position: &checkpoint.Position{File: "mysql-bin.000001", Pos: 500}},
		},
	}
	cap := &Capture{
		Config: definition.Source{
			StartupMode: definition.StartupInitial, ChunkSize: 2, Tables: sel,
			NewlyAddedTable: false, BinlogNewlyAddedTable: true, SchemaChange: true,
		},
		State: checkpoint.State{
			AckedOffset:    &checkpoint.Position{File: "mysql-bin.000001", Pos: 400},
			CapturedTables: []string{"db.user"},
		},
		DB: db, Stream: stream, Spill: OpenSpill(t.TempDir()),
	}
	msgs := collect(t, cap)
	var orderSeen, crtDDL, crtRow bool
	for _, msg := range msgs {
		if msg.Err != nil {
			t.Fatal(msg.Err)
		}
		if msg.Event != nil && msg.Event.Table == "order" {
			orderSeen = true
		}
		if msg.Event != nil && msg.Event.Table == "crt" && msg.Event.Op == event.OpDDL {
			crtDDL = true
		}
		if msg.Event != nil && msg.Event.Table == "crt" && msg.Event.Op == event.OpCreate {
			crtRow = true
		}
	}
	if orderSeen {
		t.Fatal("A=false+C=true must ignore config-diff DML on order")
	}
	if !crtDDL || !crtRow {
		t.Fatalf("runtime CREATE follow missing ddl=%v row=%v", crtDDL, crtRow)
	}
	if len(stream.limited) == 0 || stream.limited[0] != "db.user" {
		t.Fatalf("LimitTables should stay on captured set, got %#v", stream.limited)
	}
}

func TestNewlyAddedFalseSkipsTablesOutsideCheckpoint(t *testing.T) {
	db := newMemDB([]string{"1", "2"})
	db.tables = []schema.Table{
		{Database: "db", Table: "user", Columns: []schema.Column{{Name: "id", Type: "int"}}, PrimaryKeys: []string{"id"}},
		{Database: "db", Table: "order", Columns: []schema.Column{{Name: "id", Type: "int"}}, PrimaryKeys: []string{"id"}},
	}
	sel, err := definition.CompileSelector(`db\..*`, "")
	if err != nil {
		t.Fatal(err)
	}
	stream := &memStream{
		events: []Commit{
			{Event: &event.Event{Op: event.OpCreate, Database: "db", Table: "user", SourceDatabase: "db", SourceTable: "user", After: map[string]any{"id": "9"}}},
			{Event: &event.Event{Op: event.OpCreate, Database: "db", Table: "order", SourceDatabase: "db", SourceTable: "order", After: map[string]any{"id": "8"}}},
			{Position: &checkpoint.Position{File: "mysql-bin.000001", Pos: 500}},
		},
	}
	cap := &Capture{
		Config: definition.Source{StartupMode: definition.StartupInitial, ChunkSize: 2, Tables: sel, NewlyAddedTable: false},
		State: checkpoint.State{
			AckedOffset:    &checkpoint.Position{File: "mysql-bin.000001", Pos: 400},
			CapturedTables: []string{"db.user"},
		},
		DB: db, Stream: stream, Spill: OpenSpill(t.TempDir()),
	}
	msgs := collect(t, cap)
	for _, msg := range msgs {
		if msg.Err != nil {
			t.Fatal(msg.Err)
		}
		if msg.Schema != nil && msg.Schema.Table == "order" {
			t.Fatal("order schema should be ignored")
		}
		if msg.Event != nil && msg.Event.Table == "order" {
			t.Fatalf("order event should be ignored: %#v", msg.Event)
		}
	}
	var userSeen bool
	for _, msg := range msgs {
		if msg.Event != nil && msg.Event.Table == "user" && msg.Event.After["id"] == "9" {
			userSeen = true
		}
	}
	if !userSeen {
		t.Fatal("user binlog row missing")
	}
	if stream.got.Pos != 400 {
		t.Fatalf("binlog pos %d", stream.got.Pos)
	}
	if db.reads != 0 {
		t.Fatalf("unexpected snapshot reads %d", db.reads)
	}
}

func TestNewlyAddedSkipSnapshotFollowsBinlogOnly(t *testing.T) {
	db := newMemDB([]string{"1", "2"})
	db.tables = []schema.Table{
		{Database: "db", Table: "user", Columns: []schema.Column{{Name: "id", Type: "int"}}, PrimaryKeys: []string{"id"}},
		{Database: "db", Table: "order", Columns: []schema.Column{{Name: "id", Type: "int"}}, PrimaryKeys: []string{"id"}},
	}
	sel, err := definition.CompileSelector(`db\..*`, "")
	if err != nil {
		t.Fatal(err)
	}
	stream := &memStream{
		events: []Commit{
			{Event: &event.Event{Op: event.OpCreate, Database: "db", Table: "order", SourceDatabase: "db", SourceTable: "order", After: map[string]any{"id": "8"}}},
			{Position: &checkpoint.Position{File: "mysql-bin.000001", Pos: 500}},
		},
	}
	var warns int
	cap := &Capture{
		Config: definition.Source{
			StartupMode: definition.StartupInitial, ChunkSize: 2, Tables: sel,
			NewlyAddedTable: true, NewlyAddedSnapshot: false,
		},
		State: checkpoint.State{
			AckedOffset:    &checkpoint.Position{File: "mysql-bin.000001", Pos: 400},
			CapturedTables: []string{"db.user"},
		},
		DB: db, Stream: stream, Spill: OpenSpill(t.TempDir()),
		Warn: func(string, ...any) { warns++ },
	}
	msgs := collect(t, cap)
	var orderBinlog, captured bool
	for _, msg := range msgs {
		if msg.Err != nil {
			t.Fatal(msg.Err)
		}
		if msg.Event != nil && msg.Event.Table == "order" && msg.Event.After["id"] == "8" {
			orderBinlog = true
		}
		if msg.Event != nil && msg.Event.Op == event.OpRead {
			t.Fatalf("unexpected snapshot row: %#v", msg.Event)
		}
		if msg.Captured == "db.order" {
			captured = true
		}
	}
	if !orderBinlog || !captured {
		t.Fatalf("orderBinlog=%v captured=%v", orderBinlog, captured)
	}
	if db.reads != 0 {
		t.Fatalf("snapshot reads %d", db.reads)
	}
	if warns != 1 {
		t.Fatalf("warns %d", warns)
	}
}

func TestResumeDoesNotRewriteSpilledXID(t *testing.T) {
	spill := OpenSpill(t.TempDir())
	xid500 := checkpoint.Position{File: "mysql-bin.000001", Pos: 500}
	ev := event.Event{
		Op: event.OpCreate, Database: "db", Table: "order",
		SourceDatabase: "db", SourceTable: "order", After: map[string]any{"id": "8"},
	}
	if err := spill.AppendBatch("db.order", xid500, []event.Event{ev}); err != nil {
		t.Fatal(err)
	}
	db := newMemDB([]string{"1"})
	db.tables = []schema.Table{
		{Database: "db", Table: "order", Columns: []schema.Column{{Name: "id", Type: "int"}}, PrimaryKeys: []string{"id"}},
	}
	db.blockTable = "order"
	db.blockRead = make(chan struct{})
	sel, err := definition.CompileSelector(`db\..*`, "")
	if err != nil {
		t.Fatal(err)
	}
	stream := &memStream{
		started: make(chan struct{}),
		events: []Commit{
			{Event: &event.Event{Op: event.OpCreate, Database: "db", Table: "order", SourceDatabase: "db", SourceTable: "order", After: map[string]any{"id": "8"}}},
			{Position: &checkpoint.Position{File: "mysql-bin.000001", Pos: 500}},
			{Event: &event.Event{Op: event.OpCreate, Database: "db", Table: "order", SourceDatabase: "db", SourceTable: "order", After: map[string]any{"id": "9"}}},
			{Position: &checkpoint.Position{File: "mysql-bin.000001", Pos: 600}},
		},
	}
	cap := &Capture{
		Config: definition.Source{StartupMode: definition.StartupInitial, ChunkSize: 2, Tables: sel, NewlyAddedTable: false},
		State: checkpoint.State{
			AckedOffset: &checkpoint.Position{File: "mysql-bin.000001", Pos: 500},
			SnapshotTables: map[string]checkpoint.Position{
				"db.order": {File: "mysql-bin.000001", Pos: 500},
			},
		},
		DB: db, Stream: stream, Spill: spill,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, err := cap.Messages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for msg := range ch {
			if msg.CheckpointBarrier && msg.BarrierDone != nil {
				close(msg.BarrierDone)
			}
			if msg.Err != nil {
				t.Errorf("msg err: %v", msg.Err)
			}
		}
	}()
	select {
	case <-stream.started:
	case <-time.After(time.Second):
		t.Fatal("binlog did not start")
	}
	deadline := time.After(2 * time.Second)
	for {
		last, has, err := spill.LastPosition("db.order")
		if err != nil {
			t.Fatal(err)
		}
		if has && last.Pos == 600 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("last %#v has %v", last, has)
		case <-time.After(10 * time.Millisecond):
		}
	}
	lines := countSpillLines(t, spill, "db.order")
	if lines != 2 {
		t.Fatalf("spill lines at xid rewrite check: %d want 2 (one for 500, one for 600)", lines)
	}
	close(db.blockRead)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func countSpillLines(t *testing.T, spill *Spill, id string) int {
	t.Helper()
	path, _ := spillPaths(spill.dir, id)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var row spillLine
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		if row.Event != nil && !row.Fence {
			n++
		}
	}
	return n
}
