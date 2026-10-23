package doris

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
)

type fakeSQL struct {
	mu       sync.Mutex
	pingErr  error
	execs    []string
	execErr  error
	columns  map[string][]descColumn
	descErr  error
	loads    []string
	load     loadState
	loadErr  error
	execHook func(f *fakeSQL, statement string)
	closed   int
}

func newFakeSQL() *fakeSQL {
	return &fakeSQL{columns: map[string][]descColumn{}}
}

func (f *fakeSQL) Ping(context.Context) error { return f.pingErr }

func (f *fakeSQL) Exec(_ context.Context, statement string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.execErr != nil {
		return f.execErr
	}
	f.execs = append(f.execs, statement)
	if f.execHook != nil {
		f.execHook(f, statement)
	}
	return nil
}

func (f *fakeSQL) descTable(_ context.Context, database, table string) ([]descColumn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.descErr != nil {
		return nil, f.descErr
	}
	id := database + "." + table
	columns, ok := f.columns[id]
	if !ok {
		return nil, fmt.Errorf("sink.doris: table %s does not exist or has no columns", id)
	}
	return append([]descColumn(nil), columns...), nil
}

func (f *fakeSQL) showLoad(_ context.Context, database, label string) (loadState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if database == "" {
		return loadState{}, fmt.Errorf("sink.doris: show load requires a database")
	}
	f.loads = append(f.loads, label)
	if f.loadErr != nil {
		return loadState{}, f.loadErr
	}
	return f.load, nil
}

func (f *fakeSQL) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return nil
}

func (f *fakeSQL) statements() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.execs...)
}

func testSink(t *testing.T, server *httptest.Server, sql sqlAPI, cfg definition.Sink) *Sink {
	t.Helper()
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return &Sink{
		cfg: cfg,
		sql: sql,
		http: &httpClient{
			client: server.Client(), base: base, user: "root", pass: "secret",
			timeout: 5 * time.Second,
		},
		tables: map[string]schema.Table{},
	}
}

func successServer(t *testing.T, requests *[]string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*requests = append(*requests, r.URL.Path)
		label := r.Header.Get("label")
		_, _ = io.WriteString(w, fmt.Sprintf(`{"Status":"Success","Label":%q,"NumberTotalRows":1,"NumberLoadedRows":1}`, label))
	}))
	t.Cleanup(server.Close)
	return server
}

func createEvent(id int64, name string) event.Event {
	return event.Event{
		Op: event.OpCreate, Database: "db", Table: "orders",
		SourceDatabase: "db", SourceTable: "orders",
		Columns: []string{"id", "name"}, Keys: []string{"id"},
		After: map[string]any{"id": id, "name": name},
	}
}

func TestSinkOpenPing(t *testing.T) {
	f := newFakeSQL()
	f.pingErr = fmt.Errorf("sink.doris: sql ping: refused")
	s := &Sink{cfg: definition.Sink{Type: "doris"}, sql: f, tables: map[string]schema.Table{}}
	if err := s.Open(context.Background()); err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("err=%v", err)
	}
}

func TestSinkCreateTableAutoCreate(t *testing.T) {
	f := newFakeSQL()
	f.execHook = func(f *fakeSQL, statement string) {
		if strings.Contains(statement, "CREATE TABLE") {
			f.columns["db.orders"] = []descColumn{
				{Name: "id", Type: "BIGINT", Nullable: false, Key: true},
				{Name: "name", Type: "VARCHAR(32)", Nullable: true},
			}
		}
	}
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris", AutoCreate: true, Buckets: "auto"})

	if err := s.CreateTable(validateSource()); err != nil {
		t.Fatal(err)
	}
	statements := f.statements()
	if len(statements) != 2 {
		t.Fatalf("statements %#v", statements)
	}
	if !strings.Contains(statements[0], "CREATE DATABASE IF NOT EXISTS `db`") {
		t.Fatalf("database DDL: %s", statements[0])
	}
	if !strings.Contains(statements[1], "CREATE TABLE IF NOT EXISTS `db`.`orders`") {
		t.Fatalf("table DDL: %s", statements[1])
	}
	if _, ok := s.tables["db.orders"]; !ok {
		t.Fatal("validated table must be cached")
	}
}

func TestSinkCreateTableExistingTargetIsValidated(t *testing.T) {
	f := newFakeSQL()
	f.columns["db.orders"] = validTarget()
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris", AutoCreate: false})

	if err := s.CreateTable(validateSource()); err != nil {
		t.Fatal(err)
	}
	if len(f.statements()) != 0 {
		t.Fatalf("auto-create off must not execute DDL: %#v", f.statements())
	}
}

func TestSinkCreateTableMissingTargetFails(t *testing.T) {
	f := newFakeSQL()
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris", AutoCreate: false})
	if err := s.CreateTable(validateSource()); err == nil {
		t.Fatal("missing target with auto-create off must fail")
	}
}

func TestSinkCreateTableIncompatibleTargetFails(t *testing.T) {
	f := newFakeSQL()
	f.columns["db.orders"] = []descColumn{
		{Name: "id", Type: "BIGINT", Nullable: false, Key: true},
		{Name: "name", Type: "VARCHAR(4)", Nullable: true},
	}
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris", AutoCreate: false})
	if err := s.CreateTable(validateSource()); err == nil {
		t.Fatal("incompatible target must fail")
	}
	if len(s.tables) != 0 {
		t.Fatalf("failed validation must not cache the table: %#v", s.tables)
	}
}

func TestSinkCreateTableDDLErrorIsReturned(t *testing.T) {
	f := newFakeSQL()
	f.execErr = fmt.Errorf("sink.doris: access denied")
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris", AutoCreate: true})
	if err := s.CreateTable(validateSource()); err == nil || !strings.Contains(err.Error(), "access denied") {
		t.Fatalf("err=%v", err)
	}
}

func TestSinkWriteRequiresCachedOrDescribableTable(t *testing.T) {
	f := newFakeSQL()
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris"})
	if err := s.Write([]event.Event{createEvent(1, "a")}); err == nil {
		t.Fatal("missing table metadata must fail")
	}
}

func TestSinkWriteDescribesTargetAfterRestart(t *testing.T) {
	// After a restart the sink cache is empty; Runner sends no Schema for
	// already-captured tables, so the target must be read and checked lazily.
	var requests []string
	server := successServer(t, &requests)
	f := newFakeSQL()
	f.columns["db.orders"] = validTarget()
	s := testSink(t, server, f, definition.Sink{Type: "doris"})
	if err := s.Write([]event.Event{createEvent(1, "a")}); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 || requests[0] != "/api/db/orders/_stream_load" {
		t.Fatalf("requests %#v", requests)
	}
	if _, ok := s.tables["db.orders"]; !ok {
		t.Fatal("described table must be cached")
	}
}

func TestSinkWriteRejectsKeyDriftAfterRestart(t *testing.T) {
	var requests []string
	server := successServer(t, &requests)
	f := newFakeSQL()
	f.columns["db.orders"] = []descColumn{
		{Name: "id", Type: "BIGINT", Nullable: false, Key: true},
		{Name: "tenant", Type: "VARCHAR(8)", Nullable: false, Key: true},
		{Name: "name", Type: "VARCHAR(64)", Nullable: true},
	}
	s := testSink(t, server, f, definition.Sink{Type: "doris"})
	err := s.Write([]event.Event{createEvent(1, "a")})
	if err == nil || !strings.Contains(err.Error(), "do not match target UNIQUE KEY") {
		t.Fatalf("err=%v", err)
	}
	if len(requests) != 0 {
		t.Fatalf("key drift must fail before Stream Load: %#v", requests)
	}
}

func TestSinkWriteRejectsTargetWithoutKeyAfterRestart(t *testing.T) {
	var requests []string
	server := successServer(t, &requests)
	f := newFakeSQL()
	f.columns["db.orders"] = []descColumn{
		{Name: "id", Type: "BIGINT", Nullable: false},
		{Name: "name", Type: "VARCHAR(64)", Nullable: true},
	}
	s := testSink(t, server, f, definition.Sink{Type: "doris"})
	err := s.Write([]event.Event{createEvent(1, "a")})
	if err == nil || !strings.Contains(err.Error(), "no UNIQUE KEY") {
		t.Fatalf("err=%v", err)
	}
}

func TestSinkWriteRejectsDelete(t *testing.T) {
	f := newFakeSQL()
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris"})
	s.cacheTable(validateSource())
	item := createEvent(1, "a")
	item.Op = event.OpDelete
	if err := s.Write([]event.Event{item}); err == nil || !strings.Contains(err.Error(), "DELETE is not supported") {
		t.Fatalf("err=%v", err)
	}
}

func TestSinkWriteRejectsIncompleteAfter(t *testing.T) {
	// A partial row must never be padded with NULL by the JSON load.
	f := newFakeSQL()
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris"})
	s.cacheTable(validateSource())
	item := event.Event{
		Op: event.OpUpdate, Database: "db", Table: "orders",
		SourceDatabase: "db", SourceTable: "orders",
		Columns: []string{"id", "name"}, Keys: []string{"id"},
		After: map[string]any{"id": int64(1)},
	}
	if err := s.Write([]event.Event{item}); err == nil || !strings.Contains(err.Error(), "missing column name") {
		t.Fatalf("err=%v", err)
	}
}

func TestSinkWriteRejectsUnknownColumn(t *testing.T) {
	f := newFakeSQL()
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris"})
	s.cacheTable(validateSource())
	item := createEvent(1, "a")
	item.Columns = []string{"id", "name", "ghost"}
	item.After["ghost"] = "x"
	if err := s.Write([]event.Event{item}); err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("err=%v", err)
	}
}

func TestSinkWriteRejectsMissingKey(t *testing.T) {
	f := newFakeSQL()
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris"})
	s.cacheTable(validateSource())
	item := createEvent(1, "a")
	item.Keys = nil
	if err := s.Write([]event.Event{item}); err == nil || !strings.Contains(err.Error(), "no primary key") {
		t.Fatalf("err=%v", err)
	}
}

func TestSinkWriteGroupsTablesInStableOrderAndCoalesces(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		paths = append(paths, r.URL.Path)
		bodies = append(bodies, string(raw))
		mu.Unlock()
		_, _ = io.WriteString(w, fmt.Sprintf(`{"Status":"Success","Label":%q,"NumberTotalRows":1,"NumberLoadedRows":1}`, r.Header.Get("label")))
	}))
	defer server.Close()

	f := newFakeSQL()
	s := testSink(t, server, f, definition.Sink{Type: "doris", MaxRetries: 0})
	s.cacheTable(validateSource())
	other := schema.Table{
		Database: "db", Table: "users",
		Columns:     []schema.Column{{Name: "id", Type: "bigint"}, {Name: "name", Type: "varchar(32)"}},
		PrimaryKeys: []string{"id"},
	}
	s.cacheTable(other)

	a1 := createEvent(1, "first")
	a2 := createEvent(1, "second")
	b1 := createEvent(2, "user")
	b1.Table = "users"
	b1.SourceTable = "users"
	if err := s.Write([]event.Event{b1, a1, a2}); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 {
		t.Fatalf("paths %#v", paths)
	}
	if paths[0] != "/api/db/orders/_stream_load" || paths[1] != "/api/db/users/_stream_load" {
		t.Fatalf("unstable table order: %#v", paths)
	}
	if strings.Count(bodies[0], "\n") != 1 || !strings.Contains(bodies[0], "second") {
		t.Fatalf("duplicate keys must coalesce to the last event: %s", bodies[0])
	}
	if strings.Contains(bodies[0], "first") {
		t.Fatalf("stale event kept: %s", bodies[0])
	}
}

func TestSinkWriteStopsAfterFirstTableFailure(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.Path)
		if strings.Contains(r.URL.Path, "/orders/") {
			_, _ = io.WriteString(w, `{"Status":"Fail","Message":"bad rows"}`)
			return
		}
		_, _ = io.WriteString(w, `{"Status":"Success","NumberTotalRows":1,"NumberLoadedRows":1}`)
	}))
	defer server.Close()

	f := newFakeSQL()
	s := testSink(t, server, f, definition.Sink{Type: "doris", MaxRetries: 2})
	s.cacheTable(validateSource())
	other := schema.Table{
		Database: "db", Table: "users",
		Columns:     []schema.Column{{Name: "id", Type: "bigint"}, {Name: "name", Type: "varchar(32)"}},
		PrimaryKeys: []string{"id"},
	}
	s.cacheTable(other)
	user := createEvent(2, "user")
	user.Table = "users"
	user.SourceTable = "users"
	err := s.Write([]event.Event{createEvent(1, "a"), user})
	if err == nil || !strings.Contains(err.Error(), "bad rows") {
		t.Fatalf("err=%v", err)
	}
	if len(requests) != 1 {
		t.Fatalf("first failure must stop later tables: %#v", requests)
	}
}

func TestSinkWriteUnknownResultQueriesSameLabel(t *testing.T) {
	var requests int
	var labels []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		labels = append(labels, r.Header.Get("label"))
		_, _ = io.WriteString(w, "not json")
	}))
	defer server.Close()

	f := newFakeSQL()
	f.load = loadState{State: existingJobFinished}
	s := testSink(t, server, f, definition.Sink{Type: "doris", MaxRetries: 3})
	s.cacheTable(validateSource())
	if err := s.Write([]event.Event{createEvent(1, "a")}); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("a confirmed label must not be reloaded: %d", requests)
	}
	if len(f.loads) != 1 || f.loads[0] != labels[0] {
		t.Fatalf("queried labels %#v, request labels %#v", f.loads, labels)
	}
}

func TestSinkWriteUnknownBusinessStatusQueriesShowLoad(t *testing.T) {
	var requests int
	var labels []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		label := r.Header.Get("label")
		labels = append(labels, label)
		_, _ = io.WriteString(w, fmt.Sprintf(`{"Status":"Something New","Label":%q}`, label))
	}))
	defer server.Close()

	f := newFakeSQL()
	f.load = loadState{State: existingJobFinished}
	s := testSink(t, server, f, definition.Sink{Type: "doris", MaxRetries: 3})
	s.cacheTable(validateSource())
	if err := s.Write([]event.Event{createEvent(1, "a")}); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("SHOW LOAD success must not re-PUT Stream Load: %d", requests)
	}
	if len(f.loads) != 1 || f.loads[0] != labels[0] {
		t.Fatalf("queried labels %#v, request labels %#v", f.loads, labels)
	}
}

func TestSinkWriteLabelAlreadyExistsCancelledIsFatal(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = io.WriteString(w, fmt.Sprintf(
			`{"Status":"Label Already Exists","Label":%q,"ExistingJobStatus":"CANCELLED"}`,
			r.Header.Get("label")))
	}))
	defer server.Close()

	f := newFakeSQL()
	s := testSink(t, server, f, definition.Sink{Type: "doris", MaxRetries: 3})
	s.cacheTable(validateSource())
	err := s.Write([]event.Event{createEvent(1, "a")})
	if err == nil || !strings.Contains(err.Error(), "cancelled") {
		t.Fatalf("err=%v", err)
	}
	if requests != 1 {
		t.Fatalf("cancelled label must not retry Stream Load: %d", requests)
	}
	if len(f.loads) != 0 {
		t.Fatalf("cancelled from Stream Load must not fall through to SHOW LOAD: %#v", f.loads)
	}
}

func TestSinkWriteRetriesSameLabelWhenRunning(t *testing.T) {
	var mu sync.Mutex
	var labels []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		label := r.Header.Get("label")
		mu.Lock()
		labels = append(labels, label)
		count := len(labels)
		mu.Unlock()
		if count == 1 {
			_, _ = io.WriteString(w, fmt.Sprintf(`{"Status":"Label Already Exists","Label":%q,"ExistingJobStatus":"RUNNING"}`, label))
			return
		}
		_, _ = io.WriteString(w, fmt.Sprintf(`{"Status":"Success","Label":%q,"NumberTotalRows":1,"NumberLoadedRows":1}`, label))
	}))
	defer server.Close()

	f := newFakeSQL()
	s := testSink(t, server, f, definition.Sink{Type: "doris", MaxRetries: 2})
	s.cacheTable(validateSource())
	if err := s.Write([]event.Event{createEvent(1, "a")}); err != nil {
		t.Fatal(err)
	}
	if len(labels) != 2 {
		t.Fatalf("expected one retry, got %d requests", len(labels))
	}
	if labels[0] != labels[1] {
		t.Fatalf("retry changed the label: %#v", labels)
	}
}

func TestSinkWriteFailsWhenLabelNeverConfirms(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = io.WriteString(w, fmt.Sprintf(`{"Status":"Label Already Exists","Label":%q,"ExistingJobStatus":"RUNNING"}`, r.Header.Get("label")))
	}))
	defer server.Close()

	f := newFakeSQL()
	s := testSink(t, server, f, definition.Sink{Type: "doris", MaxRetries: 1})
	s.cacheTable(validateSource())
	err := s.Write([]event.Event{createEvent(1, "a")})
	if err == nil || !strings.Contains(err.Error(), "not confirmed") {
		t.Fatalf("err=%v", err)
	}
	if requests != 2 {
		t.Fatalf("retries must be bounded, got %d", requests)
	}
}

func TestSinkApplySchemaDisabled(t *testing.T) {
	f := newFakeSQL()
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris", SchemaChange: false})
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "db", Table: "orders",
		AddColumns: []string{"note"}, ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
	}
	if err := s.ApplySchema(change); err == nil || !strings.Contains(err.Error(), "schema.change.enabled") {
		t.Fatalf("err=%v", err)
	}
}

func TestSinkApplySchemaAddsColumn(t *testing.T) {
	old := schemaChangePollInterval
	schemaChangePollInterval = time.Millisecond
	t.Cleanup(func() { schemaChangePollInterval = old })

	f := newFakeSQL()
	f.columns["db.orders"] = validTarget()
	f.execHook = func(f *fakeSQL, statement string) {
		if strings.Contains(statement, "ADD COLUMN") {
			f.columns["db.orders"] = append(f.columns["db.orders"], descColumn{Name: "note", Type: "VARCHAR(32)", Nullable: true})
		}
	}
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris", SchemaChange: true, SchemaChangeInclude: []string{"add.column"}})
	table := validateSource()
	s.cacheTable(table)

	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "db", Table: "orders",
		AddColumns: []string{"note"}, ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
	}
	if err := s.ApplySchema(change); err != nil {
		t.Fatal(err)
	}
	statements := f.statements()
	if len(statements) != 1 || !strings.Contains(statements[0], "ADD COLUMN `note` VARCHAR(32) NULL") {
		t.Fatalf("statements %#v", statements)
	}
	found := false
	for _, column := range s.tables["db.orders"].Columns {
		if column.Name == "note" {
			found = true
		}
	}
	if !found {
		t.Fatalf("cached schema not extended: %#v", s.tables["db.orders"])
	}
}

func TestSinkApplySchemaRepeatedCompatibleIsNoop(t *testing.T) {
	f := newFakeSQL()
	f.columns["db.orders"] = append(validTarget(), descColumn{Name: "note", Type: "VARCHAR(32)", Nullable: true})
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris", SchemaChange: true, SchemaChangeInclude: []string{"add.column"}})
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "db", Table: "orders",
		AddColumns: []string{"note"}, ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
	}
	if err := s.ApplySchema(change); err != nil {
		t.Fatal(err)
	}
	if len(f.statements()) != 0 {
		t.Fatalf("repeated compatible ADD must not execute DDL: %#v", f.statements())
	}
}

func TestSinkApplySchemaRepeatedIncompatibleFails(t *testing.T) {
	f := newFakeSQL()
	f.columns["db.orders"] = append(validTarget(), descColumn{Name: "note", Type: "INT", Nullable: true})
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris", SchemaChange: true, SchemaChangeInclude: []string{"add.column"}})
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "db", Table: "orders",
		AddColumns: []string{"note"}, ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
	}
	if err := s.ApplySchema(change); err == nil {
		t.Fatal("incompatible existing column must fail")
	}
	if len(f.statements()) != 0 {
		t.Fatalf("failed check must not execute DDL: %#v", f.statements())
	}
}

func TestSinkApplySchemaRejectsMultiColumnAdd(t *testing.T) {
	f := newFakeSQL()
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris", SchemaChange: true, SchemaChangeInclude: []string{"add.column"}})
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "db", Table: "orders",
		AddColumns: []string{"a", "b"},
		ColumnDef:  &schema.Column{Name: "a", Type: "int"},
	}
	if err := s.ApplySchema(change); err == nil || !strings.Contains(err.Error(), "exactly one column") {
		t.Fatalf("err=%v", err)
	}
}

func TestSinkApplySchemaRejectsMissingMetadata(t *testing.T) {
	f := newFakeSQL()
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris", SchemaChange: true, SchemaChangeInclude: []string{"add.column"}})
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "db", Table: "orders", AddColumns: []string{"note"},
	}
	if err := s.ApplySchema(change); err == nil || !strings.Contains(err.Error(), "structured column metadata") {
		t.Fatalf("err=%v", err)
	}
}

func TestSinkApplySchemaRejectsUnsupportedAction(t *testing.T) {
	f := newFakeSQL()
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris", SchemaChange: true, SchemaChangeInclude: []string{"add.column"}})
	change := schema.Change{Action: schema.ActionDropColumn, Database: "db", Table: "orders", Column: "note"}
	if err := s.ApplySchema(change); err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("err=%v", err)
	}
}

func TestSinkApplySchemaWaitTimeoutFails(t *testing.T) {
	old := schemaChangePollInterval
	schemaChangePollInterval = time.Millisecond
	t.Cleanup(func() { schemaChangePollInterval = old })

	f := newFakeSQL()
	f.columns["db.orders"] = validTarget()
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris", SchemaChange: true, SchemaChangeInclude: []string{"add.column"}})
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "db", Table: "orders",
		AddColumns: []string{"note"}, ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
	}
	if err := s.ApplySchema(change); err == nil || !strings.Contains(err.Error(), "was not confirmed") {
		t.Fatalf("err=%v", err)
	}
}

func TestSinkFlushIsNoop(t *testing.T) {
	s := &Sink{cfg: definition.Sink{Type: "doris"}, tables: map[string]schema.Table{}}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(nil); err != nil {
		t.Fatal(err)
	}
}

func TestSinkCloseReleasesClients(t *testing.T) {
	f := newFakeSQL()
	server := successServer(t, &[]string{})
	s := testSink(t, server, f, definition.Sink{Type: "doris"})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if f.closed != 1 {
		t.Fatalf("closed=%d", f.closed)
	}
}
