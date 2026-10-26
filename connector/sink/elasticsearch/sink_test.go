package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/sink/elasticsearch/client"
)

func newTestSink(t *testing.T, mutate func(*definition.Sink)) (*Sink, *client.Fake) {
	t.Helper()
	cfg := definition.Sink{
		Type:           "elasticsearch",
		Hosts:          []string{"http://127.0.0.1:9200"},
		MaxRetries:     2,
		RequestTimeout: 5 * time.Second,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	fake := client.NewFake()
	s.SetClient(fake)
	s.sleep = func(context.Context, time.Duration) error { return nil }
	return s, fake
}

func registeredSink(t *testing.T, table schema.Table) (*Sink, *client.Fake) {
	t.Helper()
	s, fake := newTestSink(t, func(cfg *definition.Sink) { cfg.AutoCreate = true })
	if err := s.CreateTable(table); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	return s, fake
}

// mappingsFor converts derived properties into the shape GetMapping returns.
func mappingsFor(t *testing.T, table schema.Table) map[string]any {
	t.Helper()
	props, err := deriveProperties(table)
	if err != nil {
		t.Fatalf("deriveProperties: %v", err)
	}
	raw, err := json.Marshal(map[string]any{"properties": props})
	if err != nil {
		t.Fatalf("marshal mappings: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal mappings: %v", err)
	}
	return out
}

func typesRow(id uint64) map[string]any {
	return map[string]any{
		"ID":      id,
		"Flag":    int64(1),
		"Amount":  "10.50",
		"Day":     "2024-01-02",
		"Seen":    "2024-01-02 03:04:05.000000",
		"Updated": "2024-01-02T03:04:05Z",
		"Alarm":   "12:34:56.000000",
		"Name":    "widget",
		"Note":    "note",
		"Meta":    `{"b":2,"a":1}`,
		"Blob":    []byte("hi"),
		"Status":  "done",
		"Tags":    "a,c",
	}
}

func typesEvent(op event.Op, id uint64) event.Event {
	ev := event.Event{
		Op: op, Database: "shop", Table: "types",
		SourceDatabase: "src", SourceTable: "types",
	}
	if op == event.OpDelete {
		ev.Before = typesRow(id)
	} else {
		ev.After = typesRow(id)
	}
	return ev
}

func okItems(ops []client.BulkOperation) []client.BulkItem {
	items := make([]client.BulkItem, 0, len(ops))
	for _, op := range ops {
		status := 201
		if op.Action == client.ActionDelete {
			status = 200
		}
		items = append(items, client.BulkItem{Action: string(op.Action), Index: op.Index, ID: op.ID, Status: status})
	}
	return items
}

func TestNewValidatesConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  definition.Sink
	}{
		{name: "wrong type", cfg: definition.Sink{Type: "mysql", Hosts: []string{"http://h:9200"}}},
		{name: "no hosts", cfg: definition.Sink{Type: "elasticsearch"}},
		{name: "negative retries", cfg: definition.Sink{Type: "elasticsearch", Hosts: []string{"http://h:9200"}, MaxRetries: -1}},
		{name: "negative bulk-max-bytes", cfg: definition.Sink{Type: "elasticsearch", Hosts: []string{"http://h:9200"}, BulkMaxBytes: -1}},
		{name: "bulk-max-bytes below the parser minimum", cfg: definition.Sink{Type: "elasticsearch", Hosts: []string{"http://h:9200"}, BulkMaxBytes: definition.MinBulkMaxBytes - 1}},
		{name: "bulk-max-bytes above the parser maximum", cfg: definition.Sink{Type: "elasticsearch", Hosts: []string{"http://h:9200"}, BulkMaxBytes: definition.MaxBulkMaxBytes + 1}},
	}
	for _, tc := range cases {
		if _, err := New(tc.cfg); err == nil {
			t.Errorf("%s: expected an error", tc.name)
		}
	}

	s, err := New(definition.Sink{Type: "elasticsearch", Hosts: []string{"http://h:9200"}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.timeout != DefaultRequestTimeout {
		t.Fatalf("timeout = %v, want %v", s.timeout, DefaultRequestTimeout)
	}
	// Zero keeps meaning "not configured": the default budget applies, and the
	// accepted range matches the one the YAML parser enforces.
	if s.bulkMaxBytes != DefaultBulkMaxBytes {
		t.Fatalf("bulk-max-bytes = %d, want the default %d", s.bulkMaxBytes, DefaultBulkMaxBytes)
	}
	if DefaultBulkMaxBytes != definition.DefaultBulkMaxBytes {
		t.Fatalf("default bulk budget %d disagrees with the parser's %d",
			DefaultBulkMaxBytes, definition.DefaultBulkMaxBytes)
	}
	bounded, err := New(definition.Sink{
		Type: "elasticsearch", Hosts: []string{"http://h:9200"},
		BulkMaxBytes: definition.MaxBulkMaxBytes,
	})
	if err != nil {
		t.Fatalf("New at the upper bound: %v", err)
	}
	if bounded.bulkMaxBytes != definition.MaxBulkMaxBytes {
		t.Fatalf("bulk-max-bytes = %d, want %d", bounded.bulkMaxBytes, definition.MaxBulkMaxBytes)
	}
}

// The programmatic path accepts zero as "use the default" and rejects the same
// out-of-range values as the YAML parser, with the same reason.
func TestNewBulkMaxBytesMatchesParserRange(t *testing.T) {
	_, err := New(definition.Sink{
		Type: "elasticsearch", Hosts: []string{"http://h:9200"},
		BulkMaxBytes: definition.MinBulkMaxBytes - 1,
	})
	if err == nil || !strings.Contains(err.Error(), "sink.bulk-max-bytes") ||
		!strings.Contains(err.Error(), "0 (use the default") {
		t.Fatalf("error = %v, want the reason the parser reports", err)
	}
	for _, n := range []int{0, definition.MinBulkMaxBytes, definition.MaxBulkMaxBytes} {
		if _, err := New(definition.Sink{
			Type: "elasticsearch", Hosts: []string{"http://h:9200"}, BulkMaxBytes: n,
		}); err != nil {
			t.Fatalf("bulk-max-bytes %d: %v", n, err)
		}
	}
}

func TestOpenPingsAndCloseClosesClient(t *testing.T) {
	s, fake := newTestSink(t, nil)
	if err := s.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if fake.PingCalls != 1 {
		t.Fatalf("PingCalls = %d, want 1", fake.PingCalls)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !fake.Closed {
		t.Fatal("Close did not close the client")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestOpenReportsPingFailure(t *testing.T) {
	s, fake := newTestSink(t, nil)
	fake.PingErr = errors.New("connection refused")
	err := s.Open(context.Background())
	if err == nil || !strings.Contains(err.Error(), "ping") {
		t.Fatalf("Open error = %v, want a ping failure", err)
	}
}

// A password embedded in a host URL never reaches an error message.
func TestOpenDoesNotLeakHostCredentials(t *testing.T) {
	s, fake := newTestSink(t, func(cfg *definition.Sink) {
		cfg.Hosts = []string{"http://elastic:sup3rsecret@127.0.0.1:9200"}
	})
	fake.PingErr = errors.New("connection refused")
	err := s.Open(context.Background())
	if err == nil {
		t.Fatal("Open: want a ping failure")
	}
	if strings.Contains(err.Error(), "sup3rsecret") {
		t.Fatalf("error leaks the password: %v", err)
	}
	if !strings.Contains(err.Error(), "127.0.0.1:9200") {
		t.Fatalf("error lost the configured host: %v", err)
	}
}

func TestCloseReportsClientError(t *testing.T) {
	s, fake := newTestSink(t, nil)
	fake.CloseErr = errors.New("transport stuck")
	if err := s.Close(); err == nil || !strings.Contains(err.Error(), "transport stuck") {
		t.Fatalf("Close error = %v, want the client error", err)
	}
}

func TestCreateTableRequiresPrimaryKey(t *testing.T) {
	table := typesTable()
	table.PrimaryKeys = nil
	s, fake := newTestSink(t, func(cfg *definition.Sink) { cfg.AutoCreate = true })
	err := s.CreateTable(table)
	if err == nil || !strings.Contains(err.Error(), "primary key") {
		t.Fatalf("error = %v, want a primary key failure", err)
	}
	if len(fake.Created) != 0 {
		t.Fatal("an index was created for a table without a primary key")
	}
}

func TestCreateTableRejectsPrimaryKeyOutsideColumns(t *testing.T) {
	table := typesTable()
	table.PrimaryKeys = []string{"Missing"}
	s, _ := newTestSink(t, func(cfg *definition.Sink) { cfg.AutoCreate = true })
	err := s.CreateTable(table)
	if err == nil || !strings.Contains(err.Error(), `"Missing"`) {
		t.Fatalf("error = %v, want the missing key column", err)
	}
}

func TestCreateTableAutoCreateDisabled(t *testing.T) {
	s, fake := newTestSink(t, nil)
	err := s.CreateTable(typesTable())
	if err == nil || !strings.Contains(err.Error(), "auto-create is false") {
		t.Fatalf("error = %v, want an auto-create failure", err)
	}
	if len(fake.Created) != 0 {
		t.Fatal("auto-create=false still created an index")
	}
}

func TestCreateTableCreatesIndexFromSchema(t *testing.T) {
	s, fake := newTestSink(t, func(cfg *definition.Sink) { cfg.AutoCreate = true })
	if err := s.CreateTable(typesTable()); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if len(fake.Created) != 1 {
		t.Fatalf("Created = %d, want 1", len(fake.Created))
	}
	call := fake.Created[0]
	if call.Index != "shop.types" {
		t.Fatalf("index = %q, want shop.types", call.Index)
	}
	var body struct {
		Mappings struct {
			Properties map[string]esField `json:"properties"`
		} `json:"mappings"`
	}
	if err := json.Unmarshal(call.Body, &body); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	if got := body.Mappings.Properties["ID"].Type; got != "unsigned_long" {
		t.Errorf("ID type = %q, want unsigned_long", got)
	}
	if got := body.Mappings.Properties["Amount"].Type; got != stringKeyword {
		t.Errorf("Amount type = %q, want keyword", got)
	}
	if got := body.Mappings.Properties["Seen"].Format; got != datetimeFormat {
		t.Errorf("Seen format = %q, want %q", got, datetimeFormat)
	}

	// Registering the same table again must not create another index.
	if err := s.CreateTable(typesTable()); err != nil {
		t.Fatalf("second CreateTable: %v", err)
	}
	if len(fake.Created) != 1 {
		t.Fatalf("Created = %d after re-registration, want 1", len(fake.Created))
	}
}

func TestCreateTableNeverModifiesExistingIndex(t *testing.T) {
	table := typesTable()
	s, fake := newTestSink(t, func(cfg *definition.Sink) { cfg.AutoCreate = true })
	fake.AddIndex("shop.types", mappingsFor(t, table))
	if err := s.CreateTable(table); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if len(fake.Created) != 0 {
		t.Fatal("an existing index was recreated")
	}
}

func TestCreateTableRejectsExistingIncompatibleMapping(t *testing.T) {
	table := typesTable()
	mappings := mappingsFor(t, table)
	props := mappings["properties"].(map[string]any)
	props["ID"] = map[string]any{"type": "integer"}

	s, fake := newTestSink(t, nil)
	fake.AddIndex("shop.types", mappings)
	err := s.CreateTable(table)
	if err == nil || !strings.Contains(err.Error(), "range") {
		t.Fatalf("error = %v, want a range conflict for bigint unsigned", err)
	}
}

func TestCreateTableFailsBeforeCreatingOnMappingFileConflict(t *testing.T) {
	s, fake := newTestSink(t, func(cfg *definition.Sink) {
		cfg.AutoCreate = true
		cfg.Mapping = json.RawMessage(`{"mappings":{"properties":{"ID":{"type":"long"}}}}`)
	})
	err := s.CreateTable(typesTable())
	if err == nil || !strings.Contains(err.Error(), "range") {
		t.Fatalf("error = %v, want a mapping conflict", err)
	}
	if len(fake.Created) != 0 {
		t.Fatal("the index was created despite a mapping conflict")
	}
}

func TestCreateTableKeepsSettingsFromMappingFile(t *testing.T) {
	s, fake := newTestSink(t, func(cfg *definition.Sink) {
		cfg.AutoCreate = true
		cfg.Mapping = json.RawMessage(`{"settings":{"number_of_shards":1},"aliases":{"types":{"is_write_index":true}}}`)
	})
	if err := s.CreateTable(typesTable()); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(fake.Created[0].Body, &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if _, ok := body["settings"]; !ok {
		t.Fatalf("settings were dropped: %s", fake.Created[0].Body)
	}
	if _, ok := body["aliases"]; !ok {
		t.Fatalf("aliases were dropped: %s", fake.Created[0].Body)
	}
}

func TestCreateTableHandlesConcurrentCreate(t *testing.T) {
	table := typesTable()
	s, fake := newTestSink(t, func(cfg *definition.Sink) { cfg.AutoCreate = true })
	// The index appears between the existence check and the create call.
	fake.AddIndex("shop.types", mappingsFor(t, table))
	fake.ExistsHook = func(string) (bool, error) { return false, nil }
	fake.CreateErr = &client.Error{Op: "indices.create", Status: 400, Type: "resource_already_exists_exception"}
	if err := s.CreateTable(table); err != nil {
		t.Fatalf("CreateTable after a lost race: %v", err)
	}
	if len(fake.Created) != 0 {
		t.Fatal("the race fallback recorded a create call")
	}
}

func TestCreateTableReportsExistenceError(t *testing.T) {
	s, fake := newTestSink(t, nil)
	fake.ExistsErr = errors.New("cluster unavailable")
	err := s.CreateTable(typesTable())
	if err == nil || !strings.Contains(err.Error(), "cluster unavailable") {
		t.Fatalf("error = %v, want the existence failure", err)
	}
}

func TestCreateTableSharedIndexChecksEveryTable(t *testing.T) {
	tableA := schema.Table{
		Database: "shop", Table: "a", PrimaryKeys: []string{"id"},
		Columns: []schema.Column{{Name: "id", Type: "int(11)"}, {Name: "name", Type: "varchar(10)"}},
	}
	tableB := schema.Table{
		Database: "shop", Table: "b", PrimaryKeys: []string{"id"},
		Columns: []schema.Column{{Name: "id", Type: "int(11)"}, {Name: "name", Type: "varchar(10)"}},
	}
	tableC := schema.Table{
		Database: "shop", Table: "c", PrimaryKeys: []string{"id"},
		Columns: []schema.Column{
			{Name: "id", Type: "int(11)"}, {Name: "name", Type: "varchar(10)"},
			{Name: "extra", Type: "varchar(10)"},
		},
	}
	s, fake := newTestSink(t, func(cfg *definition.Sink) {
		cfg.AutoCreate = true
		cfg.IndexName = "shared"
	})
	if err := s.CreateTable(tableA); err != nil {
		t.Fatalf("CreateTable A: %v", err)
	}
	if err := s.CreateTable(tableB); err != nil {
		t.Fatalf("CreateTable B: %v", err)
	}
	if len(fake.Created) != 1 {
		t.Fatalf("Created = %d, want a single index", len(fake.Created))
	}
	err := s.CreateTable(tableC)
	if err == nil || !strings.Contains(err.Error(), `"extra"`) {
		t.Fatalf("error = %v, want the unmapped column", err)
	}
}

func TestCreateTableRejectsInvalidExplicitIndexName(t *testing.T) {
	s, fake := newTestSink(t, func(cfg *definition.Sink) {
		cfg.AutoCreate = true
		cfg.IndexName = "Bad_Name"
	})
	err := s.CreateTable(typesTable())
	if err == nil || !strings.Contains(err.Error(), "lowercase") {
		t.Fatalf("error = %v, want an index name failure", err)
	}
	if len(fake.Created) != 0 {
		t.Fatal("an invalid index name reached the cluster")
	}
}

func TestWriteRequiresOpen(t *testing.T) {
	s, err := New(definition.Sink{Type: "elasticsearch", Hosts: []string{"http://h:9200"}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	err = s.Write([]event.Event{typesEvent(event.OpCreate, 1)})
	if err == nil || !strings.Contains(err.Error(), "not open") {
		t.Fatalf("error = %v, want a not-open failure", err)
	}
}

func TestWriteSendsIndexOperations(t *testing.T) {
	s, fake := registeredSink(t, typesTable())
	if err := s.Write([]event.Event{typesEvent(event.OpCreate, 1)}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(fake.BulkCalls) != 1 || len(fake.BulkCalls[0].Ops) != 1 {
		t.Fatalf("bulk calls = %d", len(fake.BulkCalls))
	}
	op := fake.BulkCalls[0].Ops[0]
	if op.Action != client.ActionIndex || op.Index != "shop.types" || op.ID == "" {
		t.Fatalf("op = %+v", op)
	}
}

func TestWriteDeleteUsesTheSameId(t *testing.T) {
	s, fake := registeredSink(t, typesTable())
	if err := s.Write([]event.Event{typesEvent(event.OpCreate, 7)}); err != nil {
		t.Fatalf("index: %v", err)
	}
	if err := s.Write([]event.Event{typesEvent(event.OpDelete, 7)}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	indexID := fake.BulkCalls[0].Ops[0].ID
	deleteOp := fake.BulkCalls[1].Ops[0]
	if deleteOp.Action != client.ActionDelete {
		t.Fatalf("action = %q, want delete", deleteOp.Action)
	}
	if deleteOp.ID != indexID {
		t.Fatalf("delete _id %q != index _id %q", deleteOp.ID, indexID)
	}
}

func TestWriteRejectsUnregisteredTable(t *testing.T) {
	s, fake := newTestSink(t, nil)
	err := s.Write([]event.Event{typesEvent(event.OpCreate, 1)})
	if err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("error = %v, want a registration failure", err)
	}
	if len(fake.BulkCalls) != 0 {
		t.Fatal("an unregistered table reached the cluster")
	}
}

func TestWriteItemFailureReturnsError(t *testing.T) {
	s, fake := registeredSink(t, typesTable())
	fake.BulkHook = func(batch client.BulkBatch) (client.BulkResult, error) {
		ops := batch.Ops
		items := okItems(ops)
		items[0].Status = 400
		items[0].ErrType = "document_parsing_exception"
		items[0].ErrReason = "failed to parse field"
		return client.BulkResult{Items: items}, nil
	}
	err := s.Write([]event.Event{typesEvent(event.OpCreate, 1)})
	if err == nil || !strings.Contains(err.Error(), "document_parsing_exception") {
		t.Fatalf("error = %v, want the item failure", err)
	}
	if !strings.Contains(err.Error(), "1 of 1") {
		t.Fatalf("error %q does not report the failure count", err)
	}
	if len(fake.BulkCalls) != 1 {
		t.Fatalf("permanent item failures were retried: %d calls", len(fake.BulkCalls))
	}
}

func TestWriteRetriesOnlyTransientItems(t *testing.T) {
	s, fake := registeredSink(t, typesTable())
	attempts := 0
	fake.BulkHook = func(batch client.BulkBatch) (client.BulkResult, error) {
		ops := batch.Ops
		attempts++
		items := okItems(ops)
		if attempts == 1 {
			items[len(items)-1].Status = 429
			items[len(items)-1].ErrType = "es_rejected_execution_exception"
		}
		return client.BulkResult{Items: items}, nil
	}
	err := s.Write([]event.Event{typesEvent(event.OpCreate, 1), typesEvent(event.OpCreate, 2)})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(fake.BulkCalls) != 2 {
		t.Fatalf("bulk calls = %d, want 2", len(fake.BulkCalls))
	}
	if len(fake.BulkCalls[0].Ops) != 2 || len(fake.BulkCalls[1].Ops) != 1 {
		t.Fatalf("retry sent %d items after %d, want only the failed item",
			len(fake.BulkCalls[1].Ops), len(fake.BulkCalls[0].Ops))
	}
}

func TestWriteExhaustsRetries(t *testing.T) {
	s, fake := registeredSink(t, typesTable())
	fake.BulkHook = func(batch client.BulkBatch) (client.BulkResult, error) {
		ops := batch.Ops
		items := okItems(ops)
		for i := range items {
			items[i].Status = 503
			items[i].ErrType = "unavailable_shards_exception"
		}
		return client.BulkResult{Items: items}, nil
	}
	err := s.Write([]event.Event{typesEvent(event.OpCreate, 1)})
	if err == nil || !strings.Contains(err.Error(), "3 attempt(s)") {
		t.Fatalf("error = %v, want the exhausted retry count", err)
	}
	if len(fake.BulkCalls) != 3 {
		t.Fatalf("bulk calls = %d, want maxRetries+1", len(fake.BulkCalls))
	}
}

func TestWriteRetriesTransportError(t *testing.T) {
	s, fake := registeredSink(t, typesTable())
	calls := 0
	fake.BulkHook = func(batch client.BulkBatch) (client.BulkResult, error) {
		ops := batch.Ops
		calls++
		if calls == 1 {
			return client.BulkResult{}, &client.Error{Op: "bulk", Err: errors.New("dial tcp: connection refused")}
		}
		return client.BulkResult{Items: okItems(ops)}, nil
	}
	if err := s.Write([]event.Event{typesEvent(event.OpCreate, 1)}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if calls != 2 {
		t.Fatalf("bulk calls = %d, want 2", calls)
	}
}

func TestWriteDoesNotRetryAuthFailure(t *testing.T) {
	s, fake := registeredSink(t, typesTable())
	calls := 0
	fake.BulkHook = func(client.BulkBatch) (client.BulkResult, error) {
		calls++
		return client.BulkResult{}, &client.Error{Op: "bulk", Status: 401, Reason: "Unauthorized"}
	}
	err := s.Write([]event.Event{typesEvent(event.OpCreate, 1)})
	if err == nil || !strings.Contains(err.Error(), "Unauthorized") {
		t.Fatalf("error = %v, want the auth failure", err)
	}
	if calls != 1 {
		t.Fatalf("auth failures were retried: %d calls", calls)
	}
}

func TestWriteTreatsDeleteNotFoundAsSuccess(t *testing.T) {
	s, fake := registeredSink(t, typesTable())
	fake.BulkHook = func(batch client.BulkBatch) (client.BulkResult, error) {
		ops := batch.Ops
		return client.BulkResult{Items: []client.BulkItem{{
			Action: "delete", Index: ops[0].Index, ID: ops[0].ID, Status: 404,
		}}}, nil
	}
	if err := s.Write([]event.Event{typesEvent(event.OpDelete, 1)}); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

func TestWriteRejectsItemCountMismatch(t *testing.T) {
	s, fake := registeredSink(t, typesTable())
	fake.BulkHook = func(client.BulkBatch) (client.BulkResult, error) {
		return client.BulkResult{}, nil
	}
	err := s.Write([]event.Event{typesEvent(event.OpCreate, 1)})
	if err == nil || !strings.Contains(err.Error(), "response has 0 item(s)") {
		t.Fatalf("error = %v, want a response shape failure", err)
	}
}

func TestBulkStopsOnCanceledContext(t *testing.T) {
	s, fake := registeredSink(t, typesTable())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ops := []client.BulkOperation{{Action: client.ActionIndex, Index: "i", ID: "1", Document: []byte(`{}`)}}
	if err := s.bulk(ctx, ops); err == nil {
		t.Fatal("expected a cancellation error")
	}
	if len(fake.BulkCalls) != 0 {
		t.Fatal("a canceled context still sent a bulk request")
	}
}

func TestFlushReturnsNil(t *testing.T) {
	s, _ := newTestSink(t, nil)
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestApplySchemaIsUnsupported(t *testing.T) {
	s, _ := newTestSink(t, nil)
	err := s.ApplySchema(schema.Change{Database: "shop", Table: "types", Action: schema.ActionAddColumn})
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("error = %v, want an unsupported change failure", err)
	}
}

func TestWriteOnClosedSinkFails(t *testing.T) {
	s, _ := registeredSink(t, typesTable())
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	err := s.Write([]event.Event{typesEvent(event.OpCreate, 1)})
	if err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("error = %v, want a closed-sink failure", err)
	}
}

func TestPrepareCatalogRegistersEveryTable(t *testing.T) {
	s, fake := newTestSink(t, func(cfg *definition.Sink) { cfg.AutoCreate = true })
	tables := []schema.Table{
		{Database: "shop", Table: "orders", Columns: []schema.Column{{Name: "id", Type: "bigint"}}, PrimaryKeys: []string{"id"}},
		{Database: "shop", Table: "items", Columns: []schema.Column{{Name: "id", Type: "int"}}, PrimaryKeys: []string{"id"}},
	}
	if err := s.PrepareCatalog(tables); err != nil {
		t.Fatalf("PrepareCatalog: %v", err)
	}
	if len(fake.Created) != 2 {
		t.Fatalf("Created = %d, want 2", len(fake.Created))
	}
	for _, table := range tables {
		if _, ok := s.metaFor(table.Database + "." + table.Table); !ok {
			t.Fatalf("table %s.%s was not registered", table.Database, table.Table)
		}
	}
}

func TestPrepareCatalogStopsAtFirstFailure(t *testing.T) {
	s, fake := newTestSink(t, func(cfg *definition.Sink) { cfg.AutoCreate = true })
	tables := []schema.Table{
		{Database: "shop", Table: "broken", Columns: []schema.Column{{Name: "id", Type: "bigint"}}, PrimaryKeys: []string{"id"}},
		{Database: "shop", Table: "later", Columns: []schema.Column{{Name: "id", Type: "bigint"}}, PrimaryKeys: []string{"id"}},
	}
	fake.ExistsHook = func(index string) (bool, error) {
		if index == "shop.broken" {
			return false, errors.New("cluster unreachable")
		}
		return false, nil
	}
	err := s.PrepareCatalog(tables)
	if err == nil || !strings.Contains(err.Error(), "cluster unreachable") {
		t.Fatalf("error = %v, want the catalog failure", err)
	}
	if _, ok := s.metaFor("shop.later"); ok {
		t.Fatal("preparation continued after a failure")
	}
}

func TestPrepareCatalogSkipsUnchangedTables(t *testing.T) {
	s, fake := newTestSink(t, func(cfg *definition.Sink) { cfg.AutoCreate = true })
	table := typesTable()
	if err := s.PrepareCatalog([]schema.Table{table}); err != nil {
		t.Fatalf("PrepareCatalog: %v", err)
	}
	checks := 0
	fake.ExistsHook = func(index string) (bool, error) {
		checks++
		return true, nil
	}
	// The startup catalog and the snapshot Schema message describe the same
	// table; the second pass must not hit the cluster again.
	if err := s.PrepareCatalog([]schema.Table{table}); err != nil {
		t.Fatalf("second PrepareCatalog: %v", err)
	}
	if checks != 0 {
		t.Fatalf("unchanged table was re-checked: %d lookups", checks)
	}
}

func TestPrepareCatalogRechecksChangedSchema(t *testing.T) {
	s, fake := newTestSink(t, func(cfg *definition.Sink) { cfg.AutoCreate = true })
	table := typesTable()
	if err := s.PrepareCatalog([]schema.Table{table}); err != nil {
		t.Fatalf("PrepareCatalog: %v", err)
	}
	// A retyped column changes the mapping plan, so the sink must verify the
	// index again instead of trusting the cached plan.
	changed := typesTable()
	changed.Columns[0].Type = "text"
	checks := 0
	fake.ExistsHook = func(index string) (bool, error) {
		checks++
		return true, nil
	}
	err := s.PrepareCatalog([]schema.Table{changed})
	if err == nil || !strings.Contains(err.Error(), "cannot be written to mapping type") {
		t.Fatalf("error = %v, want an incompatible mapping failure", err)
	}
	if checks != 1 {
		t.Fatalf("changed schema was not re-checked: %d lookups", checks)
	}
}
