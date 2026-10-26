package composer

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	esSink "github.com/PyRSA/go-cdc/connector/sink/elasticsearch"
	"github.com/PyRSA/go-cdc/connector/sink/elasticsearch/client"
	"github.com/PyRSA/go-cdc/connector/source"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
)

func elasticsearchSink(t *testing.T) (*esSink.Sink, *client.Fake) {
	return elasticsearchSinkWith(t, nil)
}

func elasticsearchSinkWith(t *testing.T, mutate func(*definition.Sink)) (*esSink.Sink, *client.Fake) {
	t.Helper()
	cfg := definition.Sink{
		Type:       "elasticsearch",
		Hosts:      []string{"http://127.0.0.1:9200"},
		AutoCreate: true,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	sk, err := esSink.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	fake := client.NewFake()
	sk.SetClient(fake)
	if err := sk.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return sk, fake
}

func elasticsearchMessages() chan source.Message {
	msgs := make(chan source.Message, 3)
	msgs <- source.Message{Schema: &schema.Table{
		Database: "db", Table: "users",
		Columns:     []schema.Column{{Name: "id", Type: "bigint"}, {Name: "name", Type: "varchar(32)"}},
		PrimaryKeys: []string{"id"},
	}}
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpCreate, Database: "db", Table: "users",
		SourceDatabase: "db", SourceTable: "users",
		After:   map[string]any{"id": int64(1), "name": "alice"},
		Columns: []string{"id", "name"}, Keys: []string{"id"},
	}}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 42}}
	close(msgs)
	return msgs
}

func elasticsearchPipeline() *definition.Pipeline {
	return &definition.Pipeline{
		Sink:       definition.Sink{Type: "elasticsearch", AutoCreate: true},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
}

func TestElasticsearchRunnerWritesAndAdvancesCheckpoint(t *testing.T) {
	sk, fake := elasticsearchSink(t)
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	if err := Run(context.Background(), elasticsearchPipeline(), elasticsearchMessages(), sk,
		checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	// The catalog pass created the index and indexed exactly one document.
	if len(fake.Created) != 1 || fake.Created[0].Index != "db.users" {
		t.Fatalf("created %#v", fake.Created)
	}
	if len(fake.BulkCalls) != 1 || len(fake.BulkCalls[0].Ops) != 1 {
		t.Fatalf("bulk calls %#v", fake.BulkCalls)
	}
	if fake.BulkCalls[0].Ops[0].Action != client.ActionIndex || fake.BulkCalls[0].Ops[0].ID == "" {
		t.Fatalf("operation %#v", fake.BulkCalls[0].Ops[0])
	}
	state, err := (checkpoint.Store{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if state.AckedOffset == nil || state.AckedOffset.Pos != 42 {
		t.Fatalf("checkpoint %#v", state)
	}
}

func TestElasticsearchBulkItemErrorBlocksCheckpoint(t *testing.T) {
	sk, fake := elasticsearchSink(t)
	fake.BulkHook = func(batch client.BulkBatch) (client.BulkResult, error) {
		ops := batch.Ops
		return client.BulkResult{Items: []client.BulkItem{{
			Action: "index", Index: ops[0].Index, ID: ops[0].ID,
			Status: 400, ErrType: "mapper_parsing_exception", ErrReason: "failed to parse field [id]",
		}}}, nil
	}
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	err := Run(context.Background(), elasticsearchPipeline(), elasticsearchMessages(), sk,
		checkpoint.Store{Path: path}, checkpoint.State{}, nil)
	if err == nil || !strings.Contains(err.Error(), "mapper_parsing_exception") {
		t.Fatalf("bulk item failure must fail the job, got %v", err)
	}
	state, loadErr := (checkpoint.Store{Path: path}).Load()
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if state.AckedOffset != nil {
		t.Fatalf("checkpoint advanced although the document was rejected: %#v", state)
	}
}

// The runner's write-batch-size and the sink's bulk-max-bytes act together: the
// runner flushes on the count, and the sink splits each flush on the bytes. The
// checkpoint must still advance only after every request was accepted.
func TestElasticsearchRunnerSplitsFlushByBytes(t *testing.T) {
	const (
		rows   = 5
		budget = 1 << 20
	)
	payload := strings.Repeat("x", 600<<10)
	msgs := make(chan source.Message, rows+2)
	msgs <- source.Message{Schema: &schema.Table{
		Database: "db", Table: "users",
		Columns:     []schema.Column{{Name: "id", Type: "bigint"}, {Name: "name", Type: "longtext"}},
		PrimaryKeys: []string{"id"},
	}}
	for i := 1; i <= rows; i++ {
		msgs <- source.Message{Event: &event.Event{
			Op: event.OpCreate, Database: "db", Table: "users",
			SourceDatabase: "db", SourceTable: "users",
			After:   map[string]any{"id": int64(i), "name": payload},
			Columns: []string{"id", "name"}, Keys: []string{"id"},
		}}
	}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 99}}
	close(msgs)

	pipe := elasticsearchPipeline()
	pipe.Sink.WriteBatchSize = 2
	sk, fake := elasticsearchSinkWith(t, func(cfg *definition.Sink) { cfg.BulkMaxBytes = budget })
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	if err := Run(context.Background(), pipe, msgs, sk,
		checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Two 600 KiB documents exceed the budget, so every flush of two becomes two
	// requests; the count limit never lets a third document join them.
	if len(fake.BulkCalls) != rows {
		t.Fatalf("bulk requests = %d, want %d", len(fake.BulkCalls), rows)
	}
	seen := make(map[string]struct{}, rows)
	for i, call := range fake.BulkCalls {
		if len(call.Ops) != 1 {
			t.Fatalf("request %d carries %d operation(s), want 1", i+1, len(call.Ops))
		}
		if size := len(call.Body); size > budget {
			t.Fatalf("request %d body = %d bytes, over the %d-byte budget", i+1, size, budget)
		}
		seen[call.Ops[0].ID] = struct{}{}
	}
	if len(seen) != rows {
		t.Fatalf("distinct documents = %d, want %d", len(seen), rows)
	}
	state, err := (checkpoint.Store{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if state.AckedOffset == nil || state.AckedOffset.Pos != 99 {
		t.Fatalf("checkpoint %#v", state)
	}
}

// A Write split into several requests still commits as a whole: when an earlier
// request was accepted and a later one failed, the checkpoint must not move and
// the whole batch must be replayed.
func TestElasticsearchRunnerSplitFailureBlocksCheckpoint(t *testing.T) {
	const (
		rows   = 2
		budget = 1 << 20
	)
	payload := strings.Repeat("x", 600<<10)
	msgs := make(chan source.Message, rows+2)
	msgs <- source.Message{Schema: &schema.Table{
		Database: "db", Table: "users",
		Columns:     []schema.Column{{Name: "id", Type: "bigint"}, {Name: "name", Type: "longtext"}},
		PrimaryKeys: []string{"id"},
	}}
	for i := 1; i <= rows; i++ {
		msgs <- source.Message{Event: &event.Event{
			Op: event.OpCreate, Database: "db", Table: "users",
			SourceDatabase: "db", SourceTable: "users",
			After:   map[string]any{"id": int64(i), "name": payload},
			Columns: []string{"id", "name"}, Keys: []string{"id"},
		}}
	}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 77}}
	close(msgs)

	sk, fake := elasticsearchSinkWith(t, func(cfg *definition.Sink) { cfg.BulkMaxBytes = budget })
	calls := 0
	fake.BulkHook = func(batch client.BulkBatch) (client.BulkResult, error) {
		calls++
		items := make([]client.BulkItem, 0, len(batch.Ops))
		for _, op := range batch.Ops {
			item := client.BulkItem{Action: string(op.Action), Index: op.Index, ID: op.ID, Status: 201}
			if calls == 2 {
				item.Status = 400
				item.ErrType = "illegal_argument_exception"
				item.ErrReason = "request rejected"
			}
			items = append(items, item)
		}
		return client.BulkResult{Items: items}, nil
	}

	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	err := Run(context.Background(), elasticsearchPipeline(), msgs, sk,
		checkpoint.Store{Path: path}, checkpoint.State{}, nil)
	if err == nil || !strings.Contains(err.Error(), "request 2/2") {
		t.Fatalf("a failing request must fail the job, got %v", err)
	}
	if len(fake.BulkCalls) != 2 {
		t.Fatalf("bulk requests = %d, want 2 (first accepted, second rejected)", len(fake.BulkCalls))
	}
	state, loadErr := (checkpoint.Store{Path: path}).Load()
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if state.AckedOffset != nil {
		t.Fatalf("checkpoint advanced although the second request failed: %#v", state)
	}
}

func TestElasticsearchUnregisteredTableBlocksCheckpoint(t *testing.T) {
	// A write for a table the catalog never prepared must fail instead of being
	// skipped, so the checkpoint cannot move past a lost row.
	sk, _ := elasticsearchSink(t)
	msgs := make(chan source.Message, 2)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpCreate, Database: "db", Table: "ghost",
		SourceDatabase: "db", SourceTable: "ghost",
		After: map[string]any{"id": int64(1)}, Columns: []string{"id"}, Keys: []string{"id"},
	}}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 43}}
	close(msgs)
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	err := Run(context.Background(), elasticsearchPipeline(), msgs, sk,
		checkpoint.Store{Path: path}, checkpoint.State{}, nil)
	if err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("unregistered table must fail the job, got %v", err)
	}
	state, loadErr := (checkpoint.Store{Path: path}).Load()
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if state.AckedOffset != nil {
		t.Fatalf("checkpoint advanced for an unregistered table: %#v", state)
	}
}
