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
	t.Helper()
	sk, err := esSink.New(definition.Sink{
		Type:       "elasticsearch",
		Hosts:      []string{"http://127.0.0.1:9200"},
		AutoCreate: true,
	})
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
	if len(fake.BulkCalls) != 1 || len(fake.BulkCalls[0]) != 1 {
		t.Fatalf("bulk calls %#v", fake.BulkCalls)
	}
	if fake.BulkCalls[0][0].Action != client.ActionIndex || fake.BulkCalls[0][0].ID == "" {
		t.Fatalf("operation %#v", fake.BulkCalls[0][0])
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
	fake.BulkHook = func(ops []client.BulkOperation) (client.BulkResult, error) {
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
