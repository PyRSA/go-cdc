package composer

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/source"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
)

// catalogSink is a sink that declares the optional CatalogSink capability.
type catalogSink struct {
	prepared []schema.Table
	writes   int
	after    bool
	err      error
}

func (s *catalogSink) Open(context.Context) error { return nil }

func (s *catalogSink) CreateTable(table schema.Table) error {
	return s.PrepareCatalog([]schema.Table{table})
}

func (s *catalogSink) PrepareCatalog(tables []schema.Table) error {
	if s.err != nil {
		return s.err
	}
	if s.writes > 0 {
		s.after = true
	}
	s.prepared = append(s.prepared, tables...)
	return nil
}

func (s *catalogSink) ApplySchema(schema.Change) error { return nil }
func (s *catalogSink) Write([]event.Event) error       { s.writes++; return nil }
func (s *catalogSink) Flush() error                    { return nil }
func (s *catalogSink) Close() error                    { return nil }

func TestCatalogSinkPreparesProjectedRoutedTableBeforeWrite(t *testing.T) {
	sk := &catalogSink{}
	msgs := make(chan source.Message, 2)
	msgs <- source.Message{Schema: &schema.Table{
		Database: "db", Table: "users",
		Columns: []schema.Column{
			{Name: "id", Type: "int", Nullable: false},
			{Name: "name", Type: "varchar(32)"},
			{Name: "amount", Type: "decimal(10,2)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}}
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpCreate, Database: "es", Table: "users_out",
		SourceDatabase: "db", SourceTable: "users",
		After:   map[string]any{"id": int64(1), "name": "alice"},
		Columns: []string{"id", "name"}, Keys: []string{"id"},
	}}
	close(msgs)
	pipe := &definition.Pipeline{
		// auto-create stays at the elasticsearch default: preparing the catalog
		// must happen whether or not the sink may create indexes.
		Sink: definition.Sink{Type: "elasticsearch"},
		Transforms: []definition.Transform{{
			SourceTable: `db\.users`, Projection: "id, name",
		}},
		Routes: []definition.Route{{
			SourceTable: `db\.users`, SinkTable: "es.users_out",
		}},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	if err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	if len(sk.prepared) != 1 || sk.writes != 1 {
		t.Fatalf("prepared %#v writes %d", sk.prepared, sk.writes)
	}
	got := sk.prepared[0]
	if got.Database != "es" || got.Table != "users_out" {
		t.Fatalf("catalog must use the routed name: %#v", got)
	}
	if len(got.Columns) != 2 || got.Columns[0].Name != "id" || got.Columns[1].Name != "name" {
		t.Fatalf("catalog must use the projection: %#v", got.Columns)
	}
	if len(got.PrimaryKeys) != 1 || got.PrimaryKeys[0] != "id" {
		t.Fatalf("primary keys %#v", got.PrimaryKeys)
	}
	if sk.after {
		t.Fatal("catalog preparation ran after a write")
	}
}

func TestCatalogSinkPrepareErrorFailsRun(t *testing.T) {
	sk := &catalogSink{err: errors.New("index \"cdc.db.users\" does not exist")}
	msgs := make(chan source.Message, 2)
	msgs <- source.Message{Schema: &schema.Table{
		Database: "db", Table: "users",
		Columns:     []schema.Column{{Name: "id", Type: "int", Nullable: false}},
		PrimaryKeys: []string{"id"},
	}}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 55}}
	close(msgs)
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "elasticsearch"},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil)
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("catalog error must fail the job, got %v", err)
	}
	state, loadErr := (checkpoint.Store{Path: path}).Load()
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if state.AckedOffset != nil {
		t.Fatalf("checkpoint advanced although the catalog failed: %#v", state)
	}
	if sk.writes != 0 {
		t.Fatalf("writes %d", sk.writes)
	}
}

func TestPrepareCatalogFiltersSelectorAndKeepsCaptured(t *testing.T) {
	selector, err := definition.CompileSelector(`db\..*`, "")
	if err != nil {
		t.Fatal(err)
	}
	sk := &catalogSink{}
	pipe := &definition.Pipeline{
		Sink:    definition.Sink{Type: "elasticsearch"},
		Source:  definition.Source{Tables: selector},
		Routes:  []definition.Route{{SourceTable: `db\.a`, SinkTable: "es.a_out"}},
		Runtime: definition.Runtime{RouteMode: definition.RouteAllMatch},
	}
	tables := []schema.Table{
		{Database: "db", Table: "a", Columns: []schema.Column{{Name: "id", Type: "bigint"}}, PrimaryKeys: []string{"id"}},
		{Database: "db", Table: "b", Columns: []schema.Column{{Name: "id", Type: "bigint"}}, PrimaryKeys: []string{"id"}},
		{Database: "other", Table: "c", Columns: []schema.Column{{Name: "id", Type: "bigint"}}, PrimaryKeys: []string{"id"}},
		{Database: "gone", Table: "y", Columns: []schema.Column{{Name: "id", Type: "bigint"}}, PrimaryKeys: []string{"id"}},
	}
	state := checkpoint.State{CapturedTables: []string{"gone.y"}}
	if err := PrepareCatalog(pipe, sk, tables, state); err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(sk.prepared))
	for _, table := range sk.prepared {
		names = append(names, table.Database+"."+table.Table)
	}
	want := []string{"es.a_out", "db.b", "gone.y"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("prepared %v want %v", names, want)
	}
}

func TestPrepareCatalogIgnoresSinkWithoutCapability(t *testing.T) {
	pipe := &definition.Pipeline{
		Sink:    definition.Sink{Type: "stdout"},
		Runtime: definition.Runtime{RouteMode: definition.RouteAllMatch},
	}
	tables := []schema.Table{{Database: "db", Table: "a", Columns: []schema.Column{{Name: "id"}}}}
	if err := PrepareCatalog(pipe, &orderSink{}, tables, checkpoint.State{}); err != nil {
		t.Fatal(err)
	}
}

func TestDDLCreateDoesNotInventColumnsForElasticsearch(t *testing.T) {
	// A CREATE without catalog metadata cannot describe real column types, so a
	// non-MySQL sink must not receive a synthesized schema.
	sk := &orderSink{}
	msgs := make(chan source.Message, 1)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpDDL, Database: "db", Table: "t", SourceDatabase: "db", SourceTable: "t",
		DDL: "CREATE TABLE db.t (id int)", Columns: []string{"id"},
	}}
	close(msgs)
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "elasticsearch", AutoCreate: true},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	if err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	if len(sk.created) != 0 {
		t.Fatalf("synthesized table reached the sink: %#v", sk.created)
	}
	state, err := (checkpoint.Store{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if state.HasCaptured("db.t") {
		t.Fatalf("invented table marked captured: %#v", state)
	}
}
