package composer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	kafkasink "github.com/PyRSA/go-cdc/connector/sink/kafka"
	"github.com/PyRSA/go-cdc/connector/sink/stdout"
	"github.com/PyRSA/go-cdc/connector/source"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
)

func TestRunRecordsSplitAndCommit(t *testing.T) {
	var buf bytes.Buffer
	sk := stdout.New(&buf)
	high := "2"
	msgs := make(chan source.Message, 4)
	msgs <- source.Message{Watermark: &source.Position{File: "mysql-bin.000001", Pos: 10}}
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpCreate, Database: "db", Table: "t", SourceDatabase: "db", SourceTable: "t",
		After: map[string]any{"id": int64(1), "name": "a"},
	}}
	msgs <- source.Message{Split: &source.Split{Table: "db.t", Low: "1", High: &high}}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 40}}
	close(msgs)

	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "stdout"},
		Runtime:    definition.Runtime{Name: "demo", RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	if err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	state, err := (checkpoint.Store{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if state.LowWatermark.Pos != 10 || state.AckedOffset.Pos != 40 {
		t.Fatalf("%#v", state)
	}
	if !state.HasSplit("db.t", checkpoint.Split{Low: "1", High: &high}) {
		t.Fatalf("splits %#v", state.FinishedSplits)
	}
	if !bytes.Contains(buf.Bytes(), []byte(`"op":"c"`)) && !bytes.Contains(buf.Bytes(), []byte(`"op":"r"`)) {
		t.Fatal(buf.String())
	}
}

func TestCreateFailureDoesNotExclude(t *testing.T) {
	sk := &failCreate{}
	msgs := make(chan source.Message, 2)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpDDL, Database: "db", Table: "t", SourceDatabase: "db", SourceTable: "t",
		DDL: "CREATE TABLE db.t (id int)", Columns: []string{"id"},
	}}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 80}}
	close(msgs)
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "mysql", AutoCreate: true},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	if err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	state, err := (checkpoint.Store{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if state.HasExcluded("db.t") {
		t.Fatalf("create failure must not exclude: %#v", state)
	}
	if !state.HasCaptured("db.t") || state.AckedOffset.Pos != 80 {
		t.Fatalf("%#v", state)
	}
}

func TestCreateWithoutColumnMetadataDoesNotMarkCaptured(t *testing.T) {
	// Empty-column CREATE without a prior Schema apply must not pretend the sink exists.
	sk := &orderSink{}
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	msgs := make(chan source.Message, 1)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpDDL, Database: "db", Table: "t", SourceDatabase: "db", SourceTable: "t",
		DDL: "CREATE TABLE db.t (id int)",
	}}
	close(msgs)
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "mysql", AutoCreate: true},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	if err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	if len(sk.created) != 0 {
		t.Fatalf("created %#v", sk.created)
	}
	state, err := (checkpoint.Store{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if state.HasCaptured("db.t") {
		t.Fatalf("empty CREATE must not mark captured: %#v", state)
	}
}

func TestSchemaOnCreateMessageMarksCaptured(t *testing.T) {
	// Production CREATE commits carry Schema on the same message (catalogApplied).
	sk := &orderSink{}
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	msgs := make(chan source.Message, 1)
	msgs <- source.Message{
		Schema: &schema.Table{
			Database: "db", Table: "t",
			Columns:     []schema.Column{{Name: "id", Type: "int", Nullable: false}},
			PrimaryKeys: []string{"id"},
		},
		Event: &event.Event{
			Op: event.OpDDL, Database: "db", Table: "t", SourceDatabase: "db", SourceTable: "t",
			DDL: "CREATE TABLE db.t (id int)",
		},
	}
	close(msgs)
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "mysql", AutoCreate: true},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	if err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	state, err := (checkpoint.Store{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if !state.HasCaptured("db.t") {
		t.Fatalf("schema+CREATE should mark captured: %#v", state)
	}
}

func TestCreateFailureStillCapturesTheSource(t *testing.T) {
	sk := &failCreate{}
	msgs := make(chan source.Message, 2)
	msgs <- source.Message{Schema: &schema.Table{
		Database: "db", Table: "t",
		Columns:     []schema.Column{{Name: "id", Type: "int", Nullable: false}},
		PrimaryKeys: []string{"id"},
	}}
	msgs <- source.Message{Captured: "db.t"}
	close(msgs)
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "mysql", AutoCreate: true},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	if err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	state, err := (checkpoint.Store{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if state.HasExcluded("db.t") || !state.HasCaptured("db.t") {
		t.Fatalf("%#v", state)
	}
}

func TestFanoutWriteToMissingSinkFailsJob(t *testing.T) {
	sk := &failNamed{name: "sink.bad"}
	msgs := make(chan source.Message, 2)
	msgs <- source.Message{Schema: &schema.Table{
		Database: "db", Table: "t",
		Columns:     []schema.Column{{Name: "id", Type: "int", Nullable: false}},
		PrimaryKeys: []string{"id"},
	}}
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpCreate, Database: "db", Table: "t", SourceDatabase: "db", SourceTable: "t",
		After: map[string]any{"id": int64(1)}, Columns: []string{"id"}, Keys: []string{"id"},
	}}
	close(msgs)
	pipe := &definition.Pipeline{
		Sink: definition.Sink{Type: "mysql", AutoCreate: true, WriteBatchSize: 1},
		Routes: []definition.Route{
			{SourceTable: `db\.t`, SinkTable: "sink.good"},
			{SourceTable: `db\.t`, SinkTable: "sink.bad"},
		},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil)
	if err == nil {
		t.Fatal("write to missing sink table should fail the job")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("err=%v", err)
	}
	state, errLoad := (checkpoint.Store{Path: path}).Load()
	if errLoad != nil {
		t.Fatal(errLoad)
	}
	if state.HasExcluded("sink.bad") {
		t.Fatalf("must not exclude on create failure: %#v", state)
	}
}

func TestRouteMarksRoutedBeforeSink(t *testing.T) {
	sk := &batchSink{}
	msgs := make(chan source.Message, 2)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpCreate, Database: "db", Table: "t", SourceDatabase: "db", SourceTable: "t",
		Columns: []string{"id"}, Keys: []string{"id"}, After: map[string]any{"id": int64(1)},
	}}
	// No rule covers db.other.
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpCreate, Database: "db", Table: "other", SourceDatabase: "db", SourceTable: "other",
		Columns: []string{"id"}, Keys: []string{"id"}, After: map[string]any{"id": int64(2)},
	}}
	close(msgs)
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "mysql", WriteBatchSize: 10},
		Routes:     []definition.Route{{SourceTable: `db\.t`, SinkTable: "db.t"}},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	if err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	if len(sk.batches) != 1 || len(sk.batches[0]) != 2 {
		t.Fatalf("batches %#v", sk.batches)
	}
	// The identity route matches even though the database name is unchanged.
	if !sk.batches[0][0].Routed {
		t.Fatalf("routed event lost the mark: %#v", sk.batches[0][0])
	}
	if sk.batches[0][1].Routed {
		t.Fatalf("unmatched event must stay unmarked: %#v", sk.batches[0][1])
	}
}

func TestBatchFlushesAtConfiguredSize(t *testing.T) {
	sk := &orderSink{}
	msgs := make(chan source.Message, 4)
	for id := 1; id <= 3; id++ {
		msgs <- source.Message{Event: &event.Event{
			Op: event.OpCreate, Database: "db", Table: "t", SourceDatabase: "db", SourceTable: "t",
			After: map[string]any{"id": int64(id)},
		}}
	}
	close(msgs)
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "stdout", WriteBatchSize: 2},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	if err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	if len(sk.sizes) != 2 || sk.sizes[0] != 2 || sk.sizes[1] != 1 {
		t.Fatalf("batch sizes %#v", sk.sizes)
	}
}

func TestKafkaDDLRoutesLikeDML(t *testing.T) {
	sk, err := kafkasink.Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc_default", EmitDDL: true,
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	fake := &kafkasink.FakeProducer{}
	sk.AttachFake(fake)

	msgs := make(chan source.Message, 4)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpCreate, Database: "db", Table: "orders",
		SourceDatabase: "db", SourceTable: "orders",
		After: map[string]any{"id": int64(1)}, Keys: []string{"id"},
	}}
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpDDL, Database: "db", Table: "orders",
		SourceDatabase: "db", SourceTable: "orders",
		DDL: "ALTER TABLE db.orders ADD COLUMN x int", SourceTSMs: 11,
	}}
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpDDL, Database: "db", Table: "users",
		SourceDatabase: "db", SourceTable: "users",
		DDL: "ALTER TABLE db.users ADD COLUMN y int", SourceTSMs: 12,
	}}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 50}}
	close(msgs)

	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	pipe := &definition.Pipeline{
		Sink: definition.Sink{Type: "kafka", Topic: "cdc_default", EmitDDL: true},
		Routes: []definition.Route{
			{SourceTable: `db\.orders`, SinkTable: "kafka.orders"},
			{SourceTable: `db\.users`, SinkTable: "kafka.users"},
		},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	if err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	topics := make([]string, 0, len(fake.Records))
	for _, r := range fake.Records {
		topics = append(topics, r.Topic)
	}
	wantOrders, wantUsers := 0, 0
	for _, topic := range topics {
		switch topic {
		case "kafka.orders":
			wantOrders++
		case "kafka.users":
			wantUsers++
		case "cdc_default":
			t.Fatalf("DDL/DML leaked to default topic: %v", topics)
		}
	}
	if wantOrders < 2 || wantUsers < 1 {
		t.Fatalf("topics=%v (orders=%d users=%d)", topics, wantOrders, wantUsers)
	}
	state, err := (checkpoint.Store{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if state.AckedOffset == nil || state.AckedOffset.Pos != 50 {
		t.Fatalf("checkpoint %#v", state)
	}
}

func TestKafkaProduceErrorBlocksRunnerCheckpoint(t *testing.T) {
	sk, err := kafkasink.Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc_default",
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	fake := &kafkasink.FakeProducer{Fail: errors.New("broker down")}
	sk.AttachFake(fake)

	msgs := make(chan source.Message, 2)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpCreate, Database: "db", Table: "t",
		SourceDatabase: "db", SourceTable: "t",
		After: map[string]any{"id": int64(1)}, Keys: []string{"id"},
	}}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 99}}
	close(msgs)

	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "kafka", Topic: "cdc_default"},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	if err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err == nil {
		t.Fatal("expected produce/flush error to fail Run")
	}
	state, err := (checkpoint.Store{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if state.AckedOffset != nil && state.AckedOffset.Pos == 99 {
		t.Fatalf("checkpoint advanced despite kafka produce error: %#v", state)
	}
}

func TestKafkaDDLApplySchemaErrorBlocksCheckpoint(t *testing.T) {
	sk, err := kafkasink.Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc_default", EmitDDL: true,
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	fake := &kafkasink.FakeProducer{Fail: errors.New("broker down")}
	sk.AttachFake(fake)

	msgs := make(chan source.Message, 2)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpDDL, Database: "db", Table: "t",
		SourceDatabase: "db", SourceTable: "t",
		DDL: "ALTER TABLE db.t ADD COLUMN z int",
	}}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 77}}
	close(msgs)

	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	pipe := &definition.Pipeline{
		Sink: definition.Sink{Type: "kafka", Topic: "cdc_default", EmitDDL: true},
		Routes: []definition.Route{
			{SourceTable: `db\.t`, SinkTable: "kafka.t"},
		},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	if err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err == nil {
		t.Fatal("expected DDL produce error to fail Run")
	}
	state, err := (checkpoint.Store{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if state.AckedOffset != nil && state.AckedOffset.Pos == 77 {
		t.Fatalf("checkpoint advanced despite DDL produce error: %#v", state)
	}
}

func TestEmptyBatchStillFlushesBeforeCheckpoint(t *testing.T) {
	sk := &countingFlushSink{}
	msgs := make(chan source.Message, 1)
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 100}}
	close(msgs)
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "stdout"},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	if err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	if sk.flushes < 1 {
		t.Fatalf("expected Flush on empty-batch checkpoint, got %d", sk.flushes)
	}
	state, err := (checkpoint.Store{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if state.AckedOffset.Pos != 100 {
		t.Fatalf("%#v", state)
	}
}

func TestFlushErrorBlocksCheckpoint(t *testing.T) {
	sk := &countingFlushSink{err: errors.New("broker down")}
	msgs := make(chan source.Message, 1)
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 100}}
	close(msgs)
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "stdout"},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	if err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err == nil {
		t.Fatal("expected flush error to fail the job")
	}
	state, err := (checkpoint.Store{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if state.AckedOffset != nil && state.AckedOffset.Pos == 100 {
		t.Fatalf("checkpoint advanced despite flush error: %#v", state)
	}
}

func TestShutdownSaveErrorIsReturned(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "stdout"},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	err := Run(ctx, pipe, nil, &orderSink{}, checkpoint.Store{Path: filepath.Join(t.TempDir(), "missing", "checkpoint.ckpt")}, checkpoint.State{}, nil)
	if err != nil {
		t.Fatal("cancelled run with a writable path should save and return nil, got", err)
	}
	blocked := filepath.Join(t.TempDir(), "not-a-directory")
	if writeErr := os.WriteFile(blocked, []byte("x"), 0o644); writeErr != nil {
		t.Fatal(writeErr)
	}
	err = Run(ctx, pipe, nil, &orderSink{}, checkpoint.Store{Path: filepath.Join(blocked, "checkpoint.ckpt")}, checkpoint.State{}, nil)
	if err == nil {
		t.Fatal("save failure on shutdown was ignored")
	}
}

// TestCanceledWriteDoesNotAdvanceCheckpoint pins the shutdown contract at the
// runner level: when the sink reports the cancellation, Run returns from save()
// before store.Save, so the batch is replayed on restart instead of skipped.
func TestCanceledWriteDoesNotAdvanceCheckpoint(t *testing.T) {
	sk := &canceledWriteSink{}
	msgs := make(chan source.Message, 2)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpCreate, Database: "db", Table: "t",
		SourceDatabase: "db", SourceTable: "t",
		After: map[string]any{"id": int64(1)}, Columns: []string{"id"}, Keys: []string{"id"},
	}}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 42}}
	close(msgs)
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "postgresql"},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v", err)
	}
	if _, statErr := os.Stat(path); statErr == nil {
		t.Fatal("checkpoint advanced past a canceled write")
	}
}

func TestCatalogCreatesProjectedTableBeforeRows(t *testing.T) {
	sk := &orderSink{}
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
		Op: event.OpCreate, Database: "db", Table: "users", SourceDatabase: "db", SourceTable: "users",
		After:   map[string]any{"id": int64(1), "name": "alice", "amount": "1.00"},
		Columns: []string{"id", "name", "amount"}, Keys: []string{"id"},
	}}
	close(msgs)
	pipe := &definition.Pipeline{
		Sink: definition.Sink{Type: "mysql", AutoCreate: true},
		Transforms: []definition.Transform{{
			SourceTable: `db\.users`, Projection: "id, name",
		}},
		Routes: []definition.Route{{
			SourceTable: `db\.users`, SinkTable: "sink.users_out",
		}},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	if err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	if sk.writeBeforeCreate || len(sk.created) != 1 || sk.writes != 1 {
		t.Fatalf("created %#v writes %d early %v", sk.created, sk.writes, sk.writeBeforeCreate)
	}
	if sk.created[0].Database != "sink" || sk.created[0].Table != "users_out" || len(sk.created[0].Columns) != 2 {
		t.Fatalf("%#v", sk.created[0])
	}
	if sk.created[0].Columns[0].Name != "id" || sk.created[0].Columns[1].Name != "name" {
		t.Fatalf("columns %#v", sk.created[0].Columns)
	}
}

type orderSink struct {
	created           []schema.Table
	writes            int
	sizes             []int
	writeBeforeCreate bool
}

type failNamed struct {
	name  string
	wrote []string
}

func (f *failNamed) Open(context.Context) error { return nil }
func (f *failNamed) CreateTable(table schema.Table) error {
	if table.Database+"."+table.Table == f.name {
		return errCreate
	}
	return nil
}
func (f *failNamed) ApplySchema(schema.Change) error { return nil }
func (f *failNamed) Write(batch []event.Event) error {
	for _, item := range batch {
		id := item.Database + "." + item.Table
		if id == f.name {
			return fmt.Errorf("sink.mysql: table %s does not exist", id)
		}
		f.wrote = append(f.wrote, id)
	}
	return nil
}
func (f *failNamed) Flush() error { return nil }
func (f *failNamed) Close() error { return nil }

func (s *orderSink) Open(context.Context) error { return nil }
func (s *orderSink) CreateTable(table schema.Table) error {
	s.created = append(s.created, table)
	return nil
}
func (s *orderSink) ApplySchema(schema.Change) error { return nil }
func (s *orderSink) Write(batch []event.Event) error {
	if len(s.created) == 0 {
		s.writeBeforeCreate = true
	}
	s.writes += len(batch)
	s.sizes = append(s.sizes, len(batch))
	return nil
}
func (s *orderSink) Flush() error { return nil }
func (s *orderSink) Close() error { return nil }

type countingFlushSink struct {
	flushes int
	err     error
}

func (s *countingFlushSink) Open(context.Context) error     { return nil }
func (s *countingFlushSink) CreateTable(schema.Table) error { return nil }
func (s *countingFlushSink) ApplySchema(schema.Change) error {
	return nil
}
func (s *countingFlushSink) Write([]event.Event) error { return nil }
func (s *countingFlushSink) Flush() error {
	s.flushes++
	return s.err
}
func (s *countingFlushSink) Close() error { return nil }

func TestStdoutDDLPrintsSourceSQL(t *testing.T) {
	sk := &schemaSink{}
	msgs := make(chan source.Message, 1)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpDDL, Database: "sink", Table: "users_out",
		SourceDatabase: "db", SourceTable: "users",
		DDL: "ALTER TABLE db.users ADD COLUMN name varchar(32)",
	}}
	close(msgs)
	pipe := &definition.Pipeline{
		Sink: definition.Sink{Type: "stdout"},
		Routes: []definition.Route{{
			SourceTable: `db\.users`, SinkTable: "sink.users_out",
		}},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	if err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: filepath.Join(t.TempDir(), "checkpoint.ckpt")}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	if len(sk.applied) != 1 || sk.applied[0] != "ALTER TABLE db.users ADD COLUMN name varchar(32)" {
		t.Fatalf("ddl %#v", sk.applied)
	}
	if len(sk.names) != 1 || sk.names[0] != "sink.users_out" {
		t.Fatalf("routed names %#v", sk.names)
	}
	if len(sk.sources) != 1 || sk.sources[0] != "db.users" {
		t.Fatalf("source names %#v", sk.sources)
	}
}

func TestFanoutCapturesSourceWhenOneSinkCreateFails(t *testing.T) {
	sk := &failNamed{name: "sink.bad"}
	msgs := make(chan source.Message, 2)
	msgs <- source.Message{Schema: &schema.Table{
		Database: "db", Table: "t",
		Columns:     []schema.Column{{Name: "id", Type: "int", Nullable: false}},
		PrimaryKeys: []string{"id"},
	}}
	msgs <- source.Message{Captured: "db.t"}
	close(msgs)
	pipe := &definition.Pipeline{
		Sink: definition.Sink{Type: "mysql", AutoCreate: true},
		Routes: []definition.Route{
			{SourceTable: `db\.t`, SinkTable: "sink.good"},
			{SourceTable: `db\.t`, SinkTable: "sink.bad"},
		},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	if err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	state, err := (checkpoint.Store{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if !state.HasCaptured("db.t") || state.HasExcluded("sink.bad") || state.HasExcluded("db.t") {
		t.Fatalf("%#v", state)
	}
}

func TestAddColumnOutsideProjectionIsNotExecuted(t *testing.T) {
	sk := &schemaSink{}
	msgs := make(chan source.Message, 2)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpDDL, Database: "db", Table: "users", SourceDatabase: "db", SourceTable: "users",
		DDL: "ALTER TABLE db.users ADD COLUMN note varchar(32)",
	}}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 90}}
	close(msgs)
	pipe := &definition.Pipeline{
		Sink: definition.Sink{Type: "mysql", AutoCreate: true},
		Transforms: []definition.Transform{{
			SourceTable: `db\.users`, Projection: "id, name",
		}},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	if err := Run(context.Background(), pipe, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	if len(sk.applied) != 0 {
		t.Fatalf("excluded column was executed: %#v", sk.applied)
	}
}

func TestCapturedSaveThenRemovesSpill(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	spill := &recordingSpill{}
	msgs := make(chan source.Message, 1)
	msgs <- source.Message{Captured: "db.order"}
	close(msgs)
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "stdout"},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	if err := Run(context.Background(), pipe, msgs, &orderSink{}, checkpoint.Store{Path: path}, checkpoint.State{}, spill); err != nil {
		t.Fatal(err)
	}
	if len(spill.removed) != 1 || spill.removed[0] != "db.order" {
		t.Fatalf("removed %#v", spill.removed)
	}
	state, err := (checkpoint.Store{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if !state.HasCaptured("db.order") {
		t.Fatalf("%#v", state)
	}
}

func TestCapturedSaveFailureSkipsRemove(t *testing.T) {
	spill := &recordingSpill{}
	blocked := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocked, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	msgs := make(chan source.Message, 1)
	msgs <- source.Message{Captured: "db.order"}
	close(msgs)
	pipe := &definition.Pipeline{
		Sink:       definition.Sink{Type: "stdout"},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
	err := Run(context.Background(), pipe, msgs, &orderSink{}, checkpoint.Store{Path: filepath.Join(blocked, "checkpoint.ckpt")}, checkpoint.State{}, spill)
	if err == nil {
		t.Fatal("expected save failure")
	}
	if len(spill.removed) != 0 {
		t.Fatalf("Remove ran despite save failure: %#v", spill.removed)
	}
}

// postgresRunnerSink records the PostgreSQL Runner calls and can fail either.
type postgresRunnerSink struct {
	created   []schema.Table
	applied   []schema.Change
	order     []string
	createErr error
	applyErr  error
	flushErr  error
}

func (s *postgresRunnerSink) Open(context.Context) error { return nil }
func (s *postgresRunnerSink) CreateTable(table schema.Table) error {
	s.order = append(s.order, "create")
	if s.createErr != nil {
		return s.createErr
	}
	s.created = append(s.created, table)
	return nil
}
func (s *postgresRunnerSink) ApplySchema(change schema.Change) error {
	s.order = append(s.order, "ddl")
	if s.applyErr != nil {
		return s.applyErr
	}
	s.applied = append(s.applied, change)
	return nil
}
func (s *postgresRunnerSink) Write([]event.Event) error {
	s.order = append(s.order, "write")
	return nil
}
func (s *postgresRunnerSink) Flush() error {
	s.order = append(s.order, "flush")
	return s.flushErr
}
func (s *postgresRunnerSink) Close() error { return nil }

func postgresRunnerPipeline(sink definition.Sink, routes ...definition.Route) *definition.Pipeline {
	sink.Type = "postgresql"
	return &definition.Pipeline{
		Sink:       sink,
		Routes:     routes,
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
}

// runPostgresDDL sends one DDL event through Run and returns the sink it used.
func runPostgresDDL(t *testing.T, ddl string, mutate func(*definition.Pipeline)) (*postgresRunnerSink, error) {
	t.Helper()
	sk := &postgresRunnerSink{}
	msgs := make(chan source.Message, 1)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpDDL, Database: "db", Table: "orders",
		SourceDatabase: "db", SourceTable: "orders",
		DDL: ddl,
	}}
	close(msgs)
	pipeline := postgresRunnerPipeline(definition.Sink{})
	if mutate != nil {
		mutate(pipeline)
	}
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	return sk, Run(context.Background(), pipeline, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil)
}

func TestPostgresCatalogCreateMarksCapturedOnlyAfterCreate(t *testing.T) {
	sk := &postgresRunnerSink{}
	msgs := make(chan source.Message, 2)
	msgs <- source.Message{
		Schema: &schema.Table{
			Database: "db", Table: "orders",
			Columns: []schema.Column{
				{Name: "id", Type: "bigint", Nullable: false},
				{Name: "note", Type: "varchar(32)", Nullable: true},
			},
			PrimaryKeys: []string{"id"},
		},
		Event: &event.Event{
			Op: event.OpDDL, Database: "db", Table: "orders",
			SourceDatabase: "db", SourceTable: "orders",
			DDL: "CREATE TABLE `db`.`orders` (id bigint primary key)",
		},
	}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 120}}
	close(msgs)

	pipeline := postgresRunnerPipeline(
		definition.Sink{AutoCreate: true},
		definition.Route{SourceTable: `db\.orders`, SinkTable: "ods.orders"},
	)
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	if err := Run(context.Background(), pipeline, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	if len(sk.created) != 1 || sk.created[0].Database != "ods" || sk.created[0].Table != "orders" {
		t.Fatalf("created %#v", sk.created)
	}
	if len(sk.created[0].Columns) != 2 || len(sk.created[0].PrimaryKeys) != 1 {
		t.Fatalf("created table lost the source definition: %#v", sk.created[0])
	}
	if len(sk.applied) != 0 {
		t.Fatalf("CREATE must not be re-applied as raw DDL: %#v", sk.applied)
	}
	state, err := (checkpoint.Store{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if !state.HasCaptured("db.orders") {
		t.Fatalf("created table must be captured: %#v", state)
	}
}

// TestPostgresUnroutedCreateLeavesSchemaToSink pins the multi-schema rule: only a
// matched route names a schema, so an unrouted table is created in the schema the
// sink was configured with rather than in one taken from the MySQL database name.
func TestPostgresUnroutedCreateLeavesSchemaToSink(t *testing.T) {
	sk := &postgresRunnerSink{}
	msgs := make(chan source.Message, 1)
	msgs <- source.Message{Schema: &schema.Table{
		Database: "db", Table: "orders",
		Columns:     []schema.Column{{Name: "id", Type: "bigint", Nullable: false}},
		PrimaryKeys: []string{"id"},
	}}
	close(msgs)
	pipeline := postgresRunnerPipeline(definition.Sink{AutoCreate: true})
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	if err := Run(context.Background(), pipeline, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	if len(sk.created) != 1 || sk.created[0].Database != "" || sk.created[0].Table != "orders" {
		t.Fatalf("created %#v", sk.created)
	}
}

func TestPostgresCreateFailureStopsRunBeforeCheckpoint(t *testing.T) {
	sk := &postgresRunnerSink{createErr: errors.New("sink.postgresql: table ods.orders does not exist")}
	msgs := make(chan source.Message, 2)
	msgs <- source.Message{Schema: &schema.Table{
		Database: "db", Table: "orders",
		Columns:     []schema.Column{{Name: "id", Type: "bigint", Nullable: false}},
		PrimaryKeys: []string{"id"},
	}}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 130}}
	close(msgs)

	pipeline := postgresRunnerPipeline(
		definition.Sink{},
		definition.Route{SourceTable: `db\.orders`, SinkTable: "ods.orders"},
	)
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	err := Run(context.Background(), pipeline, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil)
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("err=%v", err)
	}
	if _, statErr := os.Stat(path); statErr == nil {
		t.Fatal("checkpoint advanced past a failed CREATE")
	}
}

func TestPostgresCreateWithoutCatalogFails(t *testing.T) {
	_, err := runPostgresDDL(t, "CREATE TABLE `db`.`orders` (id bigint primary key)", nil)
	if err == nil || !strings.Contains(err.Error(), "without a source table definition") {
		t.Fatalf("err=%v", err)
	}
}

func TestPostgresAddColumnForwardsStructuredMetadata(t *testing.T) {
	sk := &postgresRunnerSink{}
	msgs := make(chan source.Message, 2)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpDDL, Database: "db", Table: "orders",
		SourceDatabase: "db", SourceTable: "orders",
		DDL: "ALTER TABLE `db`.`orders` ADD COLUMN `note` varchar(32) NULL",
		Change: &schema.Change{
			Action: schema.ActionAddColumn, AddColumns: []string{"note"},
			ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
		},
	}}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 140}}
	close(msgs)

	pipeline := postgresRunnerPipeline(
		definition.Sink{SchemaChange: true, SchemaChangeInclude: []string{"add.column"}},
		definition.Route{SourceTable: `db\.orders`, SinkTable: "ods.orders"},
	)
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	if err := Run(context.Background(), pipeline, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	if len(sk.applied) != 1 {
		t.Fatalf("applied %#v", sk.applied)
	}
	change := sk.applied[0]
	if change.Database != "ods" || change.Table != "orders" {
		t.Fatalf("change not routed: %#v", change)
	}
	if change.SourceDatabase != "db" || change.SourceTable != "orders" {
		t.Fatalf("the sink must read the source definition: %#v", change)
	}
	if len(change.AddColumns) != 1 || change.AddColumns[0] != "note" {
		t.Fatalf("AddColumns lost: %#v", change)
	}
	if change.ColumnDef == nil || change.ColumnDef.Type != "varchar(32)" {
		t.Fatalf("ColumnDef lost: %#v", change.ColumnDef)
	}
	if change.DDL != "ALTER TABLE `db`.`orders` ADD COLUMN `note` varchar(32) NULL" {
		t.Fatalf("DDL rewritten for the sink: %q", change.DDL)
	}
}

func TestPostgresUnroutedChangeLeavesSchemaToSink(t *testing.T) {
	sk, err := runPostgresDDL(t, "ALTER TABLE `db`.`orders` ADD COLUMN `note` varchar(32) NULL", func(pipe *definition.Pipeline) {
		pipe.Sink.SchemaChange = true
		pipe.Sink.SchemaChangeInclude = []string{"add.column"}
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sk.applied) != 1 || sk.applied[0].Database != "" {
		t.Fatalf("an unrouted change must not carry the source database: %#v", sk.applied)
	}
}

func TestPostgresApplySchemaErrorStopsRun(t *testing.T) {
	sk := &postgresRunnerSink{applyErr: errors.New("sink.postgresql: modify.column refused")}
	msgs := make(chan source.Message, 2)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpDDL, Database: "db", Table: "orders",
		SourceDatabase: "db", SourceTable: "orders",
		DDL: "ALTER TABLE `db`.`orders` MODIFY COLUMN `note` varchar(16) NULL",
	}}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 150}}
	close(msgs)

	pipeline := postgresRunnerPipeline(definition.Sink{SchemaChange: true})
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	err := Run(context.Background(), pipeline, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil)
	if err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("err=%v", err)
	}
	if _, statErr := os.Stat(path); statErr == nil {
		t.Fatal("checkpoint advanced past a failed DDL")
	}
}

// TestPostgresFlushesPendingRowsBeforeDDL pins the ordering the sink needs: a
// batch that was captured before the statement has to be written with the old
// shape, and must not be applied after the table changed.
func TestPostgresFlushesPendingRowsBeforeDDL(t *testing.T) {
	sk := &postgresRunnerSink{}
	msgs := make(chan source.Message, 3)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpCreate, Database: "db", Table: "orders",
		SourceDatabase: "db", SourceTable: "orders",
		After: map[string]any{"id": int64(1)}, Columns: []string{"id"}, Keys: []string{"id"},
	}}
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpDDL, Database: "db", Table: "orders",
		SourceDatabase: "db", SourceTable: "orders",
		DDL: "ALTER TABLE `db`.`orders` ADD COLUMN `note` varchar(32) NULL",
		Change: &schema.Change{
			Action: schema.ActionAddColumn, AddColumns: []string{"note"},
			ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
		},
	}}
	close(msgs)

	pipeline := postgresRunnerPipeline(definition.Sink{SchemaChange: true})
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	if err := Run(context.Background(), pipeline, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	write, flush, ddl := -1, -1, -1
	for i, step := range sk.order {
		switch step {
		case "write":
			write = i
		case "flush":
			if flush < 0 {
				flush = i
			}
		case "ddl":
			ddl = i
		}
	}
	if ddl < 0 || write < 0 || flush < 0 || flush > ddl || write > ddl {
		t.Fatalf("DDL ran before the pending rows were flushed: %#v", sk.order)
	}
}

func TestPostgresFlushFailureDoesNotRunDDL(t *testing.T) {
	sk := &postgresRunnerSink{flushErr: errors.New("sink.postgresql: write batch: connection refused")}
	msgs := make(chan source.Message, 3)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpCreate, Database: "db", Table: "orders",
		SourceDatabase: "db", SourceTable: "orders",
		After: map[string]any{"id": int64(1)}, Columns: []string{"id"}, Keys: []string{"id"},
	}}
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpDDL, Database: "db", Table: "orders",
		SourceDatabase: "db", SourceTable: "orders",
		DDL: "ALTER TABLE `db`.`orders` ADD COLUMN `note` varchar(32) NULL",
	}}
	close(msgs)

	pipeline := postgresRunnerPipeline(definition.Sink{SchemaChange: true})
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	err := Run(context.Background(), pipeline, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil)
	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("err=%v", err)
	}
	if len(sk.applied) != 0 {
		t.Fatalf("DDL ran after the batch failed to flush: %#v", sk.applied)
	}
	if _, statErr := os.Stat(path); statErr == nil {
		t.Fatal("checkpoint advanced past a failed flush")
	}
}

// TestPostgresOtherSinksKeepTheirOwnOrdering guards the flush rule above: only
// PostgreSQL is drained ahead of a DDL event.
func TestPostgresOtherSinksKeepTheirOwnOrdering(t *testing.T) {
	for _, sinkType := range []string{"mysql", "stdout", "kafka", "doris"} {
		sk := &postgresRunnerSink{}
		msgs := make(chan source.Message, 2)
		msgs <- source.Message{Event: &event.Event{
			Op: event.OpCreate, Database: "db", Table: "orders",
			SourceDatabase: "db", SourceTable: "orders",
			After: map[string]any{"id": int64(1)}, Columns: []string{"id"}, Keys: []string{"id"},
		}}
		msgs <- source.Message{Event: &event.Event{
			Op: event.OpDDL, Database: "db", Table: "orders",
			SourceDatabase: "db", SourceTable: "orders",
			DDL: "ALTER TABLE `db`.`orders` ADD COLUMN `note` varchar(32) NULL",
			Change: &schema.Change{
				Action: schema.ActionAddColumn, AddColumns: []string{"note"},
				ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
			},
		}}
		close(msgs)
		pipeline := &definition.Pipeline{
			Sink:       definition.Sink{Type: sinkType, SchemaChange: true},
			Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
			Checkpoint: definition.Checkpoint{Interval: time.Hour},
		}
		path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
		if err := Run(context.Background(), pipeline, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
			t.Fatalf("%s: %v", sinkType, err)
		}
		// The pending batch reaches the sink only through the shutdown flush, so a
		// sink other than PostgreSQL sees the statement first.
		if len(sk.order) == 0 || sk.order[0] != "ddl" {
			t.Fatalf("%s: the batch was drained before DDL: %#v", sinkType, sk.order)
		}
	}
}

// TestPostgresSinkRefusesDisabledSchemaChange covers the switch from the sink
// side: the runner hands the change over, and the sink answers with an error
// rather than a silent skip.
func TestPostgresSinkRefusesDisabledSchemaChange(t *testing.T) {
	sk, err := runPostgresDDL(t, "ALTER TABLE `db`.`orders` ADD COLUMN `note` varchar(32) NULL", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(sk.applied) != 1 {
		t.Fatalf("the sink must see the change it has to refuse: %#v", sk.applied)
	}
}

// batchSink records the batches the runner hands to the sink.
type batchSink struct {
	batches [][]event.Event
}

func (s *batchSink) Open(context.Context) error     { return nil }
func (s *batchSink) CreateTable(schema.Table) error { return nil }
func (s *batchSink) ApplySchema(schema.Change) error {
	return nil
}
func (s *batchSink) Write(batch []event.Event) error {
	s.batches = append(s.batches, append([]event.Event(nil), batch...))
	return nil
}
func (s *batchSink) Flush() error { return nil }
func (s *batchSink) Close() error { return nil }

// canceledWriteSink reports the shutdown cancellation the PostgreSQL sink wraps
// when the signal lands while a batch is in flight.
type canceledWriteSink struct{ batchSink }

func (s *canceledWriteSink) Write([]event.Event) error { return context.Canceled }

type recordingSpill struct {
	removed []string
	err     error
}

func (r *recordingSpill) Remove(id string) error {
	r.removed = append(r.removed, id)
	return r.err
}

type schemaSink struct {
	applied []string
	names   []string
	sources []string
}

func (s *schemaSink) Open(context.Context) error     { return nil }
func (s *schemaSink) CreateTable(schema.Table) error { return nil }
func (s *schemaSink) ApplySchema(change schema.Change) error {
	s.applied = append(s.applied, change.DDL)
	s.names = append(s.names, change.Database+"."+change.Table)
	src := change.SourceDatabase + "." + change.SourceTable
	if change.SourceDatabase == "" {
		src = change.Database + "." + change.Table
	}
	s.sources = append(s.sources, src)
	return nil
}
func (s *schemaSink) Write([]event.Event) error { return nil }
func (s *schemaSink) Flush() error              { return nil }
func (s *schemaSink) Close() error              { return nil }

type failCreate struct{}

func (f *failCreate) Open(context.Context) error      { return nil }
func (f *failCreate) CreateTable(schema.Table) error  { return errCreate }
func (f *failCreate) ApplySchema(schema.Change) error { return nil }
func (f *failCreate) Write([]event.Event) error       { return nil }
func (f *failCreate) Flush() error                    { return nil }
func (f *failCreate) Close() error                    { return nil }

type createError struct{}

func (createError) Error() string { return "create failed" }

var errCreate = createError{}

// dorisRunnerSink records the Doris-specific Runner calls and can fail either.
type dorisRunnerSink struct {
	created   []schema.Table
	applied   []schema.Change
	createErr error
	applyErr  error
}

func (s *dorisRunnerSink) Open(context.Context) error { return nil }
func (s *dorisRunnerSink) CreateTable(table schema.Table) error {
	if s.createErr != nil {
		return s.createErr
	}
	s.created = append(s.created, table)
	return nil
}
func (s *dorisRunnerSink) ApplySchema(change schema.Change) error {
	if s.applyErr != nil {
		return s.applyErr
	}
	s.applied = append(s.applied, change)
	return nil
}
func (s *dorisRunnerSink) Write([]event.Event) error { return nil }
func (s *dorisRunnerSink) Flush() error              { return nil }
func (s *dorisRunnerSink) Close() error              { return nil }

func dorisPipeline() *definition.Pipeline {
	return &definition.Pipeline{
		Sink: definition.Sink{Type: "doris", AutoCreate: true},
		Routes: []definition.Route{{
			SourceTable: `db\.orders`, SinkTable: "doris.orders",
		}},
		Runtime:    definition.Runtime{RouteMode: definition.RouteAllMatch},
		Checkpoint: definition.Checkpoint{Interval: time.Hour},
	}
}

func TestDorisCatalogCreateMarksCapturedOnlyAfterValidation(t *testing.T) {
	sk := &dorisRunnerSink{}
	msgs := make(chan source.Message, 2)
	msgs <- source.Message{
		Schema: &schema.Table{
			Database: "db", Table: "orders",
			Columns: []schema.Column{
				{Name: "id", Type: "bigint", Nullable: false},
				{Name: "note", Type: "varchar(32)", Nullable: true},
			},
			PrimaryKeys: []string{"id"},
		},
		Event: &event.Event{
			Op: event.OpDDL, Database: "db", Table: "orders",
			SourceDatabase: "db", SourceTable: "orders",
			DDL: "CREATE TABLE `db`.`orders` (id bigint primary key)",
		},
	}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 120}}
	close(msgs)

	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	if err := Run(context.Background(), dorisPipeline(), msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	if len(sk.created) != 1 || sk.created[0].Database != "doris" || sk.created[0].Table != "orders" {
		t.Fatalf("created %#v", sk.created)
	}
	if len(sk.created[0].PrimaryKeys) != 1 || len(sk.created[0].Columns) != 2 {
		t.Fatalf("projected table lost metadata: %#v", sk.created[0])
	}
	if len(sk.applied) != 0 {
		t.Fatalf("CREATE must not be re-applied as raw DDL: %#v", sk.applied)
	}
	state, err := (checkpoint.Store{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if !state.HasCaptured("db.orders") {
		t.Fatalf("validated CREATE must be captured: %#v", state)
	}
	if state.AckedOffset == nil || state.AckedOffset.Pos != 120 {
		t.Fatalf("checkpoint %#v", state)
	}
}

func TestDorisCreateFailureStopsRunBeforeCheckpoint(t *testing.T) {
	sk := &dorisRunnerSink{createErr: errors.New("doris fe unavailable")}
	msgs := make(chan source.Message, 2)
	msgs <- source.Message{Schema: &schema.Table{
		Database: "db", Table: "orders",
		Columns:     []schema.Column{{Name: "id", Type: "bigint", Nullable: false}},
		PrimaryKeys: []string{"id"},
	}}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 130}}
	close(msgs)

	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	err := Run(context.Background(), dorisPipeline(), msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil)
	if err == nil || !strings.Contains(err.Error(), "doris fe unavailable") {
		t.Fatalf("err=%v", err)
	}
	state, loadErr := (checkpoint.Store{Path: path}).Load()
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if state.HasCaptured("db.orders") {
		t.Fatalf("failed CREATE must not be captured: %#v", state)
	}
	if state.AckedOffset != nil && state.AckedOffset.Pos == 130 {
		t.Fatalf("checkpoint advanced despite CREATE failure: %#v", state)
	}
}

func TestDorisCreateWithoutCatalogFails(t *testing.T) {
	sk := &dorisRunnerSink{}
	msgs := make(chan source.Message, 1)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpDDL, Database: "db", Table: "orders",
		SourceDatabase: "db", SourceTable: "orders",
		DDL: "CREATE TABLE `db`.`orders` (id bigint primary key)",
	}}
	close(msgs)

	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	err := Run(context.Background(), dorisPipeline(), msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil)
	if err == nil || !strings.Contains(err.Error(), "without a source table definition") {
		t.Fatalf("err=%v", err)
	}
}

func TestDorisAddColumnForwardsStructuredMetadata(t *testing.T) {
	sk := &dorisRunnerSink{}
	msgs := make(chan source.Message, 2)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpDDL, Database: "db", Table: "orders",
		SourceDatabase: "db", SourceTable: "orders",
		DDL: "ALTER TABLE `db`.`orders` ADD COLUMN `note` varchar(32) NULL",
		Change: &schema.Change{
			Action: schema.ActionAddColumn, AddColumns: []string{"note"},
			ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
		},
	}}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 140}}
	close(msgs)

	pipeline := dorisPipeline()
	pipeline.Sink.SchemaChange = true
	pipeline.Sink.SchemaChangeInclude = []string{"add.column"}
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	if err := Run(context.Background(), pipeline, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil); err != nil {
		t.Fatal(err)
	}
	if len(sk.applied) != 1 {
		t.Fatalf("applied %#v", sk.applied)
	}
	change := sk.applied[0]
	if change.Database != "doris" || change.Table != "orders" {
		t.Fatalf("change not routed: %#v", change)
	}
	if len(change.AddColumns) != 1 || change.AddColumns[0] != "note" {
		t.Fatalf("AddColumns lost: %#v", change)
	}
	if change.ColumnDef == nil || change.ColumnDef.Type != "varchar(32)" {
		t.Fatalf("ColumnDef lost: %#v", change.ColumnDef)
	}
}

func TestDorisApplySchemaErrorStopsRun(t *testing.T) {
	sk := &dorisRunnerSink{applyErr: errors.New("schema change rejected")}
	msgs := make(chan source.Message, 2)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpDDL, Database: "db", Table: "orders",
		SourceDatabase: "db", SourceTable: "orders",
		DDL: "ALTER TABLE `db`.`orders` ADD COLUMN `note` varchar(32) NULL",
		Change: &schema.Change{
			Action: schema.ActionAddColumn, AddColumns: []string{"note"},
			ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
		},
	}}
	msgs <- source.Message{Position: &source.Position{File: "mysql-bin.000001", Pos: 150}}
	close(msgs)

	pipeline := dorisPipeline()
	pipeline.Sink.SchemaChange = true
	pipeline.Sink.SchemaChangeInclude = []string{"add.column"}
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	err := Run(context.Background(), pipeline, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil)
	if err == nil || !strings.Contains(err.Error(), "schema change rejected") {
		t.Fatalf("err=%v", err)
	}
}

func TestDorisUnsupportedDDLIsFatalWhenEnabled(t *testing.T) {
	sk := &dorisRunnerSink{}
	msgs := make(chan source.Message, 1)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpDDL, Database: "db", Table: "orders",
		SourceDatabase: "db", SourceTable: "orders",
		DDL: "DROP TABLE `db`.`orders`",
	}}
	close(msgs)

	pipeline := dorisPipeline()
	pipeline.Sink.DropTruncate = true
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	err := Run(context.Background(), pipeline, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil)
	if err == nil || !strings.Contains(err.Error(), "not supported in v1") {
		t.Fatalf("err=%v", err)
	}
	if len(sk.applied) != 0 {
		t.Fatalf("unsupported DDL must not reach the sink: %#v", sk.applied)
	}
}

func runDorisDDL(t *testing.T, ddl string, mutate func(*definition.Pipeline)) error {
	t.Helper()
	sk := &dorisRunnerSink{}
	msgs := make(chan source.Message, 1)
	msgs <- source.Message{Event: &event.Event{
		Op: event.OpDDL, Database: "db", Table: "orders",
		SourceDatabase: "db", SourceTable: "orders",
		DDL: ddl,
	}}
	close(msgs)
	pipeline := dorisPipeline()
	if mutate != nil {
		mutate(pipeline)
	}
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	return Run(context.Background(), pipeline, msgs, sk, checkpoint.Store{Path: path}, checkpoint.State{}, nil)
}

func TestDorisPrimaryKeyChangeIsFatal(t *testing.T) {
	// DROP PRIMARY KEY classifies as ActionIndex with Execute=false; silently
	// skipping it would leave the sink keyed differently from the source.
	err := runDorisDDL(t, "ALTER TABLE `db`.`orders` DROP PRIMARY KEY", nil)
	if err == nil || !strings.Contains(err.Error(), "primary key change") {
		t.Fatalf("err=%v", err)
	}
}

func TestDorisSecondaryIndexChangeIsIgnored(t *testing.T) {
	// The Doris target never carries secondary indexes, so their changes are a
	// no-op rather than an error.
	for _, ddl := range []string{
		"ALTER TABLE `db`.`orders` ADD INDEX idx_name (`name`)",
		"ALTER TABLE `db`.`orders` DROP INDEX idx_name",
		"CREATE INDEX idx_name ON `db`.`orders` (`name`)",
	} {
		if err := runDorisDDL(t, ddl, nil); err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
	}
}

func TestDorisRenameTableIsFatal(t *testing.T) {
	// RENAME cannot be followed on the target and must not be skipped just
	// because drop.truncate is disabled.
	err := runDorisDDL(t, "RENAME TABLE `db`.`orders` TO `db`.`orders_old`", nil)
	if err == nil || !strings.Contains(err.Error(), "not supported in v1") {
		t.Fatalf("err=%v", err)
	}
}

func TestDorisDropTableSkippedWhenDropTruncateDisabled(t *testing.T) {
	err := runDorisDDL(t, "DROP TABLE `db`.`orders`", func(pipe *definition.Pipeline) {
		pipe.Sink.DropTruncate = false
	})
	if err != nil {
		t.Fatalf("explicit opt-out must not fail: %v", err)
	}
}
