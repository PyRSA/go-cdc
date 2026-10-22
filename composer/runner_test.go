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
