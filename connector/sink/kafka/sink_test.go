package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
)

func TestOpenAllowAutoCreateTopicsClientOpts(t *testing.T) {
	orig := newKgoClient
	t.Cleanup(func() { newKgoClient = orig })

	optCount := map[bool]int{}
	for _, allow := range []bool{true, false} {
		allow := allow
		newKgoClient = func(opts ...kgo.Opt) (*kgo.Client, error) {
			optCount[allow] = len(opts)
			return nil, errors.New("skip dial")
		}
		sk, err := Open(definition.Sink{
			Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc",
			AllowAutoCreateTopics: allow,
		}, "demo")
		if err != nil {
			t.Fatal(err)
		}
		if sk.allowAutoCreateTopics != allow {
			t.Fatalf("stored allowAutoCreateTopics=%v want %v", sk.allowAutoCreateTopics, allow)
		}
		if err := sk.Open(context.Background()); err == nil {
			t.Fatal("expected skip-dial error from newKgoClient")
		}
	}
	if optCount[true] != optCount[false]+1 {
		t.Fatalf("client opt count true=%d false=%d; want true == false+1 (AllowAutoTopicCreation)",
			optCount[true], optCount[false])
	}
}

func TestOpenProducerBatchMaxBytes(t *testing.T) {
	origApply := applyProducerBatchMaxBytes
	t.Cleanup(func() { applyProducerBatchMaxBytes = origApply })

	var captured []int32
	applyProducerBatchMaxBytes = func(opts []kgo.Opt, n int32) []kgo.Opt {
		captured = append(captured, n)
		return append(opts, kgo.ProducerBatchMaxBytes(n))
	}

	sk, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc",
		AllowAutoCreateTopics: true,
		ProducerBatchMaxBytes: 2 << 20,
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	if sk.producerBatchMaxBytes != 2<<20 {
		t.Fatalf("stored=%d", sk.producerBatchMaxBytes)
	}
	_ = sk.clientOpts()
	if len(captured) != 1 || captured[0] != 2<<20 {
		t.Fatalf("clientOpts batch max=%v want [%d]", captured, 2<<20)
	}

	// zero cfg → default 10MiB reaches applyProducerBatchMaxBytes
	captured = nil
	sk2, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc",
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	if sk2.producerBatchMaxBytes != int32(10<<20) {
		t.Fatalf("default stored=%d", sk2.producerBatchMaxBytes)
	}
	_ = sk2.clientOpts()
	if len(captured) != 1 || captured[0] != int32(10<<20) {
		t.Fatalf("default clientOpts batch max=%v want [%d]", captured, 10<<20)
	}
}

func TestYAMLProducerBatchMaxBytesToOpen(t *testing.T) {
	origClient := newKgoClient
	origApply := applyProducerBatchMaxBytes
	t.Cleanup(func() {
		newKgoClient = origClient
		applyProducerBatchMaxBytes = origApply
	})

	cases := []struct {
		name  string
		extra string
		want  int32
	}{
		{"omitted", "", int32(10 << 20)},
		{"explicit", "  producer-batch-max-bytes: 2097152\n", 2 << 20},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var applied int32
			var appliedOK bool
			applyProducerBatchMaxBytes = func(opts []kgo.Opt, n int32) []kgo.Opt {
				applied, appliedOK = n, true
				return append(opts, kgo.ProducerBatchMaxBytes(n))
			}
			newKgoClient = func(opts ...kgo.Opt) (*kgo.Client, error) {
				return nil, errors.New("skip dial")
			}
			pipe, err := definition.Parse([]byte(`
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
sink:
  type: kafka
  brokers: 127.0.0.1:9092
  topic: cdc_events
` + tc.extra + `
pipeline:
  name: demo
`))
			if err != nil {
				t.Fatal(err)
			}
			sk, err := Open(pipe.Sink, pipe.Runtime.Name)
			if err != nil {
				t.Fatal(err)
			}
			if sk.producerBatchMaxBytes != tc.want {
				t.Fatalf("sink producerBatchMaxBytes=%d want %d", sk.producerBatchMaxBytes, tc.want)
			}
			_ = sk.Open(context.Background())
			if !appliedOK || applied != tc.want {
				t.Fatalf("Open→clientOpts applied batch max=%v ok=%v want %d", applied, appliedOK, tc.want)
			}
		})
	}
}

func TestYAMLAllowAutoCreateTopicsToOpen(t *testing.T) {
	orig := newKgoClient
	t.Cleanup(func() { newKgoClient = orig })

	cases := []struct {
		name  string
		extra string
		want  bool
	}{
		{"omitted", "", true},
		{"explicit true", "  allow.auto.create.topics: true\n", true},
		{"explicit false", "  allow.auto.create.topics: false\n", false},
	}
	baseCount := -1
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pipe, err := definition.Parse([]byte(`
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
sink:
  type: kafka
  brokers: 127.0.0.1:9092
  topic: cdc_events
` + tc.extra + `
pipeline:
  name: demo
`))
			if err != nil {
				t.Fatal(err)
			}
			if pipe.Sink.AllowAutoCreateTopics != tc.want {
				t.Fatalf("parsed AllowAutoCreateTopics=%v want %v", pipe.Sink.AllowAutoCreateTopics, tc.want)
			}
			var nOpts int
			newKgoClient = func(opts ...kgo.Opt) (*kgo.Client, error) {
				nOpts = len(opts)
				return nil, errors.New("skip dial")
			}
			sk, err := Open(pipe.Sink, pipe.Runtime.Name)
			if err != nil {
				t.Fatal(err)
			}
			if sk.allowAutoCreateTopics != tc.want {
				t.Fatalf("sink allow=%v want %v", sk.allowAutoCreateTopics, tc.want)
			}
			_ = sk.Open(context.Background())
			if tc.want {
				if baseCount < 0 {
					baseCount = nOpts
				}
				if nOpts != baseCount {
					t.Fatalf("true path opt count=%d want %d", nOpts, baseCount)
				}
			} else if baseCount >= 0 && nOpts != baseCount-1 {
				t.Fatalf("false path opt count=%d want %d", nOpts, baseCount-1)
			}
		})
	}
}

func TestFlushUsesConfiguredTimeout(t *testing.T) {
	sk, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc",
		FlushTimeout: 45 * time.Second,
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	fake := &FakeProducer{}
	sk.AttachFake(fake)
	if err := sk.Flush(); err != nil {
		t.Fatal(err)
	}
	assertFlushDeadline(t, fake.LastFlushCtx, 45*time.Second)

	skClose, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc",
		FlushTimeout: 45 * time.Second,
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	fakeClose := &FakeProducer{}
	skClose.AttachFake(fakeClose)
	if err := skClose.Close(); err != nil {
		t.Fatal(err)
	}
	assertFlushDeadline(t, fakeClose.LastFlushCtx, 45*time.Second)

	skDefault, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc",
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	fakeDefault := &FakeProducer{}
	skDefault.AttachFake(fakeDefault)
	if err := skDefault.Flush(); err != nil {
		t.Fatal(err)
	}
	assertFlushDeadline(t, fakeDefault.LastFlushCtx, 30*time.Second)
}

func TestFlushTimeoutCancelsBlockedProducer(t *testing.T) {
	const flushTO = 80 * time.Millisecond
	sk, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc",
		FlushTimeout: flushTO,
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	fake := &FakeProducer{
		BlockFlushUntilCancel: true,
		FlushStarted:          started,
		safetyCap:             5 * time.Second,
	}
	sk.AttachFake(fake)

	t0 := time.Now()
	err = sk.Flush()
	elapsed := time.Since(t0)
	select {
	case <-started:
	default:
		t.Fatal("blocking Flush never started")
	}
	if err == nil {
		t.Fatal("expected flush timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	if elapsed < flushTO/2 || elapsed > 2*time.Second {
		t.Fatalf("elapsed %s outside expected timeout window (~%s)", elapsed, flushTO)
	}

	// Close path also times out a blocked flush.
	sk2, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc",
		FlushTimeout: flushTO,
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	started2 := make(chan struct{})
	fake2 := &FakeProducer{
		BlockFlushUntilCancel: true,
		FlushStarted:          started2,
		safetyCap:             5 * time.Second,
	}
	sk2.AttachFake(fake2)
	err = sk2.Close()
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close flush timeout: %v", err)
	}
	select {
	case <-started2:
	default:
		t.Fatal("Close blocking Flush never started")
	}
}

func assertFlushDeadline(t *testing.T, ctx context.Context, want time.Duration) {
	t.Helper()
	if ctx == nil {
		t.Fatal("flush context is nil")
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("flush context has no deadline")
	}
	got := time.Until(deadline)
	const slop = 3 * time.Second
	if got > want || got < want-slop {
		t.Fatalf("flush deadline remaining %s, want about %s", got, want)
	}
}

func TestTopicForDefaultAndRouted(t *testing.T) {
	topic, err := topicFor(event.Event{
		Database: "demo_db", Table: "users",
		SourceDatabase: "demo_db", SourceTable: "users",
	}, "cdc_default")
	if err != nil || topic != "cdc_default" {
		t.Fatalf("default topic=%q err=%v", topic, err)
	}
	topic, err = topicFor(event.Event{
		Database: "kafka", Table: "users_out",
		SourceDatabase: "demo_db", SourceTable: "users",
	}, "cdc_default")
	if err != nil || topic != "kafka.users_out" {
		t.Fatalf("routed topic=%q err=%v", topic, err)
	}
}

func TestWriteFlushProduceError(t *testing.T) {
	sk, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc",
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	fake := &FakeProducer{Fail: errors.New("broker down")}
	sk.AttachFake(fake)
	if err := sk.Write([]event.Event{{
		Op: event.OpCreate, Database: "db", Table: "t",
		SourceDatabase: "db", SourceTable: "t",
		After: map[string]any{"id": 1}, Keys: []string{"id"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := sk.Flush(); err == nil {
		t.Fatal("expected produce error on flush")
	}
}

func TestDDLSwitchAndRoutedTopic(t *testing.T) {
	sk, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc_default", EmitDDL: false,
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	fake := &FakeProducer{}
	sk.AttachFake(fake)
	if err := sk.ApplySchema(schema.Change{
		Database: "kafka", Table: "users",
		SourceDatabase: "db", SourceTable: "users",
		DDL: "ALTER TABLE users ADD c int",
	}); err != nil {
		t.Fatal(err)
	}
	if len(fake.Records) != 0 {
		t.Fatalf("emitDDL=false produced %d records", len(fake.Records))
	}

	sk.emitDDL = true
	if err := sk.ApplySchema(schema.Change{
		Database: "kafka", Table: "users",
		SourceDatabase: "db", SourceTable: "users",
		DDL: "ALTER TABLE users ADD c int", SourceTSMs: 1_700_000_000_123,
	}); err != nil {
		t.Fatal(err)
	}
	if len(fake.Records) != 1 {
		t.Fatalf("emitDDL=true produced %d", len(fake.Records))
	}
	if !fake.Flushed {
		t.Fatal("ApplySchema must Flush before return")
	}
	if fake.Records[0].Topic != "kafka.users" {
		t.Fatalf("DDL topic=%q want kafka.users", fake.Records[0].Topic)
	}
	var env map[string]any
	if err := json.Unmarshal(fake.Records[0].Value, &env); err != nil {
		t.Fatal(err)
	}
	src, _ := env["source"].(map[string]any)
	if src["ts_ms"] != float64(1_700_000_000_123) {
		t.Fatalf("source.ts_ms=%v", src["ts_ms"])
	}
	if src["db"] != "db" || src["table"] != "users" {
		t.Fatalf("source db/table=%v/%v", src["db"], src["table"])
	}
}

func TestDDLDefaultTopicWhenUnrouted(t *testing.T) {
	sk, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc_default", EmitDDL: true,
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	fake := &FakeProducer{}
	sk.AttachFake(fake)
	if err := sk.ApplySchema(schema.Change{
		Database: "db", Table: "misc",
		SourceDatabase: "db", SourceTable: "misc",
		DDL: "ALTER TABLE misc ADD c int",
	}); err != nil {
		t.Fatal(err)
	}
	if len(fake.Records) != 1 || fake.Records[0].Topic != "cdc_default" {
		t.Fatalf("unrouted DDL topic=%v", fake.Records)
	}
}

func TestApplySchemaProduceError(t *testing.T) {
	sk, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc", EmitDDL: true,
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	fake := &FakeProducer{Fail: errors.New("broker down")}
	sk.AttachFake(fake)
	if err := sk.ApplySchema(schema.Change{Database: "db", Table: "t", DDL: "ALTER TABLE t ADD c int"}); err == nil {
		t.Fatal("expected produce error from ApplySchema Flush")
	}
}

func TestWriteEmptyBatch(t *testing.T) {
	sk, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc",
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	fake := &FakeProducer{}
	sk.AttachFake(fake)
	if err := sk.Write(nil); err != nil {
		t.Fatal(err)
	}
	if err := sk.Write([]event.Event{}); err != nil {
		t.Fatal(err)
	}
	if len(fake.Records) != 0 {
		t.Fatalf("empty write produced %d", len(fake.Records))
	}
}

func TestWriteAfterClose(t *testing.T) {
	sk, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc",
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	fake := &FakeProducer{}
	sk.AttachFake(fake)
	if err := sk.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sk.Write([]event.Event{{
		Op: event.OpCreate, Database: "db", Table: "t",
		SourceDatabase: "db", SourceTable: "t",
		After: map[string]any{"id": 1}, Keys: []string{"id"},
	}}); err == nil {
		t.Fatal("expected write on closed sink to fail")
	}
}

func TestTopicForWhitespace(t *testing.T) {
	_, err := topicFor(event.Event{
		Database: "kafka", Table: "bad topic",
		SourceDatabase: "db", SourceTable: "t",
	}, "cdc_default")
	if err == nil {
		t.Fatal("expected whitespace topic error")
	}
}

func TestMessageKey(t *testing.T) {
	key := messageKey(event.Event{
		Op: event.OpUpdate, Keys: []string{"id", "tenant"},
		After: map[string]any{"id": 7, "tenant": "a"},
	})
	if string(key) != "7|a" {
		t.Fatalf("key=%q", key)
	}
	key = messageKey(event.Event{Op: event.OpDelete, Keys: []string{"id"}, Before: map[string]any{"id": 3}})
	if string(key) != "3" {
		t.Fatalf("delete key=%q", key)
	}
}

func TestCloseIdempotent(t *testing.T) {
	sk, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc",
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	fake := &FakeProducer{}
	sk.AttachFake(fake)
	if err := sk.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sk.Close(); err != nil {
		t.Fatal(err)
	}
	if !fake.Closed {
		t.Fatal("expected close")
	}
}

func TestWriteNotOpen(t *testing.T) {
	sk, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc",
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	if err := sk.Write([]event.Event{{
		Op: event.OpCreate, Database: "db", Table: "t",
		SourceDatabase: "db", SourceTable: "t",
		After: map[string]any{"id": 1}, Keys: []string{"id"},
	}}); err == nil || !strings.Contains(err.Error(), "not open") {
		t.Fatalf("expected not open, got %v", err)
	}
}

func TestClientFlushError(t *testing.T) {
	sk, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc",
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	fake := &FakeProducer{FlushErr: errors.New("flush timeout")}
	sk.AttachFake(fake)
	if err := sk.Flush(); err == nil {
		t.Fatal("expected client flush error")
	}
	if err := sk.Close(); err == nil {
		t.Fatal("expected close to surface flush error")
	}
}

func TestCloseSurfacesProduceError(t *testing.T) {
	sk, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc",
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	fake := &FakeProducer{Fail: errors.New("broker down")}
	sk.AttachFake(fake)
	if err := sk.Write([]event.Event{{
		Op: event.OpCreate, Database: "db", Table: "t",
		SourceDatabase: "db", SourceTable: "t",
		After: map[string]any{"id": 1}, Keys: []string{"id"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := sk.Close(); err == nil {
		t.Fatal("expected close to surface produce error")
	}
}

func TestWriteSkipsDDLWhenEmitDisabled(t *testing.T) {
	sk, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc", EmitDDL: false,
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	fake := &FakeProducer{}
	sk.AttachFake(fake)
	if err := sk.Write([]event.Event{{
		Op: event.OpDDL, Database: "db", Table: "t",
		SourceDatabase: "db", SourceTable: "t",
		DDL: "ALTER TABLE t ADD c int",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := sk.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(fake.Records) != 0 {
		t.Fatalf("emitDDL=false Write produced %d", len(fake.Records))
	}
}

func TestOpenConfigRejects(t *testing.T) {
	if _, err := Open(definition.Sink{Type: "mysql", Brokers: []string{"x"}, Topic: "t"}, ""); err == nil {
		t.Fatal("expected type reject")
	}
	if _, err := Open(definition.Sink{Type: "kafka", Topic: "t"}, ""); err == nil {
		t.Fatal("expected brokers required")
	}
	if _, err := Open(definition.Sink{Type: "kafka", Brokers: []string{"x"}}, ""); err == nil {
		t.Fatal("expected topic required")
	}
}

func TestDMLAndDDLSameRoutedTopic(t *testing.T) {
	sk, err := Open(definition.Sink{
		Type: "kafka", Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc_default", EmitDDL: true,
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	fake := &FakeProducer{}
	sk.AttachFake(fake)
	if err := sk.Write([]event.Event{{
		Op: event.OpCreate, Database: "kafka", Table: "orders",
		SourceDatabase: "db", SourceTable: "orders",
		After: map[string]any{"id": 1}, Keys: []string{"id"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := sk.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := sk.ApplySchema(schema.Change{
		Database: "kafka", Table: "orders",
		SourceDatabase: "db", SourceTable: "orders",
		DDL: "ALTER TABLE orders ADD x int",
	}); err != nil {
		t.Fatal(err)
	}
	if len(fake.Records) != 2 {
		t.Fatalf("records=%d", len(fake.Records))
	}
	if fake.Records[0].Topic != "kafka.orders" || fake.Records[1].Topic != "kafka.orders" {
		t.Fatalf("topics %q %q", fake.Records[0].Topic, fake.Records[1].Topic)
	}
}
