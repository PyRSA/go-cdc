// Package kafka writes CDC JSON events to Apache Kafka with at-least-once delivery.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/PyRSA/go-cdc/common/cdcjson"
	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/sink"
)

// defaultProducerBatchMaxBytes is the Open fallback when a programmatic
// cfg.ProducerBatchMaxBytes is zero or negative (bytes, before compression).
// YAML omit is filled in parseSink; YAML 0 is rejected there. The library
// default is ~1MiB (Kafka stock max.message.bytes);
// mega-field CDC JSON (e.g. 1–2MiB LONGBLOB → base64 envelope ≈ 2–4MiB) needs more.
// This is a client batch upper bound only — not a guarantee for arbitrary sizes;
// the broker's message.max.bytes / socket.request.max.bytes must still allow the
// record (IT compose sets message.max.bytes to the same 10MiB).
const defaultProducerBatchMaxBytes int32 = 10 << 20 // 10 MiB

// defaultFlushTimeout is the Flush/Close deadline when cfg.FlushTimeout is zero.
// parseSink supplies the same 30s when YAML omits flush-timeout.
const defaultFlushTimeout = time.Second * 30

// producer is the subset of kgo.Client used by the sink (tests inject fakes).
type producer interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	Flush(ctx context.Context) error
	Close()
}

// newKgoClient builds a franz-go client. Tests may replace it to capture opts
// without dialing a broker.
var newKgoClient = kgo.NewClient

// applyProducerBatchMaxBytes appends kgo.ProducerBatchMaxBytes(n). Tests may
// replace it to assert the configured byte cap reaches client opts construction.
var applyProducerBatchMaxBytes = func(opts []kgo.Opt, n int32) []kgo.Opt {
	return append(opts, kgo.ProducerBatchMaxBytes(n))
}

// Sink publishes Debezium-inspired JSON envelopes to Kafka.
type Sink struct {
	brokers               []string
	topic                 string
	sourceName            string
	clientID              string
	emitDDL               bool
	allowAutoCreateTopics bool
	producerBatchMaxBytes int32
	flushTimeout          time.Duration

	mu       sync.Mutex
	client   producer
	closed   bool
	inflight sync.WaitGroup
	prodErr  error
}

// Open validates config and returns an unopened sink. Call (*Sink).Open to dial Kafka.
func Open(cfg definition.Sink, pipelineName string) (*Sink, error) {
	if cfg.Type != "kafka" {
		return nil, fmt.Errorf("kafka: sink.type must be kafka")
	}
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: brokers required")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka: topic required")
	}
	name := cfg.SourceName
	if name == "" {
		name = pipelineName
	}
	if name == "" {
		name = "mysql"
	}
	clientID := cfg.ClientID
	if clientID == "" {
		clientID = "go-cdc"
		if pipelineName != "" {
			clientID = "go-cdc-" + pipelineName
		}
	}
	batch := defaultProducerBatchMaxBytes
	if cfg.ProducerBatchMaxBytes > 0 {
		batch = int32(cfg.ProducerBatchMaxBytes)
	}
	ft := defaultFlushTimeout
	if cfg.FlushTimeout > 0 {
		ft = cfg.FlushTimeout
	}
	return &Sink{
		brokers:               append([]string(nil), cfg.Brokers...),
		topic:                 cfg.Topic,
		sourceName:            name,
		clientID:              clientID,
		emitDDL:               cfg.EmitDDL,
		allowAutoCreateTopics: cfg.AllowAutoCreateTopics,
		producerBatchMaxBytes: batch,
		flushTimeout:          ft,
	}, nil
}

// Open implements sink.Sink: dial the Kafka cluster.
func (s *Sink) Open(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client != nil {
		return nil
	}
	opts := s.clientOpts()
	cl, err := newKgoClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka: open: %w", err)
	}
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return fmt.Errorf("kafka: ping: %w", err)
	}
	s.client = cl
	return nil
}

// clientOpts builds franz-go producer options from sink fields.
// When allowAutoCreateTopics is true, appends kgo.AllowAutoTopicCreation(); the
// broker may still deny create (auto.create.topics.enable / ACLs).
func (s *Sink) clientOpts() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.brokers...),
		kgo.ClientID(s.clientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	}
	if s.allowAutoCreateTopics {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}
	return applyProducerBatchMaxBytes(opts, s.producerBatchMaxBytes)
}

// CreateTable is a no-op; Kafka has no table DDL lifecycle.
func (s *Sink) CreateTable(schema.Table) error { return nil }

// ApplySchema emits a DDL envelope when schema.change.emit.enabled is true.
// It Flushes so broker ack (or produce error) completes before the runner checkpoints.
// Database/Table select the topic (routed sink id); SourceDatabase/SourceTable fill source.*.
func (s *Sink) ApplySchema(change schema.Change) error {
	if !s.emitDDL {
		return nil
	}
	srcDB, srcTable := change.SourceDatabase, change.SourceTable
	if srcDB == "" {
		srcDB, srcTable = change.Database, change.Table
	}
	if err := s.Write([]event.Event{{
		Op: event.OpDDL, Database: change.Database, Table: change.Table,
		SourceDatabase: srcDB, SourceTable: srcTable,
		DDL: change.DDL, SourceTSMs: change.SourceTSMs,
	}}); err != nil {
		return err
	}
	return s.Flush()
}

// Write encodes and produces each event. Delivery errors surface on Flush.
func (s *Sink) Write(batch []event.Event) error {
	if len(batch) == 0 {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("kafka: write on closed sink")
	}
	client := s.client
	s.mu.Unlock()
	if client == nil {
		return fmt.Errorf("kafka: not open")
	}
	for _, item := range batch {
		if item.Op == event.OpDDL && !s.emitDDL {
			continue
		}
		body, err := cdcjson.Marshal(item, cdcjson.Options{SourceName: s.sourceName})
		if err != nil {
			return err
		}
		topic, err := topicFor(item, s.topic)
		if err != nil {
			return err
		}
		rec := &kgo.Record{
			Topic: topic,
			Key:   messageKey(item),
			Value: body,
		}
		s.inflight.Add(1)
		client.Produce(context.Background(), rec, func(_ *kgo.Record, err error) {
			defer s.inflight.Done()
			if err != nil {
				s.mu.Lock()
				if s.prodErr == nil {
					s.prodErr = fmt.Errorf("kafka: produce %s: %w", topic, err)
				}
				s.mu.Unlock()
			}
		})
	}
	return nil
}

// Flush waits for outstanding produces and returns the first produce error.
func (s *Sink) Flush() error {
	s.mu.Lock()
	client := s.client
	s.mu.Unlock()
	if client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.flushTimeout)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		return fmt.Errorf("kafka: flush: %w", err)
	}
	s.inflight.Wait()
	s.mu.Lock()
	err := s.prodErr
	s.prodErr = nil
	s.mu.Unlock()
	return err
}

// Close flushes then closes the client. Safe to call more than once.
func (s *Sink) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	client := s.client
	s.client = nil
	s.mu.Unlock()
	if client == nil {
		return nil
	}
	flushErr := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), s.flushTimeout)
		defer cancel()
		if err := client.Flush(ctx); err != nil {
			return fmt.Errorf("kafka: flush on close: %w", err)
		}
		s.inflight.Wait()
		s.mu.Lock()
		err := s.prodErr
		s.prodErr = nil
		s.mu.Unlock()
		return err
	}()
	client.Close()
	return flushErr
}

// topicFor uses the default topic unless route rewrote the sink table id.
func topicFor(item event.Event, defaultTopic string) (string, error) {
	srcDB, srcTable := item.SourceDatabase, item.SourceTable
	if srcDB == "" {
		srcDB, srcTable = item.Database, item.Table
	}
	srcID := srcDB + "." + srcTable
	sinkID := item.Database + "." + item.Table
	if item.Database != "" && item.Table != "" && sinkID != srcID {
		if strings.ContainsAny(sinkID, " \t\n") {
			return "", fmt.Errorf("kafka: topic from route %q contains whitespace", sinkID)
		}
		return sinkID, nil
	}
	if defaultTopic == "" {
		return "", errors.New("kafka: default topic required")
	}
	return defaultTopic, nil
}

func messageKey(item event.Event) []byte {
	row := item.After
	if item.Op == event.OpDelete {
		row = item.Before
	}
	if len(item.Keys) == 0 || row == nil {
		return nil
	}
	parts := make([]string, 0, len(item.Keys))
	for _, k := range item.Keys {
		v, ok := row[k]
		if !ok || v == nil {
			parts = append(parts, "")
			continue
		}
		parts = append(parts, fmt.Sprint(v))
	}
	return []byte(strings.Join(parts, "|"))
}

// setProducerForTest injects a fake producer (unit tests only).
func (s *Sink) setProducerForTest(p producer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.client = p
}

var _ sink.Sink = (*Sink)(nil)
