//go:build kafka_integration

package kafka

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/composer/definition"
)

// Run with a real broker (topic auto-create enabled), e.g.:
//
//	KAFKA_BROKERS=127.0.0.1:9092 go test -tags kafka_integration ./connector/sink/kafka/ -count=1 -v
func TestIntegrationProduceAndConsume(t *testing.T) {
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		t.Skip("KAFKA_BROKERS not set")
	}
	brokers := strings.Split(brokersEnv, ",")
	topic := "go-cdc-it-" + time.Now().Format("150405")

	sk, err := Open(definition.Sink{
		Type: "kafka", Brokers: brokers, Topic: topic, SourceName: "it",
	}, "it")
	if err != nil {
		t.Fatal(err)
	}
	if err := sk.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer sk.Close()

	if err := sk.Write([]event.Event{{
		Op: event.OpCreate, Database: "db", Table: "t",
		SourceDatabase: "db", SourceTable: "t",
		After: map[string]any{"id": float64(1), "name": "a"}, Keys: []string{"id"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := sk.Flush(); err != nil {
		t.Fatal(err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("go-cdc-it-"+topic),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		fetches := consumer.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("timeout waiting for kafka record: %v", err)
		}
		if err := fetches.Err(); err != nil && ctx.Err() == nil {
			t.Fatal(err)
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			var env map[string]any
			if err := json.Unmarshal(rec.Value, &env); err != nil {
				t.Fatal(err)
			}
			if env["op"] != "c" {
				t.Fatalf("op=%v", env["op"])
			}
			if string(rec.Key) != "[1]" {
				t.Fatalf("key=%q", rec.Key)
			}
			return
		}
	}
}
