package definition

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const esBaseYAML = `
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
sink:
  type: elasticsearch
%s
pipeline:
  name: demo
`

const esHosts = `  hosts:
    - http://127.0.0.1:9200`

func parseESSink(t *testing.T, sinkBody string) (*Pipeline, error) {
	t.Helper()
	return Parse([]byte(fmt.Sprintf(esBaseYAML, sinkBody)))
}

func parseES(t *testing.T, extra string) (*Pipeline, error) {
	t.Helper()
	return parseESSink(t, esHosts+"\n"+extra)
}

func TestParseElasticsearchDefaults(t *testing.T) {
	pipe, err := parseES(t, "")
	if err != nil {
		t.Fatal(err)
	}
	sink := pipe.Sink
	if sink.Type != "elasticsearch" {
		t.Fatalf("type=%s", sink.Type)
	}
	if sink.AutoCreate {
		t.Fatal("auto-create must default to false for elasticsearch")
	}
	if sink.RequestTimeout != 30*time.Second {
		t.Fatalf("request-timeout=%s", sink.RequestTimeout)
	}
	if sink.WriteBatchSize != 1000 || sink.MaxRetries != 3 {
		t.Fatalf("batch=%d retries=%d", sink.WriteBatchSize, sink.MaxRetries)
	}
	if sink.BulkMaxBytes != 5<<20 {
		t.Fatalf("bulk-max-bytes=%d, want 5 MiB", sink.BulkMaxBytes)
	}
	if len(sink.Hosts) != 1 || sink.Hosts[0] != "http://127.0.0.1:9200" {
		t.Fatalf("hosts=%v", sink.Hosts)
	}
	if sink.IndexName != "" || sink.MappingFile != "" || len(sink.Mapping) != 0 {
		t.Fatalf("unexpected index/mapping: %+v", sink)
	}
}

func TestParseElasticsearchExplicitOptions(t *testing.T) {
	pipe, err := parseES(t, `  auto-create: true
  index-name: orders_v1
  request-timeout: 5s
  write-batch-size: 200
  max-retries: 1
  bulk-max-bytes: 1048576`)
	if err != nil {
		t.Fatal(err)
	}
	sink := pipe.Sink
	if !sink.AutoCreate {
		t.Fatal("auto-create should be true")
	}
	if sink.IndexName != "orders_v1" || sink.RequestTimeout != 5*time.Second {
		t.Fatalf("sink=%+v", sink)
	}
	if sink.WriteBatchSize != 200 || sink.MaxRetries != 1 {
		t.Fatalf("batch=%d retries=%d", sink.WriteBatchSize, sink.MaxRetries)
	}
	if sink.BulkMaxBytes != 1<<20 {
		t.Fatalf("bulk-max-bytes=%d, want 1 MiB", sink.BulkMaxBytes)
	}
}

func TestParseElasticsearchBulkMaxBytesZeroUsesDefault(t *testing.T) {
	// Zero means "not configured" in a programmatic config, so the YAML parser
	// must read it the same way instead of rejecting it.
	pipe, err := parseES(t, "  bulk-max-bytes: 0")
	if err != nil {
		t.Fatal(err)
	}
	if got := pipe.Sink.BulkMaxBytes; got != DefaultBulkMaxBytes {
		t.Fatalf("bulk-max-bytes=%d, want the default %d", got, DefaultBulkMaxBytes)
	}
}

func TestParseElasticsearchBulkMaxBytesRange(t *testing.T) {
	// The parser and the sink share ValidateBulkMaxBytes, so both bounds and the
	// message must agree.
	for _, n := range []int{MinBulkMaxBytes, MaxBulkMaxBytes} {
		if err := ValidateBulkMaxBytes(n); err != nil {
			t.Fatalf("%d: %v", n, err)
		}
	}
	for _, n := range []int{-1, MinBulkMaxBytes - 1, MaxBulkMaxBytes + 1} {
		err := ValidateBulkMaxBytes(n)
		if err == nil {
			t.Fatalf("%d: want an error", n)
		}
		if !strings.Contains(err.Error(), "0 (use the default") {
			t.Fatalf("%d: message %q must name the zero default", n, err)
		}
	}
}

func TestParseElasticsearchMappingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "orders.json")
	body := `{"settings":{"number_of_shards":1},"mappings":{"properties":{"id":{"type":"keyword"}}}}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	pipe, err := parseES(t, "  mapping-file: "+path)
	if err != nil {
		t.Fatal(err)
	}
	if pipe.Sink.MappingFile != path {
		t.Fatalf("mapping-file=%s", pipe.Sink.MappingFile)
	}
	if string(pipe.Sink.Mapping) != body {
		t.Fatalf("mapping=%s", pipe.Sink.Mapping)
	}
}

func TestParseElasticsearchRejects(t *testing.T) {
	dir := t.TempDir()
	badJSON := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(badJSON, []byte(`{"mappings":`), 0o600); err != nil {
		t.Fatal(err)
	}
	arrayJSON := filepath.Join(dir, "array.json")
	if err := os.WriteFile(arrayJSON, []byte(`[1,2]`), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		sink  string
		field string
	}{
		{"auto-create-table", esHosts + "\n  auto-create-table: true", "sink.auto-create-table"},
		{"bad host scheme", "  hosts: [ftp://127.0.0.1:9200]", "sink.hosts"},
		{"host without scheme", "  hosts: [127.0.0.1:9200]", "sink.hosts"},
		{"empty host", "  hosts: [\"\"]", "sink.hosts"},
		{"username without password", esHosts + "\n  username: elastic", "sink.username"},
		{"uppercase index", esHosts + "\n  index-name: Orders", "sink.index-name"},
		{"leading underscore index", esHosts + "\n  index-name: _orders", "sink.index-name"},
		{"space in index", esHosts + "\n  index-name: \"bad name\"", "sink.index-name"},
		{"colon in index", esHosts + "\n  index-name: \"a:b\"", "sink.index-name"},
		{"invalid request timeout", esHosts + "\n  request-timeout: 0s", "sink.request-timeout"},
		{"negative bulk-max-bytes", esHosts + "\n  bulk-max-bytes: -1", "sink.bulk-max-bytes"},
		{"bulk-max-bytes below 1 KiB", esHosts + "\n  bulk-max-bytes: 1023", "sink.bulk-max-bytes"},
		{"bulk-max-bytes above 100 MiB", esHosts + "\n  bulk-max-bytes: 104857601", "sink.bulk-max-bytes"},
		{"bulk-max-bytes not an integer", esHosts + "\n  bulk-max-bytes: 5MiB", "sink.bulk-max-bytes"},
		{"missing mapping file", esHosts + "\n  mapping-file: " + filepath.Join(dir, "absent.json"), "sink.mapping-file"},
		{"invalid mapping json", esHosts + "\n  mapping-file: " + badJSON, "sink.mapping-file"},
		{"mapping not an object", esHosts + "\n  mapping-file: " + arrayJSON, "sink.mapping-file"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseESSink(t, tc.sink)
			cfg, ok := err.(*ConfigError)
			if !ok || cfg.Field != tc.field {
				t.Fatalf("got %v want field %s", err, tc.field)
			}
		})
	}
}

func TestParseElasticsearchRequiresHosts(t *testing.T) {
	_, err := parseESSink(t, "")
	if cfg, ok := err.(*ConfigError); !ok || cfg.Field != "sink.hosts" {
		t.Fatalf("got %v", err)
	}
}

func TestParseElasticsearchKeysAreSinkScoped(t *testing.T) {
	cases := []struct {
		name  string
		sink  string
		field string
	}{
		{"hosts on stdout", "  type: stdout\n  hosts: [http://127.0.0.1:9200]", "sink.hosts"},
		{"auto-create on stdout", "  type: stdout\n  auto-create: true", "sink.auto-create"},
		{"bulk-max-bytes on stdout", "  type: stdout\n  bulk-max-bytes: 1048576", "sink.bulk-max-bytes"},
		{"auto-create-table on kafka", "  type: kafka\n  brokers: 127.0.0.1:9092\n  topic: cdc\n  auto-create-table: true", "sink.auto-create-table"},
		{"topic on elasticsearch", "  type: elasticsearch\n  hosts: [http://127.0.0.1:9200]\n  topic: cdc", "sink.topic"},
		// Doris-only keys other than request-timeout stay Doris-only.
		{"buckets on elasticsearch", "  type: elasticsearch\n  hosts: [http://127.0.0.1:9200]\n  buckets: 8", "sink.buckets"},
		{"sql-port on elasticsearch", "  type: elasticsearch\n  hosts: [http://127.0.0.1:9200]\n  sql-port: 9030", "sink.sql-port"},
		{"delete.enabled on elasticsearch", "  type: elasticsearch\n  hosts: [http://127.0.0.1:9200]\n  delete.enabled: true", "sink.delete.enabled"},
		// request-timeout is shared by Doris and Elasticsearch only.
		{"request-timeout on stdout", "  type: stdout\n  request-timeout: 10s", "sink.request-timeout"},
		// flush-timeout only bounds the Kafka producer Flush/Close.
		{"flush-timeout on elasticsearch", "  type: elasticsearch\n  hosts: [http://127.0.0.1:9200]\n  flush-timeout: 5s", "sink.flush-timeout"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(`
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
sink:
` + tc.sink + `
pipeline:
  name: demo
`))
			cfg, ok := err.(*ConfigError)
			if !ok || cfg.Field != tc.field {
				t.Fatalf("got %v want field %s", err, tc.field)
			}
		})
	}
}
