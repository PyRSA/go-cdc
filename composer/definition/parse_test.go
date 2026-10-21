package definition

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

const sample = `
source:
  type: mysql
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db1\..*,db2\..*
  tables.exclude: db1\.skip
  server-id: 5401
  scan.startup.mode: initial
  jdbc.properties.useSSL: "false"
sink:
  type: stdout
pipeline:
  name: demo
  parallelism: 4
`

func TestParseSample(t *testing.T) {
	pipe, err := Parse([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	if pipe.Source.ChunkSize != 8096 || pipe.Source.FetchSize != 1024 {
		t.Fatalf("chunk defaults %d %d", pipe.Source.ChunkSize, pipe.Source.FetchSize)
	}
	if pipe.Source.StartupMode != StartupInitial {
		t.Fatalf("startup %#v", pipe.Source)
	}
	if pipe.Source.JDBCProperties["useSSL"] != "false" {
		t.Fatalf("jdbc %#v", pipe.Source.JDBCProperties)
	}
	if pipe.Source.NewlyAddedTable || !pipe.Source.NewlyAddedSnapshot || pipe.Source.BinlogNewlyAddedTable {
		t.Fatalf("newly-added defaults %#v", pipe.Source)
	}
	if pipe.Runtime.Parallelism != 4 || pipe.Checkpoint.Storage != "file" {
		t.Fatalf("runtime %#v ckpt %#v", pipe.Runtime, pipe.Checkpoint)
	}
	if !pipe.Source.Tables.Match("db1", "orders") {
		t.Fatal("orders should match")
	}
	if pipe.Source.Tables.Match("db1", "skip") || pipe.Source.Tables.Match("mysql", "user") {
		t.Fatal("excluded or system table matched")
	}
	exact, err := CompileSelector(`db_tp_func_src\.tp01_rows`, "")
	if err != nil {
		t.Fatal(err)
	}
	if !exact.Match("db_tp_func_src", "tp01_rows") || exact.Match("db_tp_func_src", "tp01_rows_out") {
		t.Fatal("table pattern must match the whole database.table name")
	}
}

func TestParseNewlyAddedFlags(t *testing.T) {
	body := `
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
  scan.newly-added-table.enabled: true
  scan.newly-added-table.snapshot.enabled: false
  scan.binlog.newly-added-table.enabled: true
sink:
  type: stdout
pipeline:
  name: demo
`
	pipe, err := Parse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if !pipe.Source.NewlyAddedTable || pipe.Source.NewlyAddedSnapshot || !pipe.Source.BinlogNewlyAddedTable {
		t.Fatalf("%#v", pipe.Source)
	}
}

func TestParseRejects(t *testing.T) {
	base := `
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
sink:
  type: stdout
pipeline:
  name: demo
`
	cases := []struct {
		name  string
		yaml  string
		field string
	}{
		{"never", "scan.startup.mode: never\n", "source.scan.startup.mode"},
		{"schema_only", "scan.startup.mode: schema_only\n", "source.scan.startup.mode"},
		{"debezium", "debezium.snapshot.mode: initial\n", "source.debezium.snapshot.mode"},
		{"timeout", "connect.timeout: 100ms\n", "source.connect.timeout"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := stringsReplaceSource(base, "  "+tc.yaml)
			_, err := Parse([]byte(body))
			cfg, ok := err.(*ConfigError)
			if !ok || cfg.Field != tc.field {
				t.Fatalf("got %v", err)
			}
		})
	}
	_, err := Parse([]byte(`
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
sink:
  type: kafka
pipeline:
  name: demo
`))
	if cfg, ok := err.(*ConfigError); !ok || cfg.Field != "sink.brokers" {
		t.Fatalf("kafka missing brokers: %v", err)
	}
	pipe, err := Parse([]byte(`
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
sink:
  type: kafka
  brokers: 127.0.0.1:9092
  topic: cdc_events
  schema.change.emit.enabled: true
pipeline:
  name: demo
`))
	if err != nil {
		t.Fatalf("kafka parse: %v", err)
	}
	if pipe.Sink.Type != "kafka" || pipe.Sink.Topic != "cdc_events" || !pipe.Sink.EmitDDL || len(pipe.Sink.Brokers) != 1 {
		t.Fatalf("kafka sink=%+v", pipe.Sink)
	}
	if !pipe.Sink.AllowAutoCreateTopics {
		t.Fatalf("allow.auto.create.topics default: got false, want true")
	}
	for _, tc := range []struct {
		name  string
		sink  string
		field string
	}{
		{"missing topic", "type: kafka\n  brokers: 127.0.0.1:9092\n", "sink.topic"},
		{"whitespace topic", "type: kafka\n  brokers: 127.0.0.1:9092\n  topic: \"bad topic\"\n", "sink.topic"},
		{"empty broker entry", "type: kafka\n  brokers: [\"127.0.0.1:9092\", \"\"]\n  topic: cdc\n", "sink.brokers"},
	} {
		t.Run("kafka "+tc.name, func(t *testing.T) {
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
	pipe, err = Parse([]byte(`
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
sink:
  type: kafka
  brokers:
    - 127.0.0.1:9092
    - 127.0.0.1:9093
  topic: cdc_events
pipeline:
  name: demo
`))
	if err != nil {
		t.Fatalf("kafka list brokers: %v", err)
	}
	if len(pipe.Sink.Brokers) != 2 {
		t.Fatalf("brokers=%v", pipe.Sink.Brokers)
	}
	_, err = Parse([]byte(`
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
sink:
  type: stdout
pipeline:
  name: demo
checkpoint:
  storage: mysql
`))
	if cfg, ok := err.(*ConfigError); !ok || cfg.Field != "checkpoint.storage" {
		t.Fatalf("checkpoint: %v", err)
	}
}

func TestParseKafkaAllowAutoCreateTopics(t *testing.T) {
	base := `
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
sink:
  type: kafka
  brokers: 127.0.0.1:9092
  topic: cdc_events
%s
pipeline:
  name: demo
`
	cases := []struct {
		name  string
		extra string
		want  bool
	}{
		{"omitted defaults true", "", true},
		{"explicit true", "  allow.auto.create.topics: true\n", true},
		{"explicit false", "  allow.auto.create.topics: false\n", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pipe, err := Parse([]byte(fmt.Sprintf(base, tc.extra)))
			if err != nil {
				t.Fatal(err)
			}
			if pipe.Sink.AllowAutoCreateTopics != tc.want {
				t.Fatalf("AllowAutoCreateTopics=%v want %v", pipe.Sink.AllowAutoCreateTopics, tc.want)
			}
		})
	}
}

func TestParseKafkaProducerBatchMaxBytes(t *testing.T) {
	base := `
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
sink:
  type: kafka
  brokers: 127.0.0.1:9092
  topic: cdc_events
%s
pipeline:
  name: demo
`
	const wantDefault = 10 * 1024 * 1024
	cases := []struct {
		name     string
		extra    string
		want     int
		errField string
	}{
		{"omitted", "", wantDefault, ""},
		{"explicit", "  producer-batch-max-bytes: 2097152\n", 2097152, ""},
		{"zero", "  producer-batch-max-bytes: 0\n", 0, "sink.producer-batch-max-bytes"},
		{"negative", "  producer-batch-max-bytes: -1\n", 0, "sink.producer-batch-max-bytes"},
		{"below min", "  producer-batch-max-bytes: 511\n", 0, "sink.producer-batch-max-bytes"},
		{"min", "  producer-batch-max-bytes: 512\n", 512, ""},
		{"max", "  producer-batch-max-bytes: 104857600\n", 100 << 20, ""},
		{"above max", "  producer-batch-max-bytes: 104857601\n", 0, "sink.producer-batch-max-bytes"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pipe, err := Parse([]byte(fmt.Sprintf(base, tc.extra)))
			if tc.errField != "" {
				cfg, ok := err.(*ConfigError)
				if !ok || cfg.Field != tc.errField {
					t.Fatalf("got %v", err)
				}
				if !strings.Contains(cfg.Reason, "512") || !strings.Contains(cfg.Reason, "104857600") {
					t.Fatalf("reason %q should name 512..104857600", cfg.Reason)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if pipe.Sink.ProducerBatchMaxBytes != tc.want {
				t.Fatalf("got %d want %d", pipe.Sink.ProducerBatchMaxBytes, tc.want)
			}
		})
	}
}

func TestParseKafkaFlushTimeout(t *testing.T) {
	base := `
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
sink:
  type: kafka
  brokers: 127.0.0.1:9092
  topic: cdc_events
%s
pipeline:
  name: demo
`
	cases := []struct {
		name     string
		extra    string
		want     time.Duration
		errField string
	}{
		{"omitted", "", 30 * time.Second, ""},
		{"explicit", "  flush-timeout: 45s\n", 45 * time.Second, ""},
		{"zero", "  flush-timeout: 0s\n", 0, "sink.flush-timeout"},
		{"negative", "  flush-timeout: -1s\n", 0, "sink.flush-timeout"},
		{"invalid", "  flush-timeout: bogus\n", 0, "sink.flush-timeout"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pipe, err := Parse([]byte(fmt.Sprintf(base, tc.extra)))
			if tc.errField != "" {
				cfg, ok := err.(*ConfigError)
				if !ok || cfg.Field != tc.errField {
					t.Fatalf("got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if pipe.Sink.FlushTimeout != tc.want {
				t.Fatalf("got %s want %s", pipe.Sink.FlushTimeout, tc.want)
			}
		})
	}
}

func TestCheckpointFilePathLayout(t *testing.T) {
	base := `
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
sink:
  type: stdout
pipeline:
  name: demo
`
	wantDefault := checkpointFilePath("./checkpoints", "demo")
	wantCustom := checkpointFilePath("./data/ck", "demo")
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"default dir", "", wantDefault},
		{"custom dir", "checkpoint:\n  file-path: ./data/ck\n", wantCustom},
		{"custom dir trailing slash", "checkpoint:\n  file-path: ./data/ck/\n", wantCustom},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pipe, err := Parse([]byte(base + tc.yaml))
			if err != nil {
				t.Fatal(err)
			}
			if pipe.Checkpoint.FilePath != tc.want {
				t.Fatalf("file-path %q, want %q", pipe.Checkpoint.FilePath, tc.want)
			}
		})
	}
}

func TestCheckpointFilePathUsesMD5OfPipelineName(t *testing.T) {
	a := checkpointFilePath("./checkpoints", "demo")
	b := checkpointFilePath("./checkpoints", "demo")
	c := checkpointFilePath("./checkpoints", "other")
	if a != b {
		t.Fatalf("same name must hash equally: %q vs %q", a, b)
	}
	if a == c {
		t.Fatalf("different names must differ: %q", a)
	}
	if !strings.Contains(a, "/demo/") {
		t.Fatalf("path must include pipeline name dir: %q", a)
	}
	if !strings.HasSuffix(a, ".ckpt") {
		t.Fatalf("path shape %q", a)
	}
	if !strings.Contains(a, "fe01ce2a7fbac8fafaed7c982a04e229") {
		t.Fatalf("expected md5(demo) in %q", a)
	}
}

func TestSpecificOffsetRequiresFileAndPos(t *testing.T) {
	body := `
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
  scan.startup.mode: specific-offset
  scan.startup.specific-offset.file: mysql-bin.000001
sink:
  type: stdout
pipeline:
  name: demo
`
	_, err := Parse([]byte(body))
	cfg, ok := err.(*ConfigError)
	if !ok || cfg.Field != "source.scan.startup.specific-offset.pos" {
		t.Fatalf("got %v", err)
	}
}

func stringsReplaceSource(base, extra string) string {
	return insertBefore(base, "sink:", extra)
}

func insertBefore(doc, marker, extra string) string {
	i := indexOf(doc, marker)
	return doc[:i] + extra + doc[i:]
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
