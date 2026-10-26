package definition

import (
	"fmt"
	"os"
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

const dorisSource = `
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
sink:
  type: doris
`

func TestParseDorisDefaults(t *testing.T) {
	pipe, err := Parse([]byte(dorisSource + `
  hostname: fe.internal
  username: root
  password: secret
pipeline:
  name: demo
`))
	if err != nil {
		t.Fatal(err)
	}
	sink := pipe.Sink
	if sink.Port != 8030 || sink.SQLPort != 9030 {
		t.Fatalf("ports http=%d sql=%d", sink.Port, sink.SQLPort)
	}
	if sink.RequestTimeout != 30*time.Second || sink.Buckets != "auto" {
		t.Fatalf("timeout=%s buckets=%s", sink.RequestTimeout, sink.Buckets)
	}
	if sink.SchemaChange || sink.PartialUpdate || sink.DeleteEnabled || sink.DropTruncate {
		t.Fatalf("v1 switches must default off: %+v", sink)
	}
	if len(sink.SchemaChangeInclude) != 1 || sink.SchemaChangeInclude[0] != "add.column" {
		t.Fatalf("include %#v", sink.SchemaChangeInclude)
	}
	if sink.SchemaChangeWait != 5*time.Second {
		t.Fatalf("schema.change.wait-timeout default %s", sink.SchemaChangeWait)
	}
	if sink.ReplicationNum != 0 {
		t.Fatalf("replication-num default %d", sink.ReplicationNum)
	}
	if sink.Database != "" {
		t.Fatalf("database default %q", sink.Database)
	}
}

func TestParseDorisExplicitOptions(t *testing.T) {
	pipe, err := Parse([]byte(dorisSource + `
  hostname: fe.internal
  port: 8031
  sql-port: 9031
  username: cdc
  password: secret
  database: analytics
  request-timeout: 45s
  auto-create-table: false
  schema.change.enabled: true
  schema.change.include: add.column
  schema.change.wait-timeout: 20s
  buckets: 16
  replication-num: 3
  max-retries: 5
pipeline:
  name: demo
`))
	if err != nil {
		t.Fatal(err)
	}
	sink := pipe.Sink
	if sink.Hostname != "fe.internal" || sink.Port != 8031 || sink.SQLPort != 9031 {
		t.Fatalf("endpoint %+v", sink)
	}
	if sink.Username != "cdc" || sink.Database != "analytics" || sink.RequestTimeout != 45*time.Second {
		t.Fatalf("auth/database/timeout %+v", sink)
	}
	if sink.AutoCreate || !sink.SchemaChange || sink.Buckets != "16" || sink.ReplicationNum != 3 {
		t.Fatalf("switches %+v", sink)
	}
	if sink.SchemaChangeWait != 20*time.Second {
		t.Fatalf("schema.change.wait-timeout %s", sink.SchemaChangeWait)
	}
}

func TestParseDorisRejectsUnsupportedV1Options(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		field string
	}{
		{"missing hostname", "  username: root\n  password: secret\n", "sink.hostname"},
		{"hostname scheme", "  hostname: http://fe:8030\n  username: root\n  password: secret\n", "sink.hostname"},
		{"hostname path", "  hostname: fe/api\n  username: root\n  password: secret\n", "sink.hostname"},
		{"http port zero", "  hostname: fe\n  port: 0\n  username: root\n  password: secret\n", "sink.port"},
		{"http port too large", "  hostname: fe\n  port: 70000\n  username: root\n  password: secret\n", "sink.port"},
		{"sql port zero", "  hostname: fe\n  sql-port: 0\n  username: root\n  password: secret\n", "sink.sql-port"},
		{"missing username", "  hostname: fe\n  password: secret\n", "sink.username"},
		{"partial update", "  hostname: fe\n  username: root\n  password: secret\n  partial-update.enabled: true\n", "sink.partial-update.enabled"},
		{"delete", "  hostname: fe\n  username: root\n  password: secret\n  delete.enabled: true\n", "sink.delete.enabled"},
		{"drop truncate", "  hostname: fe\n  username: root\n  password: secret\n  schema.change.drop-truncate.enabled: true\n", "sink.schema.change.drop-truncate.enabled"},
		{"unsupported include", "  hostname: fe\n  username: root\n  password: secret\n  schema.change.include: add.column,drop.column\n", "sink.schema.change.include"},
		{"wait timeout zero", "  hostname: fe\n  username: root\n  password: secret\n  schema.change.wait-timeout: 0s\n", "sink.schema.change.wait-timeout"},
		{"wait timeout text", "  hostname: fe\n  username: root\n  password: secret\n  schema.change.wait-timeout: soon\n", "sink.schema.change.wait-timeout"},
		{"buckets text", "  hostname: fe\n  username: root\n  password: secret\n  buckets: many\n", "sink.buckets"},
		{"buckets zero", "  hostname: fe\n  username: root\n  password: secret\n  buckets: 0\n", "sink.buckets"},
		{"negative replication", "  hostname: fe\n  username: root\n  password: secret\n  replication-num: -1\n", "sink.replication-num"},
		{"unknown field", "  hostname: fe\n  username: root\n  password: secret\n  stream-load.enabled: true\n", "sink.stream-load.enabled"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(dorisSource + tc.body + "pipeline:\n  name: demo\n"))
			cfg, ok := err.(*ConfigError)
			if !ok || cfg.Field != tc.field {
				t.Fatalf("got %v, want field %s", err, tc.field)
			}
		})
	}
}

func TestParseDemoDorisConfig(t *testing.T) {
	body, err := os.ReadFile("../../configs/demo-doris.yaml")
	if err != nil {
		t.Fatal(err)
	}
	pipe, err := Parse(body)
	if err != nil {
		t.Fatal(err)
	}
	sink := pipe.Sink
	if sink.Type != "doris" || sink.Port != 8030 || sink.SQLPort != 9030 {
		t.Fatalf("demo endpoints %+v", sink)
	}
	if !sink.AutoCreate || sink.Buckets != "auto" || sink.ReplicationNum != 1 {
		t.Fatalf("demo options %+v", sink)
	}
	if sink.SchemaChange || sink.PartialUpdate || sink.DeleteEnabled || sink.DropTruncate {
		t.Fatalf("demo must keep v1 switches off: %+v", sink)
	}
}

func TestParseMySQLSinkKeepsMySQLDefaults(t *testing.T) {
	pipe, err := Parse([]byte(`
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
sink:
  type: mysql
  hostname: 127.0.0.1
  username: root
  password: secret
pipeline:
  name: demo
`))
	if err != nil {
		t.Fatal(err)
	}
	if pipe.Sink.Port != 3306 || pipe.Sink.SQLPort != 0 || pipe.Sink.RequestTimeout != 0 || pipe.Sink.Buckets != "" {
		t.Fatalf("Doris defaults leaked into mysql: %+v", pipe.Sink)
	}
	if pipe.Sink.SchemaChangeWait != 0 {
		t.Fatalf("schema.change.wait-timeout leaked into mysql: %s", pipe.Sink.SchemaChangeWait)
	}
}

func TestParseMySQLSinkAcceptsAutoCreateTable(t *testing.T) {
	// auto-create-table is a MySQL option the Doris sink reused, so the shared
	// key must stay accepted for both types.
	pipe, err := Parse([]byte(`
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
sink:
  type: mysql
  hostname: 127.0.0.1
  username: root
  password: secret
  auto-create-table: false
pipeline:
  name: demo
`))
	if err != nil {
		t.Fatal(err)
	}
	if pipe.Sink.AutoCreate {
		t.Fatal("sink.auto-create-table: false was ignored for mysql")
	}
}

func TestParseNonDorisRejectsDorisOnlyKeys(t *testing.T) {
	// Doris-only options must fail for other sink types instead of being
	// silently ignored (see AGENTS.md: accepted keys need a real meaning).
	const base = `
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: db\..*
pipeline:
  name: demo
`
	cases := []struct {
		name  string
		sink  string
		field string
	}{
		{"mysql delete", "  type: mysql\n  hostname: h\n  username: u\n  password: p\n  delete.enabled: true\n", "sink.delete.enabled"},
		{"mysql partial", "  type: mysql\n  hostname: h\n  username: u\n  password: p\n  partial-update.enabled: true\n", "sink.partial-update.enabled"},
		{"mysql buckets", "  type: mysql\n  hostname: h\n  username: u\n  password: p\n  buckets: 8\n", "sink.buckets"},
		{"mysql sql-port", "  type: mysql\n  hostname: h\n  username: u\n  password: p\n  sql-port: 9030\n", "sink.sql-port"},
		{"mysql request-timeout", "  type: mysql\n  hostname: h\n  username: u\n  password: p\n  request-timeout: 10s\n", "sink.request-timeout"},
		{"mysql schema.change.enabled", "  type: mysql\n  hostname: h\n  username: u\n  password: p\n  schema.change.enabled: true\n", "sink.schema.change.enabled"},
		{"mysql schema.change.include", "  type: mysql\n  hostname: h\n  username: u\n  password: p\n  schema.change.include: [add.column]\n", "sink.schema.change.include"},
		{"mysql replication-num", "  type: mysql\n  hostname: h\n  username: u\n  password: p\n  replication-num: 3\n", "sink.replication-num"},
		{"mysql schema.change.wait-timeout", "  type: mysql\n  hostname: h\n  username: u\n  password: p\n  schema.change.wait-timeout: 5s\n", "sink.schema.change.wait-timeout"},
		{"kafka delete", "  type: kafka\n  brokers: [localhost:9092]\n  topic: t\n  delete.enabled: false\n", "sink.delete.enabled"},
		{"stdout buckets", "  type: stdout\n  buckets: auto\n", "sink.buckets"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(base + "sink:\n" + tc.sink))
			cfg, ok := err.(*ConfigError)
			if !ok || cfg.Field != tc.field {
				t.Fatalf("got %v, want field %s", err, tc.field)
			}
		})
	}
}
