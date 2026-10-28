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

func postgresPipeline(sinkBody string) string {
	return `
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: app\..*
sink:
` + sinkBody + `pipeline:
  name: demo
`
}

func TestParsePostgresSinkDefaults(t *testing.T) {
	body := postgresPipeline(`  type: postgresql
  hostname: pg.internal
  username: cdc
  password: secret
  database: target
`)
	pipe, err := Parse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	sink := pipe.Sink
	if sink.Port != 5432 {
		t.Fatalf("port %d", sink.Port)
	}
	if sink.Schema != "public" || sink.SSLMode != "prefer" {
		t.Fatalf("schema %q sslmode %q", sink.Schema, sink.SSLMode)
	}
	if sink.Database != "target" {
		t.Fatalf("database %q", sink.Database)
	}
	if sink.ConnectTimeout != 30*time.Second {
		t.Fatalf("connect timeout %s", sink.ConnectTimeout)
	}
	if sink.AutoCreate {
		t.Fatal("postgresql must not default to auto-create")
	}
	if sink.SchemaChange {
		t.Fatal("postgresql must not default to schema changes")
	}
	if sink.DropTruncate {
		t.Fatal("postgresql must not default to drop/truncate")
	}
	if sink.SchemaChangeInclude != nil {
		t.Fatalf("include %v", sink.SchemaChangeInclude)
	}
	if sink.WriteBatchSize != 1000 || sink.MaxRetries != 3 || sink.WriteBatchEvery != time.Second {
		t.Fatalf("shared defaults lost: %#v", sink)
	}
}

// TestParsePostgresSinkSchemaChangeOptions pins the options the PostgreSQL sink
// owns. They stay rejected for every other sink type, so the shared key lists
// must keep their meaning.
func TestParsePostgresSinkSchemaChangeOptions(t *testing.T) {
	body := postgresPipeline(`  type: postgresql
  hostname: pg.internal
  username: cdc
  password: secret
  database: target
  auto-create-table: true
  schema.change.enabled: true
  schema.change.include: add.column,drop.column,rename.column,modify.column,drop.table,truncate
  schema.change.drop-truncate.enabled: true
`)
	pipe, err := Parse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	sink := pipe.Sink
	if !sink.AutoCreate || !sink.SchemaChange || !sink.DropTruncate {
		t.Fatalf("schema change options did not reach the sink: %#v", sink)
	}
	want := []string{"add.column", "drop.column", "rename.column", "modify.column", "drop.table", "truncate"}
	if len(sink.SchemaChangeInclude) != len(want) {
		t.Fatalf("include %v", sink.SchemaChangeInclude)
	}
	for i, kind := range want {
		if sink.SchemaChangeInclude[i] != kind {
			t.Fatalf("include %v, want %v", sink.SchemaChangeInclude, want)
		}
	}
}

// TestParsePostgresChangeKindsMatchSink is in the connector package: the sink
// owns the vocabulary and imports this package, so the equality can only be
// asserted from that side without an import cycle.

func TestParsePostgresSinkKeepsExplicitValues(t *testing.T) {
	body := postgresPipeline(`  type: postgresql
  hostname: pg.internal
  port: 6432
  username: cdc
  password: secret
  database: target
  schema: ods
  sslmode: verify-full
  connect-timeout: 5s
  write-batch-size: 50
  max-retries: 0
  auto-create-table: false
`)
	pipe, err := Parse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	sink := pipe.Sink
	if sink.Port != 6432 || sink.Schema != "ods" || sink.SSLMode != "verify-full" {
		t.Fatalf("%#v", sink)
	}
	if sink.ConnectTimeout != 5*time.Second || sink.WriteBatchSize != 50 || sink.MaxRetries != 0 {
		t.Fatalf("%#v", sink)
	}
	if sink.AutoCreate {
		t.Fatal("auto-create must stay false")
	}
}

// TestParsePostgresPortOrderIndependent guards the shared defaults: parseSink
// writes port 3306 and auto-create-table true before it knows the sink type, and
// YAML map order is undefined, so the type-aware default must win either way.
func TestParsePostgresPortOrderIndependent(t *testing.T) {
	tests := []struct {
		name string
		port string
		want int
	}{
		{name: "type before port, default port", port: "", want: 5432},
		{name: "port before type, default port", port: "", want: 5432},
		{name: "explicit 5432 after type", port: "  port: 5432\n", want: 5432},
		{name: "explicit 5432 before type", port: "  port: 5432\n", want: 5432},
		{name: "explicit 6543 after type", port: "  port: 6543\n", want: 6543},
		{name: "explicit 6543 before type", port: "  port: 6543\n", want: 6543},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Odd entries put port before type, even entries after.
			var body string
			if i%2 == 1 && tc.port != "" {
				body = postgresPipeline(tc.port + "  type: postgresql\n  hostname: pg.internal\n  username: cdc\n  password: secret\n  database: target\n")
			} else {
				body = postgresPipeline("  type: postgresql\n" + tc.port + "  hostname: pg.internal\n  username: cdc\n  password: secret\n  database: target\n")
			}
			pipe, err := Parse([]byte(body))
			if err != nil {
				t.Fatal(err)
			}
			if pipe.Sink.Port != tc.want {
				t.Fatalf("port %d want %d", pipe.Sink.Port, tc.want)
			}
		})
	}
}

func TestParsePostgresSinkRejects(t *testing.T) {
	valid := `  type: postgresql
  hostname: pg.internal
  username: cdc
  password: secret
  database: target
`
	tests := []struct {
		name  string
		body  string
		field string
	}{
		{
			name:  "missing hostname",
			body:  "  type: postgresql\n  username: cdc\n  password: secret\n  database: target\n",
			field: "sink.hostname",
		},
		{
			name:  "missing username",
			body:  "  type: postgresql\n  hostname: h\n  password: secret\n  database: target\n",
			field: "sink.username",
		},
		{
			name:  "missing password",
			body:  "  type: postgresql\n  hostname: h\n  username: cdc\n  database: target\n",
			field: "sink.password",
		},
		{
			name:  "missing database",
			body:  "  type: postgresql\n  hostname: h\n  username: cdc\n  password: secret\n",
			field: "sink.database",
		},
		{
			name:  "database with dot",
			body:  "  type: postgresql\n  hostname: h\n  username: cdc\n  password: secret\n  database: app.prod\n",
			field: "sink.database",
		},
		{
			name:  "empty database",
			body:  "  type: postgresql\n  hostname: h\n  username: cdc\n  password: secret\n  database: ''\n",
			field: "sink.database",
		},
		{
			name:  "doris wait timeout",
			body:  valid + "  schema.change.wait-timeout: 5s\n",
			field: "sink.schema.change.wait-timeout",
		},
		{
			name:  "include outside the sink vocabulary",
			body:  valid + "  schema.change.include: create.table\n",
			field: "sink.schema.change.include",
		},
		{
			name:  "doris sql port",
			body:  valid + "  sql-port: 9030\n",
			field: "sink.sql-port",
		},
		{
			name:  "doris buckets",
			body:  valid + "  buckets: 16\n",
			field: "sink.buckets",
		},
		{
			name:  "kafka brokers",
			body:  valid + "  brokers: [localhost:9092]\n",
			field: "sink.brokers",
		},
		{
			name:  "kafka topic",
			body:  valid + "  topic: cdc\n",
			field: "sink.topic",
		},
		{
			name:  "kafka allow auto create topics",
			body:  valid + "  allow.auto.create.topics: false\n",
			field: "sink.allow.auto.create.topics",
		},
		{
			name:  "kafka producer batch",
			body:  valid + "  producer-batch-max-bytes: 4096\n",
			field: "sink.producer-batch-max-bytes",
		},
		{
			name:  "kafka emit ddl",
			body:  valid + "  schema.change.emit.enabled: true\n",
			field: "sink.schema.change.emit.enabled",
		},
		{
			name:  "kafka flush timeout",
			body:  valid + "  flush-timeout: 10s\n",
			field: "sink.flush-timeout",
		},
		{
			name:  "unknown sslmode",
			body:  valid + "  sslmode: sometimes\n",
			field: "sink.sslmode",
		},
		{
			name:  "schema with dot",
			body:  valid + "  schema: a.b\n",
			field: "sink.schema",
		},
		{
			name:  "empty schema",
			body:  valid + "  schema: ''\n",
			field: "sink.schema",
		},
		{
			name:  "port out of range",
			body:  valid + "  port: 70000\n",
			field: "sink.port",
		},
		{
			name:  "connect timeout too small",
			body:  valid + "  connect-timeout: 1ms\n",
			field: "sink.connect-timeout",
		},
		{
			name:  "request timeout",
			body:  valid + "  request-timeout: 10s\n",
			field: "sink.request-timeout",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(postgresPipeline(tc.body)))
			cfg, ok := err.(*ConfigError)
			if !ok || cfg.Field != tc.field {
				t.Fatalf("got %v, want field %s", err, tc.field)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("error leaks the password: %v", err)
			}
		})
	}
}

// TestParseSinkKeyErrorMessages pins the two scope messages the PostgreSQL sink
// influences: the request-timeout rejection names the sink type, and the
// auto-create-table message names every type that accepts the key.
func TestParseSinkKeyErrorMessages(t *testing.T) {
	t.Run("postgres request-timeout names the sink type", func(t *testing.T) {
		body := postgresPipeline(`  type: postgresql
  hostname: pg.internal
  username: cdc
  password: secret
  database: target
  request-timeout: 10s
`)
		_, err := Parse([]byte(body))
		cfg, ok := err.(*ConfigError)
		if !ok || cfg.Field != "sink.request-timeout" {
			t.Fatalf("got %v, want field sink.request-timeout", err)
		}
		if !strings.Contains(err.Error(), "postgresql") {
			t.Fatalf("message %q does not name sink.type postgresql", err)
		}
	})
	t.Run("auto-create-table names every allowed sink type", func(t *testing.T) {
		// stdout has no table to create, so the key is rejected there; the message
		// must list every sink type that accepts it.
		_, err := Parse([]byte(postgresPipeline("  type: stdout\n  auto-create-table: false\n")))
		cfg, ok := err.(*ConfigError)
		if !ok || cfg.Field != "sink.auto-create-table" {
			t.Fatalf("got %v, want field sink.auto-create-table", err)
		}
		for _, sinkType := range []string{"mysql", "doris", "postgresql"} {
			if !strings.Contains(err.Error(), sinkType) {
				t.Fatalf("message %q does not name sink.type %s", err, sinkType)
			}
		}
	})
}

// TestParsePostgresFieldsRejectedForOtherSinks covers the PostgreSQL-only options
// that the other sinks reject. database is intentionally not among them; see
// TestParseDatabaseAcceptedForLegacySinks.
func TestParsePostgresFieldsRejectedForOtherSinks(t *testing.T) {
	base := `
source:
  hostname: 127.0.0.1
  username: root
  password: secret
  tables: app\..*
sink:
`
	tests := []struct {
		name  string
		body  string
		field string
	}{
		{
			name:  "mysql schema",
			body:  base + "  type: mysql\n  hostname: h\n  username: u\n  password: p\n  schema: ods\n",
			field: "sink.schema",
		},
		{
			name:  "mysql sslmode",
			body:  base + "  type: mysql\n  hostname: h\n  username: u\n  password: p\n  sslmode: require\n",
			field: "sink.sslmode",
		},
		{
			name:  "stdout sslmode",
			body:  base + "  type: stdout\n  sslmode: disable\n",
			field: "sink.sslmode",
		},
		{
			name:  "stdout connect timeout",
			body:  base + "  type: stdout\n  connect-timeout: 5s\n",
			field: "sink.connect-timeout",
		},
		{
			name:  "kafka schema",
			body:  base + "  type: kafka\n  brokers: [localhost:9092]\n  topic: t\n  schema: ods\n",
			field: "sink.schema",
		},
		{
			name:  "kafka connect timeout",
			body:  base + "  type: kafka\n  brokers: [localhost:9092]\n  topic: t\n  connect-timeout: 5s\n",
			field: "sink.connect-timeout",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.body))
			cfg, ok := err.(*ConfigError)
			if !ok || cfg.Field != tc.field {
				t.Fatalf("got %v, want field %s", err, tc.field)
			}
		})
	}
}

// TestParsePostgresFieldScopeBySinkType pins the field-scope table of the options
// this release adds. PostgreSQL owns database/schema/sslmode/connect-timeout;
// doris shares database but must fail on the other three instead of silently
// dropping them, and neither sink may lose its own options.
func TestParsePostgresFieldScopeBySinkType(t *testing.T) {
	const doris = `  type: doris
  hostname: doris.internal
  port: 8030
  username: root
  password: secret
`
	const postgres = `  type: postgresql
  hostname: pg.internal
  username: cdc
  password: secret
`
	tests := []struct {
		name      string
		body      string
		wantField string // non-empty: Parse must fail with this field
		check     func(t *testing.T, sink Sink)
	}{
		{name: "doris schema", body: doris + "  schema: ods\n", wantField: "sink.schema"},
		{name: "doris sslmode", body: doris + "  sslmode: require\n", wantField: "sink.sslmode"},
		{name: "doris connect timeout", body: doris + "  connect-timeout: 5s\n", wantField: "sink.connect-timeout"},
		{
			name: "doris database",
			body: doris + "  database: demo\n",
			check: func(t *testing.T, sink Sink) {
				if sink.Database != "demo" {
					t.Fatalf("database %q", sink.Database)
				}
			},
		},
		{
			name: "doris sql port",
			body: doris + "  sql-port: 9030\n",
			check: func(t *testing.T, sink Sink) {
				if sink.SQLPort != 9030 {
					t.Fatalf("sql-port %d", sink.SQLPort)
				}
			},
		},
		{
			name: "doris buckets",
			body: doris + "  buckets: 16\n",
			check: func(t *testing.T, sink Sink) {
				if sink.Buckets != "16" {
					t.Fatalf("buckets %q", sink.Buckets)
				}
			},
		},
		{
			name: "postgres database",
			body: postgres + "  database: target\n",
			check: func(t *testing.T, sink Sink) {
				if sink.Database != "target" {
					t.Fatalf("database %q", sink.Database)
				}
			},
		},
		{
			name: "postgres schema",
			body: postgres + "  database: target\n  schema: ods\n",
			check: func(t *testing.T, sink Sink) {
				if sink.Schema != "ods" {
					t.Fatalf("schema %q", sink.Schema)
				}
			},
		},
		{
			name: "postgres sslmode",
			body: postgres + "  database: target\n  sslmode: verify-full\n",
			check: func(t *testing.T, sink Sink) {
				if sink.SSLMode != "verify-full" {
					t.Fatalf("sslmode %q", sink.SSLMode)
				}
			},
		},
		{
			name: "postgres connect timeout",
			body: postgres + "  database: target\n  connect-timeout: 5s\n",
			check: func(t *testing.T, sink Sink) {
				if sink.ConnectTimeout != 5*time.Second {
					t.Fatalf("connect timeout %s", sink.ConnectTimeout)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pipe, err := Parse([]byte(postgresPipeline(tc.body)))
			if tc.wantField != "" {
				cfg, ok := err.(*ConfigError)
				if !ok || cfg.Field != tc.wantField {
					t.Fatalf("got %v, want field %s", err, tc.wantField)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.check != nil {
				tc.check(t, pipe.Sink)
			}
		})
	}
}

// TestParseDatabaseAcceptedForLegacySinks pins the master compatibility rule:
// mysql, kafka, and stdout parsed sink.database as a no-op long before the
// PostgreSQL sink existed, so it must stay accepted there. Only doris and
// postgresql act on the value.
func TestParseDatabaseAcceptedForLegacySinks(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "mysql", body: "  type: mysql\n  hostname: h\n  username: u\n  password: p\n  database: target\n"},
		{name: "kafka", body: "  type: kafka\n  brokers: [h:9092]\n  topic: t\n  database: target\n"},
		{name: "stdout", body: "  type: stdout\n  database: target\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pipe, err := Parse([]byte(postgresPipeline(tc.body)))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if pipe.Sink.Database != "target" {
				t.Fatalf("database %q", pipe.Sink.Database)
			}
		})
	}
}
