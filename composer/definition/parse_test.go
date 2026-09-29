package definition

import (
	"strings"
	"testing"
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
		{"newly-added", "scan.newly-added-table.enabled: true\n", "source.scan.newly-added-table.enabled"},
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
	if cfg, ok := err.(*ConfigError); !ok || cfg.Field != "sink.type" {
		t.Fatalf("kafka: %v", err)
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
