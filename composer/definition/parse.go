package definition

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultPort                  = 3306
	defaultChunkSize             = 8096
	defaultFetchSize             = 1024
	defaultParallel              = 1
	defaultBatchSize             = 1000
	defaultMaxRetries            = 3
	minConnectTimeout            = 250 * time.Millisecond
	defaultProducerBatchMaxBytes = 10 << 20 // 10 MiB
	// franz-go v1.21.7 accepts ProducerBatchMaxBytes in [512, maxBrokerWriteBytes].
	// The default maxBrokerWriteBytes is 100<<20; BrokerMaxWriteBytes is not raised.
	minProducerBatchMaxBytes = 512
	maxProducerBatchMaxBytes = 100 << 20 // 104857600
	defaultFlushTimeout      = 30 * time.Second
	defaultRequestTimeout    = 30 * time.Second
	defaultDorisHTTPPort     = 8030
	defaultDorisSQLPort      = 9030
	defaultDorisTimeout      = 30 * time.Second
	defaultDorisBuckets      = "auto"
	// DefaultDorisSchemaChangeWait is the ADD COLUMN visibility budget. Doris
	// applies ALTER TABLE asynchronously, so the sink re-reads DESC until the
	// new column appears or the budget runs out. Exported because the sink also
	// defaults a directly constructed definition.Sink.
	DefaultDorisSchemaChangeWait = 5 * time.Second
	// DefaultBulkMaxBytes caps one Elasticsearch bulk request body. Elasticsearch
	// rejects a body above its http.max_content_length (100mb by default) with 413.
	DefaultBulkMaxBytes = 5 << 20 // 5 MiB of NDJSON
	// Elasticsearch major versions the sink can write to. One value per official
	// Go client: ElasticsearchVersion7 selects go-elasticsearch/v7,
	// ElasticsearchVersion8 selects go-elasticsearch/v8, and
	// ElasticsearchVersion9 selects go-elasticsearch/v9.
	ElasticsearchVersion7 = 7
	ElasticsearchVersion8 = 8
	ElasticsearchVersion9 = 9
	// A bulk body above Elasticsearch's default limit cannot be delivered, so a
	// larger budget would only move the failure to the cluster. The sink validates
	// programmatic configs against the same range.
	MinBulkMaxBytes = 1024
	MaxBulkMaxBytes = 100 << 20
	// PostgreSQL sink defaults; applied only when the sink type is postgresql
	// because parseSink sets the shared defaults before the type is known.
	defaultPostgresPort           = 5432
	defaultPostgresSchema         = "public"
	defaultPostgresSSLMode        = "prefer"
	defaultPostgresConnectTimeout = 30 * time.Second
)

// ResolveElasticsearchVersion maps sink.version to the client major version.
// Zero means "not configured" and keeps the historical v8 client, so existing
// pipelines are unchanged. Any other unsupported value is an error: the sink
// never falls back to a different client than the configured one, and it never
// asks the cluster which version to use.
func ResolveElasticsearchVersion(configured int) (int, error) {
	switch configured {
	case 0:
		return ElasticsearchVersion8, nil
	case ElasticsearchVersion7, ElasticsearchVersion8, ElasticsearchVersion9:
		return configured, nil
	}
	return 0, fmt.Errorf("must be %d, %d or %d",
		ElasticsearchVersion7, ElasticsearchVersion8, ElasticsearchVersion9)
}

// ValidateBulkMaxBytes checks a sink.bulk-max-bytes value. Zero means "not
// configured" and selects DefaultBulkMaxBytes; every other value must stay in
// the range Elasticsearch can accept. The parser and the sink share this check
// so a YAML config and a programmatic config are rejected for the same reasons
// with the same message.
func ValidateBulkMaxBytes(n int) error {
	if n == 0 {
		return nil
	}
	if n < MinBulkMaxBytes || n > MaxBulkMaxBytes {
		return fmt.Errorf("must be 0 (use the default %d) or an integer in %d..%d",
			DefaultBulkMaxBytes, MinBulkMaxBytes, MaxBulkMaxBytes)
	}
	return nil
}

// Sink keys that only make sense for one sink type. Rejecting them keeps a
// mistyped key from being silently ignored.
var (
	esOnlySinkKeys = map[string]struct{}{
		"hosts": {}, "auto-create": {}, "index-name": {}, "mapping-file": {},
		// version selects the Elasticsearch client major version.
		"version": {},
		// bulk-max-bytes bounds the ES bulk body only.
		"bulk-max-bytes": {},
		// request-timeout is deliberately absent: it is shared with Doris and
		// rejected per sink type through requestTimeoutSinkKeys.
	}
	kafkaOnlySinkKeys = map[string]struct{}{
		"brokers": {}, "topic": {}, "source-name": {}, "client-id": {},
		"schema.change.emit.enabled": {}, "allow.auto.create.topics": {},
		"producer-batch-max-bytes": {}, "flush-timeout": {},
	}
	mysqlOnlySinkKeys = map[string]struct{}{
		"auto-create-table": {},
	}
)

// postgresSSLModes lists the sslmode values libpq (and pgx) accepts.
var postgresSSLModes = map[string]struct{}{
	"disable":     {},
	"allow":       {},
	"prefer":      {},
	"require":     {},
	"verify-ca":   {},
	"verify-full": {},
}

// mysqlOnlySinkFields are rejected for postgresql: this release does not
// create tables or apply table-level DDL.
var mysqlOnlySinkFields = []string{
	"schema.change.drop-truncate.enabled",
}

// kafkaOnlySinkFields are rejected for postgresql.
var kafkaOnlySinkFields = []string{
	"brokers",
	"topic",
	"source-name",
	"client-id",
	"schema.change.emit.enabled",
	"allow.auto.create.topics",
	"producer-batch-max-bytes",
	"flush-timeout",
}

// postgresNamespaceFields are rejected for stdout, mysql, kafka, doris, and
// elasticsearch: none of those sinks has a schema, TLS mode, or connect timeout,
// so accepting them would silently drop the setting.
// database is deliberately absent. Only doris and postgresql read it, but mysql,
// kafka, and stdout parsed it as a no-op long before postgresql existed, so
// rejecting it there would break working configurations.
var postgresNamespaceFields = []string{
	"schema",
	"sslmode",
	"connect-timeout",
}

var startupModes = map[string]struct{}{
	StartupInitial:        {},
	StartupEarliest:       {},
	StartupLatest:         {},
	StartupSpecificOffset: {},
	StartupTimestamp:      {},
	StartupSnapshot:       {},
}

var rejectedExact = map[string]struct{}{
	"snapshot-mode": {},
	"scan.parse.online.schema.changes.enabled": {},
	"metadata.list":                     {},
	"treat-tinyint1-as-boolean.enabled": {},
	"use.legacy.json.format":            {},
}

// dorisOnlySinkKeys are parsed for every sink type but only meaningful for
// Doris. mysql / kafka / stdout / elasticsearch reject them so a typo cannot
// silently no-op. request-timeout is not listed here: Doris and Elasticsearch
// both honour it (see requestTimeoutSinkKeys).
var dorisOnlySinkKeys = []string{
	"sql-port",
	"buckets",
	"replication-num",
	"schema.change.enabled",
	"schema.change.include",
	"schema.change.wait-timeout",
	"partial-update.enabled",
	"delete.enabled",
}

// requestTimeoutSinkKeys is the only Doris option Elasticsearch shares. It is
// rejected for the sinks that have no request to bound: stdout, mysql, kafka,
// and postgresql.
var requestTimeoutSinkKeys = []string{"request-timeout"}

func rejectKeysForSinkType(sinkType string, seen map[string]struct{}, keyLists ...[]string) error {
	for _, keys := range keyLists {
		for _, key := range keys {
			if _, ok := seen[key]; ok {
				return configf("sink."+key, "not supported for sink.type %s", sinkType)
			}
		}
	}
	return nil
}

// Parse decodes a pipeline YAML document and applies defaults.
// Unknown fields and rejected Flink parameters fail startup and name the field.
func Parse(data []byte) (*Pipeline, error) {
	var root map[string]any
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, configf("yaml", "%v", err)
	}
	if len(root) == 0 {
		return nil, configErr("yaml", "empty document")
	}
	for key := range root {
		switch key {
		case "source", "sink", "route", "transform", "pipeline", "checkpoint":
		default:
			return nil, configErr(key, "unknown field")
		}
	}
	source, err := parseSource(asMap(root["source"]))
	if err != nil {
		return nil, err
	}
	sink, err := parseSink(asMap(root["sink"]))
	if err != nil {
		return nil, err
	}
	routes, err := parseRoutes(root["route"])
	if err != nil {
		return nil, err
	}
	transforms, err := parseTransforms(root["transform"])
	if err != nil {
		return nil, err
	}
	runtime, err := parseRuntime(asMap(root["pipeline"]))
	if err != nil {
		return nil, err
	}
	ckpt, err := parseCheckpoint(root["checkpoint"])
	if err != nil {
		return nil, err
	}
	base := ckpt.FilePath
	if base == "" {
		base = defaultCheckpointDir
	}
	ckpt.FilePath = checkpointFilePath(base, runtime.Name)
	return &Pipeline{
		Source: source, Sink: sink, Routes: routes, Transforms: transforms,
		Runtime: runtime, Checkpoint: ckpt,
	}, nil
}

func parseSource(raw map[string]any) (Source, error) {
	if raw == nil {
		return Source{}, configErr("source", "required")
	}
	out := Source{
		Port:               defaultPort,
		TimeZone:           "UTC",
		StartupMode:        StartupInitial,
		ChunkSize:          defaultChunkSize,
		FetchSize:          defaultFetchSize,
		SchemaChange:       true,
		NewlyAddedSnapshot: true,
		ConnectTimeout:     30 * time.Second,
		HeartbeatInterval:  30 * time.Second,
		JDBCProperties:     map[string]string{},
	}
	var tables, exclude string
	for key, value := range raw {
		if _, rejected := rejectedExact[key]; rejected {
			return Source{}, configErr("source."+key, "not supported")
		}
		if strings.HasPrefix(key, "debezium.") {
			return Source{}, configErr("source."+key, "not supported")
		}
		if strings.HasPrefix(key, "jdbc.properties.") {
			name := strings.TrimPrefix(key, "jdbc.properties.")
			if name == "" {
				return Source{}, configErr("source."+key, "missing property name")
			}
			out.JDBCProperties[name] = fmt.Sprint(value)
			continue
		}
		switch key {
		case "type":
			if fmt.Sprint(value) != "mysql" {
				return Source{}, configf("source.type", "only mysql is supported, got %v", value)
			}
		case "hostname":
			out.Hostname = fmt.Sprint(value)
		case "port":
			n, err := asInt(value)
			if err != nil {
				return Source{}, configf("source.port", "%v", err)
			}
			out.Port = n
		case "username":
			out.Username = fmt.Sprint(value)
		case "password":
			out.Password = fmt.Sprint(value)
		case "tables":
			tables = fmt.Sprint(value)
		case "tables.exclude":
			exclude = fmt.Sprint(value)
		case "server-id":
			n, err := asInt(value)
			if err != nil || n <= 0 {
				return Source{}, configErr("source.server-id", "must be a positive integer")
			}
			out.ServerID = uint32(n)
		case "server-time-zone":
			out.TimeZone = fmt.Sprint(value)
		case "scan.startup.mode":
			mode := fmt.Sprint(value)
			if mode == "never" || mode == "schema_only" || mode == "snapshot-mode" {
				return Source{}, configf("source.scan.startup.mode", "%s is not a startup mode", mode)
			}
			if _, ok := startupModes[mode]; !ok {
				return Source{}, configf("source.scan.startup.mode", "unknown mode %s", mode)
			}
			out.StartupMode = mode
		case "scan.startup.specific-offset.file":
			out.SpecificFile = fmt.Sprint(value)
		case "scan.startup.specific-offset.pos":
			n, err := asInt(value)
			if err != nil || n < 0 {
				return Source{}, configErr("source.scan.startup.specific-offset.pos", "must be a non-negative integer")
			}
			out.SpecificPos = uint32(n)
		case "scan.startup.specific-offset.gtid-set":
			out.SpecificGTID = fmt.Sprint(value)
		case "scan.startup.specific-offset.skip-events":
			n, err := asInt(value)
			if err != nil || n < 0 {
				return Source{}, configErr("source.scan.startup.specific-offset.skip-events", "must be a non-negative integer")
			}
			out.SkipEvents = n
		case "scan.startup.specific-offset.skip-rows":
			n, err := asInt(value)
			if err != nil || n < 0 {
				return Source{}, configErr("source.scan.startup.specific-offset.skip-rows", "must be a non-negative integer")
			}
			out.SkipRows = n
		case "scan.startup.timestamp-millis":
			n, err := asInt(value)
			if err != nil || n < 0 {
				return Source{}, configErr("source.scan.startup.timestamp-millis", "must be a non-negative integer")
			}
			out.TimestampMillis = int64(n)
		case "scan.incremental.snapshot.chunk.size":
			n, err := asInt(value)
			if err != nil || n < 1 {
				return Source{}, configErr("source.scan.incremental.snapshot.chunk.size", "must be >= 1")
			}
			out.ChunkSize = n
		case "scan.incremental.snapshot.chunk.key-column":
			out.ChunkKeyColumn = fmt.Sprint(value)
		case "scan.snapshot.fetch.size":
			n, err := asInt(value)
			if err != nil || n < 1 {
				return Source{}, configErr("source.scan.snapshot.fetch.size", "must be >= 1")
			}
			out.FetchSize = n
		case "schema-change.enabled":
			b, err := asBool(value)
			if err != nil {
				return Source{}, configf("source.schema-change.enabled", "%v", err)
			}
			out.SchemaChange = b
		case "scan.newly-added-table.enabled":
			b, err := asBool(value)
			if err != nil {
				return Source{}, configf("source.scan.newly-added-table.enabled", "%v", err)
			}
			out.NewlyAddedTable = b
		case "scan.newly-added-table.snapshot.enabled":
			b, err := asBool(value)
			if err != nil {
				return Source{}, configf("source.scan.newly-added-table.snapshot.enabled", "%v", err)
			}
			out.NewlyAddedSnapshot = b
		case "scan.binlog.newly-added-table.enabled":
			b, err := asBool(value)
			if err != nil {
				return Source{}, configf("source.scan.binlog.newly-added-table.enabled", "%v", err)
			}
			out.BinlogNewlyAddedTable = b
		case "connect.timeout":
			d, err := asDuration(value)
			if err != nil {
				return Source{}, configf("source.connect.timeout", "%v", err)
			}
			if d < minConnectTimeout {
				return Source{}, configf("source.connect.timeout", "must be >= 250ms, got %s", d)
			}
			out.ConnectTimeout = d
		case "heartbeat.interval":
			d, err := asDuration(value)
			if err != nil {
				return Source{}, configf("source.heartbeat.interval", "%v", err)
			}
			out.HeartbeatInterval = d
		default:
			return Source{}, configErr("source."+key, "unknown field")
		}
	}
	if out.Hostname == "" {
		return Source{}, configErr("source.hostname", "required")
	}
	if out.Username == "" {
		return Source{}, configErr("source.username", "required")
	}
	if out.Password == "" {
		return Source{}, configErr("source.password", "required")
	}
	selector, err := CompileSelector(tables, exclude)
	if err != nil {
		return Source{}, err
	}
	out.Tables = selector
	out.TablesInclude = tables
	out.TablesExclude = exclude
	if out.StartupMode == StartupSpecificOffset && out.SpecificGTID == "" {
		if out.SpecificFile == "" {
			return Source{}, configErr("source.scan.startup.specific-offset.file", "required unless gtid-set is set")
		}
		if out.SpecificPos == 0 {
			return Source{}, configErr("source.scan.startup.specific-offset.pos", "required unless gtid-set is set")
		}
	}
	if out.StartupMode == StartupTimestamp && out.TimestampMillis == 0 {
		return Source{}, configErr("source.scan.startup.timestamp-millis", "required")
	}
	return out, nil
}

func parseSink(raw map[string]any) (Sink, error) {
	if raw == nil {
		return Sink{}, configErr("sink", "required")
	}
	out := Sink{
		Port:                  defaultPort,
		AutoCreate:            true,
		WriteBatchSize:        defaultBatchSize,
		WriteBatchEvery:       time.Second,
		MaxRetries:            defaultMaxRetries,
		AllowAutoCreateTopics: true, // kafka: keep prior Always-on AllowAutoTopicCreation
		ProducerBatchMaxBytes: defaultProducerBatchMaxBytes,
		FlushTimeout:          defaultFlushTimeout,
		BulkMaxBytes:          DefaultBulkMaxBytes,
	}
	if strings.EqualFold(fmt.Sprint(raw["type"]), "doris") {
		// Doris separates the FE HTTP port (Stream Load) from the SQL port.
		// Keep defaultPort=3306 untouched for MySQL and the shared source.
		out.Port = defaultDorisHTTPPort
		out.SQLPort = defaultDorisSQLPort
		out.RequestTimeout = defaultDorisTimeout
		out.Buckets = defaultDorisBuckets
		out.SchemaChangeInclude = []string{"add.column"}
		out.SchemaChangeWait = DefaultDorisSchemaChangeWait
	}
	used := make(map[string]struct{}, len(raw))
	for key, value := range raw {
		used[key] = struct{}{}
		switch key {
		case "type":
			out.Type = fmt.Sprint(value)
		case "hostname":
			out.Hostname = fmt.Sprint(value)
		case "port":
			n, err := asInt(value)
			if err != nil {
				return Sink{}, configf("sink.port", "%v", err)
			}
			out.Port = n
		case "username":
			out.Username = fmt.Sprint(value)
		case "password":
			out.Password = fmt.Sprint(value)
		case "auto-create-table":
			b, err := asBool(value)
			if err != nil {
				return Sink{}, configf("sink.auto-create-table", "%v", err)
			}
			out.AutoCreate = b
		case "auto-create":
			b, err := asBool(value)
			if err != nil {
				return Sink{}, configf("sink.auto-create", "%v", err)
			}
			out.AutoCreate = b
		case "hosts":
			list, err := asStringList(value)
			if err != nil {
				return Sink{}, configf("sink.hosts", "%v", err)
			}
			out.Hosts = list
		case "version":
			n, err := asVersion(value)
			if err != nil {
				return Sink{}, configErr("sink.version", "must be 7, 8 or 9")
			}
			out.Version = n
		case "index-name":
			out.IndexName = strings.TrimSpace(fmt.Sprint(value))
		case "mapping-file":
			out.MappingFile = strings.TrimSpace(fmt.Sprint(value))
		case "request-timeout":
			d, err := asDuration(value)
			if err != nil || d <= 0 {
				return Sink{}, configErr("sink.request-timeout", "must be a positive duration")
			}
			out.RequestTimeout = d
		case "bulk-max-bytes":
			n, err := asInt(value)
			if err != nil {
				return Sink{}, configErr("sink.bulk-max-bytes", "must be an integer")
			}
			if err := ValidateBulkMaxBytes(n); err != nil {
				return Sink{}, configf("sink.bulk-max-bytes", "%v", err)
			}
			// Zero keeps the default injected above.
			if n > 0 {
				out.BulkMaxBytes = n
			}
		case "write-batch-size":
			n, err := asInt(value)
			if err != nil || n < 1 {
				return Sink{}, configErr("sink.write-batch-size", "must be >= 1")
			}
			out.WriteBatchSize = n
		case "write-batch-interval":
			d, err := asDuration(value)
			if err != nil {
				return Sink{}, configf("sink.write-batch-interval", "%v", err)
			}
			out.WriteBatchEvery = d
		case "max-retries":
			n, err := asInt(value)
			if err != nil || n < 0 {
				return Sink{}, configErr("sink.max-retries", "must be >= 0")
			}
			out.MaxRetries = n
		case "schema.change.drop-truncate.enabled":
			b, err := asBool(value)
			if err != nil {
				return Sink{}, configf("sink.schema.change.drop-truncate.enabled", "%v", err)
			}
			out.DropTruncate = b
		case "brokers":
			list, err := asStringList(value)
			if err != nil {
				return Sink{}, configf("sink.brokers", "%v", err)
			}
			out.Brokers = list
		case "topic":
			out.Topic = strings.TrimSpace(fmt.Sprint(value))
		case "source-name":
			out.SourceName = strings.TrimSpace(fmt.Sprint(value))
		case "client-id":
			out.ClientID = strings.TrimSpace(fmt.Sprint(value))
		case "schema.change.emit.enabled":
			b, err := asBool(value)
			if err != nil {
				return Sink{}, configf("sink.schema.change.emit.enabled", "%v", err)
			}
			out.EmitDDL = b
		case "allow.auto.create.topics":
			b, err := asBool(value)
			if err != nil {
				return Sink{}, configf("sink.allow.auto.create.topics", "%v", err)
			}
			out.AllowAutoCreateTopics = b
		case "producer-batch-max-bytes":
			n, err := asInt(value)
			if err != nil || n < minProducerBatchMaxBytes || n > maxProducerBatchMaxBytes {
				return Sink{}, configf("sink.producer-batch-max-bytes", "must be an integer in %d..%d", minProducerBatchMaxBytes, maxProducerBatchMaxBytes)
			}
			out.ProducerBatchMaxBytes = n
		case "flush-timeout":
			d, err := asDuration(value)
			if err != nil || d <= 0 {
				return Sink{}, configErr("sink.flush-timeout", "must be a positive duration")
			}
			out.FlushTimeout = d
		case "sql-port":
			n, err := asInt(value)
			if err != nil || n < 1 || n > 65535 {
				return Sink{}, configErr("sink.sql-port", "must be an integer in 1..65535")
			}
			out.SQLPort = n
		case "database":
			out.Database = strings.TrimSpace(fmt.Sprint(value))
		case "schema.change.enabled":
			b, err := asBool(value)
			if err != nil {
				return Sink{}, configf("sink.schema.change.enabled", "%v", err)
			}
			out.SchemaChange = b
		case "schema.change.include":
			list, err := asStringList(value)
			if err != nil {
				return Sink{}, configf("sink.schema.change.include", "%v", err)
			}
			out.SchemaChangeInclude = list
		case "schema.change.wait-timeout":
			d, err := asDuration(value)
			if err != nil || d <= 0 {
				return Sink{}, configErr("sink.schema.change.wait-timeout", "must be a positive duration")
			}
			out.SchemaChangeWait = d
		case "partial-update.enabled":
			b, err := asBool(value)
			if err != nil {
				return Sink{}, configf("sink.partial-update.enabled", "%v", err)
			}
			out.PartialUpdate = b
		case "delete.enabled":
			b, err := asBool(value)
			if err != nil {
				return Sink{}, configf("sink.delete.enabled", "%v", err)
			}
			out.DeleteEnabled = b
		case "buckets":
			out.Buckets = strings.ToLower(strings.TrimSpace(fmt.Sprint(value)))
		case "replication-num":
			n, err := asInt(value)
			if err != nil || n < 0 {
				return Sink{}, configErr("sink.replication-num", "must be >= 0")
			}
			out.ReplicationNum = n
		case "schema":
			out.Schema = strings.TrimSpace(fmt.Sprint(value))
		case "sslmode":
			out.SSLMode = strings.TrimSpace(fmt.Sprint(value))
		case "connect-timeout":
			d, err := asDuration(value)
			if err != nil || d < minConnectTimeout {
				return Sink{}, configf("sink.connect-timeout", "must be a duration >= %s", minConnectTimeout)
			}
			out.ConnectTimeout = d
		default:
			return Sink{}, configErr("sink."+key, "unknown field")
		}
	}
	if err := validateSinkKeys(out.Type, used); err != nil {
		return Sink{}, err
	}
	switch out.Type {
	case "stdout":
		if err := rejectKeysForSinkType(out.Type, used, dorisOnlySinkKeys, requestTimeoutSinkKeys); err != nil {
			return Sink{}, err
		}
		if err := rejectSinkFields(raw, out.Type, postgresNamespaceFields...); err != nil {
			return Sink{}, err
		}
	case "mysql":
		if err := rejectKeysForSinkType(out.Type, used, dorisOnlySinkKeys, requestTimeoutSinkKeys); err != nil {
			return Sink{}, err
		}
		if err := rejectSinkFields(raw, out.Type, postgresNamespaceFields...); err != nil {
			return Sink{}, err
		}
		if out.Hostname == "" {
			return Sink{}, configErr("sink.hostname", "required")
		}
		if out.Username == "" {
			return Sink{}, configErr("sink.username", "required")
		}
		if out.Password == "" {
			return Sink{}, configErr("sink.password", "required")
		}
	case "kafka":
		if err := rejectKeysForSinkType(out.Type, used, dorisOnlySinkKeys, requestTimeoutSinkKeys); err != nil {
			return Sink{}, err
		}
		if err := rejectSinkFields(raw, out.Type, postgresNamespaceFields...); err != nil {
			return Sink{}, err
		}
		if len(out.Brokers) == 0 {
			return Sink{}, configErr("sink.brokers", "required")
		}
		for i, b := range out.Brokers {
			if strings.TrimSpace(b) == "" {
				return Sink{}, configf("sink.brokers", "entry %d is empty", i)
			}
		}
		if out.Topic == "" {
			return Sink{}, configErr("sink.topic", "required")
		}
		if strings.ContainsAny(out.Topic, " \t\n") {
			return Sink{}, configErr("sink.topic", "must not contain whitespace")
		}
	case "doris":
		// schema / sslmode / connect-timeout belong to the PostgreSQL sink only.
		if err := rejectSinkFields(raw, out.Type, postgresNamespaceFields...); err != nil {
			return Sink{}, err
		}
		if out.Hostname == "" {
			return Sink{}, configErr("sink.hostname", "required")
		}
		if strings.Contains(out.Hostname, "://") || strings.ContainsAny(out.Hostname, "/?#") {
			return Sink{}, configErr("sink.hostname", "must be a host name without scheme or path")
		}
		if out.Port < 1 || out.Port > 65535 {
			return Sink{}, configErr("sink.port", "must be an integer in 1..65535")
		}
		if out.SQLPort < 1 || out.SQLPort > 65535 {
			return Sink{}, configErr("sink.sql-port", "must be an integer in 1..65535")
		}
		if out.Username == "" {
			return Sink{}, configErr("sink.username", "required")
		}
		if out.PartialUpdate {
			return Sink{}, configErr("sink.partial-update.enabled", "partial column updates are not implemented")
		}
		if out.DeleteEnabled {
			return Sink{}, configErr("sink.delete.enabled", "DELETE is not implemented")
		}
		if out.DropTruncate {
			return Sink{}, configErr("sink.schema.change.drop-truncate.enabled", "drop/truncate is not implemented for doris in v1")
		}
		for _, item := range out.SchemaChangeInclude {
			if item != "add.column" {
				return Sink{}, configf("sink.schema.change.include", "unsupported schema change %q; v1 supports only add.column", item)
			}
		}
		if out.Buckets == "" {
			out.Buckets = defaultDorisBuckets
		}
		if out.SchemaChangeWait <= 0 {
			out.SchemaChangeWait = DefaultDorisSchemaChangeWait
		}
		if out.Buckets != "auto" {
			n, err := strconv.Atoi(out.Buckets)
			if err != nil || n < 1 {
				return Sink{}, configErr("sink.buckets", "must be auto or a positive integer")
			}
		}
	case "postgresql":
		if err := parsePostgresSink(&out, raw); err != nil {
			return Sink{}, err
		}
	case "elasticsearch":
		// request-timeout is shared with Doris; every other Doris-only key has
		// no meaning here and must not be silently ignored.
		if err := rejectKeysForSinkType(out.Type, used, dorisOnlySinkKeys); err != nil {
			return Sink{}, err
		}
		// schema / sslmode / connect-timeout belong to the PostgreSQL sink only.
		if err := rejectSinkFields(raw, out.Type, postgresNamespaceFields...); err != nil {
			return Sink{}, err
		}
		if err := validateElasticsearch(&out, used); err != nil {
			return Sink{}, err
		}
	default:
		if out.Type == "" {
			return Sink{}, configErr("sink.type", "required")
		}
		return Sink{}, configf("sink.type", "unknown type %s", out.Type)
	}
	return out, nil
}

// validateSinkKeys rejects sink keys that belong to another sink type. The
// first offending key in sorted order is reported, so messages are stable.
func validateSinkKeys(sinkType string, used map[string]struct{}) error {
	keys := make([]string, 0, len(used))
	for key := range used {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, ok := esOnlySinkKeys[key]; ok && sinkType != "elasticsearch" {
			return configf("sink."+key, "only valid for sink.type elasticsearch")
		}
		if _, ok := kafkaOnlySinkKeys[key]; ok && sinkType != "kafka" {
			return configf("sink."+key, "only valid for sink.type kafka")
		}
		if _, ok := mysqlOnlySinkKeys[key]; ok && sinkType != "mysql" && sinkType != "doris" && sinkType != "postgresql" {
			// auto-create-table is shared by the MySQL, Doris and PostgreSQL
			// sinks; the Elasticsearch sink opts into index creation with
			// auto-create.
			return configf("sink."+key, "only valid for sink.type mysql, doris or postgresql")
		}
	}
	return nil
}

// rejectSinkFields fails when the config sets a field the sink type does not accept.
func rejectSinkFields(raw map[string]any, sinkType string, fields ...string) error {
	for _, field := range fields {
		if _, ok := raw[field]; ok {
			return configf("sink."+field, "is not supported for sink type %s", sinkType)
		}
	}
	return nil
}

// validateElasticsearch applies the ES-only defaults and validation. auto-create
// defaults to false here (unlike the MySQL sink): creating an index is a side
// effect that must be opted into.
func validateElasticsearch(out *Sink, used map[string]struct{}) error {
	if _, ok := used["auto-create"]; !ok {
		out.AutoCreate = false
	}
	version, err := ResolveElasticsearchVersion(out.Version)
	if err != nil {
		return configf("sink.version", "%v", err)
	}
	out.Version = version
	if len(out.Hosts) == 0 {
		return configErr("sink.hosts", "required")
	}
	for i, host := range out.Hosts {
		if err := validateElasticsearchHost(host); err != nil {
			return configf("sink.hosts", "entry %d: %v", i, err)
		}
	}
	if (out.Username == "") != (out.Password == "") {
		return configErr("sink.username", "username and password must be configured together")
	}
	if out.RequestTimeout <= 0 {
		out.RequestTimeout = defaultRequestTimeout
	}
	if out.BulkMaxBytes <= 0 {
		out.BulkMaxBytes = DefaultBulkMaxBytes
	}
	if out.IndexName != "" {
		if err := validateElasticsearchIndexName(out.IndexName); err != nil {
			return configf("sink.index-name", "%v", err)
		}
	}
	if out.MappingFile == "" {
		return nil
	}
	body, err := os.ReadFile(out.MappingFile)
	if err != nil {
		return configf("sink.mapping-file", "read %s: %v", out.MappingFile, err)
	}
	if !json.Valid(body) {
		return configf("sink.mapping-file", "%s is not valid JSON", out.MappingFile)
	}
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil || doc == nil {
		return configf("sink.mapping-file", "%s must be a JSON object", out.MappingFile)
	}
	out.Mapping = append(json.RawMessage(nil), body...)
	return nil
}

// validateElasticsearchHost checks a host entry without echoing credentials
// that may be embedded in the URL.
func validateElasticsearchHost(value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return errors.New("must not be empty")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return errors.New("invalid URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("unsupported scheme %q, want http or https", parsed.Scheme)
	}
	if parsed.Host == "" {
		return errors.New("missing host")
	}
	return nil
}

// validateElasticsearchIndexName enforces the Elasticsearch index naming rules
// for an explicit sink.index-name.
func validateElasticsearchIndexName(name string) error {
	if name == "" {
		return errors.New("must not be empty")
	}
	if name == "." || name == ".." {
		return errors.New(`must not be "." or ".."`)
	}
	if len(name) > 255 {
		return errors.New("must not exceed 255 bytes")
	}
	if name != strings.ToLower(name) {
		return errors.New("must be lowercase")
	}
	if strings.HasPrefix(name, "_") || strings.HasPrefix(name, "-") || strings.HasPrefix(name, "+") {
		return fmt.Errorf("must not start with %q", name[:1])
	}
	if strings.ContainsAny(name, `\/*?"<>| ,#:`) {
		return errors.New(`must not contain a reserved character (\ / * ? " < > | space , # :)`)
	}
	return nil
}

// parsePostgresSink applies PostgreSQL defaults and validation. parseSink fills
// MySQL-oriented defaults (port 3306, auto-create-table true) before the type is
// known, so those two are recomputed here and explicit config always wins.
func parsePostgresSink(out *Sink, raw map[string]any) error {
	if err := rejectSinkFields(raw, out.Type, mysqlOnlySinkFields...); err != nil {
		return err
	}
	if err := rejectSinkFields(raw, out.Type, kafkaOnlySinkFields...); err != nil {
		return err
	}
	// Doris-only keys stay rejected here too: this sink has no DDL or Stream Load
	// equivalent, so accepting them would silently drop the setting.
	if err := rejectSinkFields(raw, out.Type, dorisOnlySinkKeys...); err != nil {
		return err
	}
	// request-timeout bounds a Doris Stream Load or an Elasticsearch bulk request.
	// This sink issues neither, so the value would never take effect and must not
	// be accepted as a no-op.
	if err := rejectSinkFields(raw, out.Type, requestTimeoutSinkKeys...); err != nil {
		return err
	}
	if _, ok := raw["auto-create-table"]; ok && out.AutoCreate {
		return configErr("sink.auto-create-table", "must not be true: the postgresql sink does not create tables in this release")
	}
	out.AutoCreate = false
	if _, ok := raw["port"]; !ok {
		out.Port = defaultPostgresPort
	}
	if _, ok := raw["schema"]; !ok {
		out.Schema = defaultPostgresSchema
	}
	if _, ok := raw["sslmode"]; !ok {
		out.SSLMode = defaultPostgresSSLMode
	}
	if _, ok := raw["connect-timeout"]; !ok {
		out.ConnectTimeout = defaultPostgresConnectTimeout
	}
	if out.Hostname == "" {
		return configErr("sink.hostname", "required")
	}
	if out.Username == "" {
		return configErr("sink.username", "required")
	}
	if out.Password == "" {
		return configErr("sink.password", "required")
	}
	if out.Database == "" {
		return configErr("sink.database", "required")
	}
	if strings.Contains(out.Database, ".") {
		return configErr("sink.database", "must be a single database name without dots")
	}
	if out.Port < 1 || out.Port > 65535 {
		return configErr("sink.port", "must be in 1..65535")
	}
	if out.Schema == "" || strings.Contains(out.Schema, ".") {
		return configErr("sink.schema", "must be a single schema name without dots")
	}
	if _, ok := postgresSSLModes[out.SSLMode]; !ok {
		return configf("sink.sslmode", "unknown mode %s", out.SSLMode)
	}
	return nil
}

func parseRoutes(raw any) ([]Route, error) {
	if raw == nil {
		return nil, nil
	}
	items, ok := raw.([]any)
	if !ok {
		return nil, configErr("route", "must be a list")
	}
	var out []Route
	for i, item := range items {
		fields := asMap(item)
		if fields == nil {
			return nil, configf("route", "item %d must be a map", i)
		}
		route := Route{}
		for key, value := range fields {
			switch key {
			case "source-table":
				route.SourceTable = fmt.Sprint(value)
			case "sink-table":
				route.SinkTable = fmt.Sprint(value)
			case "replace-symbol":
				route.ReplaceSymbol = fmt.Sprint(value)
			case "description":
				route.Description = fmt.Sprint(value)
			default:
				return nil, configf("route", "item %d: unknown field %s", i, key)
			}
		}
		if route.SourceTable == "" {
			return nil, configf("route", "item %d: source-table is required", i)
		}
		if route.SinkTable == "" {
			return nil, configf("route", "item %d: sink-table is required", i)
		}
		if !strings.Contains(route.SinkTable, ".") {
			return nil, configf("route", "item %d: sink-table must be database.table", i)
		}
		out = append(out, route)
	}
	return out, nil
}

func parseTransforms(raw any) ([]Transform, error) {
	if raw == nil {
		return nil, nil
	}
	items, ok := raw.([]any)
	if !ok {
		return nil, configErr("transform", "must be a list")
	}
	ignored := map[string]struct{}{
		"partition-keys": {}, "table-options": {}, "converter-after-transform": {},
	}
	var out []Transform
	for i, item := range items {
		fields := asMap(item)
		if fields == nil {
			return nil, configf("transform", "item %d must be a map", i)
		}
		rule := Transform{}
		for key, value := range fields {
			if _, skip := ignored[key]; skip {
				continue
			}
			switch key {
			case "source-table":
				rule.SourceTable = fmt.Sprint(value)
			case "projection":
				rule.Projection = fmt.Sprint(value)
			case "filter":
				rule.Filter = fmt.Sprint(value)
			case "primary-keys":
				rule.PrimaryKeys = fmt.Sprint(value)
			case "description":
				rule.Description = fmt.Sprint(value)
			default:
				return nil, configf("transform", "item %d: unknown field %s", i, key)
			}
		}
		if rule.SourceTable == "" {
			return nil, configf("transform", "item %d: source-table is required", i)
		}
		out = append(out, rule)
	}
	return out, nil
}

func parseRuntime(raw map[string]any) (Runtime, error) {
	if raw == nil {
		return Runtime{}, configErr("pipeline", "required")
	}
	if len(raw) == 0 {
		return Runtime{}, configErr("pipeline", "at least one parameter is required")
	}
	out := Runtime{Name: "cdc", Parallelism: defaultParallel, RouteMode: RouteAllMatch}
	for key, value := range raw {
		switch key {
		case "name":
			out.Name = fmt.Sprint(value)
		case "parallelism":
			n, err := asInt(value)
			if err != nil || n < 1 {
				return Runtime{}, configErr("pipeline.parallelism", "must be >= 1")
			}
			out.Parallelism = n
		case "route-mode":
			mode := fmt.Sprint(value)
			if mode != RouteAllMatch && mode != RouteFirstMatch {
				return Runtime{}, configf("pipeline.route-mode", "unknown mode %s", mode)
			}
			out.RouteMode = mode
		default:
			return Runtime{}, configErr("pipeline."+key, "unknown field")
		}
	}
	if out.Name == "" {
		return Runtime{}, configErr("pipeline.name", "required")
	}
	return out, nil
}

func parseCheckpoint(raw any) (Checkpoint, error) {
	out := Checkpoint{
		Storage:  "file",
		Interval: 10 * time.Second,
		// FilePath holds the base directory until Parse resolves the final file.
	}
	if raw == nil {
		return out, nil
	}
	fields := asMap(raw)
	if fields == nil {
		return Checkpoint{}, configErr("checkpoint", "must be a map")
	}
	for key, value := range fields {
		switch key {
		case "storage":
			storage := fmt.Sprint(value)
			if storage == "mysql" {
				return Checkpoint{}, configErr("checkpoint.storage", "mysql is not implemented in this release")
			}
			if storage != "file" {
				return Checkpoint{}, configf("checkpoint.storage", "unknown storage %s", storage)
			}
			out.Storage = storage
		case "interval":
			d, err := asDuration(value)
			if err != nil || d <= 0 {
				return Checkpoint{}, configErr("checkpoint.interval", "must be a positive duration")
			}
			out.Interval = d
		case "file-path":
			path := strings.TrimSpace(fmt.Sprint(value))
			if path == "" {
				return Checkpoint{}, configErr("checkpoint.file-path", "must not be empty")
			}
			out.FilePath = strings.TrimRight(path, `/\`)
		default:
			return Checkpoint{}, configErr("checkpoint."+key, "unknown field")
		}
	}
	return out, nil
}

const (
	ckptExt              = ".ckpt"
	defaultCheckpointDir = "./checkpoints"
)

// checkpointFilePath builds <base>/<pipeline.name>/<md5(pipeline.name)>.ckpt.
func checkpointFilePath(base, pipelineName string) string {
	sum := md5.Sum([]byte(pipelineName))
	base = strings.TrimRight(strings.TrimSpace(base), `/\`)
	if base == "" {
		base = defaultCheckpointDir
	}
	return fmt.Sprintf("%s/%s/%x%s", base, sanitizePipelineName(pipelineName), sum, ckptExt)
}

func sanitizePipelineName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "cdc"
	}
	replacer := strings.NewReplacer("/", "_", `\`, "_", "..", "_")
	return replacer.Replace(name)
}

func asMap(value any) map[string]any {
	switch typed := value.(type) {
	case map[string]any:
		return typed
	case nil:
		return nil
	default:
		return nil
	}
}

// asVersion parses sink.version strictly: asInt would read "7.5" as 7, but a
// version is a whole major number or nothing.
func asVersion(value any) (int, error) {
	if raw, ok := value.(string); ok {
		return strconv.Atoi(strings.TrimSpace(raw))
	}
	return asInt(value)
}

func asInt(value any) (int, error) {
	switch typed := value.(type) {
	case int:
		return typed, nil
	case int64:
		return int(typed), nil
	case uint64:
		return int(typed), nil
	case float64:
		if typed != float64(int(typed)) {
			return 0, fmt.Errorf("not an integer")
		}
		return int(typed), nil
	case string:
		var n int
		_, err := fmt.Sscan(typed, &n)
		return n, err
	default:
		return 0, fmt.Errorf("not an integer")
	}
}

func asBool(value any) (bool, error) {
	switch typed := value.(type) {
	case bool:
		return typed, nil
	case string:
		switch strings.ToLower(typed) {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
	}
	return false, fmt.Errorf("not a boolean")
}

func asStringList(value any) ([]string, error) {
	switch typed := value.(type) {
	case string:
		parts := strings.Split(typed, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			out = append(out, p)
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("empty list")
		}
		return out, nil
	case []any:
		if len(typed) == 0 {
			return nil, fmt.Errorf("empty list")
		}
		out := make([]string, 0, len(typed))
		for i, item := range typed {
			s := strings.TrimSpace(fmt.Sprint(item))
			if s == "" {
				return nil, fmt.Errorf("entry %d is empty", i)
			}
			out = append(out, s)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("must be a string or list")
	}
}

func asDuration(value any) (time.Duration, error) {
	switch typed := value.(type) {
	case string:
		return time.ParseDuration(typed)
	case int:
		return time.Duration(typed) * time.Millisecond, nil
	default:
		return 0, fmt.Errorf("not a duration")
	}
}
