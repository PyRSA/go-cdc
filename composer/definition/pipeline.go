package definition

import (
	"encoding/json"
	"time"
)

// Startup modes accepted from scan.startup.mode.
const (
	StartupInitial        = "initial"
	StartupEarliest       = "earliest"
	StartupLatest         = "latest"
	StartupSpecificOffset = "specific-offset"
	StartupTimestamp      = "timestamp"
	StartupSnapshot       = "snapshot"
)

// Pipeline is one YAML job.
type Pipeline struct {
	Source     Source
	Sink       Sink
	Routes     []Route
	Transforms []Transform
	Runtime    Runtime
	Checkpoint Checkpoint
}

// Source is the MySQL capture configuration.
type Source struct {
	Hostname              string
	Port                  int
	Username              string
	Password              string
	Tables                *Selector
	TablesInclude         string
	TablesExclude         string
	ServerID              uint32
	TimeZone              string
	StartupMode           string
	SpecificFile          string
	SpecificPos           uint32
	SpecificGTID          string
	SkipEvents            int
	SkipRows              int
	TimestampMillis       int64
	ChunkSize             int
	ChunkKeyColumn        string
	FetchSize             int
	SchemaChange          bool
	NewlyAddedTable       bool
	NewlyAddedSnapshot    bool
	BinlogNewlyAddedTable bool
	ConnectTimeout        time.Duration
	HeartbeatInterval     time.Duration
	JDBCProperties        map[string]string
}

// Sink is stdout, MySQL, Kafka, Doris, or Elasticsearch for this release.
type Sink struct {
	Type            string
	Hostname        string
	Port            int
	Username        string
	Password        string
	AutoCreate      bool
	WriteBatchSize  int
	WriteBatchEvery time.Duration
	MaxRetries      int
	DropTruncate    bool
	// Kafka
	Brokers               []string
	Topic                 string
	SourceName            string
	ClientID              string
	EmitDDL               bool
	AllowAutoCreateTopics bool          // allow.auto.create.topics; default true via parseSink
	ProducerBatchMaxBytes int           // producer-batch-max-bytes; bytes; default via parseSink
	FlushTimeout          time.Duration // flush-timeout; default 30s via parseSink
	// Doris
	SQLPort             int           // sql-port; FE MySQL protocol; default 9030 via parseSink
	Database            string        // database; optional default target database
	SchemaChange        bool          // schema.change.enabled; default false
	SchemaChangeInclude []string      // schema.change.include; v1 allows only add.column
	SchemaChangeWait    time.Duration // schema.change.wait-timeout; ADD COLUMN visibility budget
	PartialUpdate       bool          // partial-update.enabled; must stay false
	DeleteEnabled       bool          // delete.enabled; must stay false
	Buckets             string        // buckets; "auto" or a positive integer
	ReplicationNum      int           // replication-num; 0 omits PROPERTIES

	// Elasticsearch. AutoCreate defaults to false for this sink; mapping-file is
	// read and validated at parse time and kept as the Create Index body.
	// Version is the target major version (7, 8 or 9); Parse and New both
	// resolve the zero value to ElasticsearchVersion8.
	Version     int
	Hosts       []string
	IndexName   string
	MappingFile string
	Mapping     json.RawMessage
	// RequestTimeout is shared by Doris and Elasticsearch: request-timeout; the
	// HTTP timeout for Stream Load and the Bulk API respectively.
	RequestTimeout time.Duration
	BulkMaxBytes   int // bulk-max-bytes; encoded NDJSON bytes per bulk request
}

// Route sends one source table pattern to a sink table.
type Route struct {
	SourceTable   string
	SinkTable     string
	ReplaceSymbol string
	Description   string
}

// Transform is the first matching projection, filter, and business key.
type Transform struct {
	SourceTable string
	Projection  string
	Filter      string
	PrimaryKeys string
	Description string
}

// Runtime holds pipeline-level settings.
type Runtime struct {
	Name        string
	Parallelism int
	RouteMode   string
}

// Checkpoint holds the file checkpoint settings.
// After Parse, FilePath is the resolved file:
// <file-path>/<pipeline.name>/<md5(pipeline.name)>.ckpt
// (file-path defaults to ./checkpoints).
type Checkpoint struct {
	Storage  string
	Interval time.Duration
	FilePath string
}

const (
	RouteAllMatch   = "ALL_MATCH"
	RouteFirstMatch = "FIRST_MATCH"
)
