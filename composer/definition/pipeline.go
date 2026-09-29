package definition

import "time"

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
	Hostname          string
	Port              int
	Username          string
	Password          string
	Tables            *Selector
	TablesInclude     string
	TablesExclude     string
	ServerID          uint32
	TimeZone          string
	StartupMode       string
	SpecificFile      string
	SpecificPos       uint32
	SpecificGTID      string
	SkipEvents        int
	SkipRows          int
	TimestampMillis   int64
	ChunkSize         int
	ChunkKeyColumn    string
	FetchSize         int
	SchemaChange      bool
	ConnectTimeout    time.Duration
	HeartbeatInterval time.Duration
	JDBCProperties    map[string]string
}

// Sink is stdout or MySQL for this release.
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
