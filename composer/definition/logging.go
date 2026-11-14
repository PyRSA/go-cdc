package definition

import "time"

// Logging is the optional top-level logging: block. When Configured is false the
// process keeps the historical default (text on stderr at INFO).
type Logging struct {
	Configured bool
	Level      string // DEBUG / INFO / WARN / ERROR
	Format     string // text / json
	Console    *LoggingConsole
	File       *LoggingFile
}

// LoggingConsole is present when logging.console appears in YAML.
type LoggingConsole struct {
	Stream string // stdout / stderr
}

// LoggingFile is present when logging.file appears in YAML. Rotation fields are
// filled by the parser; Setup opens the writer from them.
type LoggingFile struct {
	Path string

	// Exactly one of SizeBytes > 0 or Every > 0.
	SizeBytes int64
	Every     time.Duration
	// DailyAt is set when rotation was an HH:MM wall-clock time. Every is then 24h
	// and DailyAt names the local time of day for the first cut.
	DailyAt *Clock

	// At most one of RetainAge > 0 or RetainCount > 0.
	RetainAge   time.Duration
	RetainCount int

	CompressGzip bool
}

// Clock is a local time of day used for daily file rotation.
type Clock struct {
	Hour   int
	Minute int
}
