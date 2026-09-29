package definition

import (
	"crypto/md5"
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultPort       = 3306
	defaultChunkSize  = 8096
	defaultFetchSize  = 1024
	defaultParallel   = 1
	defaultBatchSize  = 1000
	defaultMaxRetries = 3
	minConnectTimeout = 250 * time.Millisecond
)

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
	"scan.newly-added-table.enabled":    {},
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
		Port:              defaultPort,
		TimeZone:          "UTC",
		StartupMode:       StartupInitial,
		ChunkSize:         defaultChunkSize,
		FetchSize:         defaultFetchSize,
		SchemaChange:      true,
		ConnectTimeout:    30 * time.Second,
		HeartbeatInterval: 30 * time.Second,
		JDBCProperties:    map[string]string{},
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
		Port:            defaultPort,
		AutoCreate:      true,
		WriteBatchSize:  defaultBatchSize,
		WriteBatchEvery: time.Second,
		MaxRetries:      defaultMaxRetries,
	}
	for key, value := range raw {
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
		default:
			return Sink{}, configErr("sink."+key, "unknown field")
		}
	}
	switch out.Type {
	case "stdout":
	case "mysql":
		if out.Hostname == "" {
			return Sink{}, configErr("sink.hostname", "required")
		}
		if out.Username == "" {
			return Sink{}, configErr("sink.username", "required")
		}
		if out.Password == "" {
			return Sink{}, configErr("sink.password", "required")
		}
	case "kafka", "doris", "elasticsearch":
		return Sink{}, configf("sink.type", "%s is not implemented in this release", out.Type)
	default:
		if out.Type == "" {
			return Sink{}, configErr("sink.type", "required")
		}
		return Sink{}, configf("sink.type", "unknown type %s", out.Type)
	}
	return out, nil
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
