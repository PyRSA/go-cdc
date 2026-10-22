// Package cdcjson encodes CDC events as a shared Debezium-inspired JSON envelope
// used by the stdout and Kafka sinks.
package cdcjson

import (
	"encoding/json"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
)

const connectorMySQL = "mysql"

var (
	goMySQLVersionOnce sync.Once
	goMySQLVersion     = "unknown"
)

// SourceMeta is the fixed six-field source block.
type SourceMeta struct {
	Version   string `json:"version"`
	Connector string `json:"connector"`
	Name      string `json:"name"`
	TsMs      *int64 `json:"ts_ms"`
	DB        string `json:"db"`
	Table     string `json:"table"`
}

// Envelope is one JSON object written to stdout or Kafka.
type Envelope struct {
	Before map[string]any `json:"before"`
	After  map[string]any `json:"after"`
	Source SourceMeta     `json:"source"`
	Op     string         `json:"op"`
	TsMs   int64          `json:"ts_ms"`
	DDL    string         `json:"ddl,omitempty"`
}

// Options control envelope metadata filled at encode time.
type Options struct {
	SourceName string
	// Now is an optional wall-clock override for the envelope top-level ts_ms.
	// Zero (production) uses time.Now(); tests set a fixed value for stable assertions.
	// It is not source/binlog time — that is item.SourceTSMs → source.ts_ms.
	Now time.Time
}

// Marshal returns one JSON object for item.
func Marshal(item event.Event, opt Options) ([]byte, error) {
	env, err := Build(item, opt)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("cdcjson: encode: %w", err)
	}
	return body, nil
}

// Build constructs the envelope without marshaling.
func Build(item event.Event, opt Options) (Envelope, error) {
	name := opt.SourceName
	if name == "" {
		name = "mysql"
	}
	now := opt.Now
	if now.IsZero() {
		now = time.Now()
	}
	db, table := item.SourceDatabase, item.SourceTable
	if db == "" {
		db, table = item.Database, item.Table
	}
	src := SourceMeta{
		Version:   GoMySQLVersion(),
		Connector: connectorMySQL,
		Name:      name,
		TsMs:      sourceTsMs(item),
		DB:        db,
		Table:     table,
	}
	env := Envelope{
		Source: src,
		TsMs:   now.UnixMilli(),
	}
	switch item.Op {
	case event.OpRead:
		env.Op = "r"
		env.Before = nil
		env.After = item.After
	case event.OpCreate:
		env.Op = "c"
		env.Before = nil
		env.After = item.After
	case event.OpUpdate:
		env.Op = "u"
		env.Before = item.Before
		env.After = item.After
	case event.OpDelete:
		env.Op = "d"
		env.Before = item.Before
		env.After = nil
	case event.OpDDL:
		env.Op = "ddl"
		env.Before = nil
		env.After = nil
		env.DDL = item.DDL
	default:
		return Envelope{}, fmt.Errorf("cdcjson: unknown op %q", item.Op)
	}
	return env, nil
}

func sourceTsMs(item event.Event) *int64 {
	if item.SourceTSMs <= 0 {
		return nil
	}
	v := item.SourceTSMs
	return &v
}

// GoMySQLVersion returns the go-mysql module version from build info.
func GoMySQLVersion() string {
	goMySQLVersionOnce.Do(func() {
		info, ok := debug.ReadBuildInfo()
		if !ok {
			return
		}
		for _, mod := range info.Deps {
			if mod != nil && mod.Path == "github.com/go-mysql-org/go-mysql" && mod.Version != "" {
				goMySQLVersion = mod.Version
				return
			}
		}
	})
	return goMySQLVersion
}
