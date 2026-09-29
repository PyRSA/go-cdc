// Package sink defines the write interface. Every sink implements it.
// Sinks adapt events. They do not read binlog and they do not parse pipeline YAML.
package sink

import (
	"context"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
)

// Sink writes already transformed and routed events.
type Sink interface {
	Open(ctx context.Context) error
	CreateTable(table schema.Table) error
	ApplySchema(change schema.Change) error
	Write(batch []event.Event) error
	Flush() error
	Close() error
}
