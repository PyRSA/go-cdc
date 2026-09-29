// Package source defines the capture interface. Every source implements it.
// Snapshot and binlog are already one event shape before Read returns.
package source

import (
	"context"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
)

// Position is a binlog coordinate. GTID may be empty when the server has it off.
type Position struct {
	File string
	Pos  uint32
	GTID string
}

// Split is one finished snapshot range. A nil High means the range has no upper bound.
type Split struct {
	Table string
	Low   string
	High  *string
}

// Message is one step on the capture stream.
// An event message and a progress message are separate so the consumer can
// write the event before recording a split or a commit position.
type Message struct {
	Event *event.Event
	// Watermark is the binlog position taken before any snapshot read.
	Watermark *Position
	// Position is a commit point for the events already delivered.
	Position *Position
	// Split is a snapshot range that has been fully read.
	Split *Split
	// Captured is a table that must not be snapshotted again.
	Captured string
	// Schema is the source table definition, delivered before that table's snapshot rows.
	Schema *schema.Table
	// Err is a capture failure. The consumer must stop and return it.
	Err error
}

// Source reads one MySQL instance.
type Source interface {
	Open(ctx context.Context) error
	Read(ctx context.Context) (<-chan event.Event, error)
	Close() error
}
