// Package stdout renders one source event as CDC JSON (shared with Kafka).
// Ops: snapshot "r", insert "c", update "u", delete "d", schema "ddl".
package stdout

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/PyRSA/go-cdc/common/cdcjson"
	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/connector/sink"
)

// Sink writes newline-delimited JSON to an io.Writer.
type Sink struct {
	out        io.Writer
	sourceName string
	mu         sync.Mutex
}

// New returns a stdout sink. w is usually os.Stdout.
func New(w io.Writer) *Sink {
	return &Sink{out: w}
}

// SetSourceName sets envelope source.name (typically pipeline.name).
func (s *Sink) SetSourceName(name string) {
	s.sourceName = name
}

// Open implements sink.Sink.
func (s *Sink) Open(context.Context) error { return nil }

// CreateTable prints nothing. The following row events carry the data.
func (s *Sink) CreateTable(schema.Table) error { return nil }

// ApplySchema prints one ddl envelope. Row images are not part of a ddl line.
// SourceDatabase/SourceTable fill source.*; Database/Table are the routed sink id when set.
func (s *Sink) ApplySchema(change schema.Change) error {
	srcDB, srcTable := change.SourceDatabase, change.SourceTable
	if srcDB == "" {
		srcDB, srcTable = change.Database, change.Table
	}
	return s.writeEvent(event.Event{
		Op: event.OpDDL, Database: change.Database, Table: change.Table,
		SourceDatabase: srcDB, SourceTable: srcTable,
		DDL: change.DDL, SourceTSMs: change.SourceTSMs,
	})
}

// Write prints one CDC JSON object per event.
func (s *Sink) Write(batch []event.Event) error {
	for _, item := range batch {
		if err := s.writeEvent(item); err != nil {
			return err
		}
	}
	return nil
}

// Flush syncs the writer when it supports Sync.
func (s *Sink) Flush() error {
	if syncer, ok := s.out.(interface{ Sync() error }); ok {
		return syncer.Sync()
	}
	return nil
}

// Close implements sink.Sink. It does not close the underlying writer.
func (s *Sink) Close() error { return nil }

func (s *Sink) writeEvent(item event.Event) error {
	body, err := cdcjson.Marshal(item, cdcjson.Options{SourceName: s.sourceName})
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.out.Write(append(body, '\n')); err != nil {
		return fmt.Errorf("stdout: write: %w", err)
	}
	return nil
}

var _ sink.Sink = (*Sink)(nil)
