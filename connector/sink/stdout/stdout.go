// Package stdout renders one source event as CRUD JSON.
// Snapshot rows are "r", inserts "c", updates "u" (before+after), deletes "d".
package stdout

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/connector/sink"
)

// Sink writes newline-delimited JSON to an io.Writer.
type Sink struct {
	out io.Writer
	mu  sync.Mutex
}

// New returns a stdout sink. w is usually os.Stdout.
func New(w io.Writer) *Sink {
	return &Sink{out: w}
}

// Open implements sink.Sink.
func (s *Sink) Open(context.Context) error { return nil }

// CreateTable prints nothing. The following row events carry the data.
func (s *Sink) CreateTable(schema.Table) error { return nil }

// ApplySchema prints one ddl object. Row images are not part of a ddl line.
func (s *Sink) ApplySchema(change schema.Change) error {
	return s.writeJSON(ddlLine{Op: "ddl", Database: change.Database, Table: change.Table, DDL: change.DDL})
}

// Write prints one CRUD JSON object per event.
func (s *Sink) Write(batch []event.Event) error {
	for _, item := range batch {
		switch item.Op {
		case event.OpRead:
			if err := s.write(line{Op: "r", Database: item.Database, Table: item.Table, After: item.After}); err != nil {
				return err
			}
		case event.OpCreate:
			if err := s.write(line{Op: "c", Database: item.Database, Table: item.Table, After: item.After}); err != nil {
				return err
			}
		case event.OpUpdate:
			if err := s.write(line{
				Op: "u", Database: item.Database, Table: item.Table,
				Before: item.Before, After: item.After,
			}); err != nil {
				return err
			}
		case event.OpDelete:
			if err := s.write(line{Op: "d", Database: item.Database, Table: item.Table, Before: item.Before}); err != nil {
				return err
			}
		case event.OpDDL:
			if err := s.ApplySchema(schema.Change{Database: item.Database, Table: item.Table, DDL: item.DDL}); err != nil {
				return err
			}
		default:
			return fmt.Errorf("stdout: unknown op %q", item.Op)
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

func (s *Sink) write(item line) error {
	return s.writeJSON(item)
}

func (s *Sink) writeJSON(item any) error {
	body, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("stdout: encode: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.out.Write(append(body, '\n')); err != nil {
		return fmt.Errorf("stdout: write: %w", err)
	}
	return nil
}

type line struct {
	Op       string         `json:"op"`
	Database string         `json:"database"`
	Table    string         `json:"table"`
	Before   map[string]any `json:"before"`
	After    map[string]any `json:"after"`
}

type ddlLine struct {
	Op       string `json:"op"`
	Database string `json:"database"`
	Table    string `json:"table"`
	DDL      string `json:"ddl"`
}

var _ sink.Sink = (*Sink)(nil)
