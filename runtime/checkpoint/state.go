// Package checkpoint stores one job's progress in an atomic file.
// Snapshot and incremental phases share this JSON shape.
// excluded_tables is retained in the document but is no longer populated by create failures.
package checkpoint

import "encoding/json"

// Position is a binlog coordinate recorded in the checkpoint file.
type Position struct {
	File string `json:"file"`
	Pos  uint32 `json:"pos"`
	GTID string `json:"gtid,omitempty"`
}

// Split is one snapshot range that has been written. High is null on the last range.
type Split struct {
	Low  string  `json:"low"`
	High *string `json:"high"`
}

// State is the whole checkpoint document.
type State struct {
	LowWatermark   *Position          `json:"low_watermark"`
	AckedOffset    *Position          `json:"acked_offset"`
	FinishedSplits map[string][]Split `json:"finished_splits"`
	CapturedTables []string           `json:"captured_tables"`
	ExcludedTables []string           `json:"excluded_tables"`
}

// Normalize fills nil collections so the file matches the documented shape.
func (s *State) Normalize() {
	if s.FinishedSplits == nil {
		s.FinishedSplits = map[string][]Split{}
	}
	if s.CapturedTables == nil {
		s.CapturedTables = []string{}
	}
	if s.ExcludedTables == nil {
		s.ExcludedTables = []string{}
	}
}

// HasCaptured reports whether the table id is in captured_tables.
func (s State) HasCaptured(id string) bool {
	for _, item := range s.CapturedTables {
		if item == id {
			return true
		}
	}
	return false
}

// HasExcluded reports whether the table id is in excluded_tables.
func (s State) HasExcluded(id string) bool {
	for _, item := range s.ExcludedTables {
		if item == id {
			return true
		}
	}
	return false
}

// AddCaptured appends a table id once.
func (s *State) AddCaptured(id string) {
	s.Normalize()
	if !s.HasCaptured(id) {
		s.CapturedTables = append(s.CapturedTables, id)
	}
}

// AddExcluded appends a table id once.
func (s *State) AddExcluded(id string) {
	s.Normalize()
	if !s.HasExcluded(id) {
		s.ExcludedTables = append(s.ExcludedTables, id)
	}
}

// AddSplit records a finished range for a table.
func (s *State) AddSplit(table string, split Split) {
	s.Normalize()
	for _, existing := range s.FinishedSplits[table] {
		if existing.Low == split.Low && sameHigh(existing.High, split.High) {
			return
		}
	}
	s.FinishedSplits[table] = append(s.FinishedSplits[table], split)
}

// HasSplit reports whether an identical range was already written.
func (s State) HasSplit(table string, split Split) bool {
	for _, existing := range s.FinishedSplits[table] {
		if existing.Low == split.Low && sameHigh(existing.High, split.High) {
			return true
		}
	}
	return false
}

func sameHigh(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// MarshalJSON encodes a normalized document.
func (s State) MarshalJSON() ([]byte, error) {
	s.Normalize()
	type alias State
	return json.Marshal(alias(s))
}
