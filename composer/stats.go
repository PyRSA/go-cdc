package composer

import (
	"fmt"
	"strings"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
)

// statusInterval is how often the runner emits a pipeline status INFO line.
const statusInterval = 5 * time.Minute

// runStats holds counters for one Run invocation. Only the Run loop mutates it.
type runStats struct {
	startedAt time.Time

	events int64
	rows   int64
	ddl    int64

	// sinkRows is rows included in a batch that both Write and Flush succeeded for.
	// It is not a count of rows accepted by Write alone.
	sinkRows          int64
	sinkFailedBatches int64

	sourceErrors     int64
	sinkErrors       int64
	checkpointErrors int64

	lastFlushDuration time.Duration
	lastEventAt       time.Time

	// processed is the latest AckedOffset observed in memory (not necessarily saved).
	processed *checkpoint.Position

	// Last successful durable checkpoint snapshot.
	checkpointPos      *checkpoint.Position
	checkpointSavedAt  time.Time
	checkpointSaveDur  time.Duration
	checkpointSaveOK   bool
	checkpointEverSave bool

	// Snapshots from the previous status emission (for deltas / rates).
	prevAt       time.Time
	prevEvents   int64
	prevRows     int64
	prevDDL      int64
	prevSinkRows int64
}

func (s *runStats) noteProcessed(pos *checkpoint.Position) {
	if pos == nil {
		return
	}
	copied := *pos
	s.processed = &copied
}

func (s *runStats) noteCheckpointSuccess(pos *checkpoint.Position, savedAt time.Time, dur time.Duration) {
	s.checkpointSavedAt = savedAt
	s.checkpointSaveDur = dur
	s.checkpointSaveOK = true
	s.checkpointEverSave = true
	if pos != nil {
		copied := *pos
		s.checkpointPos = &copied
	} else {
		s.checkpointPos = nil
	}
}

func (s *runStats) noteCheckpointFailure() {
	s.checkpointSaveOK = false
	s.checkpointErrors++
}

func countRowEvents(batch []event.Event) int {
	n := 0
	for _, item := range batch {
		switch item.Op {
		case event.OpRead, event.OpCreate, event.OpUpdate, event.OpDelete:
			n++
		}
	}
	return n
}

func formatPosition(pos *checkpoint.Position) string {
	if pos == nil {
		return "none"
	}
	var parts []string
	if pos.File != "" || pos.Pos != 0 {
		parts = append(parts, fmt.Sprintf("%s:%d", pos.File, pos.Pos))
	}
	if pos.GTID != "" {
		parts = append(parts, "gtid="+pos.GTID)
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, " ")
}

func formatDurationClock(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Second)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	sec := int(d.Seconds()) % 60
	if h > 0 {
		return fmt.Sprintf("%dh%02dm%02ds", h, m, sec)
	}
	return fmt.Sprintf("%dm%02ds", m, sec)
}

func formatRate(delta int64, elapsed time.Duration) string {
	if elapsed <= 0 {
		return "0/s"
	}
	rate := float64(delta) / elapsed.Seconds()
	if rate == 0 {
		return "0/s"
	}
	if rate >= 100 || rate == float64(int64(rate)) {
		return fmt.Sprintf("%.0f/s", rate)
	}
	return fmt.Sprintf("%.2f/s", rate)
}

func (s *runStats) statusMessage(name string) string {
	now := time.Now()
	elapsed := now.Sub(s.prevAt)
	if s.prevAt.IsZero() {
		elapsed = now.Sub(s.startedAt)
	}
	deltaEvents := s.events - s.prevEvents
	deltaRows := s.rows - s.prevRows
	deltaDDL := s.ddl - s.prevDDL
	deltaSink := s.sinkRows - s.prevSinkRows

	lastEventAge := "n/a"
	if !s.lastEventAt.IsZero() {
		lastEventAge = formatDurationClock(now.Sub(s.lastEventAt))
	}

	// NONE = never attempted; SUCCESS = last save ok; FAILED = last save failed
	// (including a first attempt that never succeeded).
	saveStatus := "NONE"
	saveTime := "n/a"
	saveDur := "n/a"
	if s.checkpointEverSave || s.checkpointErrors > 0 {
		if s.checkpointSaveOK {
			saveStatus = "SUCCESS"
		} else {
			saveStatus = "FAILED"
		}
		if !s.checkpointSavedAt.IsZero() {
			saveTime = s.checkpointSavedAt.Format("15:04:05")
		}
		if s.checkpointEverSave {
			saveDur = s.checkpointSaveDur.String()
		}
	}

	flushDur := "n/a"
	if s.lastFlushDuration > 0 || s.sinkRows > 0 || s.sinkFailedBatches > 0 {
		flushDur = s.lastFlushDuration.String()
	}

	_ = name // name is logged as a slog attribute on the first line
	var b strings.Builder
	b.WriteString("Pipeline status\n")
	fmt.Fprintf(&b, "  Uptime: %s\n", formatDurationClock(now.Sub(s.startedAt)))
	b.WriteString("  Status: RUNNING\n")
	b.WriteString("\n")
	b.WriteString("  Source:\n")
	fmt.Fprintf(&b, "    Events: total=%d delta=%d rate=%s\n", s.events, deltaEvents, formatRate(deltaEvents, elapsed))
	fmt.Fprintf(&b, "    Rows: total=%d delta=%d rate=%s\n", s.rows, deltaRows, formatRate(deltaRows, elapsed))
	fmt.Fprintf(&b, "    DDL: total=%d delta=%d\n", s.ddl, deltaDDL)
	fmt.Fprintf(&b, "    Processed position: %s\n", formatPosition(s.processed))
	fmt.Fprintf(&b, "    Last event age: %s\n", lastEventAge)
	b.WriteString("\n")
	b.WriteString("  Checkpoint:\n")
	fmt.Fprintf(&b, "    Position: %s\n", formatPosition(s.checkpointPos))
	fmt.Fprintf(&b, "    Last save time: %s\n", saveTime)
	fmt.Fprintf(&b, "    Last save duration: %s\n", saveDur)
	fmt.Fprintf(&b, "    Save status: %s\n", saveStatus)
	b.WriteString("\n")
	b.WriteString("  Sink:\n")
	// sinkRows counts only after a successful Flush; Write-then-Flush-fail is excluded.
	fmt.Fprintf(&b, "    Flushed rows: total=%d delta=%d\n", s.sinkRows, deltaSink)
	fmt.Fprintf(&b, "    Failed batches: %d\n", s.sinkFailedBatches)
	fmt.Fprintf(&b, "    Last flush duration: %s\n", flushDur)
	b.WriteString("\n")
	b.WriteString("  Errors:\n")
	fmt.Fprintf(&b, "    Source errors: %d\n", s.sourceErrors)
	fmt.Fprintf(&b, "    Sink errors: %d\n", s.sinkErrors)
	fmt.Fprintf(&b, "    Checkpoint errors: %d", s.checkpointErrors)
	return b.String()
}

func (s *runStats) markStatusEmitted(at time.Time) {
	s.prevAt = at
	s.prevEvents = s.events
	s.prevRows = s.rows
	s.prevDDL = s.ddl
	s.prevSinkRows = s.sinkRows
}

func clonePosition(pos *checkpoint.Position) *checkpoint.Position {
	if pos == nil {
		return nil
	}
	copied := *pos
	return &copied
}
