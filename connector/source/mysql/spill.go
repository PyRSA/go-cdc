package mysql

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
)

// ErrSpillClosed is returned when AppendBatch runs after Drain closed the table queue.
var ErrSpillClosed = errors.New("spill closed")

// Spill is one on-disk binlog queue directory, with a file per snapshotting table.
// Filenames are hex(tableID) so distinct ids cannot collide.
//
// Durability model (at-least-once):
//   - AppendBatch is atomic per transaction via a trailing fence line + Sync.
//   - Drain delivers events through fn but does not persist replay progress.
//     Callers must establish sink+checkpoint durability (e.g. CheckpointBarrier)
//     then call PersistReplay before treating the queue as consumed, or Remove
//     the files after Captured is checkpointed. A crash before PersistReplay/
//     Remove re-delivers spill events (duplicates), never skips them.
type Spill struct {
	dir string
	mu  sync.Mutex
	tab map[string]*spillTable
}

type spillTable struct {
	mu       sync.Mutex
	f        *os.File
	path     string
	offPath  string
	replayed int64
	last     checkpoint.Position
	hasLast  bool
	closed   bool
}

// spillLine is one JSON line in the spill file.
// Event rows carry Event; a batch ends with Fence=true (Event nil).
type spillLine struct {
	Pos   checkpoint.Position `json:"pos"`
	Event *event.Event        `json:"event,omitempty"`
	Fence bool                `json:"fence,omitempty"`
}

// OpenSpill prepares a spill directory. Existing files are opened lazily per table.
func OpenSpill(dir string) *Spill {
	return &Spill{dir: dir, tab: map[string]*spillTable{}}
}

func spillPaths(dir, id string) (binlog, off string) {
	base := filepath.Join(dir, hex.EncodeToString([]byte(id)))
	return base + ".binlog", base + ".off"
}

func (s *Spill) table(id string) (*spillTable, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tab, ok := s.tab[id]; ok {
		return tab, nil
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return nil, err
	}
	binlogPath, offPath := spillPaths(s.dir, id)
	tab := &spillTable{path: binlogPath, offPath: offPath}
	if err := tab.open(); err != nil {
		return nil, err
	}
	s.tab[id] = tab
	return tab, nil
}

// tombstone keeps a closed table entry so Remove cannot be undone by a later open.
func (s *Spill) tombstone(id string) *spillTable {
	binlogPath, offPath := spillPaths(s.dir, id)
	return &spillTable{path: binlogPath, offPath: offPath, closed: true}
}

func (t *spillTable) open() error {
	f, err := os.OpenFile(t.path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	size := info.Size()
	if size > 0 {
		trimmed, err := truncatePartialLine(f, size)
		if err != nil {
			_ = f.Close()
			return err
		}
		size = trimmed
		trimmed, err = truncateIncompleteBatch(f, t.path, size)
		if err != nil {
			_ = f.Close()
			return err
		}
		size = trimmed
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return err
	}
	t.f = f
	// .off is only written by PersistReplay after sink+checkpoint durability.
	// Drain itself never persists progress, so a crash mid-Drain re-delivers.
	replayed, err := readOff(t.offPath)
	if err != nil {
		_ = f.Close()
		return err
	}
	if replayed < 0 || replayed > size {
		replayed = 0
	}
	t.replayed = replayed
	last, has, err := scanLastPosition(t.path, size)
	if err != nil {
		_ = f.Close()
		return err
	}
	t.last, t.hasLast = last, has
	return nil
}

func truncatePartialLine(f *os.File, size int64) (int64, error) {
	const chunk = 4096
	buf := make([]byte, chunk)
	pos := size
	for pos > 0 {
		start := pos - chunk
		if start < 0 {
			start = 0
		}
		n := int(pos - start)
		if _, err := f.ReadAt(buf[:n], start); err != nil {
			return 0, err
		}
		for i := n - 1; i >= 0; i-- {
			if buf[i] == '\n' {
				cut := start + int64(i) + 1
				if cut < size {
					if err := f.Truncate(cut); err != nil {
						return 0, err
					}
				}
				return cut, nil
			}
		}
		pos = start
	}
	if err := f.Truncate(0); err != nil {
		return 0, err
	}
	return 0, nil
}

// truncateIncompleteBatch drops a trailing unfenced event prefix so a crashed
// AppendBatch cannot advance LastPosition / xid dedup. Batches always end with
// a fence line; if none are present the whole file is an incomplete first
// AppendBatch and is truncated to empty.
func truncateIncompleteBatch(f *os.File, path string, size int64) (int64, error) {
	if size == 0 {
		return 0, nil
	}
	src, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer src.Close()
	reader := bufio.NewReader(src)
	var (
		offset      int64
		lastFenceAt int64
		sawFence    bool
	)
	for offset < size {
		line, err := reader.ReadBytes('\n')
		if len(line) == 0 && errors.Is(err, io.EOF) {
			break
		}
		end := offset + int64(len(line))
		if errors.Is(err, io.EOF) && (len(line) == 0 || line[len(line)-1] != '\n') {
			// incomplete trailing line already handled by truncatePartialLine
			break
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		trim := line
		if len(trim) > 0 && trim[len(trim)-1] == '\n' {
			trim = trim[:len(trim)-1]
		}
		if len(trim) == 0 {
			offset = end
			continue
		}
		var row spillLine
		if err := json.Unmarshal(trim, &row); err != nil {
			return 0, fmt.Errorf("spill: decode %s: %w", path, err)
		}
		if row.Fence {
			sawFence = true
			lastFenceAt = end
		}
		offset = end
		if errors.Is(err, io.EOF) {
			break
		}
	}
	if !sawFence {
		if err := f.Truncate(0); err != nil {
			return 0, err
		}
		return 0, nil
	}
	if lastFenceAt < size {
		if err := f.Truncate(lastFenceAt); err != nil {
			return 0, err
		}
		return lastFenceAt, nil
	}
	return size, nil
}

func readOff(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	text := strings.TrimSpace(string(data))
	if text == "" {
		return 0, nil
	}
	return strconv.ParseInt(text, 10, 64)
}

func writeOff(path string, offset int64) error {
	tmp := path + ".tmp"
	body := []byte(strconv.FormatInt(offset, 10) + "\n")
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	_ = f.Close()
	return os.Rename(tmp, path)
}

func scanLastPosition(path string, size int64) (checkpoint.Position, bool, error) {
	if size == 0 {
		return checkpoint.Position{}, false, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return checkpoint.Position{}, false, err
	}
	defer f.Close()
	var (
		last checkpoint.Position
		has  bool
	)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var row spillLine
		if err := json.Unmarshal(line, &row); err != nil {
			return checkpoint.Position{}, false, fmt.Errorf("spill: decode %s: %w", path, err)
		}
		if row.Fence {
			last, has = row.Pos, true
		}
	}
	if err := sc.Err(); err != nil {
		return checkpoint.Position{}, false, err
	}
	return last, has, nil
}

// AppendBatch writes one transaction's events for a table and syncs once.
// Progress (LastPosition / xid dedup) advances only after a fence line is synced.
func (s *Spill) AppendBatch(id string, pos checkpoint.Position, items []event.Event) error {
	tab, err := s.table(id)
	if err != nil {
		return err
	}
	tab.mu.Lock()
	defer tab.mu.Unlock()
	if tab.closed {
		return ErrSpillClosed
	}
	start, err := tab.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	rollback := func() {
		_ = tab.f.Truncate(start)
		_, _ = tab.f.Seek(0, io.SeekEnd)
	}
	for i := range items {
		ev := items[i]
		row := spillLine{Pos: pos, Event: &ev}
		body, err := json.Marshal(row)
		if err != nil {
			rollback()
			return err
		}
		if _, err := tab.f.Write(append(body, '\n')); err != nil {
			rollback()
			return err
		}
	}
	fence, err := json.Marshal(spillLine{Pos: pos, Fence: true})
	if err != nil {
		rollback()
		return err
	}
	if _, err := tab.f.Write(append(fence, '\n')); err != nil {
		rollback()
		return err
	}
	if err := tab.f.Sync(); err != nil {
		rollback()
		return err
	}
	tab.last, tab.hasLast = pos, true
	return nil
}

// Drain replays unread spill rows then closes the table queue.
// It does not persist replay offsets; see PersistReplay and package comment.
func (s *Spill) Drain(id string, fn func(event.Event) error) error {
	tab, err := s.table(id)
	if err != nil {
		return err
	}
	for {
		batch, err := tab.readBatch()
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		for _, row := range batch {
			if err := fn(row.event); err != nil {
				return err
			}
			tab.mu.Lock()
			tab.replayed = row.end
			tab.mu.Unlock()
		}
	}
}

// PersistReplay writes the in-memory replay offset after the caller has made
// delivered events durable (sink flush + checkpoint). Safe to call after Drain.
func (s *Spill) PersistReplay(id string) error {
	tab, err := s.table(id)
	if err != nil {
		return err
	}
	tab.mu.Lock()
	defer tab.mu.Unlock()
	return writeOff(tab.offPath, tab.replayed)
}

type spillBatchRow struct {
	event event.Event
	end   int64
}

func (t *spillTable) readBatch() ([]spillBatchRow, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, nil
	}
	info, err := t.f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if t.replayed >= size {
		t.closed = true
		return nil, nil
	}
	if _, err := t.f.Seek(t.replayed, io.SeekStart); err != nil {
		return nil, err
	}
	reader := bufio.NewReader(t.f)
	var batch []spillBatchRow
	offset := t.replayed
	for {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			if len(line) > 0 {
				// incomplete trailing line; leave for truncate on next open
				break
			}
			break
		}
		if err != nil {
			return nil, err
		}
		end := offset + int64(len(line))
		trim := line
		if len(trim) > 0 && trim[len(trim)-1] == '\n' {
			trim = trim[:len(trim)-1]
		}
		if len(trim) == 0 {
			offset = end
			continue
		}
		var row spillLine
		if err := json.Unmarshal(trim, &row); err != nil {
			return nil, fmt.Errorf("spill: decode: %w", err)
		}
		if row.Fence {
			offset = end
			continue
		}
		if row.Event == nil {
			offset = end
			continue
		}
		batch = append(batch, spillBatchRow{event: *row.Event, end: end})
		offset = end
	}
	if _, err := t.f.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	if len(batch) == 0 {
		t.closed = true
		return nil, nil
	}
	return batch, nil
}

// LastPosition returns the last XID position written for the table.
func (s *Spill) LastPosition(id string) (checkpoint.Position, bool, error) {
	tab, err := s.table(id)
	if err != nil {
		return checkpoint.Position{}, false, err
	}
	tab.mu.Lock()
	defer tab.mu.Unlock()
	return tab.last, tab.hasLast, nil
}

// Remove deletes the table's spill files and leaves a closed tombstone so later
// AppendBatch returns ErrSpillClosed instead of opening a new orphan file.
func (s *Spill) Remove(id string) error {
	s.mu.Lock()
	tab := s.tab[id]
	if tab == nil {
		s.tab[id] = s.tombstone(id)
	} else {
		tab.mu.Lock()
		tab.closed = true
		if tab.f != nil {
			if err := tab.f.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				tab.mu.Unlock()
				s.mu.Unlock()
				return fmt.Errorf("spill: close %s: %w", id, err)
			}
			tab.f = nil
		}
		tab.mu.Unlock()
	}
	binlogPath, offPath := spillPaths(s.dir, id)
	if tab != nil {
		binlogPath, offPath = tab.path, tab.offPath
	}
	s.mu.Unlock()
	var first error
	for _, path := range []string{binlogPath, offPath} {
		if path == "" {
			continue
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			if first == nil {
				first = fmt.Errorf("spill: remove %s: %w", path, err)
			}
		}
	}
	return first
}

func xidAlreadySpilled(last checkpoint.Position, hasLast bool, pos checkpoint.Position) bool {
	if !hasLast {
		return false
	}
	if last.File != "" && pos.File != "" {
		if binlogFileAfter(pos.File, last.File) {
			return false
		}
		if binlogFileAfter(last.File, pos.File) {
			return true
		}
		return pos.Pos <= last.Pos
	}
	if last.File == "" && pos.File == "" && last.GTID != "" && pos.GTID != "" {
		return last.GTID == pos.GTID
	}
	return false
}
