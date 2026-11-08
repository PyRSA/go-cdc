package checkpoint

import (
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.ckpt")
	store := Store{Path: path}
	high := "8096"
	state := State{
		LowWatermark: &Position{File: "mysql-bin.000018", Pos: 154, GTID: "uuid:1-1000"},
		AckedOffset:  &Position{File: "mysql-bin.000018", Pos: 451, GTID: "uuid:1-1001"},
		FinishedSplits: map[string][]Split{
			"tests.users": {{Low: "1", High: &high}, {Low: "8096", High: nil}},
		},
		CapturedTables: []string{"tests.users"},
		ExcludedTables: []string{},
	}
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temp file left behind")
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.AckedOffset.Pos != 451 || loaded.FinishedSplits["tests.users"][1].High != nil {
		t.Fatalf("loaded %#v", loaded)
	}
	if !loaded.HasSplit("tests.users", Split{Low: "8096", High: nil}) {
		t.Fatal("missing open-ended split")
	}
}

func TestLoadMissingFile(t *testing.T) {
	store := Store{Path: filepath.Join(t.TempDir(), "missing.json")}
	state, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if state.AckedOffset != nil || state.FinishedSplits == nil || state.CapturedTables == nil || state.SnapshotTables == nil {
		t.Fatalf("empty state %#v", state)
	}
}

func TestSnapshotTablesRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.ckpt")
	store := Store{Path: path}
	state := State{
		AckedOffset:    &Position{File: "mysql-bin.000018", Pos: 451},
		CapturedTables: []string{"db.user"},
		SnapshotTables: map[string]Position{
			"db.order": {File: "mysql-bin.000018", Pos: 451},
		},
	}
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.HasSnapshot("db.order") || loaded.HasSnapshot("db.user") {
		t.Fatalf("%#v", loaded.SnapshotTables)
	}
	loaded.ClearSnapshot("db.order")
	if loaded.HasSnapshot("db.order") {
		t.Fatal("snapshot flag remained")
	}
}

func TestTempFileIsNotTheCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.ckpt")
	if err := os.WriteFile(path+".tmp", []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}
	state, err := (Store{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if state.LowWatermark != nil {
		t.Fatal("temp file was loaded")
	}
}

func TestDirSyncUnsupported(t *testing.T) {
	if !dirSyncUnsupported(&os.PathError{Op: "sync", Path: "dir", Err: syscall.ENOTSUP}) {
		t.Fatal("ENOTSUP must not fail the save")
	}
	denied := &os.PathError{Op: "sync", Path: "dir", Err: syscall.Errno(5)}
	if runtime.GOOS == "windows" {
		if !dirSyncUnsupported(denied) {
			t.Fatal("Windows directory sync access denied is not a failed save")
		}
		return
	}
	if dirSyncUnsupported(denied) {
		t.Fatal("Unix EIO must fail the save")
	}
}

// TestSaveFlushesCheckpointDirectory proves the successful Save path waits for
// the parent directory flush after rename. Without that call, a power loss can
// still show the previous checkpoint entry.
func TestSaveFlushesCheckpointDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.ckpt")
	var flushed string
	orig := flushCheckpointDir
	t.Cleanup(func() { flushCheckpointDir = orig })
	flushCheckpointDir = func(got string) error {
		flushed = got
		return syncDir(got)
	}
	if err := (Store{Path: path}).Save(State{
		AckedOffset: &Position{File: "mysql-bin.000001", Pos: 100},
	}); err != nil {
		t.Fatal(err)
	}
	if flushed != dir {
		t.Fatalf("Save must flush the checkpoint directory after rename: got %q want %q", flushed, dir)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("checkpoint must still be durable: %v", err)
	}
}
