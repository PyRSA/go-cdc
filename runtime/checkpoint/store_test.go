package checkpoint

import (
	"os"
	"path/filepath"
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
	if state.AckedOffset != nil || state.FinishedSplits == nil || state.CapturedTables == nil {
		t.Fatalf("empty state %#v", state)
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
