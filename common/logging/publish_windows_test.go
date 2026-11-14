//go:build windows

package logging

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// TestMoveFileNoReplaceBypassesGoRenameReplace verifies the Windows fallback
// used when Link is unavailable: MoveFileExW with flags=0 must not overwrite.
func TestMoveFileNoReplaceBypassesGoRenameReplace(t *testing.T) {
	dir := t.TempDir()
	temp := filepath.Join(dir, ".mov-temp")
	final := filepath.Join(dir, "final.log")
	const marker = "keep-me\n"
	if err := os.WriteFile(final, []byte(marker), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(temp, []byte("replace-me\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Control: stdlib Rename on Windows replaces (MOVEFILE_REPLACE_EXISTING).
	renamed := filepath.Join(dir, "renamed-over.log")
	if err := os.WriteFile(renamed, []byte(marker), 0o644); err != nil {
		t.Fatal(err)
	}
	temp2 := filepath.Join(dir, ".mov-temp2")
	if err := os.WriteFile(temp2, []byte("via-rename\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temp2, renamed); err != nil {
		t.Fatalf("control Rename: %v", err)
	}
	if got, _ := os.ReadFile(renamed); string(got) == marker {
		t.Fatal("control expected Go Rename to replace existing file on Windows")
	}

	if err := moveFileNoReplace(temp, final); err == nil {
		t.Fatal("moveFileNoReplace must fail when final exists")
	}
	got, err := os.ReadFile(final)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != marker {
		t.Fatalf("moveFileNoReplace overwrote existing file: %q", got)
	}
	if _, err := os.Stat(temp); err != nil {
		t.Fatalf("temp must remain after failed move: %v", err)
	}
}

func TestMoveFileNoReplaceSuccessAndConflict(t *testing.T) {
	dir := t.TempDir()
	temp := filepath.Join(dir, ".ok-temp")
	final := filepath.Join(dir, "ok-final.log")
	if err := os.WriteFile(temp, []byte("ok\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := moveFileNoReplace(temp, final); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(temp); !os.IsNotExist(err) {
		t.Fatalf("temp should be moved away, stat=%v", err)
	}
	got, err := os.ReadFile(final)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "ok\n" {
		t.Fatalf("got %q", got)
	}

	temp2 := filepath.Join(dir, ".ok-temp2")
	if err := os.WriteFile(temp2, []byte("nope\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := moveFileNoReplace(temp2, final); err == nil {
		t.Fatal("expected conflict")
	}
	if got, _ = os.ReadFile(final); string(got) != "ok\n" {
		t.Fatalf("content changed: %q", got)
	}
}

func TestMoveFileNoReplaceConcurrentAtMostOne(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "race-final.log")
	const n = 16
	var success atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			temp := filepath.Join(dir, ".race-"+strconv.Itoa(i))
			if err := os.WriteFile(temp, []byte(strconv.Itoa(i)), 0o644); err != nil {
				t.Errorf("write: %v", err)
				return
			}
			if err := moveFileNoReplace(temp, final); err == nil {
				success.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if got := success.Load(); got != 1 {
		t.Fatalf("want 1 success, got %d", got)
	}
}
