//go:build !windows

package logging

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// TestPublishNoReplaceUsesHardLinkOnUnix locks the primary Linux/Unix path:
// os.Link must succeed for same-directory publishes and leave a hard link.
func TestPublishNoReplaceUsesHardLinkOnUnix(t *testing.T) {
	dir := t.TempDir()
	temp := filepath.Join(dir, ".link-temp")
	final := filepath.Join(dir, "app-2026-01-01T00-00-00.000-1.log")
	if err := os.WriteFile(temp, []byte("linked\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := publishNoReplace(temp, final); err != nil {
		t.Fatal(err)
	}
	tempInfo, err := os.Stat(temp)
	if err != nil {
		t.Fatal(err)
	}
	finalInfo, err := os.Stat(final)
	if err != nil {
		t.Fatal(err)
	}
	tempStat, ok1 := tempInfo.Sys().(*syscall.Stat_t)
	finalStat, ok2 := finalInfo.Sys().(*syscall.Stat_t)
	if !ok1 || !ok2 {
		t.Fatal("expected syscall.Stat_t")
	}
	if tempStat.Ino != finalStat.Ino {
		t.Fatalf("expected hard link same inode: temp=%d final=%d", tempStat.Ino, finalStat.Ino)
	}
	if tempStat.Nlink < 2 {
		t.Fatalf("expected nlink>=2 after Link, got %d", tempStat.Nlink)
	}
}

func TestPublishNoReplaceConflictLeavesNoEmptyClaim(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "app-2026-01-01T00-00-00.000-1.log")
	const marker = "existing\n"
	if err := os.WriteFile(final, []byte(marker), 0o644); err != nil {
		t.Fatal(err)
	}
	temp := filepath.Join(dir, ".link-temp")
	if err := os.WriteFile(temp, []byte("new\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := publishNoReplace(temp, final); err == nil {
		t.Fatal("expected conflict")
	}
	got, err := os.ReadFile(final)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != marker {
		t.Fatalf("final overwritten: %q", got)
	}
	after, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("publish must not create extra claim files: before=%d after=%d", len(before), len(after))
	}
	if _, err := os.Stat(temp); err != nil {
		t.Fatalf("temp must remain for caller cleanup: %v", err)
	}
}
