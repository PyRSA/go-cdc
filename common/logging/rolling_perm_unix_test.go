//go:build !windows

package logging

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestArchiveAndGzipPreserveSourceMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{
		Path:         path,
		SizeLimit:    16,
		CompressGzip: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	const wantMode = os.FileMode(0o640)
	if err := os.Chmod(path, wantMode); err != nil {
		t.Fatal(err)
	}
	activeInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	activePerm := activeInfo.Mode().Perm()
	if activePerm != wantMode {
		t.Fatalf("active perm=%o want %o (umask may differ; using actual %o)", activePerm, wantMode, activePerm)
	}

	if _, err := io.WriteString(r, strings.Repeat("m", 20)+"\n"); err != nil {
		t.Fatal(err)
	}
	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) == 0 {
		t.Fatal("expected archive")
	}
	var sawPlain, sawGz bool
	for _, arch := range archives {
		for _, p := range arch.paths {
			info, err := os.Stat(p)
			if err != nil {
				t.Fatal(err)
			}
			got := info.Mode().Perm()
			if got != activePerm {
				t.Fatalf("%s perm=%o want active perm %o", p, got, activePerm)
			}
			if strings.HasSuffix(p, ".gz") {
				sawGz = true
			} else {
				sawPlain = true
			}
		}
	}
	// With gzip success plain is removed; only .gz remains.
	if !sawGz && !sawPlain {
		t.Fatal("no archive paths")
	}
	if !sawGz {
		t.Fatal("expected gzip archive with preserved mode")
	}
}

func TestPlainArchivePreservesSourceMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	const wantMode = os.FileMode(0o604)
	if err := os.Chmod(path, wantMode); err != nil {
		t.Fatal(err)
	}
	activeInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	activePerm := activeInfo.Mode().Perm()

	if _, err := io.WriteString(r, strings.Repeat("p", 20)+"\n"); err != nil {
		t.Fatal(err)
	}
	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) != 1 || len(archives[0].paths) != 1 {
		t.Fatalf("want one plain archive, got %+v", archives)
	}
	info, err := os.Stat(archives[0].paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != activePerm {
		t.Fatalf("archive perm=%o want %o", info.Mode().Perm(), activePerm)
	}
}
