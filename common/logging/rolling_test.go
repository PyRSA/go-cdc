package logging

import (
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRollingFileSizeRotationAndGzip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{
		Path:         path,
		SizeLimit:    64,
		CompressGzip: true,
		RetainCount:  5,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	payload := strings.Repeat("x", 40)
	if _, err := io.WriteString(r, payload+"\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(r, payload+"\n"); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var gzCount int
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".gz") {
			gzCount++
			f, err := os.Open(filepath.Join(dir, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			gr, err := gzip.NewReader(f)
			if err != nil {
				f.Close()
				t.Fatal(err)
			}
			body, err := io.ReadAll(gr)
			_ = gr.Close()
			_ = f.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(body), "x") {
				t.Fatalf("gzip body empty: %q", body)
			}
		}
	}
	if gzCount == 0 {
		t.Fatalf("expected a gzip archive, entries=%v", names(entries))
	}
}

func TestRollingFileTimeRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{
		Path:  path,
		Every: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	base := time.Date(2026, 11, 14, 10, 0, 0, 0, time.Local)
	r.now = func() time.Time { return base }
	r.nextRotateAt = base.Add(time.Hour)
	if _, err := io.WriteString(r, "one\n"); err != nil {
		t.Fatal(err)
	}
	r.now = func() time.Time { return base.Add(time.Hour + time.Second) }
	if _, err := io.WriteString(r, "two\n"); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 2 {
		t.Fatalf("expected active + archive, got %v", names(entries))
	}
}

func TestRollingFileRetentionByCount(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{
		Path:        path,
		SizeLimit:   16,
		RetainCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for i := 0; i < 5; i++ {
		if _, err := io.WriteString(r, strings.Repeat("y", 20)+"\n"); err != nil {
			t.Fatal(err)
		}
	}
	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) > 1 {
		t.Fatalf("retain count 1, got %d archives", len(archives))
	}
}

func TestRollingFileRetentionByAge(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	old := filepath.Join(dir, "app-2000-01-01T00-00-00.000.log")
	if err := os.WriteFile(old, []byte("old\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(old, past, past); err != nil {
		t.Fatal(err)
	}
	r, err := OpenRollingFile(RollingOptions{
		Path:      path,
		SizeLimit: 16,
		RetainAge: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("old archive should be pruned on open: %v", err)
	}
}

func TestRetentionIgnoresNonArchiveNeighbors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	neighbors := map[string]string{
		"app-backup.log": "backup\n",
		"app-notes.txt":  "notes\n",
		"app.log.bak":    "bak\n",
		"readme.txt":     "readme\n",
		"app-notats.log": "notats\n",
	}
	for name, body := range neighbors {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Real archives (sequenced + historical + gzip) must still be recognized.
	realPlain := filepath.Join(dir, "app-2000-01-01T00-00-00.000-1.log")
	realHist := filepath.Join(dir, "app-2000-01-02T00-00-00.000.log")
	realGz := filepath.Join(dir, "app-2000-01-03T00-00-00.000-2.log.gz")
	for _, p := range []string{realPlain, realHist} {
		if err := os.WriteFile(p, []byte("archive\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		past := time.Now().Add(-time.Minute)
		_ = os.Chtimes(p, past, past)
	}
	if err := os.WriteFile(realGz, []byte("not-real-gzip-but-named"), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.Chtimes(realGz, time.Now().Add(-time.Minute), time.Now().Add(-time.Minute))

	r, err := OpenRollingFile(RollingOptions{
		Path:        path,
		SizeLimit:   16,
		RetainCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for name, want := range neighbors {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("neighbor %s removed: %v", name, err)
		}
		if string(got) != want {
			t.Fatalf("neighbor %s mutated: %q", name, got)
		}
	}
	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) != 1 {
		t.Fatalf("retain=1 should keep one logical archive, got %d (%v)", len(archives), archiveKeys(archives))
	}
	if _, err := io.WriteString(r, strings.Repeat("x", 20)+"\n"); err != nil {
		t.Fatal(err)
	}
	for name := range neighbors {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("neighbor %s deleted during rotate/prune: %v", name, err)
		}
	}
}

func TestIsArchivedFileName(t *testing.T) {
	cases := []struct {
		fname, name, ext string
		want             bool
	}{
		{"app-2026-11-14T12-00-00.000-1.log", "app", ".log", true},
		{"app-2026-11-14T12-00-00.000-1.log.gz", "app", ".log", true},
		{"app-2026-11-14T12-00-00.000.log", "app", ".log", true},
		{"app-backup.log", "app", ".log", false},
		{"app-notes.txt", "app", ".log", false},
		{"app.log", "app", ".log", false},
		{"app-2026-11-14T12-00-00.000-x.log", "app", ".log", false},
		{"other-2026-11-14T12-00-00.000-1.log", "app", ".log", false},
		// Extension-less active paths normalize to .log for generation/recognition.
		{"app-2026-11-14T12-00-00.000-1.log", "app", ".log", true},
		{"app-2026-11-14T12-00-00.000-1", "app", ".log", false},
		// Active path already ends with .gz: plain vs compressed (.gz.gz).
		{"app-2026-11-14T12-00-00.000-1.gz", "app", ".gz", true},
		{"app-2026-11-14T12-00-00.000-1.gz.gz", "app", ".gz", true},
		{"app-2026-11-14T12-00-00.000.gz", "app", ".gz", true},
		{"app-backup.gz", "app", ".gz", false},
	}
	for _, tc := range cases {
		if got := isArchivedFileName(tc.fname, tc.name, tc.ext); got != tc.want {
			t.Fatalf("%s name=%q ext=%q: got %v want %v", tc.fname, tc.name, tc.ext, got, tc.want)
		}
	}
}

func TestArchiveBasePartsEmptyExtUsesLog(t *testing.T) {
	name, ext := archiveBaseParts(filepath.Join("logs", "app"))
	if name != "app" || ext != ".log" {
		t.Fatalf("got name=%q ext=%q", name, ext)
	}
	name, ext = archiveBaseParts(filepath.Join("logs", "app.gz"))
	if name != "app" || ext != ".gz" {
		t.Fatalf("got name=%q ext=%q", name, ext)
	}
}

func TestRetentionPathWithoutExtGeneratesDotLog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app") // no extension → archives use .log
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 16, RetainCount: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	frozen := time.Date(2026, 11, 14, 12, 0, 0, 0, time.Local)
	r.now = func() time.Time { return frozen }
	if _, err := io.WriteString(r, strings.Repeat("n", 20)+"\n"); err != nil {
		t.Fatal(err)
	}
	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) == 0 {
		t.Fatal("archive not listed")
	}
	found := false
	for _, arch := range archives {
		for _, p := range arch.paths {
			base := filepath.Base(p)
			if strings.Contains(base, frozen.Format(archivedTimeLayout)) && strings.HasSuffix(strings.TrimSuffix(base, ".gz"), ".log") {
				found = true
			}
			if isArchivedFileName(base, "app", "") {
				t.Fatalf("must not recognize with empty ext: %s", base)
			}
		}
	}
	if !found {
		t.Fatalf("expected .log archive for extension-less path, got %+v", archives)
	}
}

func TestRetentionGzExtensionActivePath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.gz")
	plain := filepath.Join(dir, "app-2000-01-01T00-00-00.000-1.gz")
	compressed := filepath.Join(dir, "app-2000-01-02T00-00-00.000-2.gz.gz")
	neighbor := filepath.Join(dir, "app-backup.gz")
	if err := os.WriteFile(plain, []byte("plain\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(compressed, []byte("comp\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(neighbor, []byte("keep\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-time.Minute)
	_ = os.Chtimes(plain, past, past)
	_ = os.Chtimes(compressed, past, past)

	r, err := OpenRollingFile(RollingOptions{
		Path:        path,
		SizeLimit:   16,
		RetainCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if _, err := os.Stat(neighbor); err != nil {
		t.Fatalf("neighbor removed: %v", err)
	}
	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) != 1 {
		t.Fatalf("want 1 logical archive for .gz active path, got %d (%v)", len(archives), archiveKeys(archives))
	}
	key := archives[0].key
	if !strings.HasSuffix(key, ".gz") || strings.HasSuffix(key, ".gz.gz") {
		t.Fatalf("group key should be plain archive ending in .gz, got %q", key)
	}
}

func TestPathWithoutExtGzipUsesDotLogGz(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app")
	r, err := OpenRollingFile(RollingOptions{
		Path:         path,
		SizeLimit:    16,
		CompressGzip: true,
		RetainCount:  5,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := io.WriteString(r, strings.Repeat("z", 20)+"\n"); err != nil {
		t.Fatal(err)
	}
	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) == 0 {
		t.Fatal("gzip archive not listed")
	}
	var sawLogGz bool
	for _, arch := range archives {
		for _, p := range arch.paths {
			base := filepath.Base(p)
			if strings.HasSuffix(base, ".log.gz") {
				sawLogGz = true
			}
		}
	}
	if !sawLogGz {
		t.Fatalf("extension-less path should archive as *.log.gz, got %+v", archives)
	}
}

func TestRollingFileCreatesParentDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "deep", "app.log")
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := io.WriteString(r, "ok\n"); err != nil {
		t.Fatal(err)
	}
}

func TestRollingFileConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if _, err := io.WriteString(r, "concurrent\n"); err != nil {
					t.Errorf("write: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func names(entries []os.DirEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

// TestRollingFileOversizedRecordTwoRotatesInOneWrite covers pre-rotate of an
// existing file plus post-rotate of a single record that alone fills the limit.
// Archive names must stay unique under one frozen clock.
func TestRollingFileOversizedRecordTwoRotatesInOneWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{
		Path:         path,
		SizeLimit:    32,
		CompressGzip: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	frozen := time.Date(2026, 11, 14, 12, 0, 0, 0, time.Local)
	r.now = func() time.Time { return frozen }

	if _, err := io.WriteString(r, strings.Repeat("a", 20)+"\n"); err != nil {
		t.Fatal(err)
	}
	huge := strings.Repeat("b", 40) + "\n"
	if _, err := io.WriteString(r, huge); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(r, "c\n"); err != nil {
		t.Fatal(err)
	}

	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) < 2 {
		t.Fatalf("want at least two archives after pre+post rotate, got %v", names(mustReadDir(t, dir)))
	}
	seen := map[string]bool{}
	var sawHuge bool
	for _, arch := range archives {
		if seen[arch.key] {
			t.Fatalf("duplicate archive key %s", arch.key)
		}
		seen[arch.key] = true
		for _, p := range arch.paths {
			body := readMaybeGzip(t, p)
			if strings.Contains(body, "bbbb") {
				sawHuge = true
			}
		}
	}
	if !sawHuge {
		t.Fatal("oversized record missing from archives")
	}
	active, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(active), "c\n") {
		t.Fatalf("active file should hold the follow-up record: %q", active)
	}
}

func TestRollingFileCloseErrorClearsHandleAndAllowsRecover(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 1024})
	if err != nil {
		t.Fatal(err)
	}

	r.closeFile = func(f *os.File) error {
		_ = f.Close()
		return errors.New("boom close")
	}
	if err := r.Close(); err == nil {
		t.Fatal("expected close error")
	}
	if r.file != nil {
		t.Fatal("closed handle must not be retained after Close error")
	}

	r2, err := OpenRollingFile(RollingOptions{Path: filepath.Join(dir, "live.log"), SizeLimit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if _, err := io.WriteString(r2, "seed\n"); err != nil {
		t.Fatal(err)
	}
	r2.closeFile = func(f *os.File) error {
		_ = f.Close()
		return errors.New("boom close")
	}
	r2.mu.Lock()
	err = r2.rotateLocked(r2.now())
	alive := r2.file != nil
	r2.mu.Unlock()
	if err == nil {
		t.Fatal("expected close error from rotate")
	}
	if !alive {
		t.Fatal("active file must be reopened after close failure during rotate")
	}
	r2.closeFile = closeFileDefault
	if _, err := io.WriteString(r2, "recovered\n"); err != nil {
		t.Fatalf("write after close failure: %v", err)
	}
}

func TestRollingFileRenameNonConflictFailureReopensActive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := io.WriteString(r, "before\n"); err != nil {
		t.Fatal(err)
	}

	r.renameFile = func(src, dst string) error {
		return errors.New("rename exploded")
	}
	r.mu.Lock()
	err = r.rotateLocked(r.now())
	alive := r.file != nil
	r.mu.Unlock()
	if err == nil {
		t.Fatal("expected rename failure")
	}
	if !alive {
		t.Fatal("active handle must be restored after rename failure")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "before\n") {
		t.Fatalf("active bytes must survive failed rename: %q", body)
	}
	if _, err := io.WriteString(r, "after\n"); err != nil {
		t.Fatalf("subsequent write: %v", err)
	}
}

func TestRollingFileGzipFailureKeepsPlainArchiveAndActive(t *testing.T) {
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
	r.gzipFile = func(string) error { return errors.New("gzip broke") }

	if _, err := io.WriteString(r, strings.Repeat("z", 20)+"\n"); err == nil {
		t.Fatal("expected gzip error from post-rotate")
	}
	if r.file == nil {
		t.Fatal("active file must stay open after gzip failure")
	}
	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) == 0 {
		t.Fatal("plain archive must remain when gzip fails")
	}
	for _, arch := range archives {
		for _, p := range arch.paths {
			if strings.HasSuffix(p, ".gz") {
				t.Fatalf("unexpected gzip file %s", p)
			}
			body, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(body), "z") {
				t.Fatalf("archive lost data: %q", body)
			}
		}
	}
	r.gzipFile = nil
	if _, err := io.WriteString(r, "next\n"); err != nil {
		t.Fatalf("write after gzip failure: %v", err)
	}
}

func TestDailyRotateUsesCalendarDay(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	// Cut at 04:00 — exists on both sides of the spring-forward gap (02:00→03:00).
	r := &RollingFile{
		dailyAt: &clock{hour: 4, minute: 0},
		now:     time.Now,
	}
	// 2026-03-08 spring forward: after today's cut, next is calendar +1 day at 04:00.
	// now.Add(24h) lands at 05:00 because that civil day is 23 hours long.
	now := time.Date(2026, 3, 8, 5, 0, 0, 0, loc)
	next := r.computeNextRotate(now)
	want := time.Date(2026, 3, 9, 4, 0, 0, 0, loc)
	if !next.Equal(want) {
		t.Fatalf("got %s want %s", next, want)
	}
	skewed := now.Add(24 * time.Hour)
	if next.Equal(skewed) {
		t.Fatalf("calendar cut %s must differ from Add(24h)=%s on DST day", next, skewed)
	}
	before := time.Date(2026, 3, 9, 3, 0, 0, 0, loc)
	next = r.computeNextRotate(before)
	want = time.Date(2026, 3, 9, 4, 0, 0, 0, loc)
	if !next.Equal(want) {
		t.Fatalf("same-day got %s want %s", next, want)
	}
}

func TestRollingFileExistingArchiveNotOverwritten(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	frozen := time.Date(2026, 11, 14, 12, 0, 0, 0, time.Local)
	existing := filepath.Join(dir, "app-"+frozen.Format(archivedTimeLayout)+"-1.log")
	const marker = "historical-archive-content\n"
	if err := os.WriteFile(existing, []byte(marker), 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.now = func() time.Time { return frozen }
	if _, err := io.WriteString(r, "fresh-active\n"); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	err = r.rotateLocked(frozen)
	r.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(existing)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != marker {
		t.Fatalf("existing archive overwritten: got %q", got)
	}
	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	var sawFresh bool
	for _, arch := range archives {
		for _, p := range arch.paths {
			if p == existing {
				continue
			}
			body := readMaybeGzip(t, p)
			if strings.Contains(body, "fresh-active") {
				sawFresh = true
			}
		}
	}
	if !sawFresh {
		t.Fatal("rotated content missing from a new archive name")
	}
}

func TestRollingFileRestartSameTimestampDoesNotOverwrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	frozen := time.Date(2026, 11, 14, 15, 30, 0, 0, time.Local)

	r1, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	r1.now = func() time.Time { return frozen }
	if _, err := io.WriteString(r1, "instance-one\n"); err != nil {
		t.Fatal(err)
	}
	r1.mu.Lock()
	err = r1.rotateLocked(frozen)
	r1.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}

	firstArchives, err := listArchivePaths(dir, "app.log")
	if err != nil {
		t.Fatal(err)
	}
	if len(firstArchives) == 0 {
		t.Fatal("expected first instance archive")
	}
	firstBody := map[string]string{}
	for _, p := range firstArchives {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		firstBody[p] = string(b)
	}

	r2, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	r2.now = func() time.Time { return frozen }
	if _, err := io.WriteString(r2, "instance-two\n"); err != nil {
		t.Fatal(err)
	}
	r2.mu.Lock()
	err = r2.rotateLocked(frozen)
	r2.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	for p, want := range firstBody {
		got, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("historical archive missing %s: %v", p, err)
		}
		if string(got) != want {
			t.Fatalf("historical archive %s overwritten: got %q want %q", p, got, want)
		}
	}
	var sawTwo bool
	for _, p := range mustListArchivePaths(t, dir, "app.log") {
		if _, known := firstBody[p]; known {
			continue
		}
		if strings.Contains(readMaybeGzip(t, p), "instance-two") {
			sawTwo = true
		}
	}
	if !sawTwo {
		t.Fatal("second instance archive missing")
	}
}

func TestRollingFileSameTimestampManyRotatesUniqueNames(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	frozen := time.Date(2026, 11, 14, 18, 0, 0, 0, time.Local)
	r.now = func() time.Time { return frozen }

	for i := 0; i < 5; i++ {
		if _, err := io.WriteString(r, strings.Repeat("n", 12)+"\n"); err != nil {
			t.Fatal(err)
		}
	}
	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) < 5 {
		t.Fatalf("want >=5 archives, got %d", len(archives))
	}
	keys := map[string]bool{}
	for _, arch := range archives {
		if keys[arch.key] {
			t.Fatalf("duplicate logical archive %s", arch.key)
		}
		keys[arch.key] = true
	}
}

func TestRollingFileGzipRemovePlainFailureRetentionDedupes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	// Two older logical archives. With retainCount=2, correct logical counting
	// keeps the newest dual archive plus one older. Counting plain+.gz as two
	// would drop both older archives.
	older1 := filepath.Join(dir, "app-2000-01-01T00-00-00.000-1.log")
	older2 := filepath.Join(dir, "app-2000-01-02T00-00-00.000-1.log")
	for _, p := range []string{older1, older2} {
		if err := os.WriteFile(p, []byte("keep-"+filepath.Base(p)+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		past := time.Now().Add(-time.Minute)
		if err := os.Chtimes(p, past, past); err != nil {
			t.Fatal(err)
		}
	}

	r, err := OpenRollingFile(RollingOptions{
		Path:         path,
		SizeLimit:    16,
		CompressGzip: true,
		RetainCount:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	r.removeFile = func(name string) error {
		base := filepath.Base(name)
		if base != "app.log" && strings.HasPrefix(base, "app-") &&
			!strings.Contains(base, "2000-") && !strings.HasSuffix(name, ".gz") {
			return errors.New("refuse remove plain")
		}
		return os.Remove(name)
	}

	if _, err := io.WriteString(r, strings.Repeat("q", 20)+"\n"); err == nil {
		t.Fatal("expected error from plain remove after gzip")
	}

	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) != 2 {
		t.Fatalf("logical retain=2 should keep 2 archives, got %d (%v)", len(archives), archiveKeys(archives))
	}
	var dualCount, olderSurvivors int
	for _, arch := range archives {
		if len(arch.paths) >= 2 {
			dualCount++
		}
		for _, p := range arch.paths {
			if strings.Contains(p, "2000-") {
				olderSurvivors++
			}
		}
	}
	if dualCount != 1 {
		t.Fatalf("expected one dual plain+.gz logical archive, got %+v", archives)
	}
	if olderSurvivors != 1 {
		t.Fatalf("expected exactly one older archive to survive, got %+v", archives)
	}
}

func TestRollingFileGzipFailureThenRotateKeepsOriginal(t *testing.T) {
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
	frozen := time.Date(2026, 11, 14, 20, 0, 0, 0, time.Local)
	r.now = func() time.Time { return frozen }
	r.gzipFile = func(string) error { return errors.New("gzip broke") }

	if _, err := io.WriteString(r, strings.Repeat("z", 20)+"\n"); err == nil {
		t.Fatal("expected gzip error")
	}
	first, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || len(first[0].paths) != 1 {
		t.Fatalf("want one plain archive, got %+v", first)
	}
	plain := first[0].paths[0]
	wantBody, err := os.ReadFile(plain)
	if err != nil {
		t.Fatal(err)
	}

	r.gzipFile = nil
	if _, err := io.WriteString(r, strings.Repeat("w", 20)+"\n"); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(plain)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(wantBody) {
		t.Fatalf("original plain archive changed: got %q want %q", got, wantBody)
	}
	if _, err := os.Stat(plain + ".gz"); !os.IsNotExist(err) {
		t.Fatalf("failed archive must not be re-compressed in place: %v", err)
	}
}

func TestRollingFileCloseAndReopenBothFail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		r.closeFile = closeFileDefault
		r.openActive = r.openActiveDefault
		_ = r.Close()
	}()

	if _, err := io.WriteString(r, "seed\n"); err != nil {
		t.Fatal(err)
	}
	r.closeFile = func(f *os.File) error {
		_ = f.Close()
		return errors.New("close failed")
	}
	r.openActive = func() error {
		return errors.New("reopen failed")
	}

	r.mu.Lock()
	err = r.rotateLocked(r.now())
	fileNil := r.file == nil
	r.mu.Unlock()
	if err == nil {
		t.Fatal("expected combined close/reopen error")
	}
	if !strings.Contains(err.Error(), "close failed") || !strings.Contains(err.Error(), "reopen failed") {
		t.Fatalf("error should include both causes: %v", err)
	}
	if !fileNil {
		t.Fatal("file handle must stay nil when reopen fails")
	}
	if err := r.Sync(); err == nil {
		t.Fatal("Sync must not succeed without an active file")
	}

	// Restore open; Write should recover without reporting success on a nil handle.
	r.closeFile = closeFileDefault
	r.openActive = r.openActiveDefault
	if _, err := io.WriteString(r, "recovered\n"); err != nil {
		t.Fatalf("write after restoring open: %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "seed\n") || !strings.Contains(string(body), "recovered\n") {
		t.Fatalf("data lost across reopen failure: %q", body)
	}
}

func TestRollingFileSyncNoActiveFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	r.mu.Lock()
	_ = r.closeActive()
	r.mu.Unlock()
	if err := r.Sync(); err == nil {
		t.Fatal("expected sync error when active file is missing")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(); err != nil {
		t.Fatalf("Sync after Close should be nil, got %v", err)
	}
}

func TestArchiveCopyMidFailLeavesNoFinalName(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	frozen := time.Date(2026, 11, 14, 12, 0, 0, 0, time.Local)
	r.now = func() time.Time { return frozen }
	if _, err := io.WriteString(r, "payload\n"); err != nil {
		t.Fatal(err)
	}

	r.copyFn = func(dst io.Writer, src io.Reader) (int64, error) {
		buf := make([]byte, 4)
		n, _ := src.Read(buf)
		_, _ = dst.Write(buf[:n])
		return int64(n), errors.New("copy exploded")
	}

	r.mu.Lock()
	err = r.rotateLocked(frozen)
	r.mu.Unlock()
	if err == nil {
		t.Fatal("expected copy failure")
	}
	final := filepath.Join(dir, "app-"+frozen.Format(archivedTimeLayout)+"-1.log")
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatalf("final archive must not exist after copy fail: %v", err)
	}
	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) != 0 {
		t.Fatalf("listArchived must ignore temps/partials, got %+v", archives)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "payload\n") {
		t.Fatalf("source must remain: %q", body)
	}
}

func TestArchiveCopyFailCleanupFailStillNoFinal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := io.WriteString(r, "keep\n"); err != nil {
		t.Fatal(err)
	}

	r.copyFn = func(dst io.Writer, src io.Reader) (int64, error) {
		return 0, errors.New("copy failed")
	}
	r.removeFile = func(name string) error {
		if strings.Contains(filepath.Base(name), ".archive-") || strings.HasSuffix(name, ".tmp") {
			return errors.New("temp cleanup refused")
		}
		return os.Remove(name)
	}

	r.mu.Lock()
	err = r.rotateLocked(r.now())
	r.mu.Unlock()
	if err == nil {
		t.Fatal("expected failure")
	}
	if !strings.Contains(err.Error(), "cleanup temp") {
		t.Fatalf("cleanup failure should be visible: %v", err)
	}
	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) != 0 {
		t.Fatalf("temps must not count as archives: %+v", archives)
	}
}

func TestArchiveSyncFailDoesNotPublish(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	frozen := time.Date(2026, 11, 14, 13, 0, 0, 0, time.Local)
	r.now = func() time.Time { return frozen }
	if _, err := io.WriteString(r, "data\n"); err != nil {
		t.Fatal(err)
	}
	r.syncFn = func(*os.File) error { return errors.New("sync failed") }

	r.mu.Lock()
	err = r.rotateLocked(frozen)
	r.mu.Unlock()
	if err == nil {
		t.Fatal("expected sync failure")
	}
	final := filepath.Join(dir, "app-"+frozen.Format(archivedTimeLayout)+"-1.log")
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatalf("must not publish after sync fail")
	}
}

func TestArchivePublishConflictDoesNotOverwrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	frozen := time.Date(2026, 11, 14, 14, 0, 0, 0, time.Local)
	existing := filepath.Join(dir, "app-"+frozen.Format(archivedTimeLayout)+"-1.log")
	const marker = "do-not-touch\n"
	if err := os.WriteFile(existing, []byte(marker), 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.now = func() time.Time { return frozen }
	if _, err := io.WriteString(r, "new-bytes\n"); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	err = r.rotateLocked(frozen)
	r.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(existing)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != marker {
		t.Fatalf("overwritten: %q", got)
	}
}

func TestArchiveRemoveSrcFailClearsActiveNoDuplicate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 8, RetainCount: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	frozen := time.Date(2026, 11, 14, 16, 0, 0, 0, time.Local)
	r.now = func() time.Time { return frozen }

	older := filepath.Join(dir, "app-2000-01-01T00-00-00.000-1.log")
	if err := os.WriteFile(older, []byte("older\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.Chtimes(older, time.Now().Add(-time.Minute), time.Now().Add(-time.Minute))

	var published string
	r.removeFile = func(name string) error {
		base := filepath.Base(name)
		if name == path || base == "app.log" {
			return errors.New("refuse remove active")
		}
		return os.Remove(name)
	}

	if _, err := io.WriteString(r, strings.Repeat("a", 12)+"\n"); err != nil {
		t.Fatal(err)
	}
	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	// After rotate: older + one new logical archive (active cleared despite remove fail).
	if len(archives) != 2 {
		t.Fatalf("want 2 logical archives, got %d (%v)", len(archives), archiveKeys(archives))
	}
	for _, arch := range archives {
		for _, p := range arch.paths {
			if strings.Contains(p, frozen.Format(archivedTimeLayout)) {
				published = p
				body := readMaybeGzip(t, p)
				if !strings.Contains(body, "aaaa") {
					t.Fatalf("published archive missing content: %q", body)
				}
			}
		}
	}
	if published == "" {
		t.Fatal("expected published archive")
	}
	active, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(active), "aaaa") {
		t.Fatalf("active must be cleared to avoid re-archive, got %q", active)
	}

	// Next rotate must not duplicate the first payload.
	if _, err := io.WriteString(r, strings.Repeat("b", 12)+"\n"); err != nil {
		t.Fatal(err)
	}
	firstBody := readMaybeGzip(t, published)
	if strings.Contains(firstBody, "bbbb") {
		t.Fatal("first archive mutated")
	}
	archives, err = r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) != 2 {
		t.Fatalf("retain=2 should keep 2 after second rotate, got %d", len(archives))
	}
}

func TestGzipCopyFailLeavesNoFinalGz(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 16, CompressGzip: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.copyFn = func(dst io.Writer, src io.Reader) (int64, error) {
		// Fail only while writing through gzip (dst is *gzip.Writer), not archive copy.
		if _, ok := dst.(*gzip.Writer); ok {
			return 0, errors.New("gzip copy failed")
		}
		return io.Copy(dst, src)
	}
	if _, err := io.WriteString(r, strings.Repeat("z", 20)+"\n"); err == nil {
		t.Fatal("expected gzip failure")
	}
	for _, e := range mustReadDir(t, dir) {
		if strings.HasSuffix(e.Name(), ".gz") {
			t.Fatalf("final .gz must not exist: %s", e.Name())
		}
	}
	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) != 1 || len(archives[0].paths) != 1 {
		t.Fatalf("want plain-only archive, got %+v", archives)
	}
	body, err := os.ReadFile(archives[0].paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "z") {
		t.Fatalf("plain lost: %q", body)
	}
}

func TestGzipCloseFailKeepsPlain(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 16, CompressGzip: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.gzipWriterClose = func(gz *gzip.Writer) error {
		_ = gz.Close()
		return errors.New("gz close failed")
	}
	if _, err := io.WriteString(r, strings.Repeat("c", 20)+"\n"); err == nil {
		t.Fatal("expected close failure")
	}
	for _, e := range mustReadDir(t, dir) {
		if strings.HasSuffix(e.Name(), ".gz") {
			t.Fatalf("final .gz must not exist after close fail: %s", e.Name())
		}
	}
	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) != 1 {
		t.Fatalf("want plain archive, got %+v", archives)
	}
	body, err := os.ReadFile(archives[0].paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "c") {
		t.Fatalf("plain incomplete: %q", body)
	}
}

func TestGzipTempCleanupFailStillNoFinalCount(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 16, CompressGzip: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.copyFn = func(dst io.Writer, src io.Reader) (int64, error) {
		if _, ok := dst.(*gzip.Writer); ok {
			return 0, errors.New("gzip boom")
		}
		return io.Copy(dst, src)
	}
	r.removeFile = func(name string) error {
		base := filepath.Base(name)
		if strings.HasPrefix(base, ".gz-") || (strings.HasPrefix(base, ".") && strings.HasSuffix(base, ".tmp")) {
			return errors.New("refuse temp delete")
		}
		return os.Remove(name)
	}
	_, writeErr := io.WriteString(r, strings.Repeat("g", 20)+"\n")
	if writeErr == nil {
		t.Fatal("expected failure")
	}
	if !strings.Contains(writeErr.Error(), "cleanup temp") {
		t.Fatalf("cleanup error missing: %v", writeErr)
	}
	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	for _, arch := range archives {
		for _, p := range arch.paths {
			if strings.HasSuffix(p, ".gz") {
				t.Fatalf("final gz counted: %s", p)
			}
		}
	}
}

func TestGzipPublishConflictDoesNotOverwrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	frozen := time.Date(2026, 11, 14, 17, 0, 0, 0, time.Local)
	// Pre-create the plain name that seq=1 will use, then we'll place .gz beside
	// a published plain from a first rotate without compression... simpler:
	// rotate without gzip first, then compress with conflict on .gz.
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 16})
	if err != nil {
		t.Fatal(err)
	}
	r.now = func() time.Time { return frozen }
	if _, err := io.WriteString(r, strings.Repeat("p", 20)+"\n"); err != nil {
		t.Fatal(err)
	}
	_ = r.Close()

	archives := mustListArchivePaths(t, dir, "app.log")
	if len(archives) != 1 {
		t.Fatalf("want 1 plain archive, got %v", archives)
	}
	plain := archives[0]
	existingGz := plain + ".gz"
	const marker = "historical-gz"
	if err := os.WriteFile(existingGz, []byte(marker), 0o644); err != nil {
		t.Fatal(err)
	}

	r2, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 1024, CompressGzip: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	// Compress the existing plain via compressArchived directly.
	if err := r2.gzipToArchive(plain); err == nil {
		t.Fatal("expected publish conflict")
	}
	got, err := os.ReadFile(existingGz)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != marker {
		t.Fatalf("existing gz overwritten: %q", got)
	}
	plainBody, err := os.ReadFile(plain)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(plainBody), "p") {
		t.Fatalf("plain must remain: %q", plainBody)
	}
}

func TestGzipRetryAfterFailureSucceeds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 16, CompressGzip: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	failOnce := true
	r.copyFn = func(dst io.Writer, src io.Reader) (int64, error) {
		if _, ok := dst.(*gzip.Writer); ok && failOnce {
			failOnce = false
			return 0, errors.New("transient gzip fail")
		}
		return io.Copy(dst, src)
	}
	if _, err := io.WriteString(r, strings.Repeat("r", 20)+"\n"); err == nil {
		t.Fatal("expected first gzip failure")
	}
	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) != 1 || len(archives[0].paths) != 1 {
		t.Fatalf("want plain after fail, got %+v", archives)
	}
	plain := archives[0].paths[0]
	if err := r.gzipToArchive(plain); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if _, err := os.Stat(plain); !os.IsNotExist(err) {
		t.Fatal("plain should be removed after successful gzip")
	}
	gzBody := readMaybeGzip(t, plain+".gz")
	if !strings.Contains(gzBody, "r") {
		t.Fatalf("gz missing content: %q", gzBody)
	}
}

func TestTempFilesDoNotAffectRetention(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	// Plant temps that must not count, plus two real archives.
	if err := os.WriteFile(filepath.Join(dir, ".archive-leak.tmp"), []byte("tmp"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".gz-leak.tmp"), []byte("tmp"), 0o644); err != nil {
		t.Fatal(err)
	}
	old1 := filepath.Join(dir, "app-2000-01-01T00-00-00.000-1.log")
	old2 := filepath.Join(dir, "app-2000-01-02T00-00-00.000-1.log")
	for _, p := range []string{old1, old2} {
		if err := os.WriteFile(p, []byte("old\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		_ = os.Chtimes(p, time.Now().Add(-time.Minute), time.Now().Add(-time.Minute))
	}
	r, err := OpenRollingFile(RollingOptions{Path: path, SizeLimit: 16, RetainCount: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	archives, err := r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) != 2 {
		t.Fatalf("temps must not inflate archive count, got %d", len(archives))
	}
	if _, err := io.WriteString(r, strings.Repeat("n", 20)+"\n"); err != nil {
		t.Fatal(err)
	}
	archives, err = r.listArchived()
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) != 2 {
		t.Fatalf("retain=2 with temps present: got %d keys %v", len(archives), archiveKeys(archives))
	}
	var survivors int
	for _, arch := range archives {
		for _, p := range arch.paths {
			if strings.Contains(p, "2000-") {
				survivors++
			}
		}
	}
	if survivors != 1 {
		t.Fatalf("exactly one older archive should survive, got %+v", archives)
	}
}

func mustReadDir(t *testing.T, dir string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func readMaybeGzip(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(path, ".gz") {
		return string(raw)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer gr.Close()
	body, err := io.ReadAll(gr)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func listArchivePaths(dir, activeBase string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	prefix := strings.TrimSuffix(activeBase, filepath.Ext(activeBase)) + "-"
	var out []string
	for _, e := range entries {
		if e.IsDir() || e.Name() == activeBase {
			continue
		}
		if strings.HasPrefix(e.Name(), prefix) {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	return out, nil
}

func mustListArchivePaths(t *testing.T, dir, activeBase string) []string {
	t.Helper()
	out, err := listArchivePaths(dir, activeBase)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func archiveKeys(archives []archivedFile) []string {
	out := make([]string, 0, len(archives))
	for _, a := range archives {
		out = append(out, a.key)
	}
	return out
}
