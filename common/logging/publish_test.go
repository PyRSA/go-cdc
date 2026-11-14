package logging

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPublishNoReplaceSuccess(t *testing.T) {
	dir := t.TempDir()
	temp := filepath.Join(dir, ".pub-temp")
	final := filepath.Join(dir, "app-2026-01-01T00-00-00.000-1.log")
	if err := os.WriteFile(temp, []byte("fresh\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := publishNoReplace(temp, final); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(final)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "fresh\n" {
		t.Fatalf("got %q", got)
	}
}

func TestPublishNoReplaceExistingUnchanged(t *testing.T) {
	dir := t.TempDir()
	temp := filepath.Join(dir, ".pub-temp")
	final := filepath.Join(dir, "app-2026-01-01T00-00-00.000-1.log")
	const marker = "historical\n"
	if err := os.WriteFile(final, []byte(marker), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(temp, []byte("intruder\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pubErr := publishNoReplace(temp, final)
	if pubErr == nil {
		t.Fatal("expected conflict")
	}
	if !isTargetConflict(pubErr) {
		t.Fatalf("want conflict error, got %v", pubErr)
	}
	got, err := os.ReadFile(final)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != marker {
		t.Fatalf("existing overwritten: %q", got)
	}
	if _, err := os.Stat(temp); err != nil {
		t.Fatalf("temp should remain after failed publish: %v", err)
	}
}

func TestPublishNoReplaceConcurrentAtMostOne(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "app-2026-01-01T00-00-00.000-9.log")
	const n = 16
	var success atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			temp := filepath.Join(dir, ".pub-temp-"+strconv.Itoa(i))
			if err := os.WriteFile(temp, []byte("worker-"+strconv.Itoa(i)+"\n"), 0o644); err != nil {
				t.Errorf("write temp: %v", err)
				return
			}
			if err := publishNoReplace(temp, final); err == nil {
				success.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if got := success.Load(); got != 1 {
		t.Fatalf("want exactly 1 successful publish, got %d", got)
	}
	body, err := os.ReadFile(final)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) == 0 {
		t.Fatal("final empty")
	}
}

func TestPublishNoReplaceArchiveAndGzipNames(t *testing.T) {
	dir := t.TempDir()
	cases := []string{
		"app-2026-01-01T00-00-00.000-1.log",
		"app-2026-01-01T00-00-00.000-1.log.gz",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			temp := filepath.Join(dir, ".t-"+name)
			final := filepath.Join(dir, name)
			if err := os.WriteFile(temp, []byte("x\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := publishNoReplace(temp, final); err != nil {
				t.Fatal(err)
			}
			second := filepath.Join(dir, ".t2-"+name)
			if err := os.WriteFile(second, []byte("y\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := publishNoReplace(second, final); err == nil {
				t.Fatal("second publish must fail")
			}
			got, err := os.ReadFile(final)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != "x\n" {
				t.Fatalf("got %q", got)
			}
		})
	}
}
