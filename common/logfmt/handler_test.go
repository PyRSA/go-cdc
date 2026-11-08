package logfmt

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHandlerFormat(t *testing.T) {
	var buf bytes.Buffer
	h := NewHandler(&buf, slog.LevelDebug)
	logger := slog.New(h).With("module", "pipeline")
	logger.Info("Pipeline started", "name", "demo")

	line := buf.String()
	if !strings.Contains(line, " [INFO] [pipeline] Pipeline started name=demo\n") {
		t.Fatalf("unexpected line: %q", line)
	}
	if !strings.Contains(line[:23], ".") {
		t.Fatalf("expected millisecond timestamp, got %q", line)
	}
}

func TestHandlerMultilineMessage(t *testing.T) {
	var buf bytes.Buffer
	h := NewHandler(&buf, slog.LevelInfo)
	ts := time.Date(2026, 11, 7, 15, 35, 0, 0, time.Local)
	record := slog.NewRecord(ts, slog.LevelInfo, "Pipeline status\n  Uptime: 5m0s\n", 0)
	record.Add("name", "demo")
	if err := h.WithAttrs([]slog.Attr{slog.String("module", "pipeline")}).Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	wantPrefix := "2026-11-07 15:35:00.000 [INFO] [pipeline] Pipeline status name=demo\n  Uptime: 5m0s\n"
	if got != wantPrefix {
		t.Fatalf("got %q want %q", got, wantPrefix)
	}
}

func TestHandlerWithGroupPrefixesKeys(t *testing.T) {
	var buf bytes.Buffer
	h := NewHandler(&buf, slog.LevelInfo)
	logger := slog.New(h).WithGroup("req").WithGroup("peer")
	ts := time.Date(2026, 11, 8, 10, 0, 0, 0, time.Local)
	record := slog.NewRecord(ts, slog.LevelInfo, "hello", 0)
	record.Add("ip", "127.0.0.1")
	if err := logger.Handler().Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	if !strings.Contains(got, " req.peer.ip=127.0.0.1\n") {
		t.Fatalf("nested groups not applied: %q", got)
	}
}

func TestHandlerWithAttrsKeepsOpenGroupPrefix(t *testing.T) {
	var buf bytes.Buffer
	h := NewHandler(&buf, slog.LevelInfo)
	logger := slog.New(h).WithGroup("src").With("table", "orders").WithGroup("meta")
	ts := time.Date(2026, 11, 8, 10, 0, 0, 0, time.Local)
	record := slog.NewRecord(ts, slog.LevelInfo, "snap", 0)
	record.Add("chunk", 2)
	if err := logger.Handler().Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	if !strings.Contains(got, " src.table=orders") {
		t.Fatalf("WithAttrs under group lost prefix: %q", got)
	}
	if !strings.Contains(got, " src.meta.chunk=2\n") {
		t.Fatalf("later WithGroup not applied to record attrs: %q", got)
	}
}

func TestHandlerGroupAttrInline(t *testing.T) {
	var buf bytes.Buffer
	h := NewHandler(&buf, slog.LevelInfo)
	ts := time.Date(2026, 11, 8, 10, 0, 0, 0, time.Local)
	record := slog.NewRecord(ts, slog.LevelInfo, "g", 0)
	record.AddAttrs(slog.Group("auth", slog.String("user", "alice"), slog.Int("id", 7)))
	if err := h.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	if !strings.Contains(got, " auth.user=alice") || !strings.Contains(got, " auth.id=7\n") {
		t.Fatalf("KindGroup not expanded: %q", got)
	}
}

func TestHandlerEmptyWithGroupIsNoop(t *testing.T) {
	h := NewHandler(&bytes.Buffer{}, slog.LevelInfo)
	if h.WithGroup("") != h {
		t.Fatal("empty WithGroup must return the same handler")
	}
}

func TestHandlerQuotesControlCharacters(t *testing.T) {
	var buf bytes.Buffer
	h := NewHandler(&buf, slog.LevelInfo)
	ts := time.Date(2026, 11, 8, 10, 0, 0, 0, time.Local)
	record := slog.NewRecord(ts, slog.LevelInfo, "x", 0)
	record.Add("note", "a\nb\tc")
	if err := h.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	if strings.Count(got, "\n") != 1 {
		t.Fatalf("control chars must stay on one log line: %q", got)
	}
	if !strings.Contains(got, `note="a\nb\tc"`) {
		t.Fatalf("expected quoted escape, got %q", got)
	}
}

func TestHandlerSharedMutexAcrossDerived(t *testing.T) {
	var buf bytes.Buffer
	root := NewHandler(&buf, slog.LevelInfo)
	a := root.WithAttrs([]slog.Attr{slog.String("module", "a")})
	b := root.WithGroup("g")
	aH, ok2 := a.(*Handler)
	bH, ok3 := b.(*Handler)
	if !ok2 || !ok3 {
		t.Fatal("expected *Handler")
	}
	if root.mu != aH.mu || root.mu != bH.mu {
		t.Fatal("WithAttrs/WithGroup must share the writer lock")
	}

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			logger := slog.New(a)
			logger.Info("concurrent", "i", i)
		}(i)
	}
	wg.Wait()
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != n {
		t.Fatalf("got %d lines want %d (possible interleaving)", len(lines), n)
	}
	for _, line := range lines {
		if !strings.Contains(line, " [INFO] [a] concurrent i=") {
			t.Fatalf("malformed line: %q", line)
		}
	}
}
