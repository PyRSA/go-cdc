package logfmt

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
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
	logger := slog.New(h).With("module", "pipeline")
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
	_ = logger
}
