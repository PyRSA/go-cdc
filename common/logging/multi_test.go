package logging

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/logfmt"
)

func TestMultiHandlerEmptyWithGroupReturnsSame(t *testing.T) {
	var a, b bytes.Buffer
	h := newMultiHandler(
		logfmt.NewHandler(&a, slog.LevelInfo),
		logfmt.NewHandler(&b, slog.LevelInfo),
	)
	m, ok := h.(*multiHandler)
	if !ok {
		t.Fatalf("expected *multiHandler, got %T", h)
	}
	if got := m.WithGroup(""); got != m {
		t.Fatal("empty WithGroup must return the same handler")
	}
	if got := m.WithGroup("req"); got == m {
		t.Fatal("non-empty WithGroup must return a child")
	}
	_ = m.Handle(context.Background(), slog.NewRecord(time.Time{}, slog.LevelInfo, "x", 0))
}
