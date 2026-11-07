// Package logfmt provides a human-readable slog.Handler for go-cdc.
//
// Line shape:
//
//	2006-01-02 15:04:05.000 [LEVEL] [module] message key=value
package logfmt

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
)

const timeLayout = "2006-01-02 15:04:05.000"

// Handler writes slog records in the go-cdc text format.
type Handler struct {
	mu     sync.Mutex
	w      io.Writer
	level  slog.Level
	attrs  []slog.Attr
	groups []string
}

// NewHandler returns a Handler writing to w. Records below level are dropped.
func NewHandler(w io.Writer, level slog.Level) *Handler {
	return &Handler{w: w, level: level}
}

// Enabled reports whether level is emitted.
func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

// Handle formats and writes one record.
func (h *Handler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	var b strings.Builder
	b.Grow(256)
	b.WriteString(record.Time.Format(timeLayout))
	b.WriteByte(' ')
	b.WriteByte('[')
	b.WriteString(levelLabel(record.Level))
	b.WriteString("] [")

	module := "go-cdc"
	var rest []slog.Attr
	appendAttr := func(attr slog.Attr) {
		attr.Value = attr.Value.Resolve()
		if attr.Key == "module" && attr.Value.Kind() == slog.KindString {
			if v := attr.Value.String(); v != "" {
				module = v
			}
			return
		}
		if attr.Equal(slog.Attr{}) {
			return
		}
		rest = append(rest, attr)
	}
	for _, attr := range h.attrs {
		appendAttr(attr)
	}
	record.Attrs(func(attr slog.Attr) bool {
		appendAttr(attr)
		return true
	})

	b.WriteString(module)
	b.WriteString("] ")

	msg := record.Message
	first, more, cut := strings.Cut(msg, "\n")
	b.WriteString(first)
	for _, attr := range rest {
		b.WriteByte(' ')
		writeAttr(&b, attr)
	}
	b.WriteByte('\n')
	if cut {
		b.WriteString(more)
		if !strings.HasSuffix(more, "\n") {
			b.WriteByte('\n')
		}
	}

	_, err := io.WriteString(h.w, b.String())
	return err
}

// WithAttrs returns a child handler that includes attrs on every record.
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	child := &Handler{w: h.w, level: h.level, groups: append([]string(nil), h.groups...)}
	child.attrs = append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return child
}

// WithGroup returns a child handler. Group names prefix subsequent attribute keys.
func (h *Handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	child := &Handler{w: h.w, level: h.level, attrs: append([]slog.Attr(nil), h.attrs...)}
	child.groups = append(append([]string(nil), h.groups...), name)
	return child
}

func levelLabel(level slog.Level) string {
	switch {
	case level < slog.LevelInfo:
		return "DEBUG"
	case level < slog.LevelWarn:
		return "INFO"
	case level < slog.LevelError:
		return "WARN"
	default:
		return "ERROR"
	}
}

func writeAttr(b *strings.Builder, attr slog.Attr) {
	b.WriteString(attr.Key)
	b.WriteByte('=')
	writeValue(b, attr.Value)
}

func writeValue(b *strings.Builder, value slog.Value) {
	switch value.Kind() {
	case slog.KindString:
		s := value.String()
		if strings.IndexFunc(s, func(r rune) bool { return r == ' ' || r == '"' || r == '=' }) >= 0 {
			fmt.Fprintf(b, "%q", s)
		} else {
			b.WriteString(s)
		}
	case slog.KindInt64:
		fmt.Fprintf(b, "%d", value.Int64())
	case slog.KindUint64:
		fmt.Fprintf(b, "%d", value.Uint64())
	case slog.KindFloat64:
		fmt.Fprintf(b, "%g", value.Float64())
	case slog.KindBool:
		fmt.Fprintf(b, "%t", value.Bool())
	case slog.KindDuration:
		b.WriteString(value.Duration().String())
	case slog.KindTime:
		b.WriteString(value.Time().Format(timeLayout))
	default:
		fmt.Fprintf(b, "%v", value.Any())
	}
}

// Module returns a logger that stamps every record with the given module name.
func Module(module string) *slog.Logger {
	return slog.Default().With("module", module)
}

var _ slog.Handler = (*Handler)(nil)
