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
	"unicode"
)

const timeLayout = "2006-01-02 15:04:05.000"

// Handler writes slog records in the go-cdc text format.
// Derived handlers from WithAttrs / WithGroup share mu so concurrent writers
// cannot interleave bytes into w.
type Handler struct {
	mu     *sync.Mutex
	w      io.Writer
	level  slog.Level
	attrs  []slog.Attr
	groups []string
}

// NewHandler returns a Handler writing to w. Records below level are dropped.
func NewHandler(w io.Writer, level slog.Level) *Handler {
	return &Handler{mu: &sync.Mutex{}, w: w, level: level}
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
		for _, item := range expandAttr("", attr) {
			if item.Key == "module" && item.Value.Kind() == slog.KindString {
				if v := item.Value.String(); v != "" {
					module = v
				}
				continue
			}
			rest = append(rest, item)
		}
	}
	for _, attr := range h.attrs {
		appendAttr(attr)
	}
	record.Attrs(func(attr slog.Attr) bool {
		for _, item := range expandAttr(strings.Join(h.groups, "."), attr) {
			if item.Key == "module" && item.Value.Kind() == slog.KindString {
				if v := item.Value.String(); v != "" {
					module = v
				}
				continue
			}
			rest = append(rest, item)
		}
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
// Open groups are applied immediately so a later WithGroup does not re-prefix them.
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	child := &Handler{
		mu:     h.mu,
		w:      h.w,
		level:  h.level,
		groups: append([]string(nil), h.groups...),
	}
	prefix := strings.Join(h.groups, ".")
	child.attrs = append(append([]slog.Attr(nil), h.attrs...), expandAttrs(prefix, attrs)...)
	return child
}

// WithGroup returns a child handler. Group names prefix subsequent attribute keys.
func (h *Handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	child := &Handler{
		mu:    h.mu,
		w:     h.w,
		level: h.level,
		attrs: append([]slog.Attr(nil), h.attrs...),
	}
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

func expandAttrs(prefix string, attrs []slog.Attr) []slog.Attr {
	var out []slog.Attr
	for _, attr := range attrs {
		out = append(out, expandAttr(prefix, attr)...)
	}
	return out
}

func expandAttr(prefix string, attr slog.Attr) []slog.Attr {
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return nil
	}
	if attr.Value.Kind() == slog.KindGroup {
		gprefix := attr.Key
		if prefix != "" {
			if gprefix != "" {
				gprefix = prefix + "." + gprefix
			} else {
				gprefix = prefix
			}
		}
		return expandAttrs(gprefix, attr.Value.Group())
	}
	key := attr.Key
	if prefix != "" {
		key = prefix + "." + key
	}
	return []slog.Attr{{Key: key, Value: attr.Value}}
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
		if needsQuote(s) {
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
	case slog.KindGroup:
		// KindGroup should already be expanded; keep a fallback for safety.
		for i, attr := range value.Group() {
			if i > 0 {
				b.WriteByte(' ')
			}
			writeAttr(b, attr)
		}
	default:
		fmt.Fprintf(b, "%v", value.Any())
	}
}

// needsQuote reports whether a string value must use Go quoting so spaces,
// quotes, '=' and control characters cannot break the single-line attr layout.
func needsQuote(s string) bool {
	if s == "" {
		return true
	}
	return strings.IndexFunc(s, func(r rune) bool {
		return r == ' ' || r == '"' || r == '=' || r == '\\' ||
			r == '\n' || r == '\r' || r == '\t' ||
			unicode.IsControl(r)
	}) >= 0
}

// Module returns a logger that stamps every record with the given module name.
func Module(module string) *slog.Logger {
	return slog.Default().With("module", module)
}

var _ slog.Handler = (*Handler)(nil)
