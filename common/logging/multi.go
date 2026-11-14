package logging

import (
	"context"
	"log/slog"
)

// multiHandler fans a record out to every child. Enabled is true when any child
// would emit the level. Attrs and groups are applied to every child.
type multiHandler struct {
	handlers []slog.Handler
}

func newMultiHandler(handlers ...slog.Handler) slog.Handler {
	out := make([]slog.Handler, 0, len(handlers))
	for _, h := range handlers {
		if h != nil {
			out = append(out, h)
		}
	}
	if len(out) == 1 {
		return out[0]
	}
	return &multiHandler{handlers: out}
}

func (m *multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range m.handlers {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (m *multiHandler) Handle(ctx context.Context, record slog.Record) error {
	var first error
	for _, h := range m.handlers {
		if !h.Enabled(ctx, record.Level) {
			continue
		}
		if err := h.Handle(ctx, record.Clone()); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (m *multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	children := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		children[i] = h.WithAttrs(attrs)
	}
	return &multiHandler{handlers: children}
}

func (m *multiHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return m
	}
	children := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		children[i] = h.WithGroup(name)
	}
	return &multiHandler{handlers: children}
}
