package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/PyRSA/go-cdc/common/logfmt"
	"github.com/PyRSA/go-cdc/composer/definition"
)

// Setup installs slog.Default from cfg. When cfg.Configured is false the current
// default is left alone. The returned closer flushes and closes any file writer.
func Setup(cfg definition.Logging) (io.Closer, error) {
	if !cfg.Configured {
		return nopCloser{}, nil
	}
	level, err := slogLevel(cfg.Level)
	if err != nil {
		return nil, err
	}
	format := strings.ToLower(cfg.Format)
	if format == "" {
		format = "text"
	}

	var (
		handlers []slog.Handler
		closers  []io.Closer
	)

	console := cfg.Console
	file := cfg.File
	if console == nil && file == nil {
		// logging: with neither target keeps the historical stderr console.
		console = &definition.LoggingConsole{Stream: "stderr"}
	}

	if console != nil {
		w, err := consoleWriter(console.Stream)
		if err != nil {
			return nil, err
		}
		handlers = append(handlers, newHandler(w, format, level))
	}
	if file != nil {
		rolling, err := openFileFromConfig(*file)
		if err != nil {
			return nil, err
		}
		handlers = append(handlers, newHandler(rolling, format, level))
		closers = append(closers, rolling)
	}

	slog.SetDefault(slog.New(newMultiHandler(handlers...)))
	return multiCloser(closers), nil
}

func openFileFromConfig(file definition.LoggingFile) (*RollingFile, error) {
	opts := RollingOptions{
		Path:         file.Path,
		SizeLimit:    file.SizeBytes,
		Every:        file.Every,
		RetainAge:    file.RetainAge,
		RetainCount:  file.RetainCount,
		CompressGzip: file.CompressGzip,
	}
	if file.DailyAt != nil {
		opts.Daily = true
		opts.DailyHour = file.DailyAt.Hour
		opts.DailyMinute = file.DailyAt.Minute
		if opts.Every <= 0 {
			opts.Every = 24 * time.Hour
		}
	}
	return OpenRollingFile(opts)
}

func newHandler(w io.Writer, format string, level slog.Level) slog.Handler {
	switch format {
	case "json":
		return slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	default:
		return logfmt.NewHandler(w, level)
	}
}

func consoleWriter(stream string) (io.Writer, error) {
	switch strings.ToLower(stream) {
	case "", "stderr":
		return os.Stderr, nil
	case "stdout":
		return os.Stdout, nil
	default:
		return nil, fmt.Errorf("logging.console.stream: must be stdout or stderr")
	}
}

func slogLevel(level string) (slog.Level, error) {
	switch strings.ToUpper(strings.TrimSpace(level)) {
	case "", "INFO":
		return slog.LevelInfo, nil
	case "DEBUG":
		return slog.LevelDebug, nil
	case "WARN", "WARNING":
		return slog.LevelWarn, nil
	case "ERROR":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("logging.level: must be DEBUG, INFO, WARN or ERROR")
	}
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

type multiCloser []io.Closer

func (c multiCloser) Close() error {
	var first error
	for i := len(c) - 1; i >= 0; i-- {
		if err := c[i].Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
