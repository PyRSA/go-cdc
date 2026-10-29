// Command go-cdc runs one YAML pipeline.
// sink.type selects stdout, mysql, kafka, doris, elasticsearch, or postgresql.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/PyRSA/go-cdc/composer"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/sink"
	dorissink "github.com/PyRSA/go-cdc/connector/sink/doris"
	esSink "github.com/PyRSA/go-cdc/connector/sink/elasticsearch"
	kafkasink "github.com/PyRSA/go-cdc/connector/sink/kafka"
	mysqlsink "github.com/PyRSA/go-cdc/connector/sink/mysql"
	pgxsink "github.com/PyRSA/go-cdc/connector/sink/postgresql"
	"github.com/PyRSA/go-cdc/connector/sink/stdout"
	mysqlsrc "github.com/PyRSA/go-cdc/connector/source/mysql"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
	"github.com/PyRSA/go-cdc/runtime/transform"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) (code int) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Fprintln(os.Stderr, "usage: go-cdc <pipeline.yaml>")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Run one YAML pipeline (source → transform/route → sink).")
		fmt.Fprintln(os.Stderr, "Supported sinks in this release: stdout, mysql, kafka, doris, elasticsearch, postgresql.")
		return 0
	}
	if len(args) != 1 {
		logger.Error("usage: go-cdc <pipeline.yaml>")
		return 2
	}
	body, err := os.ReadFile(args[0])
	if err != nil {
		logger.Error("read pipeline", "err", err)
		return 1
	}
	pipe, err := definition.Parse(body)
	if err != nil {
		logger.Error("parse pipeline", "err", err)
		return 1
	}
	logger.Info("pipeline loaded", "name", pipe.Runtime.Name, "sink", pipe.Sink.Type, "startup", pipe.Source.StartupMode)

	// SIGINT/SIGTERM cancels startup and in-flight work. Created before the sink
	// opens so a Ctrl-C during the first connection is not ignored; the runner
	// still drains the pending batch before it saves the checkpoint.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store := checkpoint.Store{Path: pipe.Checkpoint.FilePath}
	state, err := store.Load()
	if err != nil {
		logger.Error("load checkpoint", "err", err)
		return 1
	}
	sk, err := openSink(ctx, pipe)
	if err != nil {
		// A signal during startup cancels the first connect; that is a stop, not a
		// failure, and matches the clean exit of a cancelled run.
		if errors.Is(err, context.Canceled) {
			logger.Info("pipeline stopped during startup", "name", pipe.Runtime.Name)
			return 0
		}
		logger.Error("open sink", "err", err)
		return 1
	}
	// A close failure is never swallowed: the process still exits non-zero when
	// nothing else failed, so a broken sink is visible to the supervisor.
	defer func() {
		if err := sk.Close(); err != nil {
			logger.Error("close sink", "err", err)
			if code == 0 {
				code = 1
			}
		}
	}()

	db, err := mysqlsrc.OpenDB(pipe.Source)
	if err != nil {
		logger.Error("open source", "err", err)
		return 1
	}
	defer func() {
		if err := db.Close(); err != nil {
			logger.Error("close source", "err", err)
		}
	}()
	db.SetWarn(func(msg string, args ...any) {
		logger.Warn(msg, args...)
	})
	pipe.Source.ServerID = db.ServerID()
	stream, err := mysqlsrc.OpenStream(pipe.Source, db)
	if err != nil {
		logger.Error("open binlog stream", "err", err)
		return 1
	}
	defer func() {
		if err := stream.Close(); err != nil {
			logger.Error("close binlog stream", "err", err)
		}
	}()
	rules, err := transform.Compile(pipe.Transforms)
	if err != nil {
		logger.Error("compile transform", "err", err)
		return 1
	}
	// Only a CatalogSink needs its target tables before the first event, which
	// covers restarts and binlog-only starts where the source emits no Schema.
	// Other sinks keep their existing startup cost.
	if _, ok := sk.(composer.CatalogSink); ok {
		tables, err := db.ListTables(ctx)
		if err != nil {
			logger.Error("list source tables", "err", err)
			return 1
		}
		if err := composer.PrepareCatalog(pipe, sk, tables, state); err != nil {
			logger.Error("prepare sink catalog", "err", err)
			return 1
		}
	}
	spillDir := filepath.Join(filepath.Dir(pipe.Checkpoint.FilePath), "spill")
	if err := os.MkdirAll(spillDir, 0o755); err != nil {
		logger.Error("create spill dir", "err", err)
		return 1
	}
	spill := mysqlsrc.OpenSpill(spillDir)
	for _, id := range state.CapturedTables {
		if state.HasSnapshot(id) {
			continue
		}
		if err := spill.Remove(id); err != nil {
			logger.Error("spill cleanup", "table", id, "err", err)
			return 1
		}
	}
	capture := &mysqlsrc.Capture{
		Config:      pipe.Source,
		Parallelism: pipe.Runtime.Parallelism,
		State:       state,
		DB:          db,
		Stream:      stream,
		Spill:       spill,
		// Only Doris revalidates the target from a fresh source catalogue on
		// resume. MySQL / Kafka / stdout / Elasticsearch must not see an extra
		// Schema batch.
		ReemitCapturedSchemas: strings.EqualFold(pipe.Sink.Type, "doris"),
		Warn: func(msg string, args ...any) {
			logger.Warn(msg, args...)
		},
		Project: func(database, table string, columns []string) ([]string, error) {
			if err := rules.Validate(database, table, columns); err != nil {
				return nil, err
			}
			if projected := rules.ProjectedColumns(database, table); len(projected) > 0 {
				return projected, nil
			}
			return columns, nil
		},
	}
	msgs, err := capture.Messages(ctx)
	if err != nil {
		logger.Error("start capture", "err", err)
		return 1
	}
	runErr := composer.Run(ctx, pipe, msgs, sk, store, state, spill)
	// Cancel before the deferred closes so the source goroutines stop touching the
	// connection and the binlog stream first.
	stop()
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		logger.Error("pipeline failed", "err", runErr)
		return 1
	}
	logger.Info("pipeline stopped", "name", pipe.Runtime.Name)
	return 0
}

func openSink(ctx context.Context, pipe *definition.Pipeline) (sink.Sink, error) {
	switch pipe.Sink.Type {
	case "stdout":
		sk := stdout.New(os.Stdout)
		sk.SetSourceName(pipe.Runtime.Name)
		if err := sk.Open(ctx); err != nil {
			return nil, err
		}
		return sk, nil
	case "mysql":
		sk, err := mysqlsink.Open(pipe.Sink, pipe.Source.TimeZone)
		if err != nil {
			return nil, err
		}
		if err := sk.Open(ctx); err != nil {
			return nil, err
		}
		return sk, nil
	case "kafka":
		sk, err := kafkasink.Open(pipe.Sink, pipe.Runtime.Name)
		if err != nil {
			return nil, err
		}
		if err := sk.Open(ctx); err != nil {
			return nil, err
		}
		return sk, nil
	case "doris":
		sk, err := dorissink.Open(pipe.Sink)
		if err != nil {
			return nil, err
		}
		if err := sk.Open(ctx); err != nil {
			return nil, err
		}
		return sk, nil
	case "elasticsearch":
		sk, err := esSink.New(pipe.Sink)
		if err != nil {
			return nil, err
		}
		// The source emits TIMESTAMP as the wall clock of source.server-time-zone;
		// the sink needs that zone to store the same instant in UTC.
		if err := sk.SetServerTimeZone(pipe.Source.TimeZone); err != nil {
			return nil, err
		}
		if err := sk.Open(ctx); err != nil {
			// Open already released the client it created on failure.
			return nil, err
		}
		return sk, nil
	case "postgresql":
		sk, err := pgxsink.Open(pipe.Sink, pipe.Source.TimeZone)
		if err != nil {
			return nil, err
		}
		if err := sk.Open(ctx); err != nil {
			return nil, err
		}
		return sk, nil
	default:
		return nil, fmt.Errorf("sink.type: %s is not implemented in this release", pipe.Sink.Type)
	}
}
