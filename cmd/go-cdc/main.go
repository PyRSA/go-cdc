// Command go-cdc runs one YAML pipeline.
// sink.type selects stdout, mysql, kafka, doris, elasticsearch, or postgresql.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/PyRSA/go-cdc/common/logfmt"
	"github.com/PyRSA/go-cdc/common/logging"
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
	// Bootstrap logger so YAML load failures are visible before logging: applies.
	slog.SetDefault(slog.New(logfmt.NewHandler(os.Stderr, slog.LevelInfo)))

	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Fprintln(os.Stderr, "usage: go-cdc <pipeline.yaml>")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Run one YAML pipeline (source → transform/route → sink).")
		fmt.Fprintln(os.Stderr, "Supported sinks in this release: stdout, mysql, kafka, doris, elasticsearch, postgresql.")
		return 0
	}
	if len(args) != 1 {
		logfmt.Module("config").Error("Invalid usage: expected go-cdc <pipeline.yaml>")
		return 2
	}
	body, err := os.ReadFile(args[0])
	if err != nil {
		logfmt.Module("config").Error("Failed to read pipeline file", "path", args[0], "error", err)
		return 1
	}
	pipe, err := definition.Parse(body)
	if err != nil {
		logfmt.Module("config").Error("Failed to parse pipeline", "error", err)
		return 1
	}
	logCloser, err := logging.Setup(pipe.Logging)
	if err != nil {
		logfmt.Module("config").Error("Failed to configure logging", "error", err)
		return 1
	}
	// Close errors go to stderr directly: after Close the configured slog
	// handlers (including a file-only RollingFile) may already be unusable.
	defer func() {
		code = closeLogging(logCloser, os.Stderr, code)
	}()

	configLog := logfmt.Module("config")
	pipelineLog := logfmt.Module("pipeline")
	sourceLog := logfmt.Module("source")
	sinkLog := logfmt.Module("sink")
	checkpointLog := logfmt.Module("checkpoint")

	configLog.Info("Configuration loaded successfully",
		"name", pipe.Runtime.Name, "sink", pipe.Sink.Type, "startup", pipe.Source.StartupMode)

	// SIGINT/SIGTERM cancels startup and in-flight work. Created before the sink
	// opens so a Ctrl-C during the first connection is not ignored; the runner
	// still drains the pending batch before it saves the checkpoint.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store := checkpoint.Store{Path: pipe.Checkpoint.FilePath}
	state, err := store.Load()
	if err != nil {
		checkpointLog.Error("Failed to load checkpoint", "path", pipe.Checkpoint.FilePath, "error", err)
		return 1
	}
	switch {
	case state.AckedOffset != nil:
		checkpointLog.Info("Checkpoint loaded successfully",
			"path", pipe.Checkpoint.FilePath,
			"position", formatCheckpointPosition(state.AckedOffset))
	case checkpointHasProgress(state):
		checkpointLog.Info("Checkpoint loaded without binlog position",
			"path", pipe.Checkpoint.FilePath)
	default:
		checkpointLog.Info("No checkpoint found; starting fresh",
			"path", pipe.Checkpoint.FilePath)
	}

	sk, err := openSink(ctx, pipe)
	if err != nil {
		// A signal during startup cancels the first connect; that is a stop, not a
		// failure, and matches the clean exit of a cancelled run.
		if errors.Is(err, context.Canceled) {
			pipelineLog.Info("Pipeline stopped during startup", "name", pipe.Runtime.Name)
			return 0
		}
		sinkLog.Error("Failed to open sink", "sink", pipe.Sink.Type, "error", err)
		return 1
	}
	sinkLog.Info("Sink initialized successfully", "sink", pipe.Sink.Type)
	// A close failure is never swallowed: the process still exits non-zero when
	// nothing else failed, so a broken sink is visible to the supervisor.
	defer func() {
		if err := sk.Close(); err != nil {
			sinkLog.Error("Failed to close sink", "sink", pipe.Sink.Type, "error", err)
			if code == 0 {
				code = 1
			}
		}
	}()

	db, err := mysqlsrc.OpenDB(pipe.Source)
	if err != nil {
		sourceLog.Error("Failed to open MySQL source", "error", err)
		return 1
	}
	defer func() {
		if err := db.Close(); err != nil {
			sourceLog.Error("Failed to close MySQL source", "error", err)
		}
	}()
	db.SetWarn(func(msg string, args ...any) {
		sourceLog.Warn(msg, args...)
	})
	// Captured tables stay in the binlog allow-list after the selector narrows;
	// catalogue VECTOR checks must see the same set as LimitTables.
	db.SetCapturedTables(state.CapturedTables)
	// The release decides which replication statements the source may use; it is
	// logged once so a MySQL 8.4 startup failure can be read without guessing.
	sourceLog.Info("Connected to MySQL successfully", "server_version", db.ServerVersion())
	// The PostgreSQL sink reads the current source definition to turn a column
	// change that restates it into PostgreSQL DDL. It borrows the catalogue handle
	// the capture already owns and never closes it.
	if setter, ok := sk.(interface{ SetTableDescriber(pgxsink.TableDescriber) }); ok {
		setter.SetTableDescriber(db)
	}
	pipe.Source.ServerID = db.ServerID()
	stream, err := mysqlsrc.OpenStream(pipe.Source, db)
	if err != nil {
		sourceLog.Error("Failed to open binlog stream", "error", err)
		return 1
	}
	defer func() {
		if err := stream.Close(); err != nil {
			sourceLog.Error("Failed to close binlog stream", "error", err)
		}
	}()
	sourceLog.Info("Binlog stream initialized successfully")
	rules, err := transform.Compile(pipe.Transforms)
	if err != nil {
		pipelineLog.Error("Failed to compile transforms", "error", err)
		return 1
	}
	// Only a CatalogSink needs its target tables before the first event, which
	// covers restarts and binlog-only starts where the source emits no Schema.
	// Other sinks keep their existing startup cost.
	if _, ok := sk.(composer.CatalogSink); ok {
		tables, err := db.ListTables(ctx)
		if err != nil {
			sourceLog.Error("Failed to list source tables", "error", err)
			return 1
		}
		if err := composer.PrepareCatalog(pipe, sk, tables, state); err != nil {
			sinkLog.Error("Failed to prepare sink catalog", "error", err)
			return 1
		}
	}
	spillDir := filepath.Join(filepath.Dir(pipe.Checkpoint.FilePath), "spill")
	if err := os.MkdirAll(spillDir, 0o755); err != nil {
		pipelineLog.Error("Failed to create spill directory", "path", spillDir, "error", err)
		return 1
	}
	spill := mysqlsrc.OpenSpill(spillDir)
	defer func() {
		if err := spill.Close(); err != nil {
			pipelineLog.Error("Failed to close spill", "path", spillDir, "error", err)
			if code == 0 {
				code = 1
			}
		}
	}()
	for _, id := range state.CapturedTables {
		if state.HasSnapshot(id) {
			continue
		}
		if err := spill.Remove(id); err != nil {
			pipelineLog.Error("Failed to clean spill file", "table", id, "error", err)
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
		// Only Doris re-emits Schema from a fresh source catalogue on resume so
		// the sink can re-check the target. Other sinks still Describe captured
		// tables on default resume (VECTOR rejection) but must not see an extra
		// Schema batch.
		ReemitCapturedSchemas: strings.EqualFold(pipe.Sink.Type, "doris"),
		Warn: func(msg string, args ...any) {
			sourceLog.Warn(msg, args...)
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
		sourceLog.Error("Failed to start capture", "error", err)
		return 1
	}
	runErr := composer.Run(ctx, pipe, msgs, sk, store, state, spill)
	// Cancel before the deferred closes so the source goroutines stop touching the
	// connection and the binlog stream first.
	stop()
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		pipelineLog.Error("Pipeline failed", "name", pipe.Runtime.Name, "error", runErr)
		return 1
	}
	pipelineLog.Info("Pipeline stopped", "name", pipe.Runtime.Name)
	return 0
}

// closeLogging closes the logging closer and reports failures on stderrWriter
// so diagnostics remain visible after file-only handlers are shut down.
func closeLogging(closer io.Closer, stderrWriter io.Writer, code int) int {
	if closer == nil {
		return code
	}
	if err := closer.Close(); err != nil {
		fmt.Fprintf(stderrWriter, "go-cdc: failed to close logging: %v\n", err)
		if code == 0 {
			return 1
		}
	}
	return code
}

// checkpointHasProgress reports snapshot or capture progress saved before any binlog offset.
func checkpointHasProgress(state checkpoint.State) bool {
	if state.LowWatermark != nil || len(state.CapturedTables) > 0 || len(state.SnapshotTables) > 0 {
		return true
	}
	for _, splits := range state.FinishedSplits {
		if len(splits) > 0 {
			return true
		}
	}
	return false
}

func formatCheckpointPosition(pos *checkpoint.Position) string {
	if pos == nil {
		return "none"
	}
	if pos.File != "" || pos.Pos != 0 {
		out := fmt.Sprintf("%s:%d", pos.File, pos.Pos)
		if pos.GTID != "" {
			return out + " gtid=" + pos.GTID
		}
		return out
	}
	if pos.GTID != "" {
		return "gtid=" + pos.GTID
	}
	return "none"
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
