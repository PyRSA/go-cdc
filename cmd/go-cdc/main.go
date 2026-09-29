// Command go-cdc runs one YAML pipeline.
// sink.type selects stdout or mysql. Kafka, Doris, and Elasticsearch are rejected at startup.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/PyRSA/go-cdc/composer"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/sink"
	mysqlsink "github.com/PyRSA/go-cdc/connector/sink/mysql"
	"github.com/PyRSA/go-cdc/connector/sink/stdout"
	mysqlsrc "github.com/PyRSA/go-cdc/connector/source/mysql"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
	"github.com/PyRSA/go-cdc/runtime/transform"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Fprintln(os.Stderr, "usage: go-cdc <pipeline.yaml>")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Run one YAML pipeline (source → transform/route → sink).")
		fmt.Fprintln(os.Stderr, "Supported sinks in this release: stdout, mysql.")
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

	store := checkpoint.Store{Path: pipe.Checkpoint.FilePath}
	state, err := store.Load()
	if err != nil {
		logger.Error("load checkpoint", "err", err)
		return 1
	}
	sk, err := openSink(pipe)
	if err != nil {
		logger.Error("open sink", "err", err)
		return 1
	}
	defer sk.Close()

	db, err := mysqlsrc.OpenDB(pipe.Source)
	if err != nil {
		logger.Error("open source", "err", err)
		return 1
	}
	pipe.Source.ServerID = db.ServerID()
	stream, err := mysqlsrc.OpenStream(pipe.Source)
	if err != nil {
		_ = db.Close()
		logger.Error("open binlog stream", "err", err)
		return 1
	}
	rules, err := transform.Compile(pipe.Transforms)
	if err != nil {
		_ = db.Close()
		_ = stream.Close()
		logger.Error("compile transform", "err", err)
		return 1
	}
	capture := &mysqlsrc.Capture{
		Config:      pipe.Source,
		Parallelism: pipe.Runtime.Parallelism,
		State:       state,
		DB:          db,
		Stream:      stream,
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
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	msgs, err := capture.Messages(ctx)
	if err != nil {
		logger.Error("start capture", "err", err)
		return 1
	}
	if err := composer.Run(ctx, pipe, msgs, sk, store, state); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("pipeline failed", "err", err)
		return 1
	}
	logger.Info("pipeline stopped", "name", pipe.Runtime.Name)
	return 0
}

func openSink(pipe *definition.Pipeline) (sink.Sink, error) {
	switch pipe.Sink.Type {
	case "stdout":
		sk := stdout.New(os.Stdout)
		if err := sk.Open(context.Background()); err != nil {
			return nil, err
		}
		return sk, nil
	case "mysql":
		sk, err := mysqlsink.Open(pipe.Sink, pipe.Source.TimeZone)
		if err != nil {
			return nil, err
		}
		if err := sk.Open(context.Background()); err != nil {
			return nil, err
		}
		return sk, nil
	default:
		return nil, fmt.Errorf("sink.type: %s is not implemented in this release", pipe.Sink.Type)
	}
}
