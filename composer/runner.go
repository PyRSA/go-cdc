// Package composer loads a pipeline and applies transform, route, and checkpoint rules.
package composer

import (
	"context"
	"fmt"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/sink"
	"github.com/PyRSA/go-cdc/connector/source"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
	"github.com/PyRSA/go-cdc/runtime/route"
	"github.com/PyRSA/go-cdc/runtime/schemaevol"
	"github.com/PyRSA/go-cdc/runtime/transform"
)

// Run reads capture messages, writes them through one sink, and replaces the
// checkpoint file on the configured interval and on shutdown.
func Run(ctx context.Context, pipe *definition.Pipeline, msgs <-chan source.Message, sk sink.Sink, store checkpoint.Store, state checkpoint.State) error {
	state.Normalize()
	rules, err := transform.Compile(pipe.Transforms)
	if err != nil {
		return err
	}
	routes, err := route.Compile(pipe.Routes, pipe.Runtime.RouteMode)
	if err != nil {
		return err
	}
	interval := pipe.Checkpoint.Interval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var batchTick <-chan time.Time
	if pipe.Sink.WriteBatchEvery > 0 {
		batchTicker := time.NewTicker(pipe.Sink.WriteBatchEvery)
		defer batchTicker.Stop()
		batchTick = batchTicker.C
	}

	var batch []event.Event
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := sk.Write(batch); err != nil {
			return err
		}
		batch = batch[:0]
		return sk.Flush()
	}
	save := func() error {
		if err := flush(); err != nil {
			return err
		}
		return store.Save(state)
	}

	for {
		select {
		case <-ctx.Done():
			return save()
		case <-ticker.C:
			if err := save(); err != nil {
				return err
			}
		case <-batchTick:
			if err := flush(); err != nil {
				return err
			}
		case msg, ok := <-msgs:
			if !ok {
				return save()
			}
			if msg.Err != nil {
				if err := save(); err != nil {
					return err
				}
				return msg.Err
			}
			if msg.Schema != nil {
				if err := flush(); err != nil {
					return err
				}
			}
			if err := applyMessage(pipe, rules, routes, sk, &state, &batch, msg); err != nil {
				return err
			}
			if msg.Split != nil || msg.Position != nil || msg.Watermark != nil || msg.Captured != "" || (pipe.Sink.WriteBatchSize > 0 && len(batch) >= pipe.Sink.WriteBatchSize) {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

func applyMessage(pipe *definition.Pipeline, rules *transform.Set, routes *route.Set, sk sink.Sink, state *checkpoint.State, batch *[]event.Event, msg source.Message) error {
	if msg.Watermark != nil && state.LowWatermark == nil {
		state.LowWatermark = &checkpoint.Position{File: msg.Watermark.File, Pos: msg.Watermark.Pos, GTID: msg.Watermark.GTID}
	}
	if msg.Schema != nil {
		if err := applyCatalog(pipe, rules, routes, sk, state, *msg.Schema); err != nil {
			return err
		}
	}
	if msg.Event != nil {
		if msg.Event.Op == event.OpDDL {
			return applyDDL(pipe, rules, routes, sk, state, *msg.Event, msg.Schema != nil)
		}
		projected, ok := rules.Apply(*msg.Event)
		if ok {
			*batch = append(*batch, routes.Apply(projected)...)
		}
	}
	if msg.Split != nil {
		state.AddSplit(msg.Split.Table, checkpoint.Split{Low: msg.Split.Low, High: msg.Split.High})
	}
	if msg.Captured != "" {
		state.AddCaptured(msg.Captured)
	}
	if msg.Position != nil {
		state.AckedOffset = &checkpoint.Position{File: msg.Position.File, Pos: msg.Position.Pos, GTID: msg.Position.GTID}
	}
	return nil
}

func applyCatalog(pipe *definition.Pipeline, rules *transform.Set, routes *route.Set, sk sink.Sink, state *checkpoint.State, table schema.Table) error {
	names := make([]string, len(table.Columns))
	for i, column := range table.Columns {
		names[i] = column.Name
	}
	if err := rules.Validate(table.Database, table.Table, names); err != nil {
		return err
	}
	if pipe.Sink.Type != "mysql" || !pipe.Sink.AutoCreate {
		return nil
	}
	projected := rules.ProjectTable(table)
	routed := routes.Apply(event.Event{
		Database: table.Database, Table: table.Table,
		SourceDatabase: table.Database, SourceTable: table.Table,
	})
	for _, copy := range routed {
		sinkTable := projected
		sinkTable.Database = copy.Database
		sinkTable.Table = copy.Table
		// Create is best-effort: failure must not exclude the table or stop the job.
		// A later Write against a missing table fails the job.
		_ = sk.CreateTable(sinkTable)
	}
	return nil
}

func applyDDL(pipe *definition.Pipeline, rules *transform.Set, routes *route.Set, sk sink.Sink, state *checkpoint.State, item event.Event, catalogApplied bool) error {
	routed := routes.Apply(item)
	for _, copy := range routed {
		projection := rules.ProjectedColumns(copy.SourceDatabase, copy.SourceTable)
		decision := schemaevol.Decide(copy.DDL, projection, pipe.Sink.DropTruncate)
		change := decision.Change
		change.Database = copy.Database
		change.Table = copy.Table
		change.DDL = schemaevol.RewriteTable(copy.DDL, copy.Database, copy.Table)
		if pipe.Sink.Type == "stdout" {
			change.DDL = copy.DDL
			if copy.SourceDatabase != "" {
				change.Database = copy.SourceDatabase
				change.Table = copy.SourceTable
			}
			if err := sk.ApplySchema(change); err != nil {
				return err
			}
			continue
		}
		if decision.Change.Action == schema.ActionCreate && pipe.Sink.AutoCreate {
			if catalogApplied {
				state.AddCaptured(copy.SourceID())
				continue
			}
			if len(copy.Columns) == 0 {
				return fmt.Errorf("mysql: CREATE %s has no column metadata", copy.SourceID())
			}
			table := schema.Table{Database: copy.Database, Table: copy.Table, Columns: columnsOf(copy), PrimaryKeys: copy.Keys}
			_ = sk.CreateTable(table)
			state.AddCaptured(copy.SourceID())
			continue
		}
		if decision.Execute {
			if err := sk.ApplySchema(change); err != nil {
				return err
			}
		}
	}
	return nil
}

func columnsOf(item event.Event) []schema.Column {
	cols := make([]schema.Column, 0, len(item.Columns))
	for _, name := range item.Columns {
		cols = append(cols, schema.Column{Name: name, Type: "text", Nullable: true})
	}
	return cols
}
