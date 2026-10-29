// Package composer loads a pipeline and applies transform, route, and checkpoint rules.
package composer

import (
	"context"
	"fmt"
	"strings"
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

// SpillFiles removes per-table spill artifacts after a durable checkpoint save.
type SpillFiles interface {
	Remove(id string) error
}

// Run reads capture messages, writes them through one sink, and replaces the
// checkpoint file on the configured interval and on shutdown.
func Run(ctx context.Context, pipe *definition.Pipeline, msgs <-chan source.Message, sk sink.Sink, store checkpoint.Store, state checkpoint.State, spill SpillFiles) error {
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
		// Always Flush so out-of-band sink writes (e.g. Kafka ApplySchema DDL)
		// are acknowledged before checkpoint, even when the DML batch is empty.
		if len(batch) > 0 {
			if err := sk.Write(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
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
			if err := applyMessage(pipe, rules, routes, sk, &state, &batch, msg, save, spill); err != nil {
				return err
			}
			if msg.Split != nil || msg.Position != nil || msg.Watermark != nil || msg.Captured != "" || msg.SnapshotTable != "" || (pipe.Sink.WriteBatchSize > 0 && len(batch) >= pipe.Sink.WriteBatchSize) {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

func applyMessage(pipe *definition.Pipeline, rules *transform.Set, routes *route.Set, sk sink.Sink, state *checkpoint.State, batch *[]event.Event, msg source.Message, save func() error, spill SpillFiles) error {
	if msg.Watermark != nil && state.LowWatermark == nil {
		state.LowWatermark = &checkpoint.Position{File: msg.Watermark.File, Pos: msg.Watermark.Pos, GTID: msg.Watermark.GTID}
	}
	if msg.SnapshotTable != "" && msg.SnapshotMark != nil {
		state.AddSnapshot(msg.SnapshotTable, checkpoint.Position{
			File: msg.SnapshotMark.File, Pos: msg.SnapshotMark.Pos, GTID: msg.SnapshotMark.GTID,
		})
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
		state.ClearSnapshot(msg.Captured)
		if save != nil {
			if err := save(); err != nil {
				return err
			}
		}
		if spill != nil {
			if err := spill.Remove(msg.Captured); err != nil {
				return err
			}
		}
	}
	if msg.Position != nil {
		state.AckedOffset = &checkpoint.Position{File: msg.Position.File, Pos: msg.Position.Pos, GTID: msg.Position.GTID}
	}
	if msg.CheckpointBarrier {
		if save != nil {
			if err := save(); err != nil {
				return err
			}
		}
		if msg.BarrierDone != nil {
			close(msg.BarrierDone)
		}
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
	if pipe.Sink.Type == "doris" {
		// Doris must validate every routed table before it becomes writable.
		// Unlike the MySQL branch below, a failure stops the job.
		projected := rules.ProjectTable(table)
		routed := routes.Apply(event.Event{
			Database: table.Database, Table: table.Table,
			SourceDatabase: table.Database, SourceTable: table.Table,
		})
		for _, dst := range routed {
			sinkTable := projected
			sinkTable.Database = dst.Database
			sinkTable.Table = dst.Table
			if err := sk.CreateTable(sinkTable); err != nil {
				return err
			}
		}
		return nil
	}
	// A CatalogSink prepares its own structures and must fail the job when it
	// cannot: an unprepared sink would only fail later, after Checkpoint moved.
	if catalog, ok := sk.(CatalogSink); ok {
		targets := catalogTargets(rules, routes, table)
		if err := checkCatalogTargets(targets); err != nil {
			return fmt.Errorf("%s.%s: %w", table.Database, table.Table, err)
		}
		return catalog.PrepareCatalog(targets)
	}
	if pipe.Sink.Type != "mysql" || !pipe.Sink.AutoCreate {
		return nil
	}
	for _, sinkTable := range catalogTargets(rules, routes, table) {
		// Create is best-effort: failure must not exclude the table or stop the job.
		// A later Write against a missing table fails the job.
		_ = sk.CreateTable(sinkTable)
	}
	return nil
}

func applyDDL(pipe *definition.Pipeline, rules *transform.Set, routes *route.Set, sk sink.Sink, state *checkpoint.State, item event.Event, catalogApplied bool) error {
	// The PostgreSQL sink does not map types or create tables in this release, so
	// a schema change stops the job instead of letting the target table drift.
	if pipe.Sink.Type == "postgresql" {
		return fmt.Errorf("sink.postgresql: schema change is not supported in this release (table %s.%s, ddl %q)", item.Database, item.Table, item.DDL)
	}
	routed := routes.Apply(item)
	for _, dst := range routed {
		projection := rules.ProjectedColumns(dst.SourceDatabase, dst.SourceTable)
		decision := schemaevol.Decide(dst.DDL, projection, pipe.Sink.DropTruncate)
		change := decision.Change
		change.Database = dst.Database
		change.Table = dst.Table
		change.DDL = schemaevol.RewriteTable(dst.DDL, dst.Database, dst.Table)
		if item.Change != nil {
			// Structured metadata recovered by the source. The sink must not
			// re-infer column type/nullability/default from DDL text.
			change.AddColumns = item.Change.AddColumns
			change.ColumnDef = item.Change.ColumnDef
		}
		if pipe.Sink.Type == "stdout" || pipe.Sink.Type == "kafka" {
			// Keep Database/Table as the routed sink id (topic = DML). Envelope source.*
			// uses SourceDatabase/SourceTable from the capturing table.
			change.DDL = dst.DDL
			change.SourceTSMs = item.SourceTSMs
			change.SourceDatabase = dst.SourceDatabase
			change.SourceTable = dst.SourceTable
			if err := sk.ApplySchema(change); err != nil {
				return err
			}
			continue
		}
		if pipe.Sink.Type == "doris" {
			if err := applyDorisDDL(sk, state, dst, decision, change, catalogApplied); err != nil {
				return err
			}
			continue
		}
		if decision.Change.Action == schema.ActionCreate && pipe.Sink.AutoCreate {
			if catalogApplied {
				state.AddCaptured(dst.SourceID())
				continue
			}
			// Only the MySQL sink can create a table from the event's column names.
			// Every other sink prepares tables through the catalog path above, so a
			// CREATE without catalog metadata must not invent a sink table; the
			// following Write then fails instead of silently dropping rows.
			if pipe.Sink.Type != "mysql" || len(dst.Columns) == 0 {
				continue
			}
			table := schema.Table{Database: dst.Database, Table: dst.Table, Columns: columnsOf(dst), PrimaryKeys: dst.Keys}
			_ = sk.CreateTable(table)
			state.AddCaptured(dst.SourceID())
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

// applyDorisDDL keeps Doris DDL failures fatal. CREATE was already executed and
// validated by applyCatalog, so the table only becomes capturable afterwards.
// Anything the sink cannot reproduce must fail instead of being skipped.
func applyDorisDDL(sk sink.Sink, state *checkpoint.State, dst event.Event, decision schemaevol.Decision, change schema.Change, catalogApplied bool) error {
	switch decision.Change.Action {
	case schema.ActionCreate:
		if !catalogApplied {
			return fmt.Errorf("sink.doris: CREATE for %s arrived without a source table definition", dst.SourceID())
		}
		state.AddCaptured(dst.SourceID())
		return nil
	case schema.ActionAddColumn, schema.ActionDropColumn, schema.ActionRenameColumn, schema.ActionModifyColumn:
		if !decision.Execute {
			return nil
		}
		return sk.ApplySchema(change)
	case schema.ActionDropTable, schema.ActionTruncate:
		if !decision.Execute {
			// drop.truncate.enabled=false is an explicit opt-out.
			return nil
		}
		return fmt.Errorf("sink.doris: %s on %s is not supported in v1", decision.Change.Action, dst.SinkID())
	case schema.ActionIndex:
		if strings.Contains(strings.ToUpper(decision.Change.DDL), "PRIMARY") {
			return fmt.Errorf("sink.doris: primary key change on %s is not supported in v1: %s", dst.SinkID(), decision.Change.DDL)
		}
		// Secondary indexes are never created on the Doris target, so their
		// changes have no effect there.
		return nil
	default:
		return fmt.Errorf("sink.doris: schema change %s on %s is not supported in v1", decision.Change.Action, dst.SinkID())
	}
}
