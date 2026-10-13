package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/connector/source"
)

func (c *Capture) resumeWithOptionalSnapshot(ctx context.Context, out chan<- source.Message) error {
	if c.State.AckedOffset == nil {
		return fmt.Errorf("source.mysql: resume requires acked_offset")
	}
	acked := *c.State.AckedOffset
	snapshotting := map[string]struct{}{}
	for id := range c.State.SnapshotTables {
		snapshotting[id] = struct{}{}
	}
	live := map[string]struct{}{}
	for _, id := range c.State.CapturedTables {
		live[id] = struct{}{}
	}

	var newIDs []string
	if c.Config.NewlyAddedTable {
		tables, err := c.DB.ListTables(ctx)
		if err != nil {
			return err
		}
		var matching []string
		for _, table := range tables {
			if c.Config.Tables != nil && !c.Config.Tables.Match(table.Database, table.Table) {
				continue
			}
			matching = append(matching, table.Database+"."+table.Table)
		}
		newIDs = NewlyAdded(matching, c.State)
		if c.Config.NewlyAddedSnapshot {
			for _, id := range newIDs {
				parts := strings.SplitN(id, ".", 2)
				if len(parts) != 2 {
					continue
				}
				if c.SkipSnapshot != nil && c.SkipSnapshot(parts[0], parts[1]) {
					continue
				}
				mark := toSourcePos(acked)
				if err := c.send(ctx, out, source.Message{SnapshotTable: id, SnapshotMark: mark}); err != nil {
					return err
				}
				snapshotting[id] = struct{}{}
			}
		} else if len(newIDs) > 0 {
			c.warn("scan.newly-added-table.snapshot.enabled=false; go-cdc will not snapshot or verify sink history for newly added tables", "tables", strings.Join(newIDs, ","))
			for _, id := range newIDs {
				if err := c.send(ctx, out, source.Message{Captured: id}); err != nil {
					return err
				}
				live[id] = struct{}{}
			}
		}
	}

	needBarrier := len(snapshotting) > 0
	if needBarrier {
		done := make(chan struct{})
		if err := c.send(ctx, out, source.Message{CheckpointBarrier: true, BarrierDone: done}); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
		}
	}

	c.restrictNewTables(live, snapshotting)

	route := map[string]bool{}
	for id := range snapshotting {
		route[id] = true
	}
	for id := range live {
		route[id] = false
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := c.binlogFrom(ctx, out, acked, route); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
			cancel()
		}
	}()
	go func() {
		defer wg.Done()
		if err := c.snapshotResumeTables(ctx, out, snapshotting); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
			cancel()
		}
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Capture) warn(msg string, args ...any) {
	if c.Warn != nil {
		c.Warn(msg, args...)
		return
	}
}

func (c *Capture) restrictNewTables(live, snapshotting map[string]struct{}) {
	if c.Stream == nil {
		return
	}
	// Always pin allow = captured ∪ snapshotting. When C=true, runtime CREATE is
	// admitted dynamically in the stream handler — clearing LimitTables would also
	// let config-diff DML through (A=false + C=true must still ignore those tables).
	var ids []string
	for id := range live {
		ids = append(ids, id)
	}
	for id := range snapshotting {
		ids = append(ids, id)
	}
	c.Stream.LimitTables(ids)
}

func (c *Capture) snapshotResumeTables(ctx context.Context, out chan<- source.Message, snapshotting map[string]struct{}) error {
	if len(snapshotting) == 0 {
		return nil
	}
	tables, err := c.DB.ListTables(ctx)
	if err != nil {
		return err
	}
	byID := map[string]schema.Table{}
	for _, table := range tables {
		byID[table.Database+"."+table.Table] = table
	}
	ordered := make([]string, 0, len(snapshotting))
	for id := range snapshotting {
		ordered = append(ordered, id)
	}
	// Stable-ish: matching ListTables order first, then leftovers.
	var serial []string
	seen := map[string]struct{}{}
	for _, table := range tables {
		id := table.Database + "." + table.Table
		if _, ok := snapshotting[id]; ok {
			serial = append(serial, id)
			seen[id] = struct{}{}
		}
	}
	for _, id := range ordered {
		if _, ok := seen[id]; !ok {
			serial = append(serial, id)
		}
	}
	for _, id := range serial {
		if err := ctx.Err(); err != nil {
			return err
		}
		table, ok := byID[id]
		if !ok {
			return fmt.Errorf("source.mysql: snapshot table %s not found", id)
		}
		if err := c.snapshotTable(ctx, out, table); err != nil {
			return err
		}
		if c.Spill != nil {
			if err := c.Spill.Drain(id, func(item event.Event) error {
				copied := item
				return c.send(ctx, out, source.Message{Event: &copied})
			}); err != nil {
				return err
			}
			// Sink flush + checkpoint before persisting spill replay offset.
			// Channel delivery alone must not advance .off (crash would skip events).
			done := make(chan struct{})
			if err := c.send(ctx, out, source.Message{CheckpointBarrier: true, BarrierDone: done}); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-done:
			}
			if err := c.Spill.PersistReplay(id); err != nil {
				return err
			}
		}
		if err := c.send(ctx, out, source.Message{Captured: id}); err != nil {
			return err
		}
	}
	return nil
}
