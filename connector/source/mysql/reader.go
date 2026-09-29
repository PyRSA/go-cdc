package mysql

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/source"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
)

// DB is the snapshot and catalog connection. It is not the binlog dump connection.
type DB interface {
	MasterStatus(ctx context.Context) (checkpoint.Position, error)
	ListTables(ctx context.Context) ([]schema.Table, error)
	SplitKeys(ctx context.Context, database, table, key string, low *string, limit int) ([]string, error)
	ReadChunk(ctx context.Context, database, table string, columns []string, key, low string, high *string) ([]map[string]any, error)
	EarliestBinlog(ctx context.Context) (checkpoint.Position, error)
	PositionAt(ctx context.Context, millis int64) (checkpoint.Position, error)
	Close() error
}

// Commit is one binlog step. Event and Position are not set on the same value.
type Commit struct {
	Event    *event.Event
	Position *checkpoint.Position
	Schema   *schema.Table
	Err      error
}

// Stream reads binlog events from a position. RunFrom is used only here.
type Stream interface {
	ReadFrom(ctx context.Context, pos checkpoint.Position) (<-chan Commit, error)
	Close() error
}

// Capture reads snapshot splits and then one binlog stream.
type Capture struct {
	Config      definition.Source
	Parallelism int
	State       checkpoint.State
	DB          DB
	Stream      Stream
	// Project returns the snapshot SELECT list. Nil keeps every column.
	Project func(database, table string, columns []string) ([]string, error)
	// SkipSnapshot reports a table whose sink targets are already excluded.
	SkipSnapshot func(database, table string) bool
}

// Messages starts capture. The consumer must apply an event before acting on
// the progress message that follows it.
func (c *Capture) Messages(ctx context.Context) (<-chan source.Message, error) {
	if c.Parallelism < 1 {
		c.Parallelism = 1
	}
	out := make(chan source.Message)
	go func() {
		defer close(out)
		err := c.run(ctx, out)
		if err != nil && ctx.Err() == nil {
			select {
			case out <- source.Message{Err: err}:
			case <-ctx.Done():
			}
		}
	}()
	return out, nil
}

// Read implements source.Source by forwarding only row and DDL events.
func (c *Capture) Read(ctx context.Context) (<-chan event.Event, error) {
	msgs, err := c.Messages(ctx)
	if err != nil {
		return nil, err
	}
	out := make(chan event.Event)
	go func() {
		defer close(out)
		for msg := range msgs {
			if msg.Event != nil {
				out <- *msg.Event
			}
		}
	}()
	return out, nil
}

// Open implements source.Source. Connections are opened by the caller-supplied DB.
func (c *Capture) Open(context.Context) error { return nil }

// Close closes the snapshot database and the binlog stream.
func (c *Capture) Close() error {
	var err error
	if c.Stream != nil {
		err = c.Stream.Close()
	}
	if c.DB != nil {
		if dbErr := c.DB.Close(); err == nil {
			err = dbErr
		}
	}
	return err
}

func (c *Capture) run(ctx context.Context, out chan<- source.Message) error {
	switch c.Config.StartupMode {
	case definition.StartupSnapshot, definition.StartupInitial, "":
		low, err := c.snapshotPhase(ctx, out)
		if err != nil {
			return err
		}
		if c.Config.StartupMode == definition.StartupSnapshot {
			return nil
		}
		if c.State.AckedOffset != nil {
			low = *c.State.AckedOffset
		}
		return c.binlogFrom(ctx, out, low)
	default:
		pos, err := c.resolvePosition(ctx)
		if err != nil {
			return err
		}
		return c.binlogFrom(ctx, out, pos)
	}
}

func (c *Capture) resolvePosition(ctx context.Context) (checkpoint.Position, error) {
	if pos := c.resumePosition(); pos.File != "" || pos.GTID != "" {
		return pos, nil
	}
	switch c.Config.StartupMode {
	case definition.StartupEarliest:
		if c.DB == nil {
			return checkpoint.Position{}, fmt.Errorf("source.scan.startup.mode: earliest requires a source connection")
		}
		return c.DB.EarliestBinlog(ctx)
	case definition.StartupLatest:
		if c.DB == nil {
			return checkpoint.Position{}, fmt.Errorf("source.scan.startup.mode: latest requires a source connection")
		}
		return c.DB.MasterStatus(ctx)
	case definition.StartupSpecificOffset:
		if c.Config.SpecificGTID != "" {
			return checkpoint.Position{File: c.Config.SpecificFile, Pos: c.Config.SpecificPos, GTID: c.Config.SpecificGTID}, nil
		}
		if c.Config.SpecificFile == "" || c.Config.SpecificPos == 0 {
			return checkpoint.Position{}, fmt.Errorf("source.scan.startup.specific-offset.file: required unless gtid-set is set")
		}
		return checkpoint.Position{File: c.Config.SpecificFile, Pos: c.Config.SpecificPos}, nil
	case definition.StartupTimestamp:
		if c.DB == nil {
			return checkpoint.Position{}, fmt.Errorf("source.scan.startup.timestamp-millis: required")
		}
		return c.DB.PositionAt(ctx, c.Config.TimestampMillis)
	default:
		return checkpoint.Position{}, fmt.Errorf("source.scan.startup.mode: %s has no binlog position", c.Config.StartupMode)
	}
}

func (c *Capture) resumePosition() checkpoint.Position {
	if c.State.AckedOffset != nil {
		return *c.State.AckedOffset
	}
	if c.State.LowWatermark != nil {
		return *c.State.LowWatermark
	}
	return checkpoint.Position{}
}

func (c *Capture) snapshotPhase(ctx context.Context, out chan<- source.Message) (checkpoint.Position, error) {
	low := c.State.LowWatermark
	if low == nil {
		pos, err := c.DB.MasterStatus(ctx)
		if err != nil {
			return checkpoint.Position{}, err
		}
		low = &pos
		if err := c.send(ctx, out, source.Message{Watermark: toSourcePos(pos)}); err != nil {
			return checkpoint.Position{}, err
		}
	}
	tables, err := c.DB.ListTables(ctx)
	if err != nil {
		return checkpoint.Position{}, err
	}
	var todo []schema.Table
	for _, table := range tables {
		if err := ctx.Err(); err != nil {
			return checkpoint.Position{}, err
		}
		id := table.Database + "." + table.Table
		if c.Config.Tables != nil && !c.Config.Tables.Match(table.Database, table.Table) {
			continue
		}
		if c.State.HasCaptured(id) {
			continue
		}
		if c.SkipSnapshot != nil && c.SkipSnapshot(table.Database, table.Table) {
			continue
		}
		// Config-added tables (match tables regex, not yet captured) are snapshotted
		// even when a checkpoint already exists — required for TP-09 style resume.
		if _, err := chunkColumn(table, c.Config.ChunkKeyColumn); err != nil {
			return checkpoint.Position{}, err
		}
		if c.Project != nil {
			if _, err := c.Project(table.Database, table.Table, projectedColumns(table)); err != nil {
				return checkpoint.Position{}, err
			}
		}
		todo = append(todo, table)
	}
	for _, table := range todo {
		if err := ctx.Err(); err != nil {
			return checkpoint.Position{}, err
		}
		if err := c.snapshotTable(ctx, out, table); err != nil {
			return checkpoint.Position{}, err
		}
		id := table.Database + "." + table.Table
		if err := c.send(ctx, out, source.Message{Captured: id}); err != nil {
			return checkpoint.Position{}, err
		}
	}
	if err := c.DB.Close(); err != nil {
		return checkpoint.Position{}, err
	}
	return *low, nil
}

func (c *Capture) send(ctx context.Context, out chan<- source.Message, msg source.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case out <- msg:
		return nil
	}
}

func (c *Capture) snapshotTable(ctx context.Context, out chan<- source.Message, table schema.Table) error {
	key, err := chunkColumn(table, c.Config.ChunkKeyColumn)
	if err != nil {
		return err
	}
	id := table.Database + "." + table.Table
	splits, err := PlanSplits(func(low *string, limit int) ([]string, error) {
		return c.DB.SplitKeys(ctx, table.Database, table.Table, key, low, limit)
	}, c.Config.ChunkSize)
	if err != nil {
		return fmt.Errorf("%s: %w", id, err)
	}
	var pending []checkpoint.Split
	for _, split := range splits {
		if !c.State.HasSplit(id, split) {
			pending = append(pending, split)
		}
	}
	if len(pending) == 0 {
		if len(splits) == 0 {
			return c.send(ctx, out, source.Message{Schema: cloneTable(table)})
		}
		return nil
	}
	if err := c.send(ctx, out, source.Message{Schema: cloneTable(table)}); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan checkpoint.Split)
	errCh := make(chan error, c.Parallelism)
	var wg sync.WaitGroup
	workers := c.Parallelism
	if workers > len(pending) {
		workers = len(pending)
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for split := range jobs {
				if err := c.emitSplit(ctx, out, table, key, split); err != nil {
					select {
					case errCh <- err:
					default:
					}
					cancel()
					return
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, split := range pending {
			select {
			case <-ctx.Done():
				return
			case jobs <- split:
			}
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

func (c *Capture) emitSplit(ctx context.Context, out chan<- source.Message, table schema.Table, key string, split checkpoint.Split) error {
	columns := projectedColumns(table)
	if c.Project != nil {
		projected, err := c.Project(table.Database, table.Table, columns)
		if err != nil {
			return err
		}
		columns = projected
	}
	rows, err := c.DB.ReadChunk(ctx, table.Database, table.Table, columns, key, split.Low, split.High)
	if err != nil {
		return err
	}
	return c.emitRows(ctx, out, table, columns, split, rows)
}

func (c *Capture) emitRows(ctx context.Context, out chan<- source.Message, table schema.Table, columns []string, split checkpoint.Split, rows []map[string]any) error {
	for _, row := range rows {
		item := event.Event{
			Op: event.OpRead, Database: table.Database, Table: table.Table,
			SourceDatabase: table.Database, SourceTable: table.Table,
			After: row, Columns: append([]string(nil), columns...),
			Keys: append([]string(nil), table.PrimaryKeys...),
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- source.Message{Event: &item}:
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case out <- source.Message{Split: &source.Split{Table: table.Database + "." + table.Table, Low: split.Low, High: split.High}}:
	}
	return nil
}

func (c *Capture) binlogFrom(ctx context.Context, out chan<- source.Message, pos checkpoint.Position) error {
	if pos.File == "" && pos.GTID == "" {
		return fmt.Errorf("source.scan.startup.mode: binlog position is empty")
	}
	if c.Stream == nil {
		return fmt.Errorf("source.mysql: binlog stream is not open")
	}
	commits, err := c.Stream.ReadFrom(ctx, pos)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case commit, ok := <-commits:
			if !ok {
				return nil
			}
			if commit.Err != nil {
				return commit.Err
			}
			msg := source.Message{Schema: commit.Schema}
			if commit.Event != nil {
				if !c.Config.SchemaChange && commit.Event.Op == event.OpDDL {
					continue
				}
				copied := *commit.Event
				msg.Event = &copied
			}
			if commit.Position != nil {
				msg.Position = toSourcePos(*commit.Position)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- msg:
			}
		}
	}
}

func toSourcePos(pos checkpoint.Position) *source.Position {
	return &source.Position{File: pos.File, Pos: pos.Pos, GTID: pos.GTID}
}

func chunkColumn(table schema.Table, configured string) (string, error) {
	id := table.Database + "." + table.Table
	name := configured
	if name == "" {
		if len(table.PrimaryKeys) == 0 {
			return "", fmt.Errorf("%s has no primary key and scan.incremental.snapshot.chunk.key-column is empty", id)
		}
		name = table.PrimaryKeys[0]
	}
	var column *schema.Column
	for i := range table.Columns {
		if strings.EqualFold(table.Columns[i].Name, name) {
			column = &table.Columns[i]
			break
		}
	}
	if column == nil {
		return "", fmt.Errorf("%s: scan.incremental.snapshot.chunk.key-column %s is not on the table", id, name)
	}
	if !ChunkKeyAllowed(column.Type) {
		return "", fmt.Errorf("%s: chunk key %s has type %s, which cannot be a chunk key", id, column.Name, column.Type)
	}
	if column.Nullable && !isPrimaryKey(table, column.Name) {
		return "", fmt.Errorf("%s: chunk key %s must be NOT NULL", id, column.Name)
	}
	return column.Name, nil
}

func isPrimaryKey(table schema.Table, name string) bool {
	for _, key := range table.PrimaryKeys {
		if strings.EqualFold(key, name) {
			return true
		}
	}
	return false
}

func cloneTable(table schema.Table) *schema.Table {
	table.Columns = append([]schema.Column(nil), table.Columns...)
	table.PrimaryKeys = append([]string(nil), table.PrimaryKeys...)
	return &table
}

func projectedColumns(table schema.Table) []string {
	if len(table.Columns) == 0 {
		return nil
	}
	out := make([]string, len(table.Columns))
	for i, column := range table.Columns {
		out[i] = column.Name
	}
	return out
}

// ChunkKeyAllowed reports whether a MySQL type may be a snapshot chunk key.
func ChunkKeyAllowed(mysqlType string) bool {
	base := strings.ToLower(strings.TrimSpace(mysqlType))
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = base[:i]
	}
	base = strings.TrimSpace(strings.TrimSuffix(base, " unsigned"))
	switch base {
	case "tinyint", "smallint", "int", "integer", "bigint", "decimal", "numeric", "char", "varchar", "date", "time", "datetime", "timestamp":
		return true
	default:
		return false
	}
}
