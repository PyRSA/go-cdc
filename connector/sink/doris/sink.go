package doris

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/sink"
)

// schemaChangePollInterval is the delay between structure re-reads; tests
// shorten it. The default budget is 5s, checked every 500ms.
var schemaChangePollInterval = 500 * time.Millisecond

// sqlAPI is the SQL surface the sink needs. *sqlClient implements it; tests
// substitute a fake without a live Doris FE.
type sqlAPI interface {
	Ping(ctx context.Context) error
	Exec(ctx context.Context, statement string) error
	descTable(ctx context.Context, database, table string) ([]descColumn, error)
	showCreateTable(ctx context.Context, database, table string) (string, error)
	showLoad(ctx context.Context, database, label string) (loadState, error)
	Close() error
}

// Sink writes routed events to Doris with Stream Load JSON Lines.
type Sink struct {
	cfg    definition.Sink
	sql    sqlAPI
	http   *httpClient
	labels labelSequence
	log    *slog.Logger

	mu     sync.RWMutex
	tables map[string]schema.Table
}

// Open builds the HTTP and SQL clients. Network access is not attempted until
// Open is called.
func Open(cfg definition.Sink) (*Sink, error) {
	sqlClient, err := openSQL(cfg)
	if err != nil {
		return nil, err
	}
	httpClient, err := newHTTPClient(cfg)
	if err != nil {
		_ = sqlClient.Close()
		return nil, err
	}
	return &Sink{
		cfg: cfg, sql: sqlClient, http: httpClient,
		log: slog.Default(), tables: map[string]schema.Table{},
	}, nil
}

// logger returns the sink logger. A Sink built without one still reports
// through the process logger installed by main.
func (s *Sink) logger() *slog.Logger {
	if s.log != nil {
		return s.log
	}
	return slog.Default()
}

// Open verifies the Doris SQL endpoint. Stream Load is attempted per batch.
func (s *Sink) Open(ctx context.Context) error {
	if err := s.sql.Ping(ctx); err != nil {
		return err
	}
	return nil
}

// CreateTable creates a missing database/table when auto-create-table is on,
// then always validates the live structure. A failure is returned to Runner and
// the table is not cached as writable.
func (s *Sink) CreateTable(table schema.Table) (err error) {
	table = s.withDatabase(table)
	var live schema.Table
	var pendingColumns []string
	defer func() {
		if err != nil {
			// A rejected target is never cached as writable, so this is the only
			// structured record of why it cannot be written to.
			s.logger().Warn("doris: target table rejected",
				"database", table.Database, "table", table.Table, "error", err)
			return
		}
		if len(pendingColumns) > 0 {
			s.logger().Warn("doris: target table accepted with pending source columns; a replayed ADD COLUMN must create them before the first row",
				"database", live.Database, "table", live.Table, "columns", pendingColumns)
			return
		}
		s.logger().Info("doris: target table validated",
			"database", live.Database, "table", live.Table,
			"columns", len(live.Columns), "key_columns", live.PrimaryKeys,
			"auto_create", s.cfg.AutoCreate)
	}()
	if table.Database == "" || table.Table == "" {
		return fmt.Errorf("sink.doris: database and table are required")
	}
	if len(table.PrimaryKeys) == 0 {
		return fmt.Errorf("sink.doris: table %s has no primary key", schema.Qualified(table.Database, table.Table))
	}
	if s.cfg.AutoCreate {
		if err := s.sql.Exec(context.Background(), "CREATE DATABASE IF NOT EXISTS "+schema.Quote(table.Database)); err != nil {
			return err
		}
		statement, err := createTableSQL(table, tableOptions{Buckets: s.cfg.Buckets, ReplicationNum: s.cfg.ReplicationNum})
		if err != nil {
			return err
		}
		s.logger().Debug("doris: create table statement",
			"database", table.Database, "table", table.Table, "statement", statement)
		if err := s.sql.Exec(context.Background(), statement); err != nil {
			return err
		}
	}
	columns, err := s.sql.descTable(context.Background(), table.Database, table.Table)
	if err != nil {
		return err
	}
	create, err := s.sql.showCreateTable(context.Background(), table.Database, table.Table)
	if err != nil {
		return err
	}
	model, err := parseTableModel(create)
	if err != nil {
		return err
	}
	if err := validateTableModel(table, model); err != nil {
		return err
	}
	if err := validateTarget(table, columns); err != nil {
		var missing *missingColumnsError
		if !errors.As(err, &missing) {
			return err
		}
		// A column added while the job was down is still ahead in the binlog:
		// the pending ADD COLUMN creates it before any row that carries it.
		// Failing here would make every restart hit the same wall, so accept
		// the table and let validateEvents enforce the invariant on real rows.
		pendingColumns = missing.Columns
	}
	// Cache the live DESC shape (Doris types + key casing) so Write/encode
	// always see one dialect after create or restart.
	live = schemaFromDesc(table.Database, table.Table, columns)
	s.cacheTable(live)
	return nil
}

// ApplySchema executes a Doris ADD COLUMN generated from structured metadata.
// It never executes change.DDL, which is MySQL dialect.
func (s *Sink) ApplySchema(change schema.Change) error {
	if change.Action != schema.ActionAddColumn {
		return fmt.Errorf("sink.doris: schema change %s is not supported in v1", change.Action)
	}
	if !s.cfg.SchemaChange {
		return fmt.Errorf("sink.doris: schema.change.enabled is false")
	}
	if !contains(s.cfg.SchemaChangeInclude, "add.column") {
		return fmt.Errorf("sink.doris: schema.change.include does not allow add.column")
	}
	if len(change.AddColumns) > 1 {
		return fmt.Errorf("sink.doris: ADD COLUMN on %s.%s adds %d columns (%s); v1 applies exactly one column per statement",
			change.Database, change.Table, len(change.AddColumns), strings.Join(change.AddColumns, ", "))
	}
	if change.ColumnDef == nil {
		return fmt.Errorf("sink.doris: add.column for %s has no structured column metadata", change.Database+"."+change.Table)
	}
	if len(change.AddColumns) != 1 || !strings.EqualFold(change.AddColumns[0], change.ColumnDef.Name) {
		return fmt.Errorf("sink.doris: add.column metadata for %s.%s is inconsistent", change.Database, change.Table)
	}
	table := s.withDatabase(schema.Table{Database: change.Database, Table: change.Table})
	columns, err := s.sql.descTable(context.Background(), table.Database, table.Table)
	if err != nil {
		return err
	}
	if existing, ok := findColumn(columns, change.ColumnDef.Name); ok {
		if err := validateExistingColumn(existing, *change.ColumnDef); err != nil {
			return err
		}
		// Refresh cache even on the idempotent path: the in-process schema may
		// predate an external or previously applied ADD COLUMN.
		s.logger().Debug("doris: schema change already present",
			"database", table.Database, "table", table.Table,
			"action", string(change.Action), "column", change.ColumnDef.Name)
		s.cacheTable(schemaFromDesc(table.Database, table.Table, columns))
		return nil
	}
	statement, err := addColumnSQL(table.Database, table.Table, *change.ColumnDef)
	if err != nil {
		return err
	}
	s.logger().Info("doris: sending schema change",
		"database", table.Database, "table", table.Table,
		"action", string(change.Action), "column", change.ColumnDef.Name, "statement", statement)
	if err := s.sql.Exec(context.Background(), statement); err != nil {
		s.logger().Error("doris: schema change failed",
			"database", table.Database, "table", table.Table,
			"action", string(change.Action), "column", change.ColumnDef.Name, "error", err)
		return err
	}
	started := time.Now()
	if err := s.waitForColumn(table.Database, table.Table, *change.ColumnDef); err != nil {
		s.logger().Error("doris: schema change not confirmed",
			"database", table.Database, "table", table.Table, "column", change.ColumnDef.Name,
			"wait_timeout", s.schemaChangeWait(), "error", err)
		return err
	}
	columns, err = s.sql.descTable(context.Background(), table.Database, table.Table)
	if err != nil {
		return err
	}
	s.logger().Info("doris: schema change applied",
		"database", table.Database, "table", table.Table,
		"action", string(change.Action), "column", change.ColumnDef.Name,
		"elapsed", time.Since(started))
	s.cacheTable(schemaFromDesc(table.Database, table.Table, columns))
	return nil
}

// Write groups events by routed table, coalesces repeated primary keys, and
// submits one Stream Load per table in stable order. The first failure stops
// later tables so Runner does not advance the checkpoint for that batch.
func (s *Sink) Write(batch []event.Event) error {
	if len(batch) == 0 {
		return nil
	}
	groups := map[string][]event.Event{}
	for _, item := range batch {
		item = s.withDatabaseEvent(item)
		if item.Database == "" || item.Table == "" {
			return fmt.Errorf("sink.doris: event has no routed database/table")
		}
		id := item.Database + "." + item.Table
		groups[id] = append(groups[id], item)
	}
	ids := make([]string, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		items := groups[id]
		table, err := s.tableSchema(id)
		if err != nil {
			return err
		}
		if err := validateEvents(items, table); err != nil {
			return err
		}
		merged, err := coalesce(items, table)
		if err != nil {
			return err
		}
		body, err := encodeBatch(merged, table)
		if err != nil {
			return err
		}
		label := s.labels.next(id)
		if err := s.load(id, label, body, len(merged)); err != nil {
			return err
		}
	}
	return nil
}

// Flush is a no-op: Write completes every Stream Load synchronously.
func (s *Sink) Flush() error { return nil }

// Close releases the HTTP and SQL clients. Write never leaves buffered rows.
func (s *Sink) Close() error {
	var err error
	if s.http != nil {
		err = s.http.Close()
	}
	if s.sql != nil {
		if sqlErr := s.sql.Close(); err == nil {
			err = sqlErr
		}
	}
	return err
}

func (s *Sink) load(tableID, label string, body []byte, wantRows int) error {
	parts := strings.SplitN(tableID, ".", 2)
	if len(parts) != 2 {
		return fmt.Errorf("sink.doris: invalid table id %q", tableID)
	}
	database, table := parts[0], parts[1]
	attempts := labelAttempts(s.cfg.MaxRetries)
	for attempt := 1; attempt <= attempts; attempt++ {
		started := time.Now()
		outcome, err := s.attempt(database, table, label, body, wantRows)
		switch outcome {
		case outcomeSuccess:
			s.logger().Info("doris: stream load succeeded",
				"database", database, "table", table, "label", label,
				"rows", wantRows, "attempt", attempt, "elapsed", time.Since(started))
			return nil
		case outcomeFatal:
			s.logger().Error("doris: stream load failed",
				"database", database, "table", table, "label", label, "rows", wantRows,
				"attempt", attempt, "outcome", outcome.String(), "error", err)
			return fmt.Errorf("sink.doris: %s label %s: %w", tableID, label, err)
		case outcomeRetryable, outcomeUnknown, outcomeLabelExists:
			if attempt == attempts {
				if err == nil {
					err = fmt.Errorf("load state was not confirmed")
				}
				s.logger().Error("doris: stream load was never confirmed",
					"database", database, "table", table, "label", label, "rows", wantRows,
					"attempts", attempts, "outcome", outcome.String(), "error", err)
				return fmt.Errorf("sink.doris: %s label %s not confirmed after %d attempts: %w", tableID, label, attempts, err)
			}
			s.logger().Warn("doris: stream load not confirmed; retrying",
				"database", database, "table", table, "label", label, "rows", wantRows,
				"attempt", attempt, "attempts", attempts, "outcome", outcome.String(), "error", err)
			time.Sleep(backoff(attempt))
		default:
			return fmt.Errorf("sink.doris: %s label %s: unexpected outcome", tableID, label)
		}
	}
	return fmt.Errorf("sink.doris: %s label %s failed", tableID, label)
}

func (s *Sink) attempt(database, table, label string, body []byte, wantRows int) (loadOutcome, error) {
	resp, err := s.http.streamLoad(context.Background(), database, table, label, body)
	if err != nil {
		return s.resolveError(database, label, err)
	}
	outcome, classifyErr := classifyLoad(resp, label, wantRows)
	switch outcome {
	case outcomeSuccess, outcomeFatal, outcomeRetryable, outcomeLabelExists:
		return outcome, classifyErr
	case outcomeUnknown:
		// Parsed 2xx body with unknown Status (or missing ExistingJobStatus):
		// query the same label before any Stream Load retry.
		return s.resolveError(database, label, classifyErr)
	default:
		return s.resolveError(database, label, classifyErr)
	}
}

// resolveError never treats an unconfirmed load as success. Every transport or
// HTTP failure first queries the same label, because the request may have
// committed even when the response was lost. Retrying reuses the same label.
func (s *Sink) resolveError(database, label string, err error) (loadOutcome, error) {
	state, stateErr := s.sql.showLoad(context.Background(), database, label)
	if stateErr == nil {
		outcome, stateOutcomeErr := classifyLoadState(state)
		s.logger().Debug("doris: stream load state resolved from SHOW LOAD",
			"database", database, "label", label, "state", state.State, "outcome", outcome.String())
		if stateOutcomeErr != nil {
			return outcome, fmt.Errorf("%w (%v)", err, stateOutcomeErr)
		}
		return outcome, err
	}
	if !errors.Is(stateErr, errLoadNotFound) {
		return outcomeFatal, fmt.Errorf("%w (label state unavailable: %v)", err, stateErr)
	}
	var status *httpStatusError
	if errors.As(err, &status) {
		if status.Code >= 500 || status.Code == 429 {
			return outcomeRetryable, err
		}
		return outcomeFatal, err
	}
	return outcomeRetryable, err
}

// schemaChangeWait is the configured ADD COLUMN visibility budget.
func (s *Sink) schemaChangeWait() time.Duration {
	if s.cfg.SchemaChangeWait > 0 {
		return s.cfg.SchemaChangeWait
	}
	return definition.DefaultDorisSchemaChangeWait
}

func (s *Sink) waitForColumn(database, table string, column schema.Column) error {
	timeout := s.schemaChangeWait()
	interval := schemaChangePollInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	deadline := time.Now().Add(timeout)
	polls := 0
	var last error
	for {
		polls++
		columns, err := s.sql.descTable(context.Background(), database, table)
		if err == nil {
			if existing, ok := findColumn(columns, column.Name); ok {
				return validateExistingColumn(existing, column)
			}
			last = fmt.Errorf("column %s is not visible yet", column.Name)
		} else {
			last = err
		}
		// Sleep up to the deadline instead of stopping one interval early: a
		// wait-timeout below one interval must still give Doris that full budget.
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		if remaining > interval {
			remaining = interval
		}
		time.Sleep(remaining)
	}
	return fmt.Errorf("sink.doris: add column %s.%s.%s was not confirmed within %s (%d polls): %w",
		database, table, column.Name, timeout, polls, last)
}

func (s *Sink) tableSchema(tableID string) (schema.Table, error) {
	s.mu.RLock()
	table, ok := s.tables[tableID]
	s.mu.RUnlock()
	if ok {
		return table, nil
	}
	parts := strings.SplitN(tableID, ".", 2)
	if len(parts) != 2 {
		return schema.Table{}, fmt.Errorf("sink.doris: invalid table id %q", tableID)
	}
	columns, err := s.sql.descTable(context.Background(), parts[0], parts[1])
	if err != nil {
		return schema.Table{}, err
	}
	// Fallback when no catalogue was applied for this table (sink used without
	// a source, or the source table is gone): only the target model can be
	// checked here. A non-UNIQUE table must never be written to.
	create, err := s.sql.showCreateTable(context.Background(), parts[0], parts[1])
	if err != nil {
		return schema.Table{}, err
	}
	model, err := parseTableModel(create)
	if err != nil {
		return schema.Table{}, err
	}
	if !model.UniqueKey {
		return schema.Table{}, fmt.Errorf("sink.doris: target %s is not a UNIQUE KEY table", schema.Qualified(parts[0], parts[1]))
	}
	target := schemaFromDesc(parts[0], parts[1], columns)
	if len(target.PrimaryKeys) == 0 {
		return schema.Table{}, fmt.Errorf("sink.doris: target %s has no UNIQUE KEY column", schema.Qualified(parts[0], parts[1]))
	}
	s.logger().Debug("doris: target table loaded from DESC",
		"database", target.Database, "table", target.Table,
		"columns", len(target.Columns), "key_columns", target.PrimaryKeys)
	s.cacheTable(target)
	return target, nil
}

func schemaFromDesc(database, table string, columns []descColumn) schema.Table {
	target := schema.Table{Database: database, Table: table}
	for _, column := range columns {
		target.Columns = append(target.Columns, schema.Column{
			Name: column.Name, Type: column.Type, Nullable: column.Nullable,
		})
		if column.Key {
			target.PrimaryKeys = append(target.PrimaryKeys, column.Name)
		}
	}
	return target
}

func (s *Sink) cacheTable(table schema.Table) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tables[table.Database+"."+table.Table] = table
}

func (s *Sink) withDatabase(table schema.Table) schema.Table {
	if table.Database == "" {
		table.Database = s.cfg.Database
	}
	return table
}

func (s *Sink) withDatabaseEvent(item event.Event) event.Event {
	if item.Database == "" {
		item.Database = s.cfg.Database
	}
	return item
}

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

func findColumn(columns []descColumn, name string) (descColumn, bool) {
	for _, column := range columns {
		if strings.EqualFold(column.Name, name) {
			return column, true
		}
	}
	return descColumn{}, false
}

func validateEvents(batch []event.Event, table schema.Table) error {
	known := make(map[string]struct{}, len(table.Columns))
	for _, column := range table.Columns {
		known[strings.ToLower(column.Name)] = struct{}{}
	}
	expectedKeys := make(map[string]struct{}, len(table.PrimaryKeys))
	for _, key := range table.PrimaryKeys {
		expectedKeys[strings.ToLower(key)] = struct{}{}
	}
	for _, item := range batch {
		switch item.Op {
		case event.OpRead, event.OpCreate, event.OpUpdate:
		case event.OpDelete:
			return fmt.Errorf("sink.doris: %s DELETE is not supported in v1 (delete.enabled=false)", item.SinkID())
		default:
			return fmt.Errorf("sink.doris: %s unsupported event op %q", item.SinkID(), item.Op)
		}
		if item.After == nil {
			return fmt.Errorf("sink.doris: %s has no After image", item.SinkID())
		}
		if len(item.Keys) == 0 {
			return fmt.Errorf("sink.doris: %s event has no primary key", item.SinkID())
		}
		actualKeys := make(map[string]struct{}, len(item.Keys))
		for _, key := range item.Keys {
			if _, ok := mapValueCI(item.After, key); !ok {
				return fmt.Errorf("sink.doris: %s has no primary key value for %s", item.SinkID(), key)
			}
			actualKeys[strings.ToLower(key)] = struct{}{}
		}
		if len(actualKeys) != len(expectedKeys) {
			return fmt.Errorf("sink.doris: %s event keys %v do not match target UNIQUE KEY %v",
				item.SinkID(), item.Keys, table.PrimaryKeys)
		}
		for key := range expectedKeys {
			if _, ok := actualKeys[key]; !ok {
				return fmt.Errorf("sink.doris: %s event keys %v do not match target UNIQUE KEY %v",
					item.SinkID(), item.Keys, table.PrimaryKeys)
			}
		}
		for _, name := range item.Columns {
			if _, ok := known[strings.ToLower(name)]; !ok {
				return fmt.Errorf("sink.doris: %s column %s is not in the target table", item.SinkID(), name)
			}
		}
	}
	return nil
}

// coalesce keeps the last event per primary key. Doris Stream Load does not
// guarantee ordering of duplicate keys inside one batch.
func coalesce(batch []event.Event, table schema.Table) ([]event.Event, error) {
	keys := table.PrimaryKeys
	if len(keys) == 0 {
		return batch, nil
	}
	index := make(map[string]int, len(batch))
	out := make([]event.Event, 0, len(batch))
	for _, item := range batch {
		values := make([]any, 0, len(keys))
		for _, key := range keys {
			value, ok := mapValueCI(item.After, key)
			if !ok {
				return nil, fmt.Errorf("sink.doris: %s has no primary key value for %s", item.SinkID(), key)
			}
			values = append(values, value)
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			return nil, fmt.Errorf("sink.doris: %s primary key: %w", item.SinkID(), err)
		}
		id := string(encoded)
		if at, ok := index[id]; ok {
			out[at] = item
			continue
		}
		index[id] = len(out)
		out = append(out, item)
	}
	return out, nil
}

var _ sink.Sink = (*Sink)(nil)
