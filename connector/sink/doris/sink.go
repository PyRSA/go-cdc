package doris

import (
	"bytes"
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
	defer func() {
		if err != nil {
			// A rejected target is never cached as writable, so this is the only
			// structured record of why it cannot be written to.
			s.logger().Warn("doris: target table rejected",
				"database", table.Database, "table", table.Table, "error", err)
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
		// A missing source column is never waited for: a column that reached the
		// source catalogue before the checkpoint has no ADD COLUMN left to replay,
		// and the snapshot path emits rows carrying it before any binlog DDL. The
		// table is reconciled now, or the job stops with an actionable error.
		columns, err = s.reconcileMissingColumns(table, missing.Columns)
		if err != nil {
			return err
		}
		// Doris cannot roll back an ALTER that already ran, so a partially
		// reconciled table is re-validated as a whole before it is cached.
		if err := validateTarget(table, columns); err != nil {
			return fmt.Errorf("sink.doris: target %s is still not compatible after adding the missing columns: %w",
				schema.Qualified(table.Database, table.Table), err)
		}
	}
	// Cache the live DESC shape (Doris types + key casing) so Write/encode
	// always see one dialect after create or restart.
	live = schemaFromDesc(table.Database, table.Table, columns)
	s.cacheTable(live)
	return nil
}

// reconcileMissingColumns brings an existing target up to the source catalogue
// shape. This is the only ALTER path besides ApplySchema, and it runs only when
// schema evolution is enabled and grants add.column: auto-create-table covers
// creation only and never authorises changing an existing table.
//
// Doris DDL has no rollback. When column N fails, the columns added before it
// stay, so the error names the failing column and the table is returned as not
// validated (and therefore not cached) to stop the job at that table.
func (s *Sink) reconcileMissingColumns(table schema.Table, missing []string) ([]descColumn, error) {
	qualified := schema.Qualified(table.Database, table.Table)
	if !s.cfg.SchemaChange || !contains(s.cfg.SchemaChangeInclude, "add.column") {
		return nil, fmt.Errorf("sink.doris: target %s is missing column %s and this sink may not alter an existing table: set sink.schema.change.enabled: true with sink.schema.change.include: [add.column], or run ALTER TABLE %s ADD COLUMN ... manually",
			qualified, strings.Join(missing, ", "), qualified)
	}
	byName := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		byName[strings.ToLower(column.Name)] = column
	}
	for _, name := range missing {
		column, ok := byName[strings.ToLower(name)]
		if !ok {
			return nil, fmt.Errorf("sink.doris: target %s is missing column %s but the source definition has no such column",
				qualified, name)
		}
		if err := s.addMissingColumn(table, column); err != nil {
			return nil, err
		}
	}
	// Re-read the structure instead of assuming the ALTER result: the caller
	// re-validates the whole target before caching it.
	return s.sql.descTable(context.Background(), table.Database, table.Table)
}

// addMissingColumn runs one ADD COLUMN and proves it took effect. Doris applies
// schema changes asynchronously, so the DESC poll after Exec doubles as the
// visibility check. A failed Exec is reconciled through DESC first: another
// writer may have added the same compatible column, which is a success here.
func (s *Sink) addMissingColumn(table schema.Table, column schema.Column) error {
	statement, err := addColumnSQL(table.Database, table.Table, column)
	if err != nil {
		return fmt.Errorf("sink.doris: target %s cannot be reconciled: %w",
			schema.Qualified(table.Database, table.Table), err)
	}
	s.logger().Info("doris: adding missing column",
		"database", table.Database, "table", table.Table,
		"column", column.Name, "statement", statement)
	if execErr := s.sql.Exec(context.Background(), statement); execErr != nil {
		return s.awaitColumnAfterDDLError(table.Database, table.Table, column, execErr)
	}
	return s.waitForColumn(table.Database, table.Table, column)
}

// awaitColumnAfterDDLError resolves a failed ADD COLUMN. A rejected response does
// not prove the DDL did not run: the FE may have committed it and lost the
// response, and Doris applies schema changes asynchronously. The column is
// therefore polled for the same bounded budget ApplySchema uses.
//
// A DESC failure is never read as "column missing": when the wait expires the
// error keeps the original DDL failure, the number of polls, and whether the
// structure was readable but the column never appeared.
func (s *Sink) awaitColumnAfterDDLError(database, table string, column schema.Column, ddlErr error) error {
	timeout := s.schemaChangeWait()
	interval := schemaChangePollInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	deadline := time.Now().Add(timeout)
	polls := 0
	var lastDescErr error
	notVisible := false
	for {
		polls++
		existing, ok, descErr := s.lookupColumn(database, table, column.Name)
		switch {
		case descErr != nil:
			lastDescErr = descErr
		case ok:
			if compatErr := validateExistingColumn(existing, column); compatErr != nil {
				return fmt.Errorf("sink.doris: add column %s.%s.%s failed (%v) and the column now present is not the source definition: %w",
					database, table, column.Name, ddlErr, compatErr)
			}
			// The DDL did take effect despite the failed response.
			s.logger().Info("doris: missing column was already added",
				"database", database, "table", table, "column", column.Name,
				"polls", polls, "ddl_error", ddlErr)
			return nil
		default:
			// DESC answered and the column is still absent, so an earlier read
			// failure no longer describes the target.
			lastDescErr = nil
			notVisible = true
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		if remaining > interval {
			remaining = interval
		}
		time.Sleep(remaining)
	}
	detail := ""
	if lastDescErr != nil {
		detail += fmt.Sprintf("; the structure re-read failed: %v", lastDescErr)
	}
	if notVisible {
		detail += "; DESC succeeded but the column never became visible"
	}
	return fmt.Errorf("sink.doris: add column %s.%s.%s did not take effect within %s (%d polls): %w%s",
		database, table, column.Name, timeout, polls, ddlErr, detail)
}

// lookupColumn re-reads DESC to tell a concurrent ADD COLUMN apart from a real
// failure.
func (s *Sink) lookupColumn(database, table, name string) (descColumn, bool, error) {
	columns, err := s.sql.descTable(context.Background(), database, table)
	if err != nil {
		return descColumn{}, false, err
	}
	existing, ok := findColumn(columns, name)
	return existing, ok, nil
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
	if execErr := s.sql.Exec(context.Background(), statement); execErr != nil {
		// A rejected response does not prove the DDL did not run: the FE may have
		// committed it and lost the response. Resolve it through the same bounded
		// DESC wait the target-validation path uses before failing the job.
		if resolveErr := s.awaitColumnAfterDDLError(table.Database, table.Table, *change.ColumnDef, execErr); resolveErr != nil {
			s.logger().Error("doris: schema change failed",
				"database", table.Database, "table", table.Table,
				"action", string(change.Action), "column", change.ColumnDef.Name, "error", resolveErr)
			return resolveErr
		}
		s.logger().Info("doris: schema change had already taken effect despite the failed response",
			"database", table.Database, "table", table.Table, "column", change.ColumnDef.Name,
			"ddl_error", execErr)
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
			return fmt.Errorf("sink.doris: %s DELETE is not supported (delete.enabled=false)", item.SinkID())
		default:
			return fmt.Errorf("sink.doris: %s unsupported event op %q", item.SinkID(), item.Op)
		}
		if item.After == nil {
			return fmt.Errorf("sink.doris: %s has no After image", item.SinkID())
		}
		if len(item.Keys) == 0 {
			return fmt.Errorf("sink.doris: %s event has no primary key", item.SinkID())
		}
		if item.Op == event.OpUpdate {
			if item.Before == nil {
				// Without the Before image a moved primary key cannot be told apart
				// from a value-only update, and writing After as an upsert would
				// leave the old key behind. Refuse instead of guessing.
				return fmt.Errorf("sink.doris: %s UPDATE has no Before image, so a primary key change cannot be ruled out: Doris v1 cannot retract the old row; a sink supporting DELETE + INSERT (for example mysql-sink) is required for such events",
					item.SinkID())
			}
			if err := checkPrimaryKeyUnchanged(item); err != nil {
				return err
			}
		}
		actualKeys := make(map[string]struct{}, len(item.Keys))
		for _, key := range item.Keys {
			value, ok := mapValueCI(item.After, key)
			if !ok {
				return fmt.Errorf("sink.doris: %s has no primary key value for %s", item.SinkID(), key)
			}
			if value == nil {
				// A NULL key is not deduplicable: coalesce would fold every NULL
				// key of the batch into one row and drop the others silently.
				return fmt.Errorf("sink.doris: %s has a NULL value for primary key %s; a NULL key cannot be deduplicated", item.SinkID(), key)
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

// checkPrimaryKeyUnchanged rejects an UPDATE whose primary key moved. Doris v1
// has no DELETE, so writing the After image would insert a new row and leave the
// Before row behind: the target would silently hold both. The source always
// sends a full Before image (binlog_row_image=FULL), so a key that is absent
// from Before is a broken image and is reported as such instead of being read as
// SQL NULL.
func checkPrimaryKeyUnchanged(item event.Event) error {
	for _, key := range item.Keys {
		before, ok := mapValueCI(item.Before, key)
		if !ok {
			return fmt.Errorf("sink.doris: %s UPDATE has no Before value for primary key %s", item.SinkID(), key)
		}
		if before == nil {
			return fmt.Errorf("sink.doris: %s UPDATE has a NULL value for primary key %s in Before", item.SinkID(), key)
		}
		after, ok := mapValueCI(item.After, key)
		if !ok {
			return fmt.Errorf("sink.doris: %s UPDATE has no After value for primary key %s", item.SinkID(), key)
		}
		if after == nil {
			return fmt.Errorf("sink.doris: %s UPDATE has a NULL value for primary key %s in After", item.SinkID(), key)
		}
		if sameKeyValue(before, after) {
			continue
		}
		return fmt.Errorf("sink.doris: %s UPDATE changed primary key %s (%s -> %s); Doris v1 cannot retract the old row, so a key change cannot be applied as an upsert: use a sink that supports DELETE + INSERT (for example mysql-sink), or do not modify primary keys on the source",
			item.SinkID(), key, formatKeyValue(before), formatKeyValue(after))
	}
	return nil
}

// sameKeyValue compares one primary-key value from Before and After. Both
// images are built by connector/source/mysql.Normalize, so one column keeps one
// Go type across them: int64, uint64, string (decimals and temporals), []byte
// (binaries) or nil. Equality is therefore decided per concrete type rather than
// by fmt.Sprint, which would equate 1, 1.0 and "1" and could hide a real key
// change. nil is only equal to nil.
func sameKeyValue(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if left, ok := a.([]byte); ok {
		right, ok := b.([]byte)
		return ok && bytes.Equal(left, right)
	}
	switch left := a.(type) {
	case int64:
		right, ok := b.(int64)
		return ok && left == right
	case uint64:
		right, ok := b.(uint64)
		return ok && left == right
	case string:
		right, ok := b.(string)
		return ok && left == right
	case bool:
		right, ok := b.(bool)
		return ok && left == right
	default:
		// Normalize folds every unknown type into a string, so this branch only
		// sees values that bypassed it. Two of those are never proven equal:
		// claiming equality could hide a real primary key change.
		return false
	}
}

// formatKeyValue renders a key value for an error message without dumping raw
// bytes at the operator.
func formatKeyValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return "NULL"
	case []byte:
		return fmt.Sprintf("%q", string(typed))
	case string:
		return fmt.Sprintf("%q", typed)
	default:
		return fmt.Sprint(typed)
	}
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
