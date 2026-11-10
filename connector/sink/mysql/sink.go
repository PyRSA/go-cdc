package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
	"unicode"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/sink"
)

// Sink upserts current-state rows into MySQL.
type Sink struct {
	db  *sql.DB
	cfg definition.Sink
	// columns reads one target column definition. It is a field so a test can
	// answer the lookup without a server; Open leaves it nil and columnReader
	// falls back to the SQL query.
	columns columnReader
}

// scanner is the one-row Scan surface (*sql.Row provides it).
type scanner interface {
	Scan(dest ...any) error
}

// columnReader reads the definition of one target column.
type columnReader interface {
	QueryRow(query string, args ...any) scanner
}

// sqlColumnReader adapts *sql.DB to columnReader. It is a named type because
// QueryRow on *sql.DB returns *sql.Row, not the scanner interface.
type sqlColumnReader struct {
	db *sql.DB
}

func (r sqlColumnReader) QueryRow(query string, args ...any) scanner {
	return r.db.QueryRow(query, args...)
}

// columnReader returns the configured reader, or the database when none is set.
func (s *Sink) columnReader() columnReader {
	if s.columns != nil {
		return s.columns
	}
	if s.db == nil {
		return nil
	}
	return sqlColumnReader{db: s.db}
}

// Open connects to the sink MySQL instance.
// timeZone must match source.server-time-zone so TIMESTAMP wall-clock strings
// round-trip (snapshot + binlog both emit that session's wall clock).
func Open(cfg definition.Sink, timeZone string) (*Sink, error) {
	zone := normalizeZone(timeZone)
	if !safeTimeZone(zone) {
		return nil, fmt.Errorf("sink.server-time-zone: %q", timeZone)
	}
	dsn, err := dsn(cfg.Username, cfg.Password, cfg.Hostname, cfg.Port, 30*time.Second, map[string]string{
		// Quoted so every pooled connection starts in the shared wall-clock zone.
		"time_zone": "'" + zone + "'",
	})
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("sink.mysql: %w", err)
	}
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(10 * time.Minute)
	return &Sink{db: db, cfg: cfg}, nil
}

// Open pings the database.
func (s *Sink) Open(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("sink.mysql: %w", err)
	}
	return nil
}

func normalizeZone(zone string) string {
	if zone == "" || strings.EqualFold(zone, "UTC") || zone == "Z" {
		return "+00:00"
	}
	return zone
}

func safeTimeZone(zone string) bool {
	if zone == "" || len(zone) > 64 {
		return false
	}
	for _, r := range zone {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r):
		case r == '/' || r == '_' || r == '+' || r == '-' || r == ':':
		default:
			return false
		}
	}
	return true
}

// CreateTable creates the projected table. An existing table is success.
func (s *Sink) CreateTable(table schema.Table) error {
	if table.Database != "" {
		if _, err := s.db.Exec("CREATE DATABASE IF NOT EXISTS " + schema.Quote(table.Database)); err != nil {
			return fmt.Errorf("sink.mysql: %w", err)
		}
	}
	statement, err := CreateTableSQL(table)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(statement)
	if isTableExists(err) {
		return nil
	}
	return err
}

// MySQL server errors that mean a replayed schema change may already be done.
// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
const (
	erDupFieldName       = 1060 // Duplicate column name
	erCantDropFieldOrKey = 1091 // Can't DROP; check that column/key exists
	erBadDB              = 1049 // Unknown database
	erBadTable           = 1051 // Unknown table (DROP TABLE of a missing table)
	erNoSuchTable        = 1146 // Table doesn't exist
)

// ApplySchema executes one rewritten DDL statement.
//
// ADD COLUMN, DROP COLUMN, DROP TABLE and TRUNCATE are replayed after a crash
// between the statement and the checkpoint file.
//
// A replayed DROP COLUMN that returns 1091 is not proof on its own: the same
// code covers a missing index or key. Every DROP target named by the ALTER
// (COLUMN, INDEX/KEY, PRIMARY KEY) is checked on the target; only when each is
// confirmed absent is the replay treated as done. Unrecognized clauses keep
// the original error.
//
// A replayed ADD COLUMN that finds the column already there is not proof that
// the statement is done. The column may have arrived from a different
// definition, in which case the target does not hold what the source sends. The
// target column is read back and compared with the definition this change
// carries; only an equal definition is a completed replay, and anything else is
// an error rather than a silent no-op.
//
// A TRUNCATE or DROP TABLE against a table that is gone is done as well: the
// table is absent, which is what DROP asked for and which leaves TRUNCATE with
// no rows. Permission refusals and other errors keep their original meaning.
func (s *Sink) ApplySchema(change schema.Change) error {
	if change.DDL == "" {
		return nil
	}
	if _, err := s.db.Exec(change.DDL); err != nil {
		handled, replayErr := s.replayedColumnChange(change, err)
		if handled {
			return replayErr
		}
		return fmt.Errorf("sink.mysql: %s: %w", change.DDL, err)
	}
	return nil
}

// replayedColumnChange decides what a failed schema statement means. It reports
// whether the error was a replay of a change that already reached its target
// state, and the error the caller must return for it.
func (s *Sink) replayedColumnChange(change schema.Change, execErr error) (handled bool, err error) {
	var mysqlErr *mysqldriver.MySQLError
	if !errors.As(execErr, &mysqlErr) {
		return false, nil
	}
	switch change.Action {
	case schema.ActionDropColumn:
		if mysqlErr.Number == erCantDropFieldOrKey {
			return s.verifyReplayedDropColumn(change)
		}
	case schema.ActionAddColumn:
		if mysqlErr.Number == erDupFieldName {
			return true, s.verifyReplayedAddColumn(change)
		}
	case schema.ActionDropTable:
		if mysqlErr.Number == erNoSuchTable || mysqlErr.Number == erBadTable || mysqlErr.Number == erBadDB {
			// 1146/1051 mean the table is gone; 1049 means its database is gone,
			// so the qualified target is absent too. Other codes (permission,
			// locks, …) are not absence and keep their error.
			return true, nil
		}
	case schema.ActionTruncate:
		if mysqlErr.Number == erNoSuchTable || mysqlErr.Number == erBadDB {
			// A replayed TRUNCATE can arrive after a later DROP TABLE already
			// removed the table. An absent table holds no rows, so the target is
			// in the state the statement asked for. A permission refusal is not
			// this case and keeps its error.
			return true, nil
		}
	}
	return false, nil
}

// verifyReplayedDropColumn proves a 1091 left every DROP target of the ALTER
// absent. Supported clauses are DROP COLUMN, DROP INDEX/KEY and DROP PRIMARY
// KEY. Any unrecognized clause cannot be proven, so the original error is kept.
func (s *Sink) verifyReplayedDropColumn(change schema.Change) (handled bool, err error) {
	targets, ok := dropAlterTargets(change.DDL)
	if !ok {
		return false, nil
	}
	for _, target := range targets {
		absent, lookupErr := s.dropTargetAbsent(change.Database, change.Table, target)
		if lookupErr != nil {
			return true, fmt.Errorf("sink.mysql: %s: %w", change.DDL, lookupErr)
		}
		if !absent {
			// Something the statement still names is present, so 1091 is not
			// proof the whole ALTER finished. Keep the original error.
			return false, nil
		}
	}
	return true, nil
}

type dropKind int

const (
	dropKindColumn dropKind = iota
	dropKindIndex
	dropKindPrimary
)

type dropTarget struct {
	kind dropKind
	name string
}

// dropAlterTargets lists every DROP target in an ALTER TABLE. ok is false when
// the statement is not a pure DROP ALTER or a clause cannot be recognized.
func dropAlterTargets(ddl string) ([]dropTarget, bool) {
	text := strings.TrimSpace(ddl)
	if !strings.HasPrefix(strings.ToUpper(text), "ALTER TABLE") {
		return nil, false
	}
	body := afterAlterTableRef(text)
	if body == "" {
		return nil, false
	}
	var out []dropTarget
	for _, clause := range splitTopLevelClauses(body) {
		target, ok := parseDropClause(strings.TrimSpace(clause))
		if !ok {
			return nil, false
		}
		out = append(out, target)
	}
	return out, len(out) > 0
}

func parseDropClause(clause string) (dropTarget, bool) {
	if !startsWithSQLWord(clause, "DROP") {
		return dropTarget{}, false
	}
	rest := strings.TrimSpace(clause[len("DROP"):])
	switch {
	case startsWithSQLWord(rest, "PRIMARY"):
		afterPrimary := strings.TrimSpace(rest[len("PRIMARY"):])
		if !startsWithSQLWord(afterPrimary, "KEY") {
			return dropTarget{}, false
		}
		afterKey := strings.TrimSpace(afterPrimary[len("KEY"):])
		if afterKey != "" {
			return dropTarget{}, false
		}
		return dropTarget{kind: dropKindPrimary, name: "PRIMARY"}, true
	case startsWithSQLWord(rest, "INDEX"), startsWithSQLWord(rest, "KEY"):
		keyword := "INDEX"
		if startsWithSQLWord(rest, "KEY") {
			keyword = "KEY"
		}
		body := strings.TrimSpace(rest[len(keyword):])
		name, n := readSQLIdent(body)
		if name == "" || strings.TrimSpace(body[n:]) != "" {
			return dropTarget{}, false
		}
		return dropTarget{kind: dropKindIndex, name: name}, true
	case startsWithSQLWord(rest, "FOREIGN"), startsWithSQLWord(rest, "CHECK"), startsWithSQLWord(rest, "CONSTRAINT"):
		return dropTarget{}, false
	}
	if startsWithSQLWord(rest, "COLUMN") {
		rest = strings.TrimSpace(rest[len("COLUMN"):])
	}
	name, n := readSQLIdent(rest)
	if name == "" || strings.TrimSpace(rest[n:]) != "" {
		return dropTarget{}, false
	}
	return dropTarget{kind: dropKindColumn, name: name}, true
}

func startsWithSQLWord(s, word string) bool {
	if len(s) < len(word) || !strings.EqualFold(s[:len(word)], word) {
		return false
	}
	if len(s) == len(word) {
		return true
	}
	next := rune(s[len(word)])
	return !unicode.IsLetter(next) && !unicode.IsDigit(next) && next != '_' && next != '$'
}

// dropTargetAbsent reports whether one DROP target is already gone on the sink.
func (s *Sink) dropTargetAbsent(database, table string, target dropTarget) (bool, error) {
	switch target.kind {
	case dropKindColumn:
		_, ok, err := s.liveColumn(database, table, target.name)
		if err != nil {
			return false, err
		}
		return !ok, nil
	case dropKindIndex, dropKindPrimary:
		ok, err := s.liveIndex(database, table, target.name)
		if err != nil {
			return false, err
		}
		return !ok, nil
	default:
		return false, fmt.Errorf("unsupported drop target kind %d", target.kind)
	}
}

// liveIndex reports whether an index (or PRIMARY) exists on the target table.
func (s *Sink) liveIndex(database, table, index string) (bool, error) {
	const query = `SELECT INDEX_NAME FROM information_schema.statistics
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ? LIMIT 1`
	reader := s.columnReader()
	if reader == nil {
		return false, fmt.Errorf("read index %s.%s.%s: no target connection", database, table, index)
	}
	var name string
	err := reader.QueryRow(query, database, table, index).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read index %s.%s.%s: %w", database, table, index, err)
	}
	return true, nil
}

// afterAlterTableRef returns the ALTER clause list after the table reference.
func afterAlterTableRef(sql string) string {
	upper := strings.ToUpper(sql)
	const keyword = "ALTER TABLE"
	i := strings.Index(upper, keyword)
	if i < 0 {
		return ""
	}
	rest := strings.TrimSpace(sql[i+len(keyword):])
	_, _, n := readQualifiedIdent(rest)
	if n == 0 {
		return ""
	}
	return strings.TrimSpace(rest[n:])
}

// splitTopLevelClauses splits on commas outside parentheses and quotes.
func splitTopLevelClauses(s string) []string {
	var out []string
	depth, start := 0, 0
	inSingle, inDouble, inBack := false, false, false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'':
			if !inDouble && !inBack {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && !inBack {
				inDouble = !inDouble
			}
		case '`':
			if !inSingle && !inDouble {
				inBack = !inBack
			}
		case '(':
			if !inSingle && !inDouble && !inBack {
				depth++
			}
		case ')':
			if !inSingle && !inDouble && !inBack && depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 && !inSingle && !inDouble && !inBack {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	return append(out, s[start:])
}

// readQualifiedIdent reads db.table or table from the start of s.
func readQualifiedIdent(s string) (db, table string, consumed int) {
	first, n := readSQLIdent(s)
	if n == 0 {
		return "", "", 0
	}
	rest := s[n:]
	if strings.HasPrefix(rest, ".") {
		second, n2 := readSQLIdent(rest[1:])
		return first, second, n + 1 + n2
	}
	return "", first, n
}

func readSQLIdent(s string) (string, int) {
	if s == "" {
		return "", 0
	}
	if s[0] == '`' {
		end := strings.IndexByte(s[1:], '`')
		if end < 0 {
			return strings.Trim(s, "`"), len(s)
		}
		return strings.ReplaceAll(s[1:1+end], "``", "`"), end + 2
	}
	i := 0
	for i < len(s) {
		r := rune(s[i])
		if unicode.IsLetter(r) || unicode.IsDigit(r) || s[i] == '_' || s[i] == '$' {
			i++
			continue
		}
		break
	}
	return s[:i], i
}

// verifyReplayedAddColumn proves that an ADD COLUMN rejected with 1060 left a
// column that holds what the source sends. A change without a structured
// definition cannot be proved and is refused instead of accepted blindly.
func (s *Sink) verifyReplayedAddColumn(change schema.Change) error {
	if change.ColumnDef == nil {
		return fmt.Errorf("sink.mysql: %s: the column already exists but this change carries no column definition to compare it with; the target may hold a different type",
			change.DDL)
	}
	if len(change.AddColumns) != 1 {
		return fmt.Errorf("sink.mysql: %s: the column already exists and the statement names %d columns; a multi-column ADD cannot be verified against one definition",
			change.DDL, len(change.AddColumns))
	}
	if !strings.EqualFold(change.AddColumns[0], change.ColumnDef.Name) {
		return fmt.Errorf("sink.mysql: %s: the statement names %q but its definition describes %q",
			change.DDL, change.AddColumns[0], change.ColumnDef.Name)
	}
	live, ok, err := s.liveColumn(change.Database, change.Table, change.ColumnDef.Name)
	if err != nil {
		return fmt.Errorf("sink.mysql: %s: %w", change.DDL, err)
	}
	if !ok {
		return fmt.Errorf("sink.mysql: %s: the server rejected the column as duplicate but %s.%s has no column %q",
			change.DDL, change.Database, change.Table, change.ColumnDef.Name)
	}
	if err := sameColumnDefinition(*change.ColumnDef, live); err != nil {
		return fmt.Errorf("sink.mysql: %s: %w", change.DDL, err)
	}
	return nil
}

// sameColumnDefinition reports whether the target column already holds what the
// source column describes. Type, nullability, charset and collation are compared:
// they decide whether the sink can store the source value the way the source
// sends it, and they are what an ADD COLUMN statement sets. Charset and
// collation compare case-insensitively; both empty (non-string columns) match.
func sameColumnDefinition(want, live schema.Column) error {
	if !sameColumnType(want.Type, live.Type) {
		return fmt.Errorf("column %q already exists as %s on the target but the source defines it as %s; the replayed ADD COLUMN did not apply, so the target cannot store the source value — apply the difference by hand or drop the target column",
			want.Name, live.Type, want.Type)
	}
	if want.Nullable != live.Nullable {
		return fmt.Errorf("column %q already exists %s on the target but the source defines it %s; the replayed ADD COLUMN did not apply",
			want.Name, nullabilityWord(live.Nullable), nullabilityWord(want.Nullable))
	}
	if !sameColumnText(want.Charset, live.Charset) {
		return fmt.Errorf("column %q already exists with charset %s on the target but the source defines charset %s; the replayed ADD COLUMN did not apply",
			want.Name, charsetWord(live.Charset), charsetWord(want.Charset))
	}
	if !sameColumnText(want.Collation, live.Collation) {
		return fmt.Errorf("column %q already exists with collation %s on the target but the source defines collation %s; the replayed ADD COLUMN did not apply",
			want.Name, collationWord(live.Collation), collationWord(want.Collation))
	}
	return nil
}

// sameColumnType compares two MySQL column types. information_schema reports
// them the way the source catalogue does, so the difference that matters is
// case and spacing, not spelling.
func sameColumnType(want, live string) bool {
	return normalizeColumnType(want) == normalizeColumnType(live)
}

func normalizeColumnType(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), ""))
}

// sameColumnText compares charset or collation names case-insensitively.
// Empty matches empty so numeric columns without a character set do not fail.
func sameColumnText(want, live string) bool {
	return strings.EqualFold(want, live)
}

func nullabilityWord(nullable bool) string {
	if nullable {
		return "NULL"
	}
	return "NOT NULL"
}

func charsetWord(charset string) string {
	if charset == "" {
		return "(none)"
	}
	return charset
}

func collationWord(collation string) string {
	if collation == "" {
		return "(none)"
	}
	return collation
}

// liveColumn reads one column of the target. A missing column is reported as ok
// false, because a 1060 without the column is a contradiction the caller has to
// see rather than treat as a completed replay.
func (s *Sink) liveColumn(database, table, column string) (schema.Column, bool, error) {
	const query = `SELECT COLUMN_TYPE, IS_NULLABLE, CHARACTER_SET_NAME, COLLATION_NAME
FROM information_schema.columns
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`
	reader := s.columnReader()
	if reader == nil {
		return schema.Column{}, false, fmt.Errorf("read column %s.%s.%s: no target connection", database, table, column)
	}
	var columnType, nullable string
	var charset, collation sql.NullString
	err := reader.QueryRow(query, database, table, column).Scan(&columnType, &nullable, &charset, &collation)
	if errors.Is(err, sql.ErrNoRows) {
		return schema.Column{}, false, nil
	}
	if err != nil {
		return schema.Column{}, false, fmt.Errorf("read column %s.%s.%s: %w", database, table, column, err)
	}
	return schema.Column{
		Name:      column,
		Type:      columnType,
		Nullable:  strings.EqualFold(nullable, "YES"),
		Charset:   charset.String,
		Collation: collation.String,
	}, true, nil
}

// Write applies a batch in one transaction, retrying when configured.
func (s *Sink) Write(batch []event.Event) error {
	if len(batch) == 0 {
		return nil
	}
	return writeWithRetry(s.cfg.MaxRetries, func() error { return s.writeOnce(batch) })
}

// writeWithRetry runs once until it succeeds, the attempts run out, or the
// commit itself fails. A failed COMMIT is not retried: the server may already
// have committed, and another attempt would insert a second row when the table
// has no unique key.
func writeWithRetry(maxRetries int, once func() error) error {
	attempts := maxRetries + 1
	if attempts < 1 {
		attempts = 1
	}
	var err error
	for i := 0; i < attempts; i++ {
		err = once()
		if err == nil || isCommitError(err) {
			return err
		}
	}
	return err
}

func (s *Sink) writeOnce(batch []event.Event) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	for _, item := range batch {
		statements, err := Statements(item)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		for _, statement := range statements {
			if _, err := tx.Exec(statement.SQL, statement.Args...); err != nil {
				_ = tx.Rollback()
				return formatWriteErr(item, err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return &commitError{err: err}
	}
	return nil
}

// commitError marks a failed COMMIT. The transaction may or may not be durable,
// so Write must not run the batch again.
type commitError struct {
	err error
}

func (e *commitError) Error() string {
	return fmt.Sprintf("sink.mysql: commit transaction: %v", e.err)
}

func (e *commitError) Unwrap() error { return e.err }

func isCommitError(err error) bool {
	var committed *commitError
	return errors.As(err, &committed)
}

func formatWriteErr(item event.Event, err error) error {
	if isTableMissing(err) || isUnknownDatabase(err) || isWriteDenied(err) {
		return fmt.Errorf("sink.mysql: table %s.%s does not exist: %w", item.Database, item.Table, err)
	}
	return err
}

func isTableMissing(err error) bool {
	var mysqlErr *mysqldriver.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == erNoSuchTable
}

func isUnknownDatabase(err error) bool {
	var mysqlErr *mysqldriver.MySQLError
	return errors.As(err, &mysqlErr) && (mysqlErr.Number == erBadDB || mysqlErr.Number == 1044)
}

func isWriteDenied(err error) bool {
	var mysqlErr *mysqldriver.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1142
}

func isTableExists(err error) bool {
	var mysqlErr *mysqldriver.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1050
}

// Flush commits nothing extra. Write already commits each batch.
func (s *Sink) Flush() error { return nil }

// Close closes the database handle.
func (s *Sink) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func dsn(user, password, host string, port int, timeout time.Duration, params map[string]string) (string, error) {
	cfg := mysqldriver.NewConfig()
	cfg.User = user
	cfg.Passwd = password
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(host, fmt.Sprintf("%d", port))
	cfg.Timeout = timeout
	cfg.ParseTime = false
	cfg.Params = map[string]string{}
	for key, value := range params {
		cfg.Params[key] = value
	}
	return cfg.FormatDSN(), nil
}

var _ sink.Sink = (*Sink)(nil)
