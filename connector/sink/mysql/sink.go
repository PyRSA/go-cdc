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

// ApplySchema executes one rewritten DDL statement.
func (s *Sink) ApplySchema(change schema.Change) error {
	if change.DDL == "" {
		return nil
	}
	_, err := s.db.Exec(change.DDL)
	if err != nil {
		return fmt.Errorf("sink.mysql: %s: %w", change.DDL, err)
	}
	return nil
}

// Write applies a batch in one transaction, retrying when configured.
func (s *Sink) Write(batch []event.Event) error {
	if len(batch) == 0 {
		return nil
	}
	attempts := s.cfg.MaxRetries + 1
	var err error
	for i := 0; i < attempts; i++ {
		err = s.writeOnce(batch)
		if err == nil {
			return nil
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
	return tx.Commit()
}

func formatWriteErr(item event.Event, err error) error {
	if isTableMissing(err) || isUnknownDatabase(err) || isWriteDenied(err) {
		return fmt.Errorf("sink.mysql: table %s.%s does not exist: %w", item.Database, item.Table, err)
	}
	return err
}

func isTableMissing(err error) bool {
	var mysqlErr *mysqldriver.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1146
}

func isUnknownDatabase(err error) bool {
	var mysqlErr *mysqldriver.MySQLError
	return errors.As(err, &mysqlErr) && (mysqlErr.Number == 1049 || mysqlErr.Number == 1044)
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
