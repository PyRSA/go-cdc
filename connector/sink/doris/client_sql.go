package doris

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
)

// errLoadNotFound means SHOW LOAD has no row for the label yet.
var errLoadNotFound = errors.New("doris: load label not found")

// sqlClient talks to the Doris FE MySQL protocol port (sql-port).
type sqlClient struct {
	db *sql.DB
}

func openSQL(cfg definition.Sink) (*sqlClient, error) {
	mysqlCfg := mysqldriver.NewConfig()
	mysqlCfg.User = cfg.Username
	mysqlCfg.Passwd = cfg.Password
	mysqlCfg.Net = "tcp"
	mysqlCfg.Addr = net.JoinHostPort(cfg.Hostname, strconv.Itoa(cfg.SQLPort))
	mysqlCfg.Timeout = cfg.RequestTimeout
	mysqlCfg.ReadTimeout = cfg.RequestTimeout
	mysqlCfg.WriteTimeout = cfg.RequestTimeout
	mysqlCfg.ParseTime = false
	// Do not select a database: Doris routes by the qualified table name and
	// the configured database is optional.
	dsn := mysqlCfg.FormatDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("sink.doris: sql: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return &sqlClient{db: db}, nil
}

func (c *sqlClient) Ping(ctx context.Context) error {
	if err := c.db.PingContext(ctx); err != nil {
		return fmt.Errorf("sink.doris: sql ping: %w", err)
	}
	return nil
}

func (c *sqlClient) Exec(ctx context.Context, statement string) error {
	if _, err := c.db.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("sink.doris: %w", err)
	}
	return nil
}

func (c *sqlClient) Close() error {
	if c == nil || c.db == nil {
		return nil
	}
	return c.db.Close()
}

// descColumn is one row of Doris DESC.
type descColumn struct {
	Name       string
	Type       string
	Nullable   bool
	Key        bool
	Default    string
	HasDefault bool
	Extra      string
}

// descTable reads the live target structure with DESC. The result is the
// source of truth for compatibility checks and ADD COLUMN idempotency.
func (c *sqlClient) descTable(ctx context.Context, database, table string) ([]descColumn, error) {
	rows, err := c.db.QueryContext(ctx, "DESC "+schema.Qualified(database, table))
	if err != nil {
		return nil, fmt.Errorf("sink.doris: describe %s: %w", schema.Qualified(database, table), err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	lower := make([]string, len(columns))
	for i, name := range columns {
		lower[i] = strings.ToLower(name)
	}
	var out []descColumn
	for rows.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]string, len(columns))
		for i, name := range lower {
			row[name] = valueString(values[i])
		}
		column := descColumn{
			Name:     row["field"],
			Type:     row["type"],
			Nullable: isYes(row["null"]),
			Key:      isYes(row["key"]),
			Extra:    row["extra"],
		}
		// DESC Default is SQL NULL when the column has no default. An empty
		// string means DEFAULT '' and must keep HasDefault=true (G-05 extras).
		defaultIdx := indexOf(lower, "default")
		if defaultIdx >= 0 {
			column.Default, column.HasDefault = descDefault(values[defaultIdx])
		}
		out = append(out, column)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("sink.doris: table %s does not exist or has no columns", schema.Qualified(database, table))
	}
	return out, nil
}

// showCreateTable returns the Doris DDL for one table. DESC reports the same
// Key=YES marker for UNIQUE, DUPLICATE and AGGREGATE tables, so the target
// model must be read from SHOW CREATE TABLE.
func (c *sqlClient) showCreateTable(ctx context.Context, database, table string) (string, error) {
	qualified := schema.Qualified(database, table)
	rows, err := c.db.QueryContext(ctx, "SHOW CREATE TABLE "+qualified)
	if err != nil {
		return "", fmt.Errorf("sink.doris: show create table %s: %w", qualified, err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	if len(columns) < 2 {
		return "", fmt.Errorf("sink.doris: show create table %s returned %d columns", qualified, len(columns))
	}
	// Doris returns (Table, Create Table); locate the DDL column by name and
	// fall back to the second column for older layouts.
	createIdx := 1
	for i, name := range columns {
		if strings.Contains(strings.ToLower(name), "create") {
			createIdx = i
			break
		}
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", fmt.Errorf("sink.doris: show create table %s: %w", qualified, err)
		}
		return "", fmt.Errorf("sink.doris: show create table %s returned no rows", qualified)
	}
	values := make([]any, len(columns))
	ptrs := make([]any, len(columns))
	for i := range values {
		ptrs[i] = &values[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return "", err
	}
	create := valueString(values[createIdx])
	if strings.TrimSpace(create) == "" {
		return "", fmt.Errorf("sink.doris: show create table %s returned empty DDL", qualified)
	}
	return create, nil
}

func isYes(value string) bool {
	return strings.EqualFold(value, "yes") || strings.EqualFold(value, "true") || strings.EqualFold(value, "1")
}

func indexOf(values []string, want string) int {
	for i, value := range values {
		if value == want {
			return i
		}
	}
	return -1
}

// descDefault maps a DESC Default cell. nil / "NULL" / "\N" mean no default;
// any other value (including "") is a real default expression or literal.
func descDefault(value any) (string, bool) {
	if value == nil {
		return "", false
	}
	text := valueString(value)
	if strings.EqualFold(text, "NULL") || text == `\N` {
		return "", false
	}
	return text, true
}

func valueString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case []byte:
		return string(typed)
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

// loadState is the subset of SHOW LOAD used by the Label state machine.
type loadState struct {
	State    string
	ErrorMsg string
}

// showLoad reads the latest load job for a label. SHOW LOAD is scoped to one
// database, so the caller passes the routed database explicitly; the label is
// validated and escaped before it is inlined.
//
// Prefer WHERE Label = …; if Doris returns no row (observed on some 2.1.x
// Stream Load paths), fall back to scanning recent SHOW LOAD rows and matching
// the label case-insensitively.
func (c *sqlClient) showLoad(ctx context.Context, database, label string) (loadState, error) {
	if database == "" {
		return loadState{}, fmt.Errorf("sink.doris: show load requires a database")
	}
	if !validLabel(label) {
		return loadState{}, fmt.Errorf("sink.doris: invalid label %q", label)
	}
	state, err := c.showLoadQuery(ctx,
		"SHOW LOAD FROM "+schema.Quote(database)+" WHERE Label = '"+escapeSQLString(label)+"'",
		label, true)
	if err == nil || !errors.Is(err, errLoadNotFound) {
		return state, err
	}
	return c.showLoadQuery(ctx,
		"SHOW LOAD FROM "+schema.Quote(database),
		label, false)
}

func (c *sqlClient) showLoadQuery(ctx context.Context, statement, label string, exactFilter bool) (loadState, error) {
	rows, err := c.db.QueryContext(ctx, statement)
	if err != nil {
		return loadState{}, fmt.Errorf("sink.doris: show load %q: %w", label, err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return loadState{}, err
	}
	lower := make([]string, len(columns))
	for i, name := range columns {
		lower[i] = strings.ToLower(name)
	}
	var out loadState
	found := false
	for rows.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return loadState{}, err
		}
		row := make(map[string]string, len(columns))
		for i, name := range lower {
			row[name] = valueString(values[i])
		}
		if !exactFilter && !strings.EqualFold(row["label"], label) {
			continue
		}
		out = loadState{State: row["state"], ErrorMsg: row["errormsg"]}
		found = true
		// Keep scanning so the last matching row wins (latest job for the label).
	}
	if err := rows.Err(); err != nil {
		return loadState{}, err
	}
	if !found {
		return loadState{}, errLoadNotFound
	}
	return out, nil
}

// validLabel limits labels to the character set Doris accepts and keeps them
// free of credentials. Generated labels are checked here as a defensive guard.
func validLabel(label string) bool {
	if label == "" || len(label) > 128 {
		return false
	}
	for _, r := range label {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_' || r == '-':
		default:
			return false
		}
	}
	return true
}
