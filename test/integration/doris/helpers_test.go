//go:build doris_integration

package doris_it

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/sink/doris"
)

// integrationConfig reads the Doris endpoint from the environment. Tests are
// skipped unless DORIS_HOST is set, so `go test ./...` never touches a cluster.
func integrationConfig(t *testing.T) definition.Sink {
	t.Helper()
	host := strings.TrimSpace(os.Getenv("DORIS_HOST"))
	if host == "" {
		if os.Getenv("CI") != "" {
			// A skipped suite in CI would report success without touching Doris.
			t.Fatal("DORIS_HOST is not set: the doris_integration suite would silently skip in CI")
		}
		t.Skip("DORIS_HOST not set; start test/integration/docker-compose.doris.yml first")
	}
	return definition.Sink{
		Type:            "doris",
		Hostname:        host,
		Port:            envInt(t, "DORIS_HTTP_PORT", 8030),
		SQLPort:         envInt(t, "DORIS_SQL_PORT", 9030),
		Username:        envString("DORIS_USER", "root"),
		Password:        os.Getenv("DORIS_PASSWORD"),
		AutoCreate:      true,
		WriteBatchSize:  1000,
		WriteBatchEvery: time.Second,
		MaxRetries:      envInt(t, "DORIS_MAX_RETRIES", 3),
		RequestTimeout:  envDuration(t, "DORIS_REQUEST_TIMEOUT", 30*time.Second),
		Buckets:         envString("DORIS_BUCKETS", "auto"),
		ReplicationNum:  envInt(t, "DORIS_REPLICATION_NUM", 1),
	}
}

func envString(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("%s=%q is not an integer: %v", name, raw, err)
	}
	return value
}

func envDuration(t *testing.T, name string, fallback time.Duration) time.Duration {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("%s=%q is not a duration: %v", name, raw, err)
	}
	return value
}

func openDB(t *testing.T, cfg definition.Sink) *sql.DB {
	t.Helper()
	mysqlCfg := mysqldriver.NewConfig()
	mysqlCfg.User = cfg.Username
	mysqlCfg.Passwd = cfg.Password
	mysqlCfg.Net = "tcp"
	mysqlCfg.Addr = fmt.Sprintf("%s:%d", cfg.Hostname, cfg.SQLPort)
	mysqlCfg.Timeout = 10 * time.Second
	mysqlCfg.ReadTimeout = 30 * time.Second
	mysqlCfg.WriteTimeout = 30 * time.Second
	mysqlCfg.ParseTime = false
	db, err := sql.Open("mysql", mysqlCfg.FormatDSN())
	if err != nil {
		t.Fatalf("open Doris SQL: %v", err)
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })
	waitForBackend(t, db)
	return db
}

func waitForBackend(t *testing.T, db *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	var lastErr error
	for {
		count, err := aliveBackends(db)
		if err == nil && count > 0 {
			return
		}
		lastErr = err
		if time.Now().After(deadline) {
			t.Fatalf("no alive Doris BE after 3m: %v", lastErr)
		}
		time.Sleep(2 * time.Second)
	}
}

func aliveBackends(db *sql.DB) (int, error) {
	rows, err := db.Query("SHOW BACKENDS")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	aliveAt := -1
	for i, name := range columns {
		if strings.EqualFold(name, "Alive") {
			aliveAt = i
			break
		}
	}
	if aliveAt < 0 {
		return 0, fmt.Errorf("SHOW BACKENDS has no Alive column: %v", columns)
	}
	alive := 0
	for rows.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return 0, err
		}
		if strings.EqualFold(cell(values[aliveAt]), "true") {
			alive++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return alive, nil
}

func openSink(t *testing.T, cfg definition.Sink) *doris.Sink {
	t.Helper()
	sk, err := doris.Open(cfg)
	if err != nil {
		t.Fatalf("open Doris sink: %v", err)
	}
	if err := sk.Open(context.Background()); err != nil {
		_ = sk.Close()
		t.Fatalf("ping Doris SQL endpoint: %v", err)
	}
	t.Cleanup(func() { _ = sk.Close() })
	return sk
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

func createDatabase(t *testing.T, db *sql.DB, database string) {
	t.Helper()
	execSQL(t, db, "CREATE DATABASE IF NOT EXISTS "+schema.Quote(database))
	t.Cleanup(func() {
		_, _ = db.Exec("DROP DATABASE IF EXISTS " + schema.Quote(database))
	})
}

func execSQL(t *testing.T, db *sql.DB, statement string) {
	t.Helper()
	if _, err := db.Exec(statement); err != nil {
		t.Fatalf("exec %s: %v", statement, err)
	}
}

func queryRow(t *testing.T, db *sql.DB, query string) map[string]string {
	t.Helper()
	row, ok, err := queryOptionalRow(db, query)
	if err != nil {
		t.Fatalf("query %s: %v", query, err)
	}
	if !ok {
		t.Fatalf("query %s returned no rows", query)
	}
	return row
}

func queryOptionalRow(db *sql.DB, query string) (map[string]string, bool, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, false, err
	}
	if !rows.Next() {
		return nil, false, rows.Err()
	}
	values := make([]any, len(columns))
	ptrs := make([]any, len(columns))
	for i := range values {
		ptrs[i] = &values[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, false, err
	}
	row := make(map[string]string, len(columns))
	for i, name := range columns {
		row[strings.ToLower(name)] = cell(values[i])
	}
	return row, true, rows.Err()
}

func queryCount(t *testing.T, db *sql.DB, database, table string) int {
	t.Helper()
	var count int64
	query := "SELECT COUNT(*) FROM " + schema.Qualified(database, table)
	if err := db.QueryRow(query).Scan(&count); err != nil {
		t.Fatalf("count %s.%s: %v", database, table, err)
	}
	return int(count)
}

// descCell keeps the raw DESC text and whether the driver returned SQL NULL.
// The Default column needs both: NULL means "no default", while an empty
// string means the column really defaults to the empty string (see
// client_sql.descDefault).
type descCell struct {
	Text   string
	IsNull bool
}

// describeTable returns the full DESC row for each column, keyed by the
// lower-cased Field name. Tests read Null/Default/Key/Extra from here instead
// of collapsing DESC to a type map.
func describeTable(t *testing.T, db *sql.DB, database, table string) map[string]map[string]descCell {
	t.Helper()
	rows, err := db.Query("DESC " + schema.Qualified(database, table))
	if err != nil {
		t.Fatalf("desc %s.%s: %v", database, table, err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	fieldAt := -1
	for i, name := range columns {
		if strings.EqualFold(name, "Field") {
			fieldAt = i
			break
		}
	}
	if fieldAt < 0 {
		t.Fatalf("DESC %s.%s has no Field column: %v", database, table, columns)
	}
	out := map[string]map[string]descCell{}
	for rows.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		row := make(map[string]descCell, len(columns))
		for i, name := range columns {
			row[strings.ToLower(name)] = descCell{Text: cell(values[i]), IsNull: values[i] == nil}
		}
		field := row["field"].Text
		if field == "" {
			t.Fatalf("DESC %s.%s returned an empty Field: %v", database, table, row)
		}
		out[strings.ToLower(field)] = row
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatalf("DESC %s.%s returned no columns", database, table)
	}
	return out
}

func describeColumns(t *testing.T, db *sql.DB, database, table string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for name, row := range describeTable(t, db, database, table) {
		out[name] = row["type"].Text
	}
	return out
}

func cell(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case []byte:
		return string(typed)
	case string:
		return typed
	case bool:
		if typed {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprint(typed)
	}
}

func makeEvent(op event.Op, database, table string, columns []string, after map[string]any) event.Event {
	return event.Event{
		Op:             op,
		Database:       database,
		Table:          table,
		SourceDatabase: database,
		SourceTable:    table,
		Columns:        columns,
		Keys:           []string{"id"},
		After:          after,
	}
}

func writeEvents(t *testing.T, sk *doris.Sink, events ...event.Event) {
	t.Helper()
	if err := sk.Write(events); err != nil {
		t.Fatalf("write events: %v", err)
	}
	if err := sk.Flush(); err != nil {
		t.Fatalf("flush events: %v", err)
	}
}

func assertText(t *testing.T, row map[string]string, key, want string) {
	t.Helper()
	if got := row[key]; got != want {
		t.Errorf("%s=%q want %q (row=%v)", key, got, want, row)
	}
}

func assertNumber(t *testing.T, row map[string]string, key, want string) {
	t.Helper()
	got, err := strconv.ParseFloat(strings.TrimSpace(row[key]), 64)
	if err != nil {
		t.Errorf("%s=%q is not numeric: %v", key, row[key], err)
		return
	}
	wantValue, err := strconv.ParseFloat(want, 64)
	if err != nil {
		t.Fatalf("bad expected number %q: %v", want, err)
	}
	if got != wantValue {
		t.Errorf("%s=%v want %v (row=%v)", key, got, wantValue, row)
	}
}

func assertPrefix(t *testing.T, row map[string]string, key, prefix string) {
	t.Helper()
	if got := row[key]; !strings.HasPrefix(got, prefix) {
		t.Errorf("%s=%q want prefix %q (row=%v)", key, got, prefix, row)
	}
}
