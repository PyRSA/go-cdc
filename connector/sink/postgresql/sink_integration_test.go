//go:build postgres_integration

package postgresql

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
)

// integrationDSN returns PG_DSN or skips the test. A skip here means the server
// was never contacted, so it must not be read as a passing write path. CI sets
// PG_IT_REQUIRED=1, which turns a missing DSN into a failure so a broken
// environment cannot turn the whole suite green.
func integrationDSN(t *testing.T) string {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("PG_DSN"))
	if dsn == "" {
		if os.Getenv("PG_IT_REQUIRED") == "1" {
			t.Fatal("PG_DSN is not set but PG_IT_REQUIRED=1: the integration environment is broken")
		}
		t.Skip("PG_DSN not set; skipping PostgreSQL integration test")
	}
	return dsn
}

func qual(schemaName, table string) string {
	return pgx.Identifier{schemaName, table}.Sanitize()
}

type itEnv struct {
	ctx    context.Context
	conn   *pgx.Conn
	schema string
	sink   *Sink
}

// newITEnv creates a dedicated schema, runs the DDL for it, and opens a sink
// that writes there.
func newITEnv(t *testing.T, ddl func(schemaName string) []string) *itEnv {
	t.Helper()
	dsn := integrationDSN(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	schemaName := fmt.Sprintf("cdc_it_%d", time.Now().UnixNano())
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schemaName}.Sanitize()); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schemaName}.Sanitize()+" CASCADE")
	})
	if ddl != nil {
		for _, statement := range ddl(schemaName) {
			if _, err := conn.Exec(ctx, statement); err != nil {
				t.Fatalf("ddl %q: %v", statement, err)
			}
		}
	}
	cfg := sinkConfigFromDSN(t, dsn, schemaName)
	sink, err := Open(cfg, "Asia/Shanghai")
	if err != nil {
		t.Fatalf("open sink: %v", err)
	}
	if err := sink.Open(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	return &itEnv{ctx: ctx, conn: conn, schema: schemaName, sink: sink}
}

func sinkConfigFromDSN(t *testing.T, dsn, schemaName string) definition.Sink {
	t.Helper()
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PG_DSN: %v", err)
	}
	sslMode := "prefer"
	if u, err := url.Parse(dsn); err == nil {
		if mode := u.Query().Get("sslmode"); mode != "" {
			sslMode = mode
		}
	}
	return definition.Sink{
		Type:           "postgresql",
		Hostname:       parsed.Host,
		Port:           int(parsed.Port),
		Username:       parsed.User,
		Password:       parsed.Password,
		Database:       parsed.Database,
		Schema:         schemaName,
		SSLMode:        sslMode,
		ConnectTimeout: 10 * time.Second,
		WriteBatchSize: 1000,
		MaxRetries:     1,
	}
}

func (e *itEnv) event(op event.Op, table string, columns, keys []string, row map[string]any) event.Event {
	return event.Event{
		Op:             op,
		Database:       e.schema,
		Table:          table,
		SourceDatabase: "app",
		SourceTable:    table,
		// A route rule maps the source database onto the target schema.
		Routed:  true,
		Columns: columns,
		Keys:    keys,
		After:   row,
	}
}

func TestPostgresSinkDML(t *testing.T) {
	env := newITEnv(t, func(s string) []string {
		return []string{"CREATE TABLE " + qual(s, "users") +
			" (id bigint PRIMARY KEY, name text, amount numeric(20,6), created_at timestamp," +
			" created_tz timestamptz, raw bytea, qty int NOT NULL)"}
	})
	columns := []string{"id", "name", "amount", "created_at", "created_tz", "raw", "qty"}
	keys := []string{"id"}

	alice := map[string]any{
		"id":         int64(1),
		"name":       "alice",
		"amount":     "12345.678901",
		"created_at": "2024-01-02 03:04:05.000000",
		"created_tz": "2024-01-02 03:04:05.000000",
		"raw":        []byte{0x00, 0x01, 0xff},
		"qty":        int64(3),
	}
	nulls := map[string]any{"id": int64(2), "name": "bob", "qty": int64(0)}
	batch := []event.Event{
		env.event(event.OpCreate, "users", columns, keys, alice),
		env.event(event.OpRead, "users", columns, keys, nulls),
	}
	if err := env.sink.Write(batch); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	name, amount, createdAt, createdTZ, raw, qty := env.readUser(t, 1)
	if name != "alice" || amount != "12345.678901" {
		t.Fatalf("name %q amount %q", name, amount)
	}
	if createdAt != "2024-01-02 03:04:05" {
		t.Fatalf("created_at %q", createdAt)
	}
	// server-time-zone Asia/Shanghai: the same text became 03:04:05+08.
	if createdTZ != "2024-01-01 19:04:05" {
		t.Fatalf("created_tz in UTC %q", createdTZ)
	}
	if string(raw) != string([]byte{0x00, 0x01, 0xff}) {
		t.Fatalf("raw %v", raw)
	}
	if qty != 3 {
		t.Fatalf("qty %d", qty)
	}

	nullName, nullAmount, nullCreated, _, nullRaw, _ := env.readUser(t, 2)
	if nullName != "bob" || nullAmount != "" || nullCreated != "" || nullRaw != nil {
		t.Fatalf("null row %q %q %q %v", nullName, nullAmount, nullCreated, nullRaw)
	}

	// Upsert: same key, new value.
	alice["name"] = "alice2"
	if err := env.sink.Write([]event.Event{env.event(event.OpCreate, "users", columns, keys, alice)}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if name, _, _, _, _, _ = env.readUser(t, 1); name != "alice2" {
		t.Fatalf("upsert did not update: %q", name)
	}
	if rows := env.count(t, "users"); rows != 2 {
		t.Fatalf("upsert inserted a duplicate: %d rows", rows)
	}

	// Business key change becomes DELETE plus UPSERT.
	moved := map[string]any{"id": int64(20), "name": "alice2", "amount": "1.5", "qty": int64(1)}
	update := env.event(event.OpUpdate, "users", columns, keys, moved)
	update.Before = map[string]any{"id": int64(1), "name": "alice2"}
	if err := env.sink.Write([]event.Event{update}); err != nil {
		t.Fatalf("key change: %v", err)
	}
	if env.exists(t, "users", 1) {
		t.Fatal("old key still present")
	}
	if !env.exists(t, "users", 20) {
		t.Fatal("new key missing")
	}

	// Delete removes the row and is idempotent.
	del := env.event(event.OpDelete, "users", columns, keys, nil)
	del.Before = map[string]any{"id": int64(20)}
	if err := env.sink.Write([]event.Event{del}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if env.exists(t, "users", 20) {
		t.Fatal("delete left the row")
	}
	if err := env.sink.Write([]event.Event{del}); err != nil {
		t.Fatalf("second delete must not fail: %v", err)
	}
}

func TestPostgresSinkRollsBackWholeBatch(t *testing.T) {
	env := newITEnv(t, func(s string) []string {
		return []string{"CREATE TABLE " + qual(s, "users") + " (id bigint PRIMARY KEY, name text, qty int NOT NULL)"}
	})
	columns := []string{"id", "name", "qty"}
	keys := []string{"id"}
	batch := []event.Event{
		env.event(event.OpCreate, "users", columns, keys, map[string]any{"id": int64(1), "name": "ok", "qty": int64(1)}),
		env.event(event.OpCreate, "users", columns, keys, map[string]any{"id": int64(2), "name": "bad"}),
	}
	err := env.sink.Write(batch)
	if err == nil {
		t.Fatal("expected NOT NULL violation")
	}
	if !strings.Contains(err.Error(), "23502") {
		t.Fatalf("error must carry the SQLSTATE: %v", err)
	}
	if !strings.Contains(err.Error(), "users") {
		t.Fatalf("error must name the table: %v", err)
	}
	if rows := env.count(t, "users"); rows != 0 {
		t.Fatalf("partial batch applied: %d rows", rows)
	}
}

func TestPostgresSinkTypeFidelity(t *testing.T) {
	env := newITEnv(t, func(s string) []string {
		return []string{"CREATE TABLE " + qual(s, "metrics") +
			" (id bigint PRIMARY KEY, big numeric(20,0), f8 double precision, d date, t timestamp, note text)"}
	})
	columns := []string{"id", "big", "f8", "d", "t", "note"}
	keys := []string{"id"}
	row := map[string]any{
		"id":   int64(1),
		"big":  fmt.Sprintf("%d", uint64(1)<<63), // uint64 above MaxInt64 keeps exact text
		"f8":   "0.123456789012345",
		"d":    "2024-02-29",
		"t":    "2024-02-29 23:59:59.123456",
		"note": "plain",
	}
	if err := env.sink.Write([]event.Event{env.event(event.OpCreate, "metrics", columns, keys, row)}); err != nil {
		t.Fatalf("write: %v", err)
	}
	var big, f8, d, ts, note string
	err := env.conn.QueryRow(env.ctx,
		"SELECT big::text, f8::text, d::text, t::text, note FROM "+qual(env.schema, "metrics")+" WHERE id = 1").
		Scan(&big, &f8, &d, &ts, &note)
	if err != nil {
		t.Fatal(err)
	}
	if big != "9223372036854775808" {
		t.Fatalf("numeric lost precision: %q", big)
	}
	if f8 != "0.123456789012345" {
		t.Fatalf("float8 %q", f8)
	}
	if d != "2024-02-29" || ts != "2024-02-29 23:59:59.123456" {
		t.Fatalf("temporal %q %q", d, ts)
	}
	if note != "plain" {
		t.Fatalf("note %q", note)
	}
}

func TestPostgresSinkTableWithoutKeyInsertsDuplicates(t *testing.T) {
	env := newITEnv(t, func(s string) []string {
		return []string{"CREATE TABLE " + qual(s, "nokey") + " (id bigint, name text)"}
	})
	columns := []string{"id", "name"}
	row := map[string]any{"id": int64(1), "name": "a"}
	item := env.event(event.OpCreate, "nokey", columns, nil, row)
	if err := env.sink.Write([]event.Event{item, item}); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Documented PR1 limitation: without a business key a replay inserts again.
	if rows := env.count(t, "nokey"); rows != 2 {
		t.Fatalf("rows %d", rows)
	}
	del := env.event(event.OpDelete, "nokey", columns, nil, nil)
	del.Before = map[string]any{"id": int64(1)}
	if err := env.sink.Write([]event.Event{del}); err == nil {
		t.Fatal("delete without key must fail")
	}
}

func TestPostgresSinkRoutedDatabaseBecomesSchema(t *testing.T) {
	env := newITEnv(t, func(s string) []string {
		return []string{"CREATE TABLE " + qual(s, "routed") + " (id bigint PRIMARY KEY, name text)"}
	})
	item := env.event(event.OpCreate, "routed", []string{"id", "name"}, []string{"id"},
		map[string]any{"id": int64(1), "name": "x"})
	// A route rewrote the database segment to the target schema name.
	item.Database = env.schema
	item.SourceDatabase = "app"
	item.Routed = true
	if err := env.sink.Write([]event.Event{item}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if rows := env.count(t, "routed"); rows != 1 {
		t.Fatalf("rows %d", rows)
	}

	// An identity route keeps the source database name; the routed database
	// segment still names the schema, so the row must land in this schema.
	identity := env.event(event.OpCreate, "routed", []string{"id", "name"}, []string{"id"},
		map[string]any{"id": int64(2), "name": "y"})
	identity.SourceDatabase = env.schema
	identity.Routed = true
	if err := env.sink.Write([]event.Event{identity}); err != nil {
		t.Fatalf("identity route write: %v", err)
	}
	if rows := env.count(t, "routed"); rows != 2 {
		t.Fatalf("rows after identity route %d", rows)
	}
}

// TestPostgresSinkRouteMissUsesSinkSchema pins the rule that only a matched
// route may name the schema. An unrouted event still carries the MySQL source
// database in Database, so a sink that trusted that field would write into a
// PostgreSQL schema named after the source database.
func TestPostgresSinkRouteMissUsesSinkSchema(t *testing.T) {
	env := newITEnv(t, func(s string) []string {
		return []string{"CREATE TABLE " + qual(s, "unrouted") + " (id bigint PRIMARY KEY, name text)"}
	})
	// Decoy schema named like the source database: the fallback is only proven
	// when the write could have succeeded there instead.
	sourceDB := fmt.Sprintf("cdc_it_src_%d", time.Now().UnixNano())
	env.exec(t, "CREATE SCHEMA "+pgx.Identifier{sourceDB}.Sanitize())
	t.Cleanup(func() {
		_, _ = env.conn.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{sourceDB}.Sanitize()+" CASCADE")
	})
	env.exec(t, "CREATE TABLE "+qual(sourceDB, "unrouted")+" (id bigint PRIMARY KEY, name text)")

	item := env.event(event.OpCreate, "unrouted", []string{"id", "name"}, []string{"id"},
		map[string]any{"id": int64(1), "name": "unrouted"})
	item.Routed = false
	item.Database = sourceDB
	item.SourceDatabase = sourceDB
	if err := env.sink.Write([]event.Event{item}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if rows := env.count(t, "unrouted"); rows != 1 {
		t.Fatalf("sink schema %q rows = %d, want 1", env.schema, rows)
	}
	if rows := env.countIn(t, sourceDB, "unrouted"); rows != 0 {
		t.Fatalf("source database %q was used as the schema: %d row(s) written there", sourceDB, rows)
	}
}

// TestPostgresSinkConflictWithoutConstraintIsPermanent covers the ON CONFLICT
// inference failure (42P10): the target table has no primary key and no unique
// index on the business key, so PostgreSQL rejects the statement while planning
// it. The error must surface unchanged, the batch must not stay applied, and the
// permanent SQLSTATE must not consume the retry budget.
func TestPostgresSinkConflictWithoutConstraintIsPermanent(t *testing.T) {
	env := newITEnv(t, func(s string) []string {
		return []string{"CREATE TABLE " + qual(s, "noconflict") + " (id bigint, name text)"}
	})
	var backoffs int
	env.sink.sleep = func(ctx context.Context, d time.Duration) error {
		backoffs++
		return sleepContext(ctx, d)
	}
	item := env.event(event.OpCreate, "noconflict", []string{"id", "name"}, []string{"id"},
		map[string]any{"id": int64(1), "name": "x"})
	err := env.sink.Write([]event.Event{item})
	if err == nil {
		t.Fatal("expected a 42P10 conflict target error")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42P10" {
		t.Fatalf("error must carry SQLSTATE 42P10: %v", err)
	}
	if !strings.Contains(err.Error(), "noconflict") {
		t.Fatalf("error must name the table: %v", err)
	}
	if retryable(err) {
		t.Fatalf("42P10 must be classified permanent: %v", err)
	}
	if backoffs != 0 {
		t.Fatalf("permanent error was retried: %d backoff(s)", backoffs)
	}
	if rows := env.count(t, "noconflict"); rows != 0 {
		t.Fatalf("failed batch left %d row(s) behind", rows)
	}
}

func TestPostgresSinkRejectsTableDDL(t *testing.T) {
	env := newITEnv(t, nil)
	if err := env.sink.CreateTable(schema.Table{Database: env.schema, Table: "users"}); err == nil {
		t.Fatal("CreateTable must fail")
	}
	change := schema.Change{Database: env.schema, Table: "users", DDL: "ALTER TABLE users ADD COLUMN c int"}
	if err := env.sink.ApplySchema(change); err == nil {
		t.Fatal("ApplySchema must fail")
	}
}

func (e *itEnv) readUser(t *testing.T, id int64) (name, amount, createdAt, createdTZ string, raw []byte, qty int) {
	t.Helper()
	query := "SELECT coalesce(name, ''), coalesce(amount::text, ''), coalesce(created_at::text, ''), " +
		"coalesce((created_tz AT TIME ZONE 'UTC')::text, ''), raw, qty FROM " +
		qual(e.schema, "users") + " WHERE id = $1"
	if err := e.conn.QueryRow(e.ctx, query, id).Scan(&name, &amount, &createdAt, &createdTZ, &raw, &qty); err != nil {
		t.Fatalf("read user %d: %v", id, err)
	}
	return name, amount, createdAt, createdTZ, raw, qty
}

func (e *itEnv) exists(t *testing.T, table string, id int64) bool {
	t.Helper()
	var found bool
	query := "SELECT true FROM " + qual(e.schema, table) + " WHERE id = $1"
	err := e.conn.QueryRow(e.ctx, query, id).Scan(&found)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return false
		}
		t.Fatalf("exists: %v", err)
	}
	return found
}

func (e *itEnv) exec(t *testing.T, statement string) {
	t.Helper()
	if _, err := e.conn.Exec(e.ctx, statement); err != nil {
		t.Fatalf("exec %q: %v", statement, err)
	}
}

func (e *itEnv) count(t *testing.T, table string) int {
	t.Helper()
	return e.countIn(t, e.schema, table)
}

func (e *itEnv) countIn(t *testing.T, schemaName, table string) int {
	t.Helper()
	var rows int
	if err := e.conn.QueryRow(e.ctx, "SELECT count(*) FROM "+qual(schemaName, table)).Scan(&rows); err != nil {
		t.Fatalf("count %s.%s: %v", schemaName, table, err)
	}
	return rows
}
