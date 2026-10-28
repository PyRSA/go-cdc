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

// TestPostgresSinkTypeFidelity writes values in the shapes the source really
// produces — an unsigned BIGINT as uint64, DECIMAL and the temporal types as
// text, a blob as a byte slice, a JSON document as text, an absent column as
// nil — and reads the stored text back, so every assertion is against what
// PostgreSQL holds rather than against the sink's own opinion.
func TestPostgresSinkTypeFidelity(t *testing.T) {
	env := newITEnv(t, func(s string) []string {
		return []string{"CREATE TABLE " + qual(s, "metrics") +
			" (id bigint PRIMARY KEY, big numeric(20,0), as_bigint bigint, f8 double precision," +
			" d date, t timestamp, note text, raw bytea, doc json)"}
	})
	columns := []string{"id", "big", "as_bigint", "f8", "d", "t", "note", "raw", "doc"}
	keys := []string{"id"}
	doc := `{"a":1,"b":[1,2]}`
	row := map[string]any{
		"id":        int64(1),
		"big":       uint64(1) << 63,   // above MaxInt64: reaches the sink as exact decimal text
		"as_bigint": uint64(1)<<63 - 1, // the largest unsigned value a bigint can hold
		"f8":        "0.123456789012345",
		"d":         "2024-02-29",
		"t":         "2024-02-29 23:59:59.123456",
		"note":      nil,
		"raw":       []byte{0x00, 0x01, 0xff},
		"doc":       doc,
	}
	if err := env.sink.Write([]event.Event{env.event(event.OpCreate, "metrics", columns, keys, row)}); err != nil {
		t.Fatalf("write: %v", err)
	}
	var big, asBig, f8, d, ts, note, docText string
	var raw []byte
	err := env.conn.QueryRow(env.ctx,
		"SELECT big::text, as_bigint::text, f8::text, d::text, t::text, coalesce(note, '<null>'), raw, doc::text FROM "+
			qual(env.schema, "metrics")+" WHERE id = 1").
		Scan(&big, &asBig, &f8, &d, &ts, &note, &raw, &docText)
	if err != nil {
		t.Fatal(err)
	}
	if big != "9223372036854775808" {
		t.Fatalf("numeric lost precision: %q", big)
	}
	if asBig != "9223372036854775807" {
		t.Fatalf("bigint %q", asBig)
	}
	if f8 != "0.123456789012345" {
		t.Fatalf("float8 %q", f8)
	}
	if d != "2024-02-29" || ts != "2024-02-29 23:59:59.123456" {
		t.Fatalf("temporal %q %q", d, ts)
	}
	if note != "<null>" {
		t.Fatalf("nil did not become SQL NULL: %q", note)
	}
	if string(raw) != string([]byte{0x00, 0x01, 0xff}) {
		t.Fatalf("bytea %v", raw)
	}
	if docText != doc {
		t.Fatalf("json was re-rendered: %q", docText)
	}

	// A value the hand-made column cannot hold is refused by the server instead
	// of being rounded or truncated: NUMERIC keeps every digit, BIGINT does not.
	overflow := env.event(event.OpCreate, "metrics", []string{"id", "as_bigint"}, keys,
		map[string]any{"id": int64(2), "as_bigint": ^uint64(0)})
	err = env.sink.Write([]event.Event{overflow})
	if err == nil || !strings.Contains(err.Error(), "22003") {
		t.Fatalf("a bigint overflow must fail with 22003: %v", err)
	}
	if retryable(err) {
		t.Fatalf("a value out of range must be permanent: %v", err)
	}
	if env.exists(t, "metrics", 2) {
		t.Fatal("the rejected row was written anyway")
	}

	// The MySQL zero date has no PostgreSQL value. The sink passes the text
	// through unchanged, so the server rejects the row: nothing turns it into
	// NULL and nothing retypes the column.
	zero := env.event(event.OpCreate, "metrics", []string{"id", "d"}, keys,
		map[string]any{"id": int64(3), "d": "0000-00-00"})
	err = env.sink.Write([]event.Event{zero})
	if err == nil {
		t.Fatal("the zero date was accepted")
	}
	if retryable(err) {
		t.Fatalf("a value the column cannot hold must be permanent: %v", err)
	}
	if env.exists(t, "metrics", 3) {
		t.Fatal("the rejected zero date was written anyway")
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

// itUsersTable is the source definition the create tests hand to the sink.
func itUsersTable() schema.Table {
	return schema.Table{
		Table: "users",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "name", Type: "varchar(64)", Nullable: true},
			{Name: "amount", Type: "decimal(10,2)", Nullable: true},
			{Name: "created_at", Type: "datetime(3)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
}

// tableExists asks the server, not the sink, whether the relation is there.
func (e *itEnv) tableExists(t *testing.T, table string) bool {
	t.Helper()
	var found bool
	if err := e.conn.QueryRow(e.ctx, "SELECT to_regclass($1) IS NOT NULL", qual(e.schema, table)).Scan(&found); err != nil {
		t.Fatalf("regclass %s: %v", table, err)
	}
	return found
}

// columnType reads one column of the live table through the same catalogue view
// the sink reads, so an assertion describes what PostgreSQL holds.
func (e *itEnv) columnType(t *testing.T, table, column string) (string, bool) {
	t.Helper()
	var dataType string
	err := e.conn.QueryRow(e.ctx,
		"SELECT format_type(atttypid, atttypmod) FROM pg_catalog.pg_attribute"+
			" WHERE attrelid = $1::regclass AND attname = $2 AND attnum > 0 AND NOT attisdropped",
		qual(e.schema, table), column).Scan(&dataType)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false
	}
	if err != nil {
		t.Fatalf("read column %s of %s: %v", column, table, err)
	}
	return dataType, true
}

// TestPostgresSinkRefusesTableDDLByDefault is the PR2 default proven on a real
// server: the sink neither creates nor alters a table until told to, and the
// refusal leaves nothing behind.
func TestPostgresSinkRefusesTableDDLByDefault(t *testing.T) {
	env := newITEnv(t, nil)
	err := env.sink.CreateTable(itUsersTable())
	if err == nil || !strings.Contains(err.Error(), "auto-create-table is false") {
		t.Fatalf("create: %v", err)
	}
	if env.tableExists(t, "users") {
		t.Fatal("a refused create still made a table")
	}
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: env.schema, Table: "users",
		Column: "note", AddColumns: []string{"note"},
		ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
		DDL:       "ALTER TABLE users ADD COLUMN note varchar(32)",
	}
	err = env.sink.ApplySchema(change)
	if err == nil || !strings.Contains(err.Error(), "schema changes are disabled") {
		t.Fatalf("apply: %v", err)
	}
}

// TestPostgresSinkCreatesTableFromSourceDefinition proves the created table is
// the one the source described: the types, the key, and a real upsert are
// checked against the server rather than against the sink's own opinion.
func TestPostgresSinkCreatesTableFromSourceDefinition(t *testing.T) {
	env := newITEnv(t, nil)
	env.sink.cfg.AutoCreate = true
	if err := env.sink.CreateTable(itUsersTable()); err != nil {
		t.Fatalf("create: %v", err)
	}
	for column, want := range map[string]string{
		"id":         "bigint",
		"name":       "character varying(64)",
		"amount":     "numeric(10,2)",
		"created_at": "timestamp(3) without time zone",
	} {
		got, ok := env.columnType(t, "users", column)
		if !ok {
			t.Fatalf("column %s is missing after the create", column)
		}
		if got != want {
			t.Fatalf("column %s is %s, want %s", column, got, want)
		}
	}
	columns := []string{"id", "name", "amount", "created_at"}
	item := env.event(event.OpCreate, "users", columns, []string{"id"}, map[string]any{
		"id": int64(1), "name": "alice", "amount": "1.25", "created_at": "2024-01-02 03:04:05",
	})
	if err := env.sink.Write([]event.Event{item}); err != nil {
		t.Fatalf("write after create: %v", err)
	}
	item.After["name"] = "bob"
	if err := env.sink.Write([]event.Event{item}); err != nil {
		t.Fatalf("upsert after create: %v", err)
	}
	if rows := env.count(t, "users"); rows != 1 {
		t.Fatalf("the created key is not a conflict target: %d rows", rows)
	}
	// Creating over an existing compatible table validates it instead of failing.
	if err := env.sink.CreateTable(itUsersTable()); err != nil {
		t.Fatalf("create again: %v", err)
	}
	// The same definition with a column the target does not have is refused.
	wider := itUsersTable()
	wider.Columns = append(wider.Columns, schema.Column{Name: "gone", Type: "text", Nullable: true})
	if err := env.sink.CreateTable(wider); err == nil {
		t.Fatal("a target that cannot hold the rows was accepted")
	}
}

// TestPostgresSinkAppliesColumnChanges runs the column changes against a real
// server. The write after the rename is the point of the test: it can only
// succeed if the statement templates of the changed target were dropped.
func TestPostgresSinkAppliesColumnChanges(t *testing.T) {
	env := newITEnv(t, func(s string) []string {
		return []string{"CREATE TABLE " + qual(s, "users") +
			" (id bigint PRIMARY KEY, name varchar(32), note text)"}
	})
	env.sink.cfg.SchemaChange = true
	env.sink.SetTableDescriber(&fakeDescriber{table: schema.Table{
		Database: "app", Table: "users",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "label", Type: "varchar(64)", Nullable: true},
			{Name: "note", Type: "text", Nullable: true},
			{Name: "amount", Type: "decimal(12,2)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}})

	add := schema.Change{
		Action: schema.ActionAddColumn, Database: env.schema, Table: "users",
		Column: "amount", AddColumns: []string{"amount"},
		ColumnDef: &schema.Column{Name: "amount", Type: "decimal(10,2)", Nullable: true},
		DDL:       "ALTER TABLE users ADD COLUMN amount decimal(10,2)",
	}
	if err := env.sink.ApplySchema(add); err != nil {
		t.Fatalf("add column: %v", err)
	}
	if got, ok := env.columnType(t, "users", "amount"); !ok || got != "numeric(10,2)" {
		t.Fatalf("amount is %q present %v", got, ok)
	}
	refused := schema.Change{
		Action: schema.ActionAddColumn, Database: env.schema, Table: "users",
		Column: "code", AddColumns: []string{"code"},
		ColumnDef: &schema.Column{Name: "code", Type: "varchar(8)", Nullable: false},
		DDL:       "ALTER TABLE users ADD COLUMN code varchar(8) NOT NULL",
	}
	if err := env.sink.ApplySchema(refused); err == nil {
		t.Fatal("NOT NULL without a default was accepted")
	}
	if _, ok := env.columnType(t, "users", "code"); ok {
		t.Fatal("the refused column was created anyway")
	}

	// CHANGE COLUMN renames and widens in one transaction.
	change := schema.Change{
		Action: schema.ActionRenameColumn, Database: env.schema, Table: "users",
		Column: "name", NewName: "label", SourceDatabase: "app", SourceTable: "users",
		DDL: "ALTER TABLE users CHANGE name label varchar(64)",
	}
	if err := env.sink.ApplySchema(change); err != nil {
		t.Fatalf("change column: %v", err)
	}
	if _, ok := env.columnType(t, "users", "name"); ok {
		t.Fatal("the old name is still there")
	}
	if got, ok := env.columnType(t, "users", "label"); !ok || got != "character varying(64)" {
		t.Fatalf("label is %q present %v", got, ok)
	}

	// A MODIFY whose statement text is behind the source. The catalogue says the
	// column is decimal(12,2) now, so that is what the target becomes and the
	// (10,2) written in the statement is ignored: a replayed statement is
	// interpreted against the definition the source holds, never against its own
	// text, which carries no type information the sink could trust.
	restate := schema.Change{
		Action: schema.ActionModifyColumn, Database: env.schema, Table: "users",
		Column: "amount", SourceDatabase: "app", SourceTable: "users",
		DDL: "ALTER TABLE users MODIFY amount decimal(10,2)",
	}
	if err := env.sink.ApplySchema(restate); err != nil {
		t.Fatalf("modify amount: %v", err)
	}
	if got, _ := env.columnType(t, "users", "amount"); got != "numeric(12,2)" {
		t.Fatalf("amount after the source restatement is %q, want the catalogue's numeric(12,2)", got)
	}

	// The next write names the new columns; a stale template would fail with
	// SQLSTATE 42703 on the column the target no longer has.
	columns := []string{"id", "label", "note", "amount"}
	item := env.event(event.OpCreate, "users", columns, []string{"id"}, map[string]any{
		"id": int64(1), "label": "alice", "note": "x", "amount": "2.50",
	})
	if err := env.sink.Write([]event.Event{item}); err != nil {
		t.Fatalf("write after the change: %v", err)
	}

	drop := schema.Change{
		Action: schema.ActionDropColumn, Database: env.schema, Table: "users", Column: "note",
		DDL: "ALTER TABLE users DROP COLUMN note",
	}
	if err := env.sink.ApplySchema(drop); err != nil {
		t.Fatalf("drop column: %v", err)
	}
	if _, ok := env.columnType(t, "users", "note"); ok {
		t.Fatal("the dropped column is still there")
	}
	// A key column cannot be dropped without taking the conflict target with it.
	keyed := schema.Change{
		Action: schema.ActionDropColumn, Database: env.schema, Table: "users", Column: "id",
		DDL: "ALTER TABLE users DROP COLUMN id",
	}
	if err := env.sink.ApplySchema(keyed); err == nil {
		t.Fatal("dropping the primary key column was accepted")
	}
	if _, ok := env.columnType(t, "users", "id"); !ok {
		t.Fatal("the refused drop removed the key")
	}

	// A plain secondary index is skipped, so the target keeps its own plan.
	index := schema.Change{
		Action: schema.ActionIndex, Database: env.schema, Table: "users",
		DDL: "ALTER TABLE users ADD INDEX idx_label (label)",
	}
	if err := env.sink.ApplySchema(index); err != nil {
		t.Fatalf("index change: %v", err)
	}
	constraints := env.countWhere(t, "pg_catalog.pg_indexes", "schemaname = $1 AND indexname = 'idx_label'")
	if constraints != 0 {
		t.Fatalf("a skipped index was created: %d", constraints)
	}

	// Destructive changes wait for their own switch.
	truncate := schema.Change{Action: schema.ActionTruncate, Database: env.schema, Table: "users",
		DDL: "TRUNCATE TABLE users"}
	if err := env.sink.ApplySchema(truncate); err == nil {
		t.Fatal("truncate without the switch was accepted")
	}
	env.sink.cfg.DropTruncate = true
	if err := env.sink.ApplySchema(truncate); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if rows := env.count(t, "users"); rows != 0 {
		t.Fatalf("truncate left %d row(s)", rows)
	}
	dropped := schema.Change{Action: schema.ActionDropTable, Database: env.schema, Table: "users",
		DDL: "DROP TABLE users"}
	if err := env.sink.ApplySchema(dropped); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if env.tableExists(t, "users") {
		t.Fatal("the table is still there after DROP TABLE")
	}
}

// TestPostgresSinkDropColumnLeavesAViewToTheServer marks the boundary of the
// sink's own dependency read. That read inspects the constraints and the
// indexes the target table owns, so a view that reads the column is not in it,
// and the guard is PostgreSQL's: DROP COLUMN carries no CASCADE, the server
// refuses to strand the view, the one transaction rolls back, and the job stops
// with the server's error rather than dropping data the target still needs.
func TestPostgresSinkDropColumnLeavesAViewToTheServer(t *testing.T) {
	env := newITEnv(t, func(s string) []string {
		return []string{
			"CREATE TABLE " + qual(s, "users") + " (id bigint PRIMARY KEY, name text, note text)",
			"CREATE VIEW " + qual(s, "user_names") + " AS SELECT name FROM " + qual(s, "users"),
		}
	})
	env.sink.cfg.SchemaChange = true
	drop := schema.Change{
		Action: schema.ActionDropColumn, Database: env.schema, Table: "users", Column: "name",
		DDL: "ALTER TABLE users DROP COLUMN name",
	}
	err := env.sink.ApplySchema(drop)
	if err == nil {
		t.Fatal("dropping a column a view reads was accepted")
	}
	if retryable(err) {
		t.Fatalf("a dependency refusal must be permanent: %v", err)
	}
	if _, ok := env.columnType(t, "users", "name"); !ok {
		t.Fatal("the refused drop removed the column")
	}
	if !env.tableExists(t, "user_names") {
		t.Fatal("the refused drop removed the view")
	}
	// The untouched column proves the sink did not need the refused one to be a
	// key: the guard fires on the object that depends on the column, not on the
	// column's role in the conflict target.
	keep := schema.Change{
		Action: schema.ActionDropColumn, Database: env.schema, Table: "users", Column: "note",
		DDL: "ALTER TABLE users DROP COLUMN note",
	}
	if err := env.sink.ApplySchema(keep); err != nil {
		t.Fatalf("drop an unreferenced column: %v", err)
	}
	if _, ok := env.columnType(t, "users", "note"); ok {
		t.Fatal("the column is still there after a clean drop")
	}
}

// countWhere reads one count from the catalogue, so an assertion about a skipped
// index comes from the server.
func (e *itEnv) countWhere(t *testing.T, relation, predicate string) int {
	t.Helper()
	var rows int
	if err := e.conn.QueryRow(e.ctx, "SELECT count(*) FROM "+relation+" WHERE "+predicate).Scan(&rows); err != nil {
		t.Fatalf("count %s: %v", relation, err)
	}
	return rows
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
