package postgresql

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/PyRSA/go-cdc/common/event"
)

// maxCachedTemplates bounds the statement cache. A template depends on the
// target table, the written columns, and the conflict keys, so a pipeline with
// stable schemas stays at a handful of entries.
const maxCachedTemplates = 2048

// Target is the resolved PostgreSQL table of one event.
type Target struct {
	Schema string
	Table  string
}

// String returns schema.table for errors and logs.
func (t Target) String() string {
	return t.Schema + "." + t.Table
}

// quote renders the target as a quoted, escaped identifier. PostgreSQL
// identifiers are double quoted; the MySQL backtick helpers do not apply.
func (t Target) quote() string {
	return pgx.Identifier{t.Schema, t.Table}.Sanitize()
}

// ResolveTarget maps a routed event onto a PostgreSQL table.
//
// Routes rewrite the event database to the sink database, which is the target
// schema. When no route matched, the configured schema is used. The source
// database is never used as a schema: PostgreSQL always connects to one
// database and only differs by schema.
func ResolveTarget(defaultSchema string, item event.Event) (Target, error) {
	if item.Table == "" {
		return Target{}, fmt.Errorf("postgresql: %s: event has no table", item.SourceID())
	}
	if strings.Contains(item.Table, ".") {
		return Target{}, fmt.Errorf("postgresql: sink-table %s must be exactly schema.table", item.SinkID())
	}
	schema := defaultSchema
	if routed(item) {
		schema = item.Database
	}
	if schema == "" {
		return Target{}, fmt.Errorf("postgresql: %s: no schema configured", item.SinkID())
	}
	return Target{Schema: schema, Table: item.Table}, nil
}

// routed reports whether a route rule matched the event. The route layer records
// the hit on the event, so an identity route or a table-only rename still names
// the schema with the routed database segment. Only an event that no rule matched
// falls back to the configured schema, which keeps a MySQL source database name
// from becoming a PostgreSQL schema by accident.
func routed(item event.Event) bool {
	return item.Routed && item.Database != ""
}

// Statement is one parameterized DML statement. Op and Table are kept so a
// failed batch can name the operation and table that failed.
type Statement struct {
	SQL   string
	Args  []any
	Op    event.Op
	Table Target
}

// Builder renders events as PostgreSQL DML. It is safe for concurrent use.
type Builder struct {
	schema string

	mu    sync.Mutex
	cache map[string]string
}

// NewBuilder returns a builder that falls back to defaultSchema for events that
// no route rewrote.
func NewBuilder(defaultSchema string) *Builder {
	return &Builder{schema: defaultSchema, cache: make(map[string]string)}
}

// Statements renders the DML of one event. An update that changes the business
// key becomes DELETE (old key) plus UPSERT (new row), matching the MySQL sink.
func (b *Builder) Statements(item event.Event) ([]Statement, error) {
	switch item.Op {
	case event.OpCreate, event.OpRead:
		one, err := b.upsert(item, item.After)
		if err != nil {
			return nil, err
		}
		return []Statement{one}, nil
	case event.OpUpdate:
		if !keyChanged(item) {
			one, err := b.upsert(item, item.After)
			if err != nil {
				return nil, err
			}
			return []Statement{one}, nil
		}
		del, err := b.deleteByKey(item, item.Before)
		if err != nil {
			return nil, err
		}
		ins, err := b.upsert(item, item.After)
		if err != nil {
			return nil, err
		}
		return []Statement{del, ins}, nil
	case event.OpDelete:
		del, err := b.deleteByKey(item, item.Before)
		if err != nil {
			return nil, err
		}
		return []Statement{del}, nil
	default:
		return nil, fmt.Errorf("postgresql: %s: unsupported operation %q", item.SinkID(), item.Op)
	}
}

// upsert renders INSERT ... ON CONFLICT (key) DO UPDATE for the row. A table
// without a business key gets a plain INSERT, because PostgreSQL has no
// table-wide duplicate-key clause to fall back on. Every caller passes the
// after image as row.
func (b *Builder) upsert(item event.Event, row map[string]any) (Statement, error) {
	target, err := ResolveTarget(b.schema, item)
	if err != nil {
		return Statement{}, err
	}
	columns := columnsOf(item, row)
	if len(columns) == 0 {
		return Statement{}, fmt.Errorf("postgresql: %s %s: no columns to write", item.Op, target)
	}
	for _, key := range item.Keys {
		if !contains(columns, key) {
			return Statement{}, fmt.Errorf("postgresql: %s %s: conflict key %q is not in the written columns", item.Op, target, key)
		}
		// A missing or NULL conflict key never conflicts with an existing row.
		// Against a nullable UNIQUE constraint the insert would succeed and leave
		// a second, unreachable row, so the image must carry every key with a value.
		value, ok := row[key]
		if !ok {
			return Statement{}, fmt.Errorf("postgresql: %s %s: key %q is not in the after image", item.Op, target, key)
		}
		if value == nil {
			return Statement{}, fmt.Errorf("postgresql: %s %s: key %q is NULL in the after image", item.Op, target, key)
		}
	}
	args := make([]any, len(columns))
	for i, column := range columns {
		args[i] = ConvertValue(row[column])
	}
	return Statement{
		SQL:   b.insertSQL(target, columns, item.Keys),
		Args:  args,
		Op:    item.Op,
		Table: target,
	}, nil
}

func (b *Builder) deleteByKey(item event.Event, row map[string]any) (Statement, error) {
	target, err := ResolveTarget(b.schema, item)
	if err != nil {
		return Statement{}, err
	}
	if len(item.Keys) == 0 {
		return Statement{}, fmt.Errorf("postgresql: %s %s has no key for delete", item.Op, target)
	}
	args := make([]any, len(item.Keys))
	for i, key := range item.Keys {
		// A key missing from the before image would become "= NULL", which matches
		// nothing and would report a successful delete of zero rows. The event was
		// already acknowledged, so a silent miss can only be repaired by hand.
		value, ok := row[key]
		if !ok {
			return Statement{}, fmt.Errorf("postgresql: %s %s: key %q is not in the before image", item.Op, target, key)
		}
		if value == nil {
			return Statement{}, fmt.Errorf("postgresql: %s %s: key %q is NULL in the before image", item.Op, target, key)
		}
		args[i] = ConvertValue(value)
	}
	return Statement{
		SQL:   b.deleteSQL(target, item.Keys),
		Args:  args,
		Op:    item.Op,
		Table: target,
	}, nil
}

func (b *Builder) insertSQL(target Target, columns, keys []string) string {
	cacheKey := "i\x00" + target.String() + "\x00" + strings.Join(columns, "\x00") + "\x00" + strings.Join(keys, "\x00")
	if cached, ok := b.lookup(cacheKey); ok {
		return cached
	}
	quoted := make([]string, len(columns))
	holders := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = quoteIdentifier(column)
		holders[i] = "$" + strconv.Itoa(i+1)
	}
	var stmt strings.Builder
	stmt.WriteString("INSERT INTO " + target.quote() + " (" + strings.Join(quoted, ", ") +
		") VALUES (" + strings.Join(holders, ", ") + ")")
	if len(keys) > 0 {
		conflict := " ON CONFLICT (" + quoteIdentifiers(keys) + ") "
		updates := nonKeyColumns(columns, keys)
		if len(updates) == 0 {
			// Every written column is part of the key: the row either exists or
			// is inserted, so an empty SET list is not needed.
			stmt.WriteString(conflict + "DO NOTHING")
		} else {
			assignments := make([]string, len(updates))
			for i, column := range updates {
				name := quoteIdentifier(column)
				assignments[i] = name + " = EXCLUDED." + name
			}
			stmt.WriteString(conflict + "DO UPDATE SET " + strings.Join(assignments, ", "))
		}
	}
	sql := stmt.String()
	b.store(cacheKey, sql)
	return sql
}

func (b *Builder) deleteSQL(target Target, keys []string) string {
	cacheKey := "d\x00" + target.String() + "\x00" + strings.Join(keys, "\x00")
	if cached, ok := b.lookup(cacheKey); ok {
		return cached
	}
	where := make([]string, len(keys))
	for i, key := range keys {
		where[i] = quoteIdentifier(key) + " = $" + strconv.Itoa(i+1)
	}
	sql := "DELETE FROM " + target.quote() + " WHERE " + strings.Join(where, " AND ")
	b.store(cacheKey, sql)
	return sql
}

func (b *Builder) lookup(key string) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sql, ok := b.cache[key]
	return sql, ok
}

func (b *Builder) store(key, sql string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.cache) >= maxCachedTemplates {
		// Bound memory on pipelines with unbounded table or column churn; the
		// templates rebuild on the next event.
		b.cache = make(map[string]string)
	}
	b.cache[key] = sql
}

// Forget drops every template rendered for one target. A schema change alters
// the column list or the conflict keys the statement embeds, so a cached
// template for the table would write the previous shape into the new table.
func (b *Builder) Forget(target Target) {
	name := target.String()
	b.mu.Lock()
	defer b.mu.Unlock()
	for key := range b.cache {
		// Both templates start with the operation, then the target name.
		parts := strings.SplitN(key, "\x00", 3)
		if len(parts) == 3 && parts[1] == name {
			delete(b.cache, key)
		}
	}
}

func columnsOf(item event.Event, row map[string]any) []string {
	if len(item.Columns) > 0 {
		return item.Columns
	}
	columns := make([]string, 0, len(row))
	for column := range row {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}

func nonKeyColumns(columns, keys []string) []string {
	out := make([]string, 0, len(columns))
	for _, column := range columns {
		if !contains(keys, column) {
			out = append(out, column)
		}
	}
	return out
}

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

func quoteIdentifier(name string) string {
	return pgx.Identifier{name}.Sanitize()
}

func quoteIdentifiers(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = quoteIdentifier(name)
	}
	return strings.Join(quoted, ", ")
}

func keyChanged(item event.Event) bool {
	if len(item.Keys) == 0 {
		return false
	}
	for _, key := range item.Keys {
		if fmt.Sprint(item.Before[key]) != fmt.Sprint(item.After[key]) {
			return true
		}
	}
	return false
}
