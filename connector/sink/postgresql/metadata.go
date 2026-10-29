package postgresql

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/PyRSA/go-cdc/common/schema"
)

// TableDescriber is the read-only view of the MySQL source catalogue that the
// PostgreSQL sink needs in order to turn a schema change into PostgreSQL DDL.
// It is satisfied by the connector's own catalogue handle, so no source code
// changes to provide it, and the sink never owns its lifecycle: the
// composition root opens it, hands it to the capture, and closes it there.
//
// The phase of schema evolution that alters an existing column type is the only
// consumer, so the interface is declared here and wired when that phase lands.
type TableDescriber interface {
	DescribeTable(ctx context.Context, database, table string) (schema.Table, error)
}

// SetTableDescriber installs the source catalogue handle the sink borrows. The
// composition root calls it once the capture connection exists, which is after
// Open, so Open must never clear it and Close must never close it: the sink does
// not own the connection it reads the source definition from.
func (s *Sink) SetTableDescriber(describer TableDescriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.describer = describer
}

// DescribeTable reads one source table definition through the injected
// catalogue. A missing catalogue is an error, not an empty definition: without
// it a change that restates a column cannot be interpreted.
func (s *Sink) DescribeTable(ctx context.Context, database, table string) (schema.Table, error) {
	s.mu.Lock()
	describer := s.describer
	s.mu.Unlock()
	if describer == nil {
		return schema.Table{}, fmt.Errorf("sink.postgresql: describe %s.%s: no source catalogue was injected, so the column definition cannot be read", database, table)
	}
	described, err := describer.DescribeTable(ctx, database, table)
	if err != nil {
		return schema.Table{}, fmt.Errorf("sink.postgresql: describe %s.%s: %w", database, table, err)
	}
	return described, nil
}

// rowScanner is the part of pgx.Rows the metadata reader uses.
type rowScanner interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

// querier is the part of *pgxpool.Pool the metadata reader uses. Keeping it
// separate from pool leaves the read path testable without a transaction.
type querier interface {
	Query(ctx context.Context, statement string, args ...any) (rowScanner, error)
}

// poolQuerier adapts the connection pool to querier.
type poolQuerier struct {
	inner *pgxpool.Pool
}

func (q poolQuerier) Query(ctx context.Context, statement string, args ...any) (rowScanner, error) {
	rows, err := q.inner.Query(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// columnState is one live PostgreSQL column.
type columnState struct {
	Name       string
	Type       string // format_type output, for example "character varying(64)"
	NotNull    bool
	Default    string // empty when HasDefault is false
	HasDefault bool
}

// tableState is the live PostgreSQL structure of one table.
type tableState struct {
	Schema string
	Name   string
	// Columns is in attnum order, which is the creation order.
	Columns []columnState
	// PrimaryKeys is in constraint order. UniqueKeys holds the other unique
	// constraints, so a business key that is not the primary key still counts as
	// usable for ON CONFLICT.
	PrimaryKeys []string
	UniqueKeys  [][]string
}

// exists reports whether the table was found, since a missing table has an
// empty state that is otherwise indistinguishable from an empty one.
func (t tableState) exists() bool { return t.Name != "" }

// column returns the live column of a name.
func (t tableState) column(name string) (columnState, bool) {
	for _, column := range t.Columns {
		if column.Name == name {
			return column, true
		}
	}
	return columnState{}, false
}

// columnFold returns the live column of a name, falling back to a
// case-insensitive match. MySQL compares column names without regard to case,
// so a statement can name a column in a different case than the catalogue
// spelling the target was created with; matching exactly would read such a
// column as absent and let the change look already applied.
func (t tableState) columnFold(name string) (columnState, bool) {
	if column, ok := t.column(name); ok {
		return column, true
	}
	for _, column := range t.Columns {
		if strings.EqualFold(column.Name, name) {
			return column, true
		}
	}
	return columnState{}, false
}

// keys returns the primary key and every unique constraint as candidate
// conflict targets.
func (t tableState) keys() [][]string {
	out := [][]string{}
	if len(t.PrimaryKeys) > 0 {
		out = append(out, t.PrimaryKeys)
	}
	return append(out, t.UniqueKeys...)
}

// metadata reads the PostgreSQL catalogue and remembers what it proved.
//
// It is the only place that decides whether a target already matches the source
// definition, so the answer always comes from the live structure: a schema or
// table that merely exists is not yet a table that can be written to.
type metadata struct {
	q querier

	mu     sync.Mutex
	states map[Target]tableState
}

func newMetadata(q querier) *metadata {
	return &metadata{q: q, states: make(map[Target]tableState)}
}

// remember caches the structure that was just created or validated. Every DDL
// statement the sink runs drops the entry of its own target before anything is
// cached again (forget, or refresh which calls it), so a cached entry always
// describes the live table rather than the one it was before the change.
func (m *metadata) remember(state tableState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.states[Target{Schema: state.Schema, Table: state.Name}] = state
}

func (m *metadata) cached(target Target) (tableState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state, ok := m.states[target]
	return state, ok
}

// forget drops the cached structure of one target. Only that target is
// affected: a change to one schema never invalidates the structure of a table
// with the same name in another schema.
func (m *metadata) forget(target Target) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.states, target)
}

// refresh re-reads one target from the catalogue after a statement changed it.
// The cached entry goes first, so a failed read can never leave the sink
// validating against a structure that is no longer true.
func (m *metadata) refresh(ctx context.Context, target Target) (tableState, bool, error) {
	m.forget(target)
	state, ok, err := m.table(ctx, target)
	if err != nil {
		return tableState{}, false, err
	}
	if ok {
		m.remember(state)
	}
	return state, ok, nil
}

// dependent is one constraint or index that uses a column.
type dependent struct {
	Name string
	// Kind is a pg_constraint contype ('p', 'u', 'f', 'c', 'x') or "i"/"ui" for
	// an index that carries no constraint.
	Kind string
}

// lookup returns the live structure, using the cached copy of a table this sink
// already created or validated.
func (m *metadata) lookup(ctx context.Context, target Target) (tableState, bool, error) {
	if state, ok := m.cached(target); ok {
		return state, state.exists(), nil
	}
	state, ok, err := m.table(ctx, target)
	if err != nil {
		return tableState{}, false, err
	}
	if ok {
		m.remember(state)
	}
	return state, ok, nil
}

// schemaExists reports whether the target schema is present.
func (m *metadata) schemaExists(ctx context.Context, name string) (bool, error) {
	const statement = `SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1)`
	return m.boolQuery(ctx, statement, name)
}

// table reads the columns, primary key, and unique constraints of one table.
func (m *metadata) table(ctx context.Context, target Target) (tableState, bool, error) {
	const columnsSQL = `SELECT a.attname,
       format_type(a.atttypid, a.atttypmod),
       a.attnotnull,
       COALESCE(pg_get_expr(ad.adbin, ad.adrelid), ''),
       ad.adbin IS NOT NULL
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
  LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = c.oid AND ad.adnum = a.attnum
 WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p')
   AND a.attnum > 0 AND NOT a.attisdropped
 ORDER BY a.attnum`
	rows, err := m.q.Query(ctx, columnsSQL, target.Schema, target.Table)
	if err != nil {
		return tableState{}, false, fmt.Errorf("sink.postgresql: read columns of %s: %w", target, err)
	}
	defer rows.Close()
	state := tableState{Schema: target.Schema, Name: target.Table}
	for rows.Next() {
		var column columnState
		if err := rows.Scan(&column.Name, &column.Type, &column.NotNull, &column.Default, &column.HasDefault); err != nil {
			return tableState{}, false, fmt.Errorf("sink.postgresql: scan columns of %s: %w", target, err)
		}
		state.Columns = append(state.Columns, column)
	}
	if err := rows.Err(); err != nil {
		return tableState{}, false, fmt.Errorf("sink.postgresql: read columns of %s: %w", target, err)
	}
	if len(state.Columns) == 0 {
		// A relation always has at least one user column, so an empty answer
		// means the table is not there.
		return tableState{}, false, nil
	}
	state.PrimaryKeys, state.UniqueKeys, err = m.keys(ctx, target)
	if err != nil {
		return tableState{}, false, err
	}
	return state, true, nil
}

// keys reads the primary key columns and the other unique constraint columns.
// The statement orders the rows inside each constraint, so the reader only has
// to collect them in arrival order.
func (m *metadata) keys(ctx context.Context, target Target) (primary []string, unique [][]string, err error) {
	const keysSQL = `SELECT con.conname, con.contype, a.attname
  FROM pg_catalog.pg_constraint con
  JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(con.conkey::int[])
 WHERE n.nspname = $1 AND c.relname = $2 AND con.contype IN ('p', 'u')
   AND con.convalidated
 ORDER BY con.conname, array_position(con.conkey::int[], a.attnum)`
	rows, err := m.q.Query(ctx, keysSQL, target.Schema, target.Table)
	if err != nil {
		return nil, nil, fmt.Errorf("sink.postgresql: read keys of %s: %w", target, err)
	}
	defer rows.Close()
	type key struct {
		kind    string
		columns []string
	}
	var constraints []key
	index := map[string]int{}
	for rows.Next() {
		var name, kind, column string
		if err := rows.Scan(&name, &kind, &column); err != nil {
			return nil, nil, fmt.Errorf("sink.postgresql: scan keys of %s: %w", target, err)
		}
		at, ok := index[name]
		if !ok {
			index[name] = len(constraints)
			at = len(constraints)
			constraints = append(constraints, key{kind: kind})
		}
		constraints[at].columns = append(constraints[at].columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("sink.postgresql: read keys of %s: %w", target, err)
	}
	for _, entry := range constraints {
		if entry.kind == "p" {
			primary = entry.columns
			continue
		}
		unique = append(unique, entry.columns)
	}
	return primary, unique, nil
}

// dependents reads the constraints and indexes that use one column. The second
// half of the union skips indexes that back a constraint, because the constraint
// row already names them and reporting both would only duplicate the same
// dependency.
//
// contype is the internal one-byte "char" type while the index branch spells the
// kind as text, so the constraint branch casts it: PostgreSQL refuses a union
// whose branches cannot be matched instead of widening one side on its own, and
// reports 'UNION types "char" and text cannot be matched' (SQLSTATE 42804).
func (m *metadata) dependents(ctx context.Context, target Target, column string) ([]dependent, error) {
	const dependentsSQL = `SELECT con.conname, con.contype::text
  FROM pg_catalog.pg_constraint con
  JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(con.conkey::int[])
 WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3
UNION
SELECT ic.relname, CASE WHEN i.indisunique THEN 'ui' ELSE 'i' END
  FROM pg_catalog.pg_index i
  JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
  JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
 WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3
   AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint con WHERE con.conindid = i.indexrelid)
 ORDER BY 1`
	rows, err := m.q.Query(ctx, dependentsSQL, target.Schema, target.Table, column)
	if err != nil {
		return nil, fmt.Errorf("sink.postgresql: read dependencies of %s.%s: %w", target, column, err)
	}
	defer rows.Close()
	var out []dependent
	for rows.Next() {
		var entry dependent
		if err := rows.Scan(&entry.Name, &entry.Kind); err != nil {
			return nil, fmt.Errorf("sink.postgresql: scan dependencies of %s.%s: %w", target, column, err)
		}
		out = append(out, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sink.postgresql: read dependencies of %s.%s: %w", target, column, err)
	}
	return out, nil
}

// boolQuery runs one statement whose single result is a boolean.
func (m *metadata) boolQuery(ctx context.Context, statement string, arg string) (bool, error) {
	rows, err := m.q.Query(ctx, statement, arg)
	if err != nil {
		return false, fmt.Errorf("sink.postgresql: %w", err)
	}
	defer rows.Close()
	var value bool
	if rows.Next() {
		if err := rows.Scan(&value); err != nil {
			return false, fmt.Errorf("sink.postgresql: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("sink.postgresql: %w", err)
	}
	return value, nil
}

// validateTargetTable compares the source definition against the live
// PostgreSQL structure. Every check reads a real attribute, because a table
// that exists under the right name can still miss a column, hold it with
// another type, or refuse the NULLs the source sends.
func validateTargetTable(table schema.Table, state tableState) error {
	var problems []string
	for _, column := range table.Columns {
		pgType, err := mapColumnType(column.Type)
		if err != nil {
			return fmt.Errorf("sink.postgresql: table %s column %s: %w",
				schema.Qualified(table.Database, table.Table), column.Name, err)
		}
		live, ok := state.column(column.Name)
		if !ok {
			problems = append(problems, fmt.Sprintf("column %q is missing", column.Name))
			continue
		}
		if !samePGType(pgType, live.Type) {
			problems = append(problems, fmt.Sprintf("column %q is %s, the source %s maps to %s",
				column.Name, live.Type, column.Type, pgType))
		}
		if live.NotNull && column.Nullable {
			problems = append(problems, fmt.Sprintf("column %q is NOT NULL while the source column is nullable", column.Name))
		}
	}
	// A column the source definition does not project still has to accept a row,
	// because the sink names only the projected columns in its statements.
	for _, live := range state.Columns {
		if _, ok := sourceColumn(table.Columns, live.Name); ok {
			continue
		}
		if live.NotNull && !live.HasDefault {
			problems = append(problems, fmt.Sprintf("column %q is not in the source definition and is NOT NULL without a default", live.Name))
		}
	}
	if len(table.PrimaryKeys) > 0 {
		if !containsKey(state.keys(), table.PrimaryKeys) {
			problems = append(problems, fmt.Sprintf("no primary key or unique constraint covers the source key (%s)",
				strings.Join(table.PrimaryKeys, ", ")))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("sink.postgresql: table %s.%s is not compatible with the source definition: %s",
			state.Schema, state.Name, strings.Join(problems, "; "))
	}
	return nil
}

func sourceColumn(columns []schema.Column, name string) (schema.Column, bool) {
	for _, column := range columns {
		if column.Name == name {
			return column, true
		}
	}
	return schema.Column{}, false
}

// sourceColumnFold returns one source column, falling back to a
// case-insensitive match. MySQL treats column names without regard to case, so
// a statement may name a column with a different spelling than the catalogue.
func sourceColumnFold(columns []schema.Column, name string) (schema.Column, bool) {
	if column, ok := sourceColumn(columns, name); ok {
		return column, true
	}
	for _, column := range columns {
		if strings.EqualFold(column.Name, name) {
			return column, true
		}
	}
	return schema.Column{}, false
}

// containsKey reports whether one key of the target holds exactly these
// columns. The order inside a constraint does not change which rows it can
// match, so the comparison ignores it.
func containsKey(keys [][]string, columns []string) bool {
	want := make([]string, len(columns))
	copy(want, columns)
	sort.Strings(want)
	for _, candidate := range keys {
		got := make([]string, len(candidate))
		copy(got, candidate)
		sort.Strings(got)
		if strings.Join(want, "\x00") == strings.Join(got, "\x00") {
			return true
		}
	}
	return false
}

// pgTypeKeywords maps the spellings PostgreSQL's format_type can return, and the
// spellings this package generates, onto one comparable name.
var pgTypeKeywords = map[string]string{
	"varchar":                     "varchar",
	"character varying":           "varchar",
	"char":                        "char",
	"character":                   "char",
	"bpchar":                      "char",
	"int2":                        "smallint",
	"smallint":                    "smallint",
	"int4":                        "integer",
	"integer":                     "integer",
	"int8":                        "bigint",
	"bigint":                      "bigint",
	"numeric":                     "numeric",
	"decimal":                     "numeric",
	"float4":                      "real",
	"real":                        "real",
	"float8":                      "double precision",
	"double precision":            "double precision",
	"timestamp without time zone": "timestamp",
	"timestamp":                   "timestamp",
	"timestamptz":                 "timestamp with time zone",
	"text":                        "text",
	"bytea":                       "bytea",
	"date":                        "date",
	"time":                        "time",
	"time without time zone":      "time",
	"boolean":                     "boolean",
	"json":                        "json",
	"jsonb":                       "jsonb",
}

// samePGType compares two spellings of one PostgreSQL type. format_type answers
// "character varying(64)" where the created column was written varchar(64), and
// numeric(10,0) where it was written numeric(10), so both the keyword and the
// argument list are normalised before they are compared.
func samePGType(want, got string) bool {
	a, ok := normalizePGType(want)
	if !ok {
		return false
	}
	b, ok := normalizePGType(got)
	if !ok {
		return false
	}
	return a == b
}

func normalizePGType(text string) (string, bool) {
	trimmed := strings.ToLower(strings.TrimSpace(text))
	name, args := trimmed, ""
	if open := strings.IndexByte(trimmed, '('); open >= 0 {
		name = strings.TrimSpace(trimmed[:open])
		closing := strings.LastIndexByte(trimmed, ')')
		if closing > open {
			args = strings.TrimSpace(trimmed[open+1 : closing])
		}
	}
	keyword, ok := pgTypeKeywords[name]
	if !ok {
		// An unmapped name is compared as written: an unknown type can still be
		// spelled identically on both sides, which is enough to prove a match.
		keyword = name
	}
	if args == "" {
		return keyword, true
	}
	parts := strings.Split(args, ",")
	kept := make([]string, 0, len(parts))
	for _, part := range parts {
		kept = append(kept, strings.TrimSpace(part))
	}
	// A trailing zero scale is implied: numeric(10) and numeric(10,0) are one
	// type, and only the second spelling comes back from the catalogue.
	for len(kept) > 1 && kept[len(kept)-1] == "0" {
		kept = kept[:len(kept)-1]
	}
	return keyword + "(" + strings.Join(kept, ",") + ")", true
}
