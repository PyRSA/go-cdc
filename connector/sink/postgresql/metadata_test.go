package postgresql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/PyRSA/go-cdc/common/schema"
)

// fakeRows answers one catalogue query. Rows are the column values in the order
// the statement selects them.
type fakeRows struct {
	rows   [][]any
	at     int
	scan   error
	err    error
	closed bool
}

func (r *fakeRows) Next() bool {
	r.at++
	return r.at < len(r.rows)
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.scan != nil {
		return r.scan
	}
	if r.at >= len(r.rows) || len(dest) != len(r.rows[r.at]) {
		return fmt.Errorf("fakeRows: %d destinations for %d values", len(dest), len(r.rows[r.at]))
	}
	for i, target := range dest {
		switch pointer := target.(type) {
		case *string:
			*pointer, _ = r.rows[r.at][i].(string)
		case *bool:
			*pointer, _ = r.rows[r.at][i].(bool)
		default:
			return fmt.Errorf("fakeRows: unsupported scan target %T", target)
		}
	}
	return nil
}

func (r *fakeRows) Err() error { return r.err }

func (r *fakeRows) Close() { r.closed = true }

// fakeQuerier routes a catalogue statement to the rows a test prepared.
type fakeQuerier struct {
	schemaExists bool
	columns      [][]any
	keys         [][]any
	// dependents answers the constraint/index query that guards a DROP COLUMN.
	dependents [][]any
	// columnsRows and keysRows are used verbatim when set, so a test can hand back
	// an empty result that means "the table is not there".
	columnsRows [][]any
	keysRows    [][]any
	queryErr    error
	scanErr     error
	statements  []string
	// answered counts the rows handed out, so a test can prove the cache shorted
	// a repeat read.
	answered int
	// beforeReads holds the column query empty for that many reads, which is how a
	// test shows a table that only exists after the sink created it.
	beforeReads int
	// columnPhase and laterColumns work the other way round: the first
	// columnPhase reads answer with columns, and every read after that answers
	// with laterColumns. That is how a test shows the structure a statement just
	// changed, because the sink always re-reads the target after running DDL.
	columnPhase  int
	laterColumns [][]any
	reads        int
}

func (f *fakeQuerier) Query(_ context.Context, statement string, _ ...any) (rowScanner, error) {
	f.statements = append(f.statements, statement)
	f.answered++
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	switch {
	case strings.Contains(statement, "pg_index"):
		return &fakeRows{rows: f.dependents, at: -1, scan: f.scanErr}, nil
	case strings.Contains(statement, "pg_constraint"):
		rows := f.keysRows
		if rows == nil {
			rows = f.keys
		}
		return &fakeRows{rows: rows, at: -1, scan: f.scanErr}, nil
	case strings.Contains(statement, "format_type"):
		f.reads++
		if f.reads <= f.beforeReads {
			return &fakeRows{rows: [][]any{}, at: -1, scan: f.scanErr}, nil
		}
		rows := f.columnsRows
		if rows == nil {
			rows = f.columns
		}
		if f.laterColumns != nil && f.reads > f.columnPhase {
			rows = f.laterColumns
		}
		return &fakeRows{rows: rows, at: -1, scan: f.scanErr}, nil
	default:
		return &fakeRows{rows: [][]any{{f.schemaExists}}, at: -1, scan: f.scanErr}, nil
	}
}

// columnRow builds one row of the column query.
func columnRow(name, pgType string, notNull bool, def string, hasDefault bool) []any {
	return []any{name, pgType, notNull, def, hasDefault}
}

func newFakeMetadata(f *fakeQuerier) *metadata { return newMetadata(f) }

func TestMetadataTableReadsStructure(t *testing.T) {
	querier := &fakeQuerier{
		columns: [][]any{
			columnRow("id", "bigint", false, "", false),
			columnRow("name", "character varying(64)", true, "", false),
			columnRow("note", "text", false, "''", true),
		},
		keys: [][]any{
			{"uq_name", "u", "name"},
			{"users_pkey", "p", "id"},
		},
	}
	state, exists, err := newFakeMetadata(querier).table(context.Background(), Target{Schema: "ods", Table: "users"})
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("table reported missing")
	}
	if len(state.Columns) != 3 || state.Columns[1].Type != "character varying(64)" || !state.Columns[1].NotNull {
		t.Fatalf("columns %#v", state.Columns)
	}
	if !state.Columns[2].HasDefault || state.Columns[2].Default != "''" {
		t.Fatalf("default %#v", state.Columns[2])
	}
	if strings.Join(state.PrimaryKeys, ",") != "id" {
		t.Fatalf("primary keys %#v", state.PrimaryKeys)
	}
	if len(state.UniqueKeys) != 1 || state.UniqueKeys[0][0] != "name" {
		t.Fatalf("unique keys %#v", state.UniqueKeys)
	}
	if len(querier.statements) != 2 {
		t.Fatalf("statements %#v", querier.statements)
	}
}

func TestMetadataMissingTable(t *testing.T) {
	querier := &fakeQuerier{columnsRows: [][]any{}}
	state, exists, err := newFakeMetadata(querier).table(context.Background(), Target{Schema: "ods", Table: "users"})
	if err != nil {
		t.Fatal(err)
	}
	if exists || state.exists() {
		t.Fatalf("exists %v state %#v", exists, state)
	}
	if len(querier.statements) != 1 {
		t.Fatalf("a missing table must not be queried for keys: %#v", querier.statements)
	}
}

func TestMetadataSchemaExists(t *testing.T) {
	querier := &fakeQuerier{schemaExists: true}
	found, err := newFakeMetadata(querier).schemaExists(context.Background(), "ods")
	if err != nil || !found {
		t.Fatalf("found %v err %v", found, err)
	}
	if !strings.Contains(querier.statements[0], "pg_namespace") {
		t.Fatalf("statement %#v", querier.statements[0])
	}
}

func TestMetadataQueryErrorNamesTarget(t *testing.T) {
	querier := &fakeQuerier{queryErr: errors.New("connection reset")}
	_, _, err := newFakeMetadata(querier).table(context.Background(), Target{Schema: "ods", Table: "users"})
	if err == nil || !strings.Contains(err.Error(), "ods.users") {
		t.Fatalf("err %v", err)
	}
}

func TestMetadataCacheShortsRepeatRead(t *testing.T) {
	querier := &fakeQuerier{columns: [][]any{columnRow("id", "bigint", false, "", false)}}
	m := newFakeMetadata(querier)
	target := Target{Schema: "ods", Table: "users"}
	if _, exists, err := m.lookup(context.Background(), target); err != nil || !exists {
		t.Fatalf("exists %v err %v", exists, err)
	}
	before := querier.answered
	state, exists, err := m.lookup(context.Background(), target)
	if err != nil || !exists {
		t.Fatalf("exists %v err %v", exists, err)
	}
	if querier.answered != before {
		t.Fatalf("cached target re-read the catalogue %d times", querier.answered-before)
	}
	if state.Schema != "ods" || state.Name != "users" {
		t.Fatalf("state %#v", state)
	}
}

func TestValidateTargetTableAcceptsMatch(t *testing.T) {
	table := schema.Table{
		Database: "ods", Table: "users",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "name", Type: "varchar(64)", Nullable: true},
			{Name: "created", Type: "datetime(3)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
	state := tableState{
		Schema: "ods", Name: "users",
		Columns: []columnState{
			{Name: "id", Type: "bigint", NotNull: true},
			{Name: "name", Type: "character varying(64)"},
			{Name: "created", Type: "timestamp(3) without time zone"},
		},
		PrimaryKeys: []string{"id"},
	}
	if err := validateTargetTable(table, state); err != nil {
		t.Fatal(err)
	}
}

func TestValidateTargetTableReportsEveryProblem(t *testing.T) {
	table := schema.Table{
		Database: "ods", Table: "users",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint"},
			{Name: "name", Type: "varchar(64)", Nullable: true},
			{Name: "gone", Type: "text", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
	state := tableState{
		Schema: "ods", Name: "users",
		Columns: []columnState{
			// Same type, different spelling, so this one must not be a problem.
			{Name: "id", Type: "int8", NotNull: true},
			// NOT NULL in the target while the source sends NULL sometimes.
			{Name: "name", Type: "text", NotNull: true},
			// Not in the source definition and impossible to fill.
			{Name: "extra", Type: "integer", NotNull: true},
		},
		PrimaryKeys: []string{"id"},
	}
	err := validateTargetTable(table, state)
	if err == nil {
		t.Fatal("expected error")
	}
	message := err.Error()
	for _, want := range []string{
		`column "gone" is missing`,
		`column "name" is text, the source varchar(64) maps to VARCHAR(64)`,
		`column "name" is NOT NULL while the source column is nullable`,
		`column "extra" is not in the source definition`,
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("error %q missing %q", message, want)
		}
	}
	if strings.Contains(message, `"id"`) {
		t.Fatalf("int8 was reported as a mismatch: %q", message)
	}
}

func TestValidateTargetTableRejectsUnmappedType(t *testing.T) {
	table := schema.Table{
		Database: "ods", Table: "users",
		Columns: []schema.Column{{Name: "state", Type: "enum('a','b')", Nullable: true}},
	}
	state := tableState{Schema: "ods", Name: "users", Columns: []columnState{{Name: "state", Type: "text"}}}
	err := validateTargetTable(table, state)
	if err == nil || !strings.Contains(err.Error(), "enum") {
		t.Fatalf("err %v", err)
	}
}

func TestValidateTargetTableAcceptsUniqueKeyOverSourcePK(t *testing.T) {
	table := schema.Table{
		Database: "ods", Table: "users",
		Columns:     []schema.Column{{Name: "id", Type: "bigint"}, {Name: "email", Type: "varchar(255)"}, {Name: "a", Type: "int"}, {Name: "b", Type: "int"}},
		PrimaryKeys: []string{"email", "id"},
	}
	state := tableState{
		Schema: "ods", Name: "users",
		Columns: []columnState{
			{Name: "id", Type: "bigint", NotNull: true},
			{Name: "email", Type: "character varying(255)", NotNull: true},
			{Name: "a", Type: "integer"},
			{Name: "b", Type: "integer"},
		},
		// The constraint holds the same columns in a different order.
		UniqueKeys: [][]string{{"id", "email"}},
	}
	if err := validateTargetTable(table, state); err != nil {
		t.Fatal(err)
	}
	table.PrimaryKeys = []string{"id", "email", "a"}
	if err := validateTargetTable(table, state); err == nil {
		t.Fatal("a key the target does not have was accepted")
	}
}

func TestSamePGTypeNormalizesSpellings(t *testing.T) {
	cases := []struct {
		want, got string
		match     bool
	}{
		{"VARCHAR(64)", "character varying(64)", true},
		{"VARCHAR(64)", "character varying(65)", false},
		{"NUMERIC(10)", "numeric(10,0)", true},
		{"NUMERIC(10,2)", "numeric(10, 2)", true},
		{"TIMESTAMP(6)", "timestamp(6) without time zone", true},
		{"TIMESTAMP", "timestamp without time zone", true},
		{"TIMESTAMP", "timestamp with time zone", false},
		{"SMALLINT", "int2", true},
		{"BYTEA", "bytea", true},
		{"TEXT", "character varying(64)", false},
		{"", "text", false},
	}
	for _, tc := range cases {
		if got := samePGType(tc.want, tc.got); got != tc.match {
			t.Errorf("samePGType(%q, %q)=%v want %v", tc.want, tc.got, got, tc.match)
		}
	}
}
