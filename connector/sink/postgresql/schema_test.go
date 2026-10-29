package postgresql

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
)

// logCapture records what the sink logs, so a test can prove a skipped change is
// reported instead of silently dropped.
type logCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *logCapture) Handle(_ context.Context, record slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, record.Clone())
	return nil
}

func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }

func (c *logCapture) WithGroup(string) slog.Handler { return c }

func (c *logCapture) messages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.records))
	for i, record := range c.records {
		out[i] = record.Message
	}
	return out
}

func (c *logCapture) contains(want string) bool {
	for _, message := range c.messages() {
		if strings.Contains(message, want) {
			return true
		}
	}
	return false
}

// ddlSink builds a sink whose configuration authorises schema changes. A test
// that checks a refusal clears the field it cares about.
func ddlSink(p pool, q *fakeQuerier) *Sink {
	sink := newTestSinkWithCatalogue(p, 0, q)
	sink.cfg.AutoCreate = true
	sink.cfg.SchemaChange = true
	return sink
}

// usersState is the catalogue answer for a table that matches usersTable.
func usersState() [][]any {
	return [][]any{
		columnRow("id", "bigint", true, "", false),
		columnRow("name", "character varying(64)", false, "", false),
		columnRow("amount", "numeric(10,2)", false, "", false),
		columnRow("created", "timestamp(3) without time zone", false, "", false),
	}
}

func usersTable() schema.Table {
	return schema.Table{
		Database: "public", Table: "users",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "name", Type: "varchar(64)", Nullable: true},
			{Name: "amount", Type: "decimal(10,2)", Nullable: true},
			{Name: "created", Type: "datetime(3)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
}

func TestCreateTableRendersSourceDefinition(t *testing.T) {
	p := &fakePool{}
	querier := &fakeQuerier{
		schemaExists: true,
		beforeReads:  1,
		columns:      usersState(),
		keys:         [][]any{{"users_pkey", "p", "id"}},
	}
	sink := ddlSink(p, querier)
	table := usersTable()
	table.Columns[1].Default = "anon"
	table.Columns[1].HasDefault = true
	table.Columns[3].Default = "CURRENT_TIMESTAMP"
	table.Columns[3].HasDefault = true
	table.Columns[3].DefaultExpr = true
	if err := sink.CreateTable(table); err != nil {
		t.Fatal(err)
	}
	want := `CREATE TABLE "public"."users" ("id" BIGINT NOT NULL, "name" VARCHAR(64) DEFAULT 'anon', ` +
		`"amount" NUMERIC(10,2), "created" TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY ("id"))`
	got := p.txs[0].sqls[0]
	if got != want {
		t.Fatalf("sql:\n got %s\nwant %s", got, want)
	}
	if len(p.txs[0].sqls) != 1 {
		t.Fatalf("statements %#v", p.txs[0].sqls)
	}
}

// TestCreateTableKeepsUniqueConstraint covers why the constraint is reproduced:
// the conflict target of every upsert has to exist on the target.
func TestCreateTableKeepsUniqueConstraint(t *testing.T) {
	p := &fakePool{}
	columns := append(usersState(), columnRow("email", "character varying(255)", true, "", false))
	querier := &fakeQuerier{
		schemaExists: true, beforeReads: 1, columns: columns,
		keys: [][]any{{"users_pkey", "p", "id"}, {"users_email_key", "u", "email"}},
	}
	sink := ddlSink(p, querier)
	table := usersTable()
	table.Columns = append(table.Columns, schema.Column{Name: "email", Type: "varchar(255)", Nullable: false})
	table.Indexes = []schema.Index{
		{Name: "idx_name", Columns: []string{"name"}, Type: "BTREE"},
		{Name: "uq_email", Unique: true, Columns: []string{"email"}, Type: "BTREE"},
		{Name: "ft_note", Unique: true, Columns: []string{"name"}, Type: "FULLTEXT"},
		{Name: "pre_name", Unique: true, Columns: []string{"name"}, Type: "BTREE", Prefixes: []int{8}},
	}
	if err := sink.CreateTable(table); err != nil {
		t.Fatal(err)
	}
	sql := p.txs[0].sqls[0]
	if !strings.Contains(sql, `UNIQUE ("email")`) {
		t.Fatalf("unique constraint missing: %s", sql)
	}
	for _, absent := range []string{"idx_name", "ft_note", "pre_name"} {
		if strings.Contains(sql, absent) {
			t.Fatalf("%s must not become a constraint: %s", absent, sql)
		}
	}
	// A plain index is not created either: PostgreSQL plans its own.
	if strings.Contains(sql, "CREATE INDEX") {
		t.Fatalf("secondary index reproduced: %s", sql)
	}
}

// TestCreateTableLeavesAutoIncrementOut covers the documented limitation: the
// sink always writes the value the source sent, so a generator on the target
// could hand out a value the source never had.
func TestCreateTableLeavesAutoIncrementOut(t *testing.T) {
	p := &fakePool{}
	querier := &fakeQuerier{
		schemaExists: true, beforeReads: 1, columns: usersState(),
		keys: [][]any{{"users_pkey", "p", "id"}},
	}
	sink := ddlSink(p, querier)
	capture := &logCapture{}
	sink.log = slog.New(capture)
	table := usersTable()
	table.Columns[0].AutoIncrement = true
	if err := sink.CreateTable(table); err != nil {
		t.Fatal(err)
	}
	sql := p.txs[0].sqls[0]
	for _, absent := range []string{"GENERATED", "IDENTITY", "SERIAL"} {
		if strings.Contains(strings.ToUpper(sql), absent) {
			t.Fatalf("%s in %s", absent, sql)
		}
	}
	if !capture.contains("auto increment is not reproduced") {
		t.Fatalf("warnings %#v", capture.messages())
	}
}

// TestCreateTableDropsZeroDateDefault is part of the zero-date decision: the
// default cannot be copied, because PostgreSQL has no such value, while the
// value itself still has to fail every write.
func TestCreateTableDropsZeroDateDefault(t *testing.T) {
	table := usersTable()
	table.Columns[3].Type = "date"
	table.Columns[3].Default = "0000-00-00"
	table.Columns[3].HasDefault = true
	clause, err := columnClause(Target{Schema: "public", Table: "users"}, table.Columns[3])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(clause, "DEFAULT") {
		t.Fatalf("zero date default copied: %s", clause)
	}
	// The value path is untouched: nothing turns the zero date into NULL.
	if got := ConvertValue("0000-00-00"); got != "0000-00-00" {
		t.Fatalf("zero date value was rewritten to %#v", got)
	}
}

// TestColumnClauseSpellsDefaultsByTargetType pins how a default becomes SQL. The
// source default is text for every column type, so only the mapped PostgreSQL
// type says whether a value is a number or a string: `DEFAULT 0` on a char
// column is not a default PostgreSQL coerces, it is a CREATE that fails with
// 42804. Numeric columns keep their literal, everything else is quoted.
func TestColumnClauseSpellsDefaultsByTargetType(t *testing.T) {
	target := Target{Schema: "public", Table: "users"}
	cases := []struct {
		name   string
		column schema.Column
		want   string
	}{
		{"numeric shaped char",
			schema.Column{Name: "status", Type: "char(1)", Nullable: true, Default: "0", HasDefault: true},
			`"status" CHAR(1) DEFAULT '0'`},
		{"exponent varchar",
			schema.Column{Name: "code", Type: "varchar(20)", Nullable: true, Default: "1e5", HasDefault: true},
			`"code" VARCHAR(20) DEFAULT '1e5'`},
		{"negative varchar",
			schema.Column{Name: "code", Type: "varchar(20)", Nullable: true, Default: "-1", HasDefault: true},
			`"code" VARCHAR(20) DEFAULT '-1'`},
		{"numeric shaped text",
			schema.Column{Name: "note", Type: "text", Nullable: true, Default: "1e5", HasDefault: true},
			`"note" TEXT DEFAULT '1e5'`},
		{"decimal",
			schema.Column{Name: "amount", Type: "decimal(10,2)", Nullable: true, Default: "0.00", HasDefault: true},
			`"amount" NUMERIC(10,2) DEFAULT 0.00`},
		{"integer",
			schema.Column{Name: "total", Type: "int", Nullable: true, Default: "42", HasDefault: true},
			`"total" INTEGER DEFAULT 42`},
		{"tinyint",
			schema.Column{Name: "flag", Type: "tinyint(1)", Nullable: true, Default: "0", HasDefault: true},
			`"flag" SMALLINT DEFAULT 0`},
		{"unsigned bigint",
			schema.Column{Name: "big", Type: "bigint unsigned", Nullable: true, Default: "18446744073709551615", HasDefault: true},
			`"big" NUMERIC(20,0) DEFAULT 18446744073709551615`},
		{"quoted string",
			schema.Column{Name: "label", Type: "varchar(20)", Nullable: true, Default: "it's", HasDefault: true},
			`"label" VARCHAR(20) DEFAULT 'it''s'`},
		{"not a literal",
			schema.Column{Name: "ratio", Type: "double", Nullable: true, Default: "NaN", HasDefault: true},
			`"ratio" DOUBLE PRECISION DEFAULT 'NaN'`},
		{"empty string",
			schema.Column{Name: "tag", Type: "varchar(4)", Nullable: true, Default: "", HasDefault: true},
			`"tag" VARCHAR(4) DEFAULT ''`},
		{"null",
			schema.Column{Name: "tag", Type: "varchar(4)", Nullable: true, Default: "NULL", HasDefault: true},
			`"tag" VARCHAR(4) DEFAULT NULL`},
		{"time expression",
			schema.Column{Name: "created", Type: "datetime", Nullable: true, Default: "CURRENT_TIMESTAMP", HasDefault: true, DefaultExpr: true},
			`"created" TIMESTAMP DEFAULT CURRENT_TIMESTAMP`},
		{"zero date",
			schema.Column{Name: "created", Type: "date", Nullable: true, Default: "0000-00-00", HasDefault: true},
			`"created" DATE`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := columnClause(target, tc.column)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("clause\n got %s\nwant %s", got, tc.want)
			}
		})
	}
}

// TestAddColumnSpellsDefaultsByTargetType is the ADD COLUMN half of the same
// rule: the statement a live or a replayed change renders spells a default
// exactly as CREATE TABLE does, so a column added by a change is never given a
// default the CREATE path would have refused.
func TestAddColumnSpellsDefaultsByTargetType(t *testing.T) {
	cases := []struct {
		name     string
		column   schema.Column
		liveType string
		liveDef  string
		want     string
	}{
		{"numeric shaped char",
			schema.Column{Name: "status", Type: "char(1)", Nullable: true, Default: "0", HasDefault: true},
			"character(1)", "'0'::bpchar",
			`ADD COLUMN IF NOT EXISTS "status" CHAR(1) DEFAULT '0'`},
		{"exponent varchar",
			schema.Column{Name: "code", Type: "varchar(20)", Nullable: true, Default: "1e5", HasDefault: true},
			"character varying(20)", "",
			`ADD COLUMN IF NOT EXISTS "code" VARCHAR(20) DEFAULT '1e5'`},
		{"negative varchar",
			schema.Column{Name: "code", Type: "varchar(20)", Nullable: true, Default: "-1", HasDefault: true},
			"character varying(20)", "",
			`ADD COLUMN IF NOT EXISTS "code" VARCHAR(20) DEFAULT '-1'`},
		{"decimal",
			schema.Column{Name: "total", Type: "decimal(10,2)", Nullable: true, Default: "0.00", HasDefault: true},
			"numeric(10,2)", "0.00",
			`ADD COLUMN IF NOT EXISTS "total" NUMERIC(10,2) DEFAULT 0.00`},
		{"quoted string",
			schema.Column{Name: "label", Type: "varchar(20)", Nullable: true, Default: "it's", HasDefault: true},
			"character varying(20)", "",
			`ADD COLUMN IF NOT EXISTS "label" VARCHAR(20) DEFAULT 'it''s'`},
		{"time expression",
			schema.Column{Name: "created_at", Type: "datetime", Nullable: true, Default: "CURRENT_TIMESTAMP", HasDefault: true, DefaultExpr: true},
			"timestamp without time zone", "CURRENT_TIMESTAMP",
			`ADD COLUMN IF NOT EXISTS "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &fakePool{}
			columns := append(usersState(), columnRow(tc.column.Name, tc.liveType, false, tc.liveDef, tc.liveDef != ""))
			sink := ddlSink(p, &fakeQuerier{columns: columns, keys: [][]any{{"users_pkey", "p", "id"}}})
			definition := tc.column
			change := schema.Change{
				Action: schema.ActionAddColumn, Database: "public", Table: "users",
				Column: definition.Name, AddColumns: []string{definition.Name},
				ColumnDef: &definition,
				DDL:       "ALTER TABLE users ADD COLUMN " + definition.Name,
			}
			if err := sink.ApplySchema(change); err != nil {
				t.Fatal(err)
			}
			if got := p.txs[0].sqls[0]; !strings.Contains(got, tc.want) {
				t.Fatalf("sql\n got %s\nwant %s", got, tc.want)
			}
		})
	}
}

func TestCreateTableRejectsUnsupportedType(t *testing.T) {
	for _, mysqlType := range []string{"enum('a','b')", "set('a','b')", "bit(8)", "time", "geometry"} {
		t.Run(mysqlType, func(t *testing.T) {
			p := &fakePool{}
			sink := ddlSink(p, &fakeQuerier{schemaExists: true})
			table := usersTable()
			table.Columns = append(table.Columns, schema.Column{Name: "extra", Type: mysqlType, Nullable: true})
			err := sink.CreateTable(table)
			if err == nil || !strings.Contains(err.Error(), "not supported") {
				t.Fatalf("err %v", err)
			}
			if p.begins != 0 {
				t.Fatalf("a refused type still ran DDL: %d", p.begins)
			}
		})
	}
}

func TestCreateTableCreatesMissingSchema(t *testing.T) {
	p := &fakePool{}
	querier := &fakeQuerier{schemaExists: false, beforeReads: 1, columns: usersState(), keys: [][]any{{"users_pkey", "p", "id"}}}
	sink := ddlSink(p, querier)
	table := usersTable()
	table.Database = "ods"
	if err := sink.CreateTable(table); err != nil {
		t.Fatal(err)
	}
	sqls := p.txs[0].sqls
	if len(sqls) != 1 || sqls[0] != `CREATE SCHEMA IF NOT EXISTS "ods"` {
		t.Fatalf("sqls %#v", sqls)
	}
	if !strings.Contains(p.txs[1].sqls[0], `CREATE TABLE "ods"."users"`) {
		t.Fatalf("table sql %s", p.txs[1].sqls[0])
	}
}

// TestCreateTableSurvivesALostRace covers two pipelines that start at once: the
// loser of CREATE TABLE must still end with a validated target.
func TestCreateTableSurvivesALostRace(t *testing.T) {
	p := &fakePool{resultsFor: func(attempt int) *fakeResults {
		if attempt == 2 {
			return &fakeResults{execErrs: []error{pgError("42P07")}}
		}
		return &fakeResults{}
	}}
	querier := &fakeQuerier{schemaExists: true, beforeReads: 1, columns: usersState(), keys: [][]any{{"users_pkey", "p", "id"}}}
	sink := ddlSink(p, querier)
	if err := sink.CreateTable(usersTable()); err != nil {
		t.Fatal(err)
	}
}

func TestCreateTableReportsAnIncompatibleTarget(t *testing.T) {
	p := &fakePool{}
	querier := &fakeQuerier{
		schemaExists: true,
		columns:      [][]any{columnRow("id", "integer", true, "", false)},
		keys:         [][]any{{"users_pkey", "p", "id"}},
	}
	sink := ddlSink(p, querier)
	err := sink.CreateTable(usersTable())
	if err == nil {
		t.Fatal("an existing target that cannot hold the rows was accepted")
	}
	if p.begins != 0 {
		t.Fatalf("a validated table must not be altered: %d", p.begins)
	}
}

// TestCreateTableFailureRollsBackNamesStatement is the PG-114 guard for CREATE:
// the operator sees the statement and the server error, and the job stops.
func TestCreateTableFailureRollsBackNamesStatement(t *testing.T) {
	p := &fakePool{resultsFor: func(int) *fakeResults {
		return &fakeResults{execErrs: []error{pgError("42601")}}
	}}
	querier := &fakeQuerier{schemaExists: true, beforeReads: 1, columns: usersState()}
	sink := ddlSink(p, querier)
	err := sink.CreateTable(usersTable())
	if err == nil {
		t.Fatal("expected error")
	}
	message := err.Error()
	if !strings.Contains(message, "schema change failed") || !strings.Contains(message, "CREATE TABLE") {
		t.Fatalf("error %q", message)
	}
	last := p.txs[len(p.txs)-1]
	if !last.rolledBack || last.committed {
		t.Fatalf("rolledBack=%v committed=%v", last.rolledBack, last.committed)
	}
}

// TestSchemaChangeKindVocabularyKeepsParserAndSinkInStep is why the vocabulary
// lives in one place: the parser rejects an include entry the sink cannot apply,
// so the two lists may never drift apart.
func TestSchemaChangeKindVocabularyKeepsParserAndSinkInStep(t *testing.T) {
	want := definition.PostgresChangeKinds()
	got := SupportedChangeKinds()
	if len(want) != len(got) {
		t.Fatalf("parser %#v sink %#v", want, got)
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("entry %d: parser %q sink %q", i, want[i], got[i])
		}
	}
}

func TestColumnChangeRefusedWhenDisabled(t *testing.T) {
	for _, action := range []schema.Action{
		schema.ActionAddColumn, schema.ActionDropColumn, schema.ActionRenameColumn, schema.ActionModifyColumn,
	} {
		change := schema.Change{Action: action, Database: "public", Table: "users", Column: "name", DDL: "ALTER TABLE users MODIFY name text"}
		p := &fakePool{}
		sink := ddlSink(p, &fakeQuerier{})
		sink.cfg.SchemaChange = false
		err := sink.ApplySchema(change)
		if err == nil || !strings.Contains(err.Error(), "schema changes are disabled") {
			t.Fatalf("%s: err %v", action, err)
		}
		// The refusal happens before the target is even read.
		if p.begins != 0 {
			t.Fatalf("%s: runs %d", action, p.begins)
		}
	}
}

// TestDisabledSchemaChangeLeavesCreateAlone pins the Q3 requirement: the switch
// that refuses column changes must not block the initial table creation, or a
// pipeline with schema.change.enabled: false could never start at all.
func TestDisabledSchemaChangeLeavesCreateAlone(t *testing.T) {
	p := &fakePool{}
	querier := &fakeQuerier{schemaExists: true, beforeReads: 1, columns: usersState(), keys: [][]any{{"users_pkey", "p", "id"}}}
	sink := ddlSink(p, querier)
	sink.cfg.SchemaChange = false
	if err := sink.CreateTable(usersTable()); err != nil {
		t.Fatal(err)
	}
	if len(p.txs) != 1 {
		t.Fatalf("transactions %d", len(p.txs))
	}
}

func TestIncludeListRefusesUnlistedChange(t *testing.T) {
	p := &fakePool{}
	sink := ddlSink(p, &fakeQuerier{})
	sink.cfg.SchemaChangeInclude = []string{"add.column"}
	change := schema.Change{Action: schema.ActionDropColumn, Database: "public", Table: "users", Column: "name"}
	err := sink.ApplySchema(change)
	if err == nil || !strings.Contains(err.Error(), `does not list "drop.column"`) {
		t.Fatalf("err %v", err)
	}
	if p.begins != 0 {
		t.Fatalf("runs %d", p.begins)
	}
}

// TestDestructiveChangeNeedsItsOwnSwitch is the Q3 fail-closed rule: listing the
// kind is not enough, the drop-truncate switch has to be on as well.
func TestDestructiveChangeNeedsItsOwnSwitch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		action schema.Action
		ddl    string
		sql    string
	}{
		{name: "drop table", action: schema.ActionDropTable, ddl: "DROP TABLE users", sql: `DROP TABLE IF EXISTS "public"."users"`},
		{name: "truncate", action: schema.ActionTruncate, ddl: "TRUNCATE TABLE users", sql: `TRUNCATE TABLE "public"."users"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			change := schema.Change{Action: tc.action, Database: "public", Table: "users", DDL: tc.ddl}
			kind, ok := changeKind(tc.action)
			if !ok {
				t.Fatalf("%s has no configuration name", tc.name)
			}
			p := &fakePool{}
			sink := ddlSink(p, &fakeQuerier{columns: usersState(), keys: [][]any{{"users_pkey", "p", "id"}}})
			sink.cfg.SchemaChangeInclude = []string{kind}
			err := sink.ApplySchema(change)
			if err == nil || !strings.Contains(err.Error(), "dropping and truncating are disabled") {
				t.Fatalf("err %v", err)
			}
			if p.begins != 0 {
				t.Fatalf("a disabled change ran: %d", p.begins)
			}
			sink.cfg.DropTruncate = true
			if err := sink.ApplySchema(change); err != nil {
				t.Fatal(err)
			}
			if got := p.txs[0].sqls[0]; got != tc.sql {
				t.Fatalf("sql %s", got)
			}
			if !p.txs[0].committed {
				t.Fatal("change not committed")
			}
		})
	}
}

// TestTruncateRefusesMissingTable covers the difference from DROP TABLE: TRUNCATE
// carries no IF EXISTS, so an absent target is a pipeline that is out of sync.
func TestTruncateRefusesMissingTable(t *testing.T) {
	sink := ddlSink(&fakePool{}, &fakeQuerier{})
	sink.cfg.SchemaChange = true
	sink.cfg.DropTruncate = true
	err := sink.ApplySchema(schema.Change{Action: schema.ActionTruncate, Database: "public", Table: "users", DDL: "TRUNCATE TABLE users"})
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("err %v", err)
	}
}

func TestSecondaryIndexChangeIsSkippedWithWarning(t *testing.T) {
	for _, ddl := range []string{
		"ALTER TABLE users ADD INDEX idx_name (name)",
		"ALTER TABLE users ADD KEY idx_name (name)",
		"ALTER TABLE users ADD FULLTEXT INDEX ft_body (body)",
		"ALTER TABLE users ADD SPATIAL INDEX sp_pt (pt)",
		"CREATE INDEX idx_name ON users (name)",
		"DROP INDEX idx_name ON users",
		"ALTER TABLE users DROP KEY idx_name",
		// Storage options do not change what the statement is: a non-unique index
		// on the target only affects how PostgreSQL plans a query.
		"ALTER TABLE users ADD INDEX idx_name (name) WITH (parallel_workers = 4) CHECKSUM",
	} {
		p := &fakePool{}
		capture := &logCapture{}
		sink := ddlSink(p, &fakeQuerier{})
		sink.log = slog.New(capture)
		change := schema.Change{Action: schema.ActionIndex, Database: "public", Table: "users", DDL: ddl}
		if err := sink.ApplySchema(change); err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		if p.begins != 0 {
			t.Fatalf("%s: runs %d", ddl, p.begins)
		}
		if !capture.contains("secondary index change skipped") {
			t.Fatalf("%s: warnings %#v", ddl, capture.messages())
		}
	}
}

// TestKeyCarryingIndexChangeFailsClosed is the other half of the Q4 split: a
// statement that touches the conflict target cannot be skipped quietly.
func TestKeyCarryingIndexChangeFailsClosed(t *testing.T) {
	for _, ddl := range []string{
		"ALTER TABLE users ADD UNIQUE INDEX uq_email (email)",
		"ALTER TABLE users ADD CONSTRAINT uq_email UNIQUE (email)",
		"ALTER TABLE users ADD PRIMARY KEY (id)",
		"ALTER TABLE users DROP PRIMARY KEY",
		"ALTER TABLE users ADD FOREIGN KEY (tenant) REFERENCES tenants (id)",
		"ALTER TABLE users ADD CHECK (amount > 0)",
		"ALTER TABLE users ADD EXCLUDE USING gist (range WITH =)",
		// A statement the check cannot name at all is not a plain index either.
		"",
		"ALTER TABLE users ADD something_exotic (name)",
	} {
		p := &fakePool{}
		sink := ddlSink(p, &fakeQuerier{})
		err := sink.ApplySchema(schema.Change{Action: schema.ActionIndex, Database: "public", Table: "users", DDL: ddl})
		if err == nil {
			t.Fatalf("%s was skipped", ddl)
		}
		if !strings.Contains(err.Error(), "apply it by hand") {
			t.Fatalf("%s: err %v", ddl, err)
		}
		if p.begins != 0 {
			t.Fatalf("%s: runs %d", ddl, p.begins)
		}
	}
}

// TestUnreclassifiableChangeFailsClosed covers RENAME TABLE and anything the
// classifier could not name: the target is part of the checkpoint identity, so
// moving or reshaping it without a verified rewrite diverges silently.
func TestUnreclassifiableChangeFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		action schema.Action
	}{
		{name: "rename table", action: schema.ActionRenameTable},
		{name: "other", action: schema.ActionOther},
		{name: "create", action: schema.ActionCreate},
	} {
		p := &fakePool{}
		sink := ddlSink(p, &fakeQuerier{})
		change := schema.Change{Action: tc.action, Database: "public", Table: "users", DDL: "ALTER TABLE users SOMETHING"}
		err := sink.ApplySchema(change)
		if err == nil {
			t.Fatalf("%s was accepted", tc.name)
		}
		if !strings.Contains(err.Error(), "not supported by this sink") || !strings.Contains(err.Error(), "ALTER TABLE users SOMETHING") {
			t.Fatalf("%s: err %v", tc.name, err)
		}
		if p.begins != 0 {
			t.Fatalf("%s: runs %d", tc.name, p.begins)
		}
	}
}

// TestChangeWithoutTableIsRefused keeps a malformed change from reaching the
// catalogue read with an empty name.
func TestChangeWithoutTableIsRefused(t *testing.T) {
	sink := ddlSink(&fakePool{}, &fakeQuerier{})
	err := sink.ApplySchema(schema.Change{Action: schema.ActionAddColumn, Database: "public"})
	if err == nil || !strings.Contains(err.Error(), "without a table name") {
		t.Fatalf("err %v", err)
	}
	sink.cfg.Schema = ""
	_, err = sink.changeTarget(schema.Change{Action: schema.ActionAddColumn, Table: "users"})
	if err == nil || !strings.Contains(err.Error(), "no schema configured") {
		t.Fatalf("err %v", err)
	}
}

// TestRoutedChangeTargetsItsOwnSchema is PG-112 at sink level: the route names
// the schema, and two schemas holding the same table name stay separate.
func TestRoutedChangeTargetsItsOwnSchema(t *testing.T) {
	p := &fakePool{}
	sink := ddlSink(p, &fakeQuerier{})
	change := schema.Change{Action: schema.ActionDropTable, Database: "ods", Table: "users", DDL: "DROP TABLE users"}
	sink.cfg.DropTruncate = true
	if err := sink.ApplySchema(change); err != nil {
		t.Fatal(err)
	}
	if got := p.txs[0].sqls[0]; got != `DROP TABLE IF EXISTS "ods"."users"` {
		t.Fatalf("sql %s", got)
	}
	other := Target{Schema: "staging", Table: "users"}
	sink.meta.remember(tableState{Schema: other.Schema, Name: other.Table})
	sink.meta.forget(Target{Schema: "ods", Table: "users"})
	if _, ok := sink.meta.cached(other); !ok {
		t.Fatal("a change to one schema invalidated another")
	}
}

// withNote is the catalogue answer after ADD COLUMN note.
func withNote(rows ...[]any) [][]any {
	return append(append([][]any{}, rows...), columnRow("note", "character varying(32)", false, "", false))
}

func TestAddColumnAppliesStructuredDefinition(t *testing.T) {
	p := &fakePool{}
	querier := &fakeQuerier{columns: withNote(usersState()...), keys: [][]any{{"users_pkey", "p", "id"}}}
	sink := ddlSink(p, querier)
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "public", Table: "users",
		Column: "note", AddColumns: []string{"note"},
		ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
		DDL:       "ALTER TABLE users ADD COLUMN note varchar(32)",
	}
	if err := sink.ApplySchema(change); err != nil {
		t.Fatal(err)
	}
	want := `ALTER TABLE "public"."users" ADD COLUMN IF NOT EXISTS "note" VARCHAR(32)`
	if got := p.txs[0].sqls[0]; got != want {
		t.Fatalf("sql %s", got)
	}
	if !p.txs[0].committed {
		t.Fatal("not committed")
	}
}

func TestAddColumnRefusesNotNullWithoutDefault(t *testing.T) {
	p := &fakePool{}
	sink := ddlSink(p, &fakeQuerier{columns: usersState()})
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "public", Table: "users",
		Column: "code", AddColumns: []string{"code"},
		ColumnDef: &schema.Column{Name: "code", Type: "varchar(32)", Nullable: false},
	}
	err := sink.ApplySchema(change)
	if err == nil || !strings.Contains(err.Error(), "NOT NULL without a default") {
		t.Fatalf("err %v", err)
	}
	if p.begins != 0 {
		t.Fatalf("the statement ran anyway: %d", p.begins)
	}
}

// TestAddColumnAcceptsNotNullWithDefault covers the half of the rule that must
// stay open: a default gives PostgreSQL the value the existing rows get.
func TestAddColumnAcceptsNotNullWithDefault(t *testing.T) {
	p := &fakePool{}
	columns := append(usersState(), columnRow("code", "character varying(32)", true, "'x'::character varying", true))
	sink := ddlSink(p, &fakeQuerier{columns: columns, keys: [][]any{{"users_pkey", "p", "id"}}})
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "public", Table: "users",
		Column: "code", AddColumns: []string{"code"},
		ColumnDef: &schema.Column{Name: "code", Type: "varchar(32)", Nullable: false, Default: "x", HasDefault: true},
	}
	if err := sink.ApplySchema(change); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(p.txs[0].sqls[0], `"code" VARCHAR(32) NOT NULL DEFAULT 'x'`) {
		t.Fatalf("sql %s", p.txs[0].sqls[0])
	}
}

func TestAddColumnRefusesUnsupportedType(t *testing.T) {
	p := &fakePool{}
	sink := ddlSink(p, &fakeQuerier{columns: usersState()})
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "public", Table: "users",
		Column: "state", AddColumns: []string{"state"},
		ColumnDef: &schema.Column{Name: "state", Type: "enum('a','b')", Nullable: true},
	}
	err := sink.ApplySchema(change)
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("err %v", err)
	}
	if p.begins != 0 {
		t.Fatalf("runs %d", p.begins)
	}
}

func TestAddColumnWithoutAnyNameIsRefused(t *testing.T) {
	sink := ddlSink(&fakePool{}, &fakeQuerier{columns: usersState()})
	err := sink.ApplySchema(schema.Change{Action: schema.ActionAddColumn, Database: "public", Table: "users"})
	if err == nil || !strings.Contains(err.Error(), "without a column name") {
		t.Fatalf("err %v", err)
	}
}

// TestAddMultipleColumnsReadsSourceCatalogue covers the shape MySQL sends for one
// ADD COLUMN a, b, b: the statement names the columns but carries no definition,
// so every type is read from the source catalogue rather than parsed from text.
func TestAddMultipleColumnsReadsSourceCatalogue(t *testing.T) {
	p := &fakePool{}
	columns := append(usersState(),
		columnRow("a", "integer", false, "", false),
		columnRow("b", "smallint", false, "", false))
	sink := ddlSink(p, &fakeQuerier{columns: columns, keys: [][]any{{"users_pkey", "p", "id"}}})
	describer := &fakeDescriber{table: schema.Table{
		Database: "app", Table: "users",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "a", Type: "int", Nullable: true},
			{Name: "b", Type: "tinyint(1)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}}
	sink.SetTableDescriber(describer)
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "public", Table: "users",
		AddColumns:     []string{"a", "b"},
		SourceDatabase: "app", SourceTable: "users",
		DDL: "ALTER TABLE users ADD COLUMN a int, ADD COLUMN b tinyint(1)",
	}
	if err := sink.ApplySchema(change); err != nil {
		t.Fatal(err)
	}
	if calls := describer.called(); len(calls) != 1 || calls[0] != "app.users" {
		t.Fatalf("calls %#v", calls)
	}
	handle := p.txs[0]
	if handle.queued != 2 {
		t.Fatalf("a multi-column change must be one transaction: queued %d", handle.queued)
	}
	if !strings.Contains(handle.sqls[0], `ADD COLUMN IF NOT EXISTS "a" INTEGER`) ||
		!strings.Contains(handle.sqls[1], `ADD COLUMN IF NOT EXISTS "b" SMALLINT`) {
		t.Fatalf("sqls %#v", handle.sqls)
	}
}

func TestAddedColumnMissingFromSourceDefinitionIsRefused(t *testing.T) {
	p := &fakePool{}
	sink := ddlSink(p, &fakeQuerier{columns: usersState()})
	sink.SetTableDescriber(&fakeDescriber{table: usersTable()})
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "public", Table: "users",
		AddColumns: []string{"gone"}, SourceDatabase: "app", SourceTable: "users",
	}
	err := sink.ApplySchema(change)
	if err == nil || !strings.Contains(err.Error(), "not in the current definition") {
		t.Fatalf("err %v", err)
	}
	if p.begins != 0 {
		t.Fatalf("runs %d", p.begins)
	}
}

func TestColumnChangeWithoutSourceTableIsRefused(t *testing.T) {
	sink := ddlSink(&fakePool{}, &fakeQuerier{columns: usersState()})
	sink.SetTableDescriber(&fakeDescriber{table: usersTable()})
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "public", Table: "users",
		AddColumns: []string{"a"},
	}
	err := sink.ApplySchema(change)
	if err == nil || !strings.Contains(err.Error(), "without the source table") {
		t.Fatalf("err %v", err)
	}
}

// TestAddColumnThatWasAlreadyThereWithANarrowerType proves the read-back is what
// decides: IF NOT EXISTS makes the statement a no-op, so only the catalogue can
// say whether the target now holds a definition that stores every source value.
// A target column narrower than the source mapping cannot, and the change stops.
func TestAddColumnThatWasAlreadyThereWithANarrowerType(t *testing.T) {
	p := &fakePool{}
	columns := append(usersState(), columnRow("note", "character varying(8)", false, "", false))
	sink := ddlSink(p, &fakeQuerier{columns: columns, keys: [][]any{{"users_pkey", "p", "id"}}})
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "public", Table: "users",
		Column: "note", AddColumns: []string{"note"},
		ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
	}
	err := sink.ApplySchema(change)
	if err == nil || !strings.Contains(err.Error(), "never narrower") {
		t.Fatalf("err %v", err)
	}
}

// TestAddColumnReplayAcceptsAWiderTarget covers the shape a restart produces:
// the ADD reached the target before the checkpoint did, a later change widened
// the column, and the replay can only see the widened result. The target still
// stores every value the source sends, so the replay converges instead of
// stopping a job whose target is ahead of the statement.
func TestAddColumnReplayAcceptsAWiderTarget(t *testing.T) {
	p := &fakePool{}
	columns := append(usersState(), columnRow("note", "text", false, "", false))
	sink := ddlSink(p, &fakeQuerier{columns: columns, keys: [][]any{{"users_pkey", "p", "id"}}})
	capture := &logCapture{}
	sink.log = slog.New(capture)
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "public", Table: "users",
		Column: "note", AddColumns: []string{"note"},
		ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
	}
	if err := sink.ApplySchema(change); err != nil {
		t.Fatal(err)
	}
	want := `ALTER TABLE "public"."users" ADD COLUMN IF NOT EXISTS "note" VARCHAR(32)`
	if got := p.txs[0].sqls[0]; got != want {
		t.Fatalf("sql %s", got)
	}
	if !capture.contains("wider type") {
		t.Fatalf("the widening was not reported: %#v", capture.messages())
	}
}

// TestAddColumnReplayAfterAWideningModify is the whole restart sequence:
// ADD COLUMN note varchar(32), MODIFY note varchar(64), crash before the
// checkpoint, replay from the ADD. The target holds varchar(64) when the ADD is
// replayed, and the replay has to converge on it.
func TestAddColumnReplayAfterAWideningModify(t *testing.T) {
	p := &fakePool{}
	querier := &fakeQuerier{
		columns:     withNote(usersState()...),
		columnPhase: 2,
		laterColumns: append(usersState(),
			columnRow("note", "character varying(64)", false, "", false)),
		keys: [][]any{{"users_pkey", "p", "id"}},
	}
	sink := ddlSink(p, querier)
	sink.SetTableDescriber(&fakeDescriber{table: schema.Table{
		Database: "app", Table: "users",
		Columns:     []schema.Column{{Name: "note", Type: "varchar(64)", Nullable: true}},
		PrimaryKeys: []string{"id"},
	}})
	add := schema.Change{
		Action: schema.ActionAddColumn, Database: "public", Table: "users",
		Column: "note", AddColumns: []string{"note"},
		ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
		DDL:       "ALTER TABLE users ADD COLUMN note varchar(32)",
	}
	if err := sink.ApplySchema(add); err != nil {
		t.Fatalf("first ADD COLUMN: %v", err)
	}
	modify := schema.Change{
		Action: schema.ActionModifyColumn, Database: "public", Table: "users",
		Column: "note", SourceDatabase: "app", SourceTable: "users",
		DDL: "ALTER TABLE users MODIFY note varchar(64)",
	}
	if err := sink.ApplySchema(modify); err != nil {
		t.Fatalf("MODIFY: %v", err)
	}
	widened := `ALTER TABLE "public"."users" ALTER COLUMN "note" TYPE VARCHAR(64)`
	if got := p.txs[1].sqls[0]; got != widened {
		t.Fatalf("the modify did not widen the column: %s", got)
	}
	// The checkpoint never covered the MODIFY, so the restart replays the ADD.
	if err := sink.ApplySchema(add); err != nil {
		t.Fatalf("replayed ADD COLUMN: %v", err)
	}
}

// TestAddColumnRefusesATargetThatIsStricterAboutNull keeps the direction of the
// nullability rule: a target may be more permissive than the source, never
// stricter, because a stricter column refuses a NULL the source sends.
func TestAddColumnRefusesATargetThatIsStricterAboutNull(t *testing.T) {
	p := &fakePool{}
	columns := append(usersState(), columnRow("note", "character varying(32)", true, "", false))
	sink := ddlSink(p, &fakeQuerier{columns: columns, keys: [][]any{{"users_pkey", "p", "id"}}})
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "public", Table: "users",
		Column: "note", AddColumns: []string{"note"},
		ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
	}
	err := sink.ApplySchema(change)
	if err == nil || !strings.Contains(err.Error(), "NOT NULL after the schema change") {
		t.Fatalf("err %v", err)
	}
}

// TestAddColumnReplayAcceptsARelaxedNull is the other nullability direction: a
// later change dropped NOT NULL, and the replayed ADD finds a column that
// accepts every row the source sends.
func TestAddColumnReplayAcceptsARelaxedNull(t *testing.T) {
	p := &fakePool{}
	sink := ddlSink(p, &fakeQuerier{columns: withNote(usersState()...), keys: [][]any{{"users_pkey", "p", "id"}}})
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "public", Table: "users",
		Column: "note", AddColumns: []string{"note"},
		ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: false, Default: "x", HasDefault: true},
	}
	if err := sink.ApplySchema(change); err != nil {
		t.Fatal(err)
	}
}

// TestAddColumnCreatesAColumnTheTargetLacks is the first-run half of the same
// read-back: the column is absent, the statement creates it, and the catalogue
// then holds exactly the definition the source asked for.
func TestAddColumnCreatesAColumnTheTargetLacks(t *testing.T) {
	p := &fakePool{}
	querier := &fakeQuerier{
		columnPhase:  0,
		laterColumns: withNote(usersState()...),
		keys:         [][]any{{"users_pkey", "p", "id"}},
	}
	sink := ddlSink(p, querier)
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "public", Table: "users",
		Column: "note", AddColumns: []string{"note"},
		ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
	}
	if err := sink.ApplySchema(change); err != nil {
		t.Fatal(err)
	}
	if got := p.txs[0].sqls[0]; got != `ALTER TABLE "public"."users" ADD COLUMN IF NOT EXISTS "note" VARCHAR(32)` {
		t.Fatalf("sql %s", got)
	}
}

func TestDropColumnRemovesAndVerifies(t *testing.T) {
	p := &fakePool{}
	querier := &fakeQuerier{
		columns:      withNote(usersState()...),
		columnPhase:  1,
		laterColumns: usersState(),
		keys:         [][]any{{"users_pkey", "p", "id"}},
	}
	sink := ddlSink(p, querier)
	change := schema.Change{
		Action: schema.ActionDropColumn, Database: "public", Table: "users", Column: "note",
		DDL: "ALTER TABLE users DROP COLUMN note",
	}
	if err := sink.ApplySchema(change); err != nil {
		t.Fatal(err)
	}
	want := `ALTER TABLE "public"."users" DROP COLUMN IF EXISTS "note"`
	if got := p.txs[0].sqls[0]; got != want {
		t.Fatalf("sql %s", got)
	}
}

// TestDropColumnReplayNeedsNoStatement keeps a replayed DDL after a restart
// idempotent: the column is gone, so there is nothing left to run.
func TestDropColumnReplayNeedsNoStatement(t *testing.T) {
	p := &fakePool{}
	sink := ddlSink(p, &fakeQuerier{columns: usersState(), keys: [][]any{{"users_pkey", "p", "id"}}})
	change := schema.Change{Action: schema.ActionDropColumn, Database: "public", Table: "users", Column: "note"}
	if err := sink.ApplySchema(change); err != nil {
		t.Fatal(err)
	}
	if p.begins != 0 {
		t.Fatalf("runs %d", p.begins)
	}
}

func TestDropColumnRefusesKeyCarryingColumn(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind string
		word string
	}{
		{name: "primary key", kind: "p", word: "primary key"},
		{name: "unique constraint", kind: "u", word: "unique constraint"},
		{name: "foreign key", kind: "f", word: "foreign key"},
		{name: "unique index", kind: "ui", word: "unique index"},
	} {
		p := &fakePool{}
		querier := &fakeQuerier{
			columns:    withNote(usersState()...),
			keys:       [][]any{{"users_pkey", "p", "id"}},
			dependents: [][]any{{"note_" + tc.kind, tc.kind}},
		}
		sink := ddlSink(p, querier)
		change := schema.Change{Action: schema.ActionDropColumn, Database: "public", Table: "users", Column: "note"}
		err := sink.ApplySchema(change)
		if err == nil || !strings.Contains(err.Error(), tc.word) {
			t.Fatalf("%s: err %v", tc.name, err)
		}
		if p.begins != 0 {
			t.Fatalf("%s: runs %d", tc.name, p.begins)
		}
	}
}

// TestDropColumnAllowsHarmlessDependency: a plain index and a check constraint
// leave with the column, which is what the source asked for.
func TestDropColumnAllowsHarmlessDependency(t *testing.T) {
	for _, kind := range []string{"i", "c"} {
		p := &fakePool{}
		querier := &fakeQuerier{
			columns:      withNote(usersState()...),
			columnPhase:  1,
			laterColumns: usersState(),
			keys:         [][]any{{"users_pkey", "p", "id"}},
			dependents:   [][]any{{"note_" + kind, kind}},
		}
		sink := ddlSink(p, querier)
		change := schema.Change{Action: schema.ActionDropColumn, Database: "public", Table: "users", Column: "note"}
		if err := sink.ApplySchema(change); err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
	}
}

func TestDropColumnOnMissingTableFails(t *testing.T) {
	sink := ddlSink(&fakePool{}, &fakeQuerier{})
	change := schema.Change{Action: schema.ActionDropColumn, Database: "public", Table: "users", Column: "note"}
	err := sink.ApplySchema(change)
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("err %v", err)
	}
}

// renamedState is the catalogue answer after name became label.
func renamedState(rows [][]any, name, pgType string, notNull bool) [][]any {
	out := make([][]any, 0, len(rows))
	for _, row := range rows {
		if row[0].(string) == "name" {
			out = append(out, columnRow(name, pgType, notNull, "", false))
			continue
		}
		out = append(out, row)
	}
	return out
}

// TestRenameColumnMovesOnlyTheName: a plain RENAME COLUMN restates nothing, so
// the source catalogue is not read and no type statement is issued.
func TestRenameColumnMovesOnlyTheName(t *testing.T) {
	p := &fakePool{}
	querier := &fakeQuerier{
		columns:     usersState(),
		columnPhase: 1, laterColumns: renamedState(usersState(), "label", "character varying(64)", false),
		keys: [][]any{{"users_pkey", "p", "id"}},
	}
	sink := ddlSink(p, querier)
	describer := &fakeDescriber{table: usersTable()}
	sink.SetTableDescriber(describer)
	change := schema.Change{
		Action: schema.ActionRenameColumn, Database: "public", Table: "users",
		Column: "name", NewName: "label", SourceDatabase: "app", SourceTable: "users",
		DDL: "ALTER TABLE users RENAME COLUMN name TO label",
	}
	if err := sink.ApplySchema(change); err != nil {
		t.Fatal(err)
	}
	handle := p.txs[0]
	if handle.queued != 1 {
		t.Fatalf("queued %d", handle.queued)
	}
	want := `ALTER TABLE "public"."users" RENAME COLUMN "name" TO "label"`
	if handle.sqls[0] != want {
		t.Fatalf("sql %s", handle.sqls[0])
	}
	if calls := describer.called(); len(calls) != 0 {
		t.Fatalf("a pure rename asked the source for a definition: %#v", calls)
	}
}

// TestChangeColumnRenamesAndRetypesTogether is the extra requirement the MySQL
// CHANGE clause carries: one statement renames and redefines, and the target must
// end up with both. The three statements run in one transaction, so a failure
// cannot leave the column under its new name with the old type.
func TestChangeColumnRenamesAndRetypesTogether(t *testing.T) {
	p := &fakePool{}
	wide := renamedState(usersState(), "name", "character varying(32)", false)
	querier := &fakeQuerier{
		columns:     wide,
		columnPhase: 1, laterColumns: renamedState(usersState(), "label", "character varying(64)", true),
		keys: [][]any{{"users_pkey", "p", "id"}},
	}
	sink := ddlSink(p, querier)
	sink.SetTableDescriber(&fakeDescriber{table: schema.Table{
		Database: "app", Table: "users",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "label", Type: "varchar(64)", Nullable: false},
			{Name: "amount", Type: "decimal(10,2)", Nullable: true},
			{Name: "created", Type: "datetime(3)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}})
	change := schema.Change{
		Action: schema.ActionRenameColumn, Database: "public", Table: "users",
		Column: "name", NewName: "label", SourceDatabase: "app", SourceTable: "users",
		DDL: "ALTER TABLE users CHANGE name label varchar(64) NOT NULL",
	}
	if err := sink.ApplySchema(change); err != nil {
		t.Fatal(err)
	}
	handle := p.txs[0]
	if handle.queued != 3 {
		t.Fatalf("rename, retype and the nullability change must be one transaction: %#v", handle.sqls)
	}
	want := []string{
		`ALTER TABLE "public"."users" RENAME COLUMN "name" TO "label"`,
		`ALTER TABLE "public"."users" ALTER COLUMN "label" TYPE VARCHAR(64)`,
		`ALTER TABLE "public"."users" ALTER COLUMN "label" SET NOT NULL`,
	}
	for i := range want {
		if handle.sqls[i] != want[i] {
			t.Fatalf("statement %d:\n got %s\nwant %s", i, handle.sqls[i], want[i])
		}
	}
	if !handle.committed {
		t.Fatal("not committed")
	}
}

// TestChangeColumnRefusesUnsafeNarrowing stops the statement before it runs: the
// target holds values the new type cannot keep.
func TestChangeColumnRefusesUnsafeNarrowing(t *testing.T) {
	p := &fakePool{}
	querier := &fakeQuerier{
		columns: renamedState(usersState(), "name", "text", false),
		keys:    [][]any{{"users_pkey", "p", "id"}},
	}
	sink := ddlSink(p, querier)
	sink.SetTableDescriber(&fakeDescriber{table: schema.Table{
		Database: "app", Table: "users",
		Columns:     []schema.Column{{Name: "label", Type: "varchar(8)", Nullable: true}},
		PrimaryKeys: []string{"id"},
	}})
	change := schema.Change{
		Action: schema.ActionRenameColumn, Database: "public", Table: "users",
		Column: "name", NewName: "label", SourceDatabase: "app", SourceTable: "users",
		DDL: "ALTER TABLE users CHANGE name label varchar(8)",
	}
	err := sink.ApplySchema(change)
	if err == nil || !strings.Contains(err.Error(), "no verified lossless conversion") {
		t.Fatalf("err %v", err)
	}
	if p.begins != 0 {
		t.Fatalf("the unsafe statement ran anyway: %d", p.begins)
	}
}

func TestModifyColumnKeepsTheNameAndRestatesTheType(t *testing.T) {
	p := &fakePool{}
	querier := &fakeQuerier{
		columns:     renamedState(usersState(), "name", "character varying(32)", false),
		columnPhase: 1, laterColumns: renamedState(usersState(), "name", "character varying(64)", false),
		keys: [][]any{{"users_pkey", "p", "id"}},
	}
	sink := ddlSink(p, querier)
	sink.SetTableDescriber(&fakeDescriber{table: schema.Table{
		Database: "app", Table: "users",
		Columns:     []schema.Column{{Name: "name", Type: "varchar(64)", Nullable: true}},
		PrimaryKeys: []string{"id"},
	}})
	change := schema.Change{
		Action: schema.ActionModifyColumn, Database: "public", Table: "users",
		Column: "name", SourceDatabase: "app", SourceTable: "users",
		DDL: "ALTER TABLE users MODIFY name varchar(64)",
	}
	if err := sink.ApplySchema(change); err != nil {
		t.Fatal(err)
	}
	handle := p.txs[0]
	want := `ALTER TABLE "public"."users" ALTER COLUMN "name" TYPE VARCHAR(64)`
	if handle.queued != 1 || handle.sqls[0] != want {
		t.Fatalf("sqls %#v", handle.sqls)
	}
	if strings.Contains(handle.sqls[0], "RENAME") {
		t.Fatal("MODIFY must not rename")
	}
}

func TestRestatedChangeWithoutCatalogueFails(t *testing.T) {
	sink := ddlSink(&fakePool{}, &fakeQuerier{columns: usersState()})
	change := schema.Change{
		Action: schema.ActionModifyColumn, Database: "public", Table: "users",
		Column: "name", SourceDatabase: "app", SourceTable: "users",
		DDL: "ALTER TABLE users MODIFY name varchar(64)",
	}
	err := sink.ApplySchema(change)
	if err == nil || !strings.Contains(err.Error(), "no source catalogue") {
		t.Fatalf("err %v", err)
	}
}

func TestColumnChangeNamingNoTargetColumnFails(t *testing.T) {
	sink := ddlSink(&fakePool{}, &fakeQuerier{
		columns: usersState(), keys: [][]any{{"users_pkey", "p", "id"}},
	})
	change := schema.Change{
		Action: schema.ActionRenameColumn, Database: "public", Table: "users",
		Column: "gone", NewName: "away", DDL: "ALTER TABLE users RENAME COLUMN gone TO away",
	}
	err := sink.ApplySchema(change)
	if err == nil || !strings.Contains(err.Error(), "neither \"gone\" nor \"away\"") {
		t.Fatalf("err %v", err)
	}
}

func TestColumnChangeWhenBothNamesExistFails(t *testing.T) {
	columns := append(usersState(), columnRow("label", "character varying(64)", false, "", false))
	sink := ddlSink(&fakePool{}, &fakeQuerier{columns: columns, keys: [][]any{{"users_pkey", "p", "id"}}})
	change := schema.Change{
		Action: schema.ActionRenameColumn, Database: "public", Table: "users",
		Column: "name", NewName: "label", DDL: "ALTER TABLE users RENAME COLUMN name TO label",
	}
	err := sink.ApplySchema(change)
	if err == nil || !strings.Contains(err.Error(), "already has both columns") {
		t.Fatalf("err %v", err)
	}
}

// TestRenamedColumnReplayStillRestatesType covers the restart case: the rename
// reached the target before the checkpoint did, so only the type is left to do.
func TestRenamedColumnReplayStillRestatesType(t *testing.T) {
	p := &fakePool{}
	querier := &fakeQuerier{
		columns:     renamedState(usersState(), "label", "character varying(32)", false),
		columnPhase: 1, laterColumns: renamedState(usersState(), "label", "character varying(64)", false),
		keys: [][]any{{"users_pkey", "p", "id"}},
	}
	sink := ddlSink(p, querier)
	sink.SetTableDescriber(&fakeDescriber{table: schema.Table{
		Database: "app", Table: "users",
		Columns:     []schema.Column{{Name: "label", Type: "varchar(64)", Nullable: true}},
		PrimaryKeys: []string{"id"},
	}})
	change := schema.Change{
		Action: schema.ActionRenameColumn, Database: "public", Table: "users",
		Column: "name", NewName: "label", SourceDatabase: "app", SourceTable: "users",
		DDL: "ALTER TABLE users CHANGE name label varchar(64)",
	}
	if err := sink.ApplySchema(change); err != nil {
		t.Fatal(err)
	}
	handle := p.txs[0]
	want := `ALTER TABLE "public"."users" ALTER COLUMN "label" TYPE VARCHAR(64)`
	if handle.queued != 1 || handle.sqls[0] != want {
		t.Fatalf("sqls %#v", handle.sqls)
	}
}

// TestSuccessfulChangeInvalidatesStatementCache is PG-113: the rendered DML names
// the columns and the conflict key, so a change that worked has to drop every
// template of its own target.
func TestSuccessfulChangeInvalidatesStatementCache(t *testing.T) {
	p := &fakePool{}
	sink := ddlSink(p, &fakeQuerier{columns: usersState()})
	if err := sink.Write([]event.Event{createEvent()}); err != nil {
		t.Fatal(err)
	}
	if len(sink.builder.cache) == 0 {
		t.Fatal("the statement cache stayed empty")
	}
	sink.cfg.DropTruncate = true
	if err := sink.ApplySchema(schema.Change{Action: schema.ActionDropTable, Database: "public", Table: "users"}); err != nil {
		t.Fatal(err)
	}
	if len(sink.builder.cache) != 0 {
		t.Fatalf("templates survived a change: %#v", sink.builder.cache)
	}
	if _, ok := sink.meta.cached(Target{Schema: "public", Table: "users"}); ok {
		t.Fatal("the structure of a dropped table is still cached")
	}
}

// TestFailedChangeKeepsStatementCache is the other half of PG-114: a change that
// did not run may not leave the sink writing with templates that assume it did.
func TestFailedChangeKeepsStatementCache(t *testing.T) {
	p := &fakePool{}
	sink := ddlSink(p, &fakeQuerier{columns: usersState()})
	if err := sink.Write([]event.Event{createEvent()}); err != nil {
		t.Fatal(err)
	}
	before := len(sink.builder.cache)
	p.resultsFor = func(int) *fakeResults { return &fakeResults{execErrs: []error{pgError("42601")}} }
	sink.cfg.DropTruncate = true
	if err := sink.ApplySchema(schema.Change{Action: schema.ActionDropTable, Database: "public", Table: "users"}); err == nil {
		t.Fatal("expected error")
	}
	if len(sink.builder.cache) != before {
		t.Fatalf("a failed change invalidated the cache: %d vs %d", len(sink.builder.cache), before)
	}
}

func TestSchemaChangeAfterCloseFails(t *testing.T) {
	p := &fakePool{}
	sink := ddlSink(p, &fakeQuerier{columns: usersState()})
	sink.cfg.DropTruncate = true
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	err := sink.ApplySchema(schema.Change{Action: schema.ActionDropTable, Database: "public", Table: "users"})
	if err == nil || !strings.Contains(err.Error(), "schema change after close") {
		t.Fatalf("err %v", err)
	}
}

// TestSchemaChangeSurvivesShutdown matches the write path: a table discovered
// while the signal is being handled must still be altered, because the runner
// checkpoints the DDL and a skipped change would be lost for good.
func TestSchemaChangeSurvivesShutdown(t *testing.T) {
	p := &fakePool{}
	sink := ddlSink(p, &fakeQuerier{columns: withNote(usersState()...), keys: [][]any{{"users_pkey", "p", "id"}}})
	ctx, cancel := context.WithCancel(context.Background())
	sink.baseCtx = ctx
	cancel()
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "public", Table: "users",
		Column: "note", AddColumns: []string{"note"},
		ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
	}
	if err := sink.ApplySchema(change); err != nil {
		t.Fatal(err)
	}
	if !p.txs[0].committed {
		t.Fatal("the change was not committed")
	}
}

// TestSchemaChangeWithAnUnprovenCommitKeepsEveryCache is the third arm of
// PG-113: the statement ran and the server never answered the COMMIT, so the
// sink cannot know whether the target changed. It reports the failure without
// invalidating anything, which leaves both caches holding the last shape the
// catalogue proved. The checkpoint does not advance, so the statement is
// replayed after a restart, and it is the read-back that follows the replay,
// not the lost acknowledgement, that rewrites the caches.
func TestSchemaChangeWithAnUnprovenCommitKeepsEveryCache(t *testing.T) {
	p := &fakePool{}
	sink := ddlSink(p, &fakeQuerier{
		columns: usersState(),
		keys:    [][]any{{"users_pkey", "p", "id"}},
	})
	if err := sink.Write([]event.Event{createEvent()}); err != nil {
		t.Fatal(err)
	}
	templates := len(sink.builder.cache)
	p.commitErr = errors.New("connection lost while committing")
	err := sink.ApplySchema(schema.Change{
		Action: schema.ActionDropColumn, Database: "public", Table: "users",
		Column: "name", DDL: "ALTER TABLE users DROP COLUMN name",
	})
	if err == nil || !strings.Contains(err.Error(), "commit schema change") {
		t.Fatalf("err %v", err)
	}
	if p.begins != 2 {
		t.Fatalf("an unproven commit must not be retried inside the sink: begins %d", p.begins)
	}
	handle := p.txs[1]
	if handle.committed || !handle.rolledBack {
		t.Fatalf("committed=%v rolledBack=%v", handle.committed, handle.rolledBack)
	}
	if len(sink.builder.cache) != templates {
		t.Fatalf("templates were dropped for a change that may not have happened: %d vs %d",
			len(sink.builder.cache), templates)
	}
	state, ok := sink.meta.cached(Target{Schema: "public", Table: "users"})
	if !ok {
		t.Fatal("the cached structure was dropped for a change that may not have happened")
	}
	if _, present := state.columnFold("name"); !present {
		t.Fatal("the cached structure lost the column the drop may not have applied")
	}
}

// TestSchemaChangeRollbackIsBoundedAndIgnoresCancellation pins the DDL path to
// the rollback contract the DML path already has: a cancelled or expired
// operation context must not reach the rollback, and the rollback must still
// carry a finite deadline so a server that stopped answering cannot hold the
// change, and the job, open forever.
func TestSchemaChangeRollbackIsBoundedAndIgnoresCancellation(t *testing.T) {
	p := &fakePool{}
	sink := ddlSink(p, &fakeQuerier{
		columns: withNote(usersState()...), keys: [][]any{{"users_pkey", "p", "id"}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink.baseCtx = ctx
	p.resultsFor = func(int) *fakeResults {
		// The signal arrives while the statement is on the wire, so the
		// operation context is already cancelled when the rollback runs.
		cancel()
		return &fakeResults{execErrs: []error{pgError("42710")}}
	}
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "public", Table: "users",
		Column: "note", AddColumns: []string{"note"},
		ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
	}
	if err := sink.ApplySchema(change); err == nil {
		t.Fatal("expected error")
	}
	handle := p.txs[0]
	if !handle.rolledBack {
		t.Fatal("transaction not rolled back")
	}
	if handle.rollbackCtxErr != nil {
		t.Fatalf("rollback context was already done: %v", handle.rollbackCtxErr)
	}
	if !handle.rollbackHasDeadline {
		t.Fatal("rollback context carries no deadline")
	}
	if remaining := time.Until(handle.rollbackDeadline); remaining <= 0 || remaining > rollbackTimeout {
		t.Fatalf("rollback deadline in %s, want (0, %s]", remaining, rollbackTimeout)
	}
}

// TestSchemaChangeRollbackFailureKeepsTheDDLError proves a failing rollback does
// not replace the statement error the runner reports, and therefore cannot
// change what stops the job or what the checkpoint keeps.
func TestSchemaChangeRollbackFailureKeepsTheDDLError(t *testing.T) {
	p := &fakePool{
		rollbackErr: context.DeadlineExceeded,
		resultsFor: func(int) *fakeResults {
			return &fakeResults{execErrs: []error{pgError("42710")}}
		},
	}
	sink := ddlSink(p, &fakeQuerier{
		columns: withNote(usersState()...), keys: [][]any{{"users_pkey", "p", "id"}},
	})
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "public", Table: "users",
		Column: "note", AddColumns: []string{"note"},
		ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
	}
	err := sink.ApplySchema(change)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "42710") {
		t.Fatalf("error %q lost the statement failure", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error %q reports the rollback failure", err)
	}
	if !p.txs[0].rolledBack {
		t.Fatal("transaction not rolled back")
	}
}

// TestSchemaChangeCommitFailureRollsBackBounded covers the second rollback site:
// a COMMIT whose result was lost still abandons the transaction, and it does so
// under the same bounded deadline.
func TestSchemaChangeCommitFailureRollsBackBounded(t *testing.T) {
	p := &fakePool{commitErr: pgError("40001")}
	sink := ddlSink(p, &fakeQuerier{
		columns: withNote(usersState()...), keys: [][]any{{"users_pkey", "p", "id"}},
	})
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "public", Table: "users",
		Column: "note", AddColumns: []string{"note"},
		ColumnDef: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
	}
	err := sink.ApplySchema(change)
	if err == nil || !strings.Contains(err.Error(), "commit schema change") {
		t.Fatalf("err %v", err)
	}
	handle := p.txs[0]
	if handle.committed || !handle.rolledBack {
		t.Fatalf("committed=%v rolledBack=%v", handle.committed, handle.rolledBack)
	}
	if !handle.rollbackHasDeadline {
		t.Fatal("rollback context carries no deadline")
	}
	if remaining := time.Until(handle.rollbackDeadline); remaining <= 0 || remaining > rollbackTimeout {
		t.Fatalf("rollback deadline in %s, want (0, %s]", remaining, rollbackTimeout)
	}
}

// TestHistoricalChangeTheSourceNoLongerShowsRunsNothing is the PG-109 replay
// case. The pipeline restarts behind a CHANGE COLUMN, and the source has since
// renamed the column again, so the current catalogue holds neither the old name
// the target carries nor the new one the statement names. The change is refused
// before a single statement runs: the structured description that decides the
// column's type is gone, and renaming on the statement text alone would leave
// the target holding a column no later row names.
func TestHistoricalChangeTheSourceNoLongerShowsRunsNothing(t *testing.T) {
	p := &fakePool{}
	sink := ddlSink(p, &fakeQuerier{
		columns: usersState(), keys: [][]any{{"users_pkey", "p", "id"}},
	})
	sink.SetTableDescriber(&fakeDescriber{table: schema.Table{
		Database: "app", Table: "users",
		Columns:     []schema.Column{{Name: "final", Type: "varchar(64)", Nullable: true}},
		PrimaryKeys: []string{"id"},
	}})
	change := schema.Change{
		Action: schema.ActionRenameColumn, Database: "public", Table: "users",
		Column: "name", NewName: "label", SourceDatabase: "app", SourceTable: "users",
		DDL: "ALTER TABLE users CHANGE name label varchar(64)",
	}
	err := sink.ApplySchema(change)
	if err == nil || !strings.Contains(err.Error(), "cannot be replayed safely") {
		t.Fatalf("err %v", err)
	}
	if len(p.txs) != 0 {
		t.Fatalf("the replay ran DDL before the source read failed: %#v", p.txs[0].sqls)
	}
}
