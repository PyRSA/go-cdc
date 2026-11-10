package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/PyRSA/go-cdc/common/schema"
)

func TestNormalizeColumnType(t *testing.T) {
	if !sameColumnType("varchar(32)", "VARCHAR(32)") {
		t.Fatal("case must not matter")
	}
	if !sameColumnType("VARCHAR(64)", "varchar( 64 )") {
		t.Fatal("spacing must not matter")
	}
	if sameColumnType("varchar(32)", "varchar(64)") {
		t.Fatal("a width difference is a real difference")
	}
	if sameColumnType("int", "bigint") {
		t.Fatal("a type family difference is a real difference")
	}
}

func TestSameColumnDefinition(t *testing.T) {
	want := schema.Column{
		Name: "note", Type: "varchar(32)", Nullable: true,
		Charset: "utf8mb4", Collation: "utf8mb4_general_ci",
	}
	if err := sameColumnDefinition(want, schema.Column{
		Name: "note", Type: "VARCHAR(32)", Nullable: true,
		Charset: "UTF8MB4", Collation: "UTF8MB4_GENERAL_CI",
	}); err != nil {
		t.Fatalf("type, nullability, charset and collation must pass case-insensitively: %v", err)
	}
	if err := sameColumnDefinition(want, schema.Column{Name: "note", Type: "varchar(64)", Nullable: true, Charset: "utf8mb4", Collation: "utf8mb4_general_ci"}); err == nil {
		t.Fatal("a different width must fail")
	}
	if err := sameColumnDefinition(want, schema.Column{Name: "note", Type: "varchar(32)", Nullable: false, Charset: "utf8mb4", Collation: "utf8mb4_general_ci"}); err == nil {
		t.Fatal("a different nullability must fail")
	}
	if err := sameColumnDefinition(want, schema.Column{Name: "note", Type: "varchar(32)", Nullable: true, Charset: "latin1", Collation: "utf8mb4_general_ci"}); err == nil {
		t.Fatal("a different charset must fail")
	}
	if err := sameColumnDefinition(want, schema.Column{Name: "note", Type: "varchar(32)", Nullable: true, Charset: "utf8mb4", Collation: "utf8mb4_bin"}); err == nil {
		t.Fatal("a different collation must fail")
	}

	bothEmpty := schema.Column{Name: "amount", Type: "int", Nullable: false}
	if err := sameColumnDefinition(bothEmpty, schema.Column{Name: "amount", Type: "int", Nullable: false}); err != nil {
		t.Fatalf("non-string columns with empty charset and collation must match: %v", err)
	}
	if err := sameColumnDefinition(bothEmpty, schema.Column{Name: "amount", Type: "int", Nullable: false, Charset: "utf8mb4"}); err == nil {
		t.Fatal("empty source charset against a live charset must fail")
	}
	if err := sameColumnDefinition(
		schema.Column{Name: "note", Type: "varchar(32)", Nullable: true, Charset: "utf8mb4", Collation: "utf8mb4_bin"},
		schema.Column{Name: "note", Type: "varchar(32)", Nullable: true},
	); err == nil {
		t.Fatal("a live column missing charset/collation must fail")
	}
}

// fakeDDLDB answers column and index lookups so DROP replay paths can be
// tested without a MySQL server.
type fakeDDLDB struct {
	live      *schema.Column
	columns   map[string]*schema.Column
	indexes   map[string]bool
	lookups   int
	err       error
	lastQuery string
	lastScan  int
}

func (f *fakeDDLDB) QueryRow(query string, args ...any) scanner {
	f.lookups++
	f.lastQuery = query
	if f.err != nil {
		return &rowScanner{fake: f, err: f.err}
	}
	name := ""
	if len(args) >= 3 {
		name, _ = args[2].(string)
	}
	if strings.Contains(strings.ToLower(query), "information_schema.statistics") {
		if f.indexPresent(name) {
			return &rowScanner{fake: f, values: []any{name}}
		}
		return &rowScanner{fake: f, err: sql.ErrNoRows}
	}
	live := f.columnPresent(name)
	if live == nil {
		return &rowScanner{fake: f, err: sql.ErrNoRows}
	}
	return &rowScanner{fake: f, values: []any{
		live.Type, nullabilityText(live.Nullable), live.Charset, live.Collation,
	}}
}

func (f *fakeDDLDB) columnPresent(name string) *schema.Column {
	if f.columns != nil {
		for key, column := range f.columns {
			if strings.EqualFold(key, name) {
				return column
			}
		}
		return nil
	}
	if f.live != nil && (name == "" || strings.EqualFold(f.live.Name, name)) {
		return f.live
	}
	return nil
}

func (f *fakeDDLDB) indexPresent(name string) bool {
	for key, present := range f.indexes {
		if present && strings.EqualFold(key, name) {
			return true
		}
	}
	return false
}

// rowScanner is the one-row shape (*sql.Row).Scan provides.
type rowScanner struct {
	fake   *fakeDDLDB
	values []any
	err    error
}

func (r *rowScanner) Scan(dest ...any) error {
	if r.fake != nil {
		r.fake.lastScan = len(dest)
	}
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return fmt.Errorf("scan: %d destinations for %d values", len(dest), len(r.values))
	}
	for i := range dest {
		switch target := dest[i].(type) {
		case *string:
			targetValue, ok := r.values[i].(string)
			if !ok {
				return fmt.Errorf("scan: value %d is not string", i)
			}
			*target = targetValue
		case *sql.NullString:
			switch value := r.values[i].(type) {
			case nil:
				*target = sql.NullString{}
			case string:
				*target = sql.NullString{String: value, Valid: true}
			default:
				return fmt.Errorf("scan: value %d is not string-or-nil", i)
			}
		default:
			return fmt.Errorf("scan: destination %d has unsupported type %T", i, dest[i])
		}
	}
	return nil
}

func nullabilityText(nullable bool) string {
	if nullable {
		return "YES"
	}
	return "NO"
}

func addColumnChange(columnType string, nullable bool) schema.Change {
	return schema.Change{
		Action:     schema.ActionAddColumn,
		Database:   "shop",
		Table:      "orders",
		DDL:        "ALTER TABLE shop.orders ADD COLUMN note " + columnType,
		AddColumns: []string{"note"},
		ColumnDef:  &schema.Column{Name: "note", Type: columnType, Nullable: nullable},
	}
}

func TestVerifyReplayedAddColumnAcceptsAnEqualColumn(t *testing.T) {
	db := &fakeDDLDB{live: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true}}
	sink := &Sink{columns: db}
	if err := sink.verifyReplayedAddColumn(addColumnChange("varchar(32)", true)); err != nil {
		t.Fatalf("an equal column is a completed replay: %v", err)
	}
	if db.lookups != 1 {
		t.Fatalf("lookups=%d", db.lookups)
	}
}

func TestLiveColumnReadsCharsetAndCollation(t *testing.T) {
	db := &fakeDDLDB{live: &schema.Column{
		Name: "note", Type: "varchar(32)", Nullable: true,
		Charset: "utf8mb4", Collation: "utf8mb4_bin",
	}}
	sink := &Sink{columns: db}
	got, ok, err := sink.liveColumn("shop", "orders", "note")
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if !contains(db.lastQuery, "CHARACTER_SET_NAME") || !contains(db.lastQuery, "COLLATION_NAME") {
		t.Fatalf("query must read charset and collation: %s", db.lastQuery)
	}
	if db.lastScan != 4 {
		t.Fatalf("scan destinations=%d, want 4", db.lastScan)
	}
	if got.Type != "varchar(32)" || !got.Nullable || got.Charset != "utf8mb4" || got.Collation != "utf8mb4_bin" {
		t.Fatalf("column %#v", got)
	}
}

func TestVerifyReplayedAddColumnRefusesDifferentCharset(t *testing.T) {
	db := &fakeDDLDB{live: &schema.Column{
		Name: "note", Type: "varchar(32)", Nullable: true,
		Charset: "latin1", Collation: "latin1_swedish_ci",
	}}
	sink := &Sink{columns: db}
	change := addColumnChange("varchar(32)", true)
	change.ColumnDef.Charset = "utf8mb4"
	change.ColumnDef.Collation = "utf8mb4_general_ci"
	err := sink.verifyReplayedAddColumn(change)
	if err == nil || !contains(err.Error(), "charset") {
		t.Fatalf("err=%v", err)
	}
}

func TestVerifyReplayedAddColumnRefusesADifferentDefinition(t *testing.T) {
	db := &fakeDDLDB{live: &schema.Column{Name: "note", Type: "varchar(64)", Nullable: true}}
	sink := &Sink{columns: db}
	err := sink.verifyReplayedAddColumn(addColumnChange("varchar(32)", true))
	if err == nil {
		t.Fatal("a narrower target column must fail the replay")
	}
	if !contains(err.Error(), "varchar(64)") || !contains(err.Error(), "varchar(32)") {
		t.Fatalf("the error must name both types: %v", err)
	}
}

func TestVerifyReplayedAddColumnRefusesDifferentNullability(t *testing.T) {
	db := &fakeDDLDB{live: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: false}}
	sink := &Sink{columns: db}
	err := sink.verifyReplayedAddColumn(addColumnChange("varchar(32)", true))
	if err == nil {
		t.Fatal("a NOT NULL target column where the source is nullable must fail")
	}
}

func TestVerifyReplayedAddColumnRefusesWithoutDefinition(t *testing.T) {
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "shop", Table: "orders",
		DDL: "ALTER TABLE shop.orders ADD COLUMN note varchar(32)",
	}
	sink := &Sink{}
	err := sink.verifyReplayedAddColumn(change)
	if err == nil || !contains(err.Error(), "no column definition") {
		t.Fatalf("err=%v", err)
	}
}

func TestVerifyReplayedAddColumnRefusesWhenTheColumnIsAbsent(t *testing.T) {
	// 1060 without the column is a contradiction: never treat it as success.
	db := &fakeDDLDB{}
	sink := &Sink{columns: db}
	err := sink.verifyReplayedAddColumn(addColumnChange("varchar(32)", true))
	if err == nil || !contains(err.Error(), "has no column") {
		t.Fatalf("err=%v", err)
	}
}

func dropColumnChange(column string) schema.Change {
	return schema.Change{
		Action:   schema.ActionDropColumn,
		Database: "shop",
		Table:    "orders",
		Column:   column,
		DDL:      "ALTER TABLE shop.orders DROP COLUMN " + column,
	}
}

func TestReplayedColumnChangeClassification(t *testing.T) {
	dup := &mysqldriver.MySQLError{Number: erDupFieldName, Message: "Duplicate column name 'note'"}
	missing := &mysqldriver.MySQLError{Number: erCantDropFieldOrKey, Message: "Can't DROP 'note'"}
	noTable := &mysqldriver.MySQLError{Number: erNoSuchTable, Message: "Table doesn't exist"}
	denied := &mysqldriver.MySQLError{Number: 1142, Message: "TRUNCATE command denied"}
	other := &mysqldriver.MySQLError{Number: 1054, Message: "Unknown column"}

	equal := &fakeDDLDB{live: &schema.Column{Name: "note", Type: "varchar(32)", Nullable: true}}
	sink := &Sink{columns: equal}

	// A duplicate ADD needs the read-back.
	handled, err := sink.replayedColumnChange(addColumnChange("varchar(32)", true), dup)
	if !handled || err != nil {
		t.Fatalf("duplicate ADD: handled=%v err=%v", handled, err)
	}

	// A TRUNCATE of a table that is gone is done, and still no read.
	before := equal.lookups
	handled, err = sink.replayedColumnChange(schema.Change{Action: schema.ActionTruncate}, noTable)
	if !handled || err != nil {
		t.Fatalf("TRUNCATE 1146: handled=%v err=%v", handled, err)
	}
	if equal.lookups != before {
		t.Fatal("TRUNCATE must not query the catalogue")
	}

	// A permission refusal is not a replay.
	handled, _ = sink.replayedColumnChange(schema.Change{Action: schema.ActionTruncate}, denied)
	if handled {
		t.Fatal("a denied TRUNCATE must keep its error")
	}

	// An unrelated error is not a replay for any action.
	for _, action := range []schema.Action{schema.ActionAddColumn, schema.ActionDropColumn, schema.ActionTruncate, schema.ActionDropTable} {
		if handled, _ := sink.replayedColumnChange(schema.Change{Action: action}, other); handled {
			t.Fatalf("%s: an unrelated error was read as a replay", action)
		}
	}

	// A client error (no server number) is never a replay.
	if handled, _ := sink.replayedColumnChange(schema.Change{Action: schema.ActionAddColumn}, errors.New("dial tcp: connection refused")); handled {
		t.Fatal("a client error was read as a replay")
	}

	// DROP COLUMN 1091 alone is not enough when the column is still present.
	handled, err = sink.replayedColumnChange(dropColumnChange("note"), missing)
	if handled {
		t.Fatalf("DROP COLUMN must not succeed while the column is still present: err=%v", err)
	}
}

func TestReplayedDropColumnSucceedsWhenColumnAlreadyGone(t *testing.T) {
	missing := &mysqldriver.MySQLError{Number: erCantDropFieldOrKey, Message: "Can't DROP 'note'; check that column/key exists"}
	db := &fakeDDLDB{} // no live column → already gone
	sink := &Sink{columns: db}

	handled, err := sink.replayedColumnChange(dropColumnChange("note"), missing)
	if !handled || err != nil {
		t.Fatalf("single DROP COLUMN with the column gone: handled=%v err=%v", handled, err)
	}
	if db.lookups != 1 {
		t.Fatalf("lookups=%d, want a catalogue check that the column is gone", db.lookups)
	}
}

func TestReplayedCompoundDropSucceedsWhenAllTargetsGone(t *testing.T) {
	missing := &mysqldriver.MySQLError{Number: erCantDropFieldOrKey, Message: "Can't DROP 'note'; check that column/key exists"}
	db := &fakeDDLDB{} // column and index both absent
	sink := &Sink{columns: db}
	change := schema.Change{
		Action:   schema.ActionDropColumn,
		Database: "shop",
		Table:    "orders",
		Column:   "note",
		DDL:      "ALTER TABLE shop.orders DROP COLUMN note, DROP INDEX idx_note",
	}

	handled, err := sink.replayedColumnChange(change, missing)
	if !handled || err != nil {
		t.Fatalf("fully applied compound DROP: handled=%v err=%v", handled, err)
	}
	if db.lookups < 2 {
		t.Fatalf("lookups=%d, want column and index checks", db.lookups)
	}
}

func TestReplayedCompoundDropKeepsErrorWhenIndexRemains(t *testing.T) {
	missing := &mysqldriver.MySQLError{Number: erCantDropFieldOrKey, Message: "Can't DROP 'note'; check that column/key exists"}
	db := &fakeDDLDB{indexes: map[string]bool{"idx_note": true}}
	sink := &Sink{columns: db}
	change := schema.Change{
		Action:   schema.ActionDropColumn,
		Database: "shop",
		Table:    "orders",
		Column:   "note",
		DDL:      "ALTER TABLE shop.orders DROP COLUMN note, DROP INDEX idx_note",
	}

	handled, err := sink.replayedColumnChange(change, missing)
	if handled {
		t.Fatalf("compound DROP must keep 1091 while the index remains: err=%v", err)
	}
}

func TestReplayedCompoundDropKeepsErrorWhenColumnRemains(t *testing.T) {
	missing := &mysqldriver.MySQLError{Number: erCantDropFieldOrKey, Message: "Can't DROP 'idx_note'; check that column/key exists"}
	db := &fakeDDLDB{
		columns: map[string]*schema.Column{
			"note": {Name: "note", Type: "varchar(32)", Nullable: true},
		},
	}
	sink := &Sink{columns: db}
	change := schema.Change{
		Action:   schema.ActionDropColumn,
		Database: "shop",
		Table:    "orders",
		Column:   "note",
		DDL:      "ALTER TABLE shop.orders DROP COLUMN note, DROP INDEX idx_note",
	}

	handled, err := sink.replayedColumnChange(change, missing)
	if handled {
		t.Fatalf("compound DROP must keep 1091 while the column remains: err=%v", err)
	}
}

func TestReplayedMultiDropColumnSucceedsWhenAllGone(t *testing.T) {
	missing := &mysqldriver.MySQLError{Number: erCantDropFieldOrKey, Message: "Can't DROP 'note'"}
	db := &fakeDDLDB{}
	sink := &Sink{columns: db}
	change := schema.Change{
		Action:   schema.ActionDropColumn,
		Database: "shop",
		Table:    "orders",
		Column:   "note",
		DDL:      "ALTER TABLE shop.orders DROP COLUMN note, DROP COLUMN extra",
	}

	handled, err := sink.replayedColumnChange(change, missing)
	if !handled || err != nil {
		t.Fatalf("multi DROP COLUMN all gone: handled=%v err=%v", handled, err)
	}
	if db.lookups != 2 {
		t.Fatalf("lookups=%d", db.lookups)
	}
}

func TestReplayedMultiDropColumnKeepsErrorWhenOneRemains(t *testing.T) {
	missing := &mysqldriver.MySQLError{Number: erCantDropFieldOrKey, Message: "Can't DROP 'note'"}
	db := &fakeDDLDB{
		columns: map[string]*schema.Column{
			"extra": {Name: "extra", Type: "int", Nullable: true},
		},
	}
	sink := &Sink{columns: db}
	change := schema.Change{
		Action:   schema.ActionDropColumn,
		Database: "shop",
		Table:    "orders",
		Column:   "note",
		DDL:      "ALTER TABLE shop.orders DROP COLUMN note, DROP COLUMN extra",
	}

	handled, err := sink.replayedColumnChange(change, missing)
	if handled {
		t.Fatalf("partial multi DROP COLUMN must keep 1091: err=%v", err)
	}
}

func TestReplayedDropWithPrimaryKeySucceedsWhenGone(t *testing.T) {
	missing := &mysqldriver.MySQLError{Number: erCantDropFieldOrKey, Message: "Can't DROP 'PRIMARY'; check that column/key exists"}
	db := &fakeDDLDB{}
	sink := &Sink{columns: db}
	change := schema.Change{
		Action:   schema.ActionDropColumn,
		Database: "shop",
		Table:    "orders",
		Column:   "note",
		DDL:      "ALTER TABLE shop.orders DROP COLUMN note, DROP PRIMARY KEY",
	}

	handled, err := sink.replayedColumnChange(change, missing)
	if !handled || err != nil {
		t.Fatalf("DROP COLUMN + DROP PRIMARY KEY all gone: handled=%v err=%v", handled, err)
	}
}

func TestReplayedDropKeepsErrorOnUnrecognizedClause(t *testing.T) {
	missing := &mysqldriver.MySQLError{Number: erCantDropFieldOrKey, Message: "Can't DROP 'note'"}
	db := &fakeDDLDB{}
	sink := &Sink{columns: db}
	change := schema.Change{
		Action:   schema.ActionDropColumn,
		Database: "shop",
		Table:    "orders",
		Column:   "note",
		DDL:      "ALTER TABLE shop.orders DROP COLUMN note, ADD COLUMN extra int",
	}

	handled, err := sink.replayedColumnChange(change, missing)
	if handled {
		t.Fatalf("mixed non-DROP clause must keep 1091: err=%v", err)
	}
	if db.lookups != 0 {
		t.Fatal("unrecognized compound ALTER must not consult the catalogue")
	}
}

func TestReplayedDropTableSucceedsWhenTableAlreadyGone(t *testing.T) {
	noTable := &mysqldriver.MySQLError{Number: erNoSuchTable, Message: "Table 'shop.orders' doesn't exist"}
	sink := &Sink{}

	handled, err := sink.replayedColumnChange(schema.Change{
		Action: schema.ActionDropTable, Database: "shop", Table: "orders",
		DDL: "DROP TABLE shop.orders",
	}, noTable)
	if !handled || err != nil {
		t.Fatalf("DROP TABLE 1146: handled=%v err=%v", handled, err)
	}

	unknownTable := &mysqldriver.MySQLError{Number: erBadTable, Message: "Unknown table 'shop.orders'"}
	handled, err = sink.replayedColumnChange(schema.Change{
		Action: schema.ActionDropTable, Database: "shop", Table: "orders",
		DDL: "DROP TABLE shop.orders",
	}, unknownTable)
	if !handled || err != nil {
		t.Fatalf("DROP TABLE 1051: handled=%v err=%v", handled, err)
	}

	noDB := &mysqldriver.MySQLError{Number: erBadDB, Message: "Unknown database 'shop'"}
	handled, err = sink.replayedColumnChange(schema.Change{
		Action: schema.ActionDropTable, Database: "shop", Table: "orders",
		DDL: "DROP TABLE shop.orders",
	}, noDB)
	if !handled || err != nil {
		t.Fatalf("DROP TABLE 1049: handled=%v err=%v", handled, err)
	}
}

func TestReplayedDropTableKeepsPermissionAndOtherErrors(t *testing.T) {
	sink := &Sink{}
	change := schema.Change{
		Action: schema.ActionDropTable, Database: "shop", Table: "orders",
		DDL: "DROP TABLE shop.orders",
	}

	denied := &mysqldriver.MySQLError{Number: 1142, Message: "DROP command denied to user"}
	handled, _ := sink.replayedColumnChange(change, denied)
	if handled {
		t.Fatal("a denied DROP TABLE must keep its error")
	}

	lockWait := &mysqldriver.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded"}
	handled, _ = sink.replayedColumnChange(change, lockWait)
	if handled {
		t.Fatal("a lock timeout on DROP TABLE must keep its error")
	}
}

func TestWriteWithRetryStopsAfterCommitError(t *testing.T) {
	calls := 0
	err := writeWithRetry(3, func() error {
		calls++
		return &commitError{err: errors.New("connection reset")}
	})
	if calls != 1 {
		t.Fatalf("commit was retried %d times", calls)
	}
	if !isCommitError(err) {
		t.Fatalf("err=%v", err)
	}
}

func TestWriteWithRetryRepeatsRolledBackStatement(t *testing.T) {
	calls := 0
	err := writeWithRetry(2, func() error {
		calls++
		return fmt.Errorf("deadlock")
	})
	if calls != 3 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
	if isCommitError(err) {
		t.Fatal("a statement error was reported as a commit")
	}
}

func TestWriteWithRetryReturnsFirstSuccess(t *testing.T) {
	calls := 0
	err := writeWithRetry(3, func() error {
		calls++
		if calls < 2 {
			return errors.New("retry")
		}
		return nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
