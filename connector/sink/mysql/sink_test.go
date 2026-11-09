package mysql

import (
	"database/sql"
	"errors"
	"fmt"
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
	want := schema.Column{Name: "note", Type: "varchar(32)", Nullable: true}
	if err := sameColumnDefinition(want, schema.Column{Name: "note", Type: "VARCHAR(32)", Nullable: true}); err != nil {
		t.Fatalf("an equal definition must pass: %v", err)
	}
	if err := sameColumnDefinition(want, schema.Column{Name: "note", Type: "varchar(64)", Nullable: true}); err == nil {
		t.Fatal("a different width must fail")
	}
	if err := sameColumnDefinition(want, schema.Column{Name: "note", Type: "varchar(32)", Nullable: false}); err == nil {
		t.Fatal("a different nullability must fail")
	}
}

// fakeDDLDB answers only the column lookup, so the replay paths can be tested
// without a MySQL server.
type fakeDDLDB struct {
	live    *schema.Column
	lookups int
	err     error
}

func (f *fakeDDLDB) QueryRow(string, ...any) scanner {
	f.lookups++
	if f.err != nil {
		return &rowScanner{err: f.err}
	}
	if f.live == nil {
		return &rowScanner{err: sql.ErrNoRows}
	}
	return &rowScanner{values: []any{f.live.Type, nullabilityText(f.live.Nullable)}}
}

// rowScanner is the one-row shape (*sql.Row).Scan provides.
type rowScanner struct {
	values []any
	err    error
}

func (r *rowScanner) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return fmt.Errorf("scan: %d destinations for %d values", len(dest), len(r.values))
	}
	for i := range dest {
		target, ok := dest[i].(*string)
		if !ok {
			return fmt.Errorf("scan: destination %d is not *string", i)
		}
		*target = r.values[i].(string)
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

func TestReplayedDropColumnKeepsErrorOnCompoundAlter(t *testing.T) {
	// 1091 may come from DROP INDEX while DROP COLUMN already succeeded, or
	// from DROP COLUMN while DROP INDEX still needs to run. Either way a
	// multi-clause statement cannot be proven done from the column lookup alone.
	missing := &mysqldriver.MySQLError{Number: erCantDropFieldOrKey, Message: "Can't DROP 'idx_note'; check that column/key exists"}
	db := &fakeDDLDB{} // column note is already gone
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
		t.Fatalf("compound ALTER must keep the original 1091, got handled with err=%v", err)
	}
	if db.lookups != 0 {
		t.Fatal("compound ALTER must not consult the catalogue just to swallow 1091")
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
