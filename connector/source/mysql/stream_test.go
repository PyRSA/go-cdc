package mysql

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/PyRSA/go-cdc/common/schema"
)

func TestBinlogReadTimeout(t *testing.T) {
	if got := binlogReadTimeout(30 * time.Second); got != 60*time.Second {
		t.Fatalf("got %v", got)
	}
	if got := binlogReadTimeout(0); got != 60*time.Second {
		t.Fatalf("zero heartbeat: got %v", got)
	}
}

func TestBinlogNewlyAddedRequiresTheFlag(t *testing.T) {
	off := &handler{include: compilePatterns(`db\..*`), allow: map[string]struct{}{"db.user": {}}}
	if off.captured("db", "order") {
		t.Fatal("order was captured without the binlog flag and without an allow entry")
	}
	closed := &handler{include: compilePatterns(`db\..*`)}
	if closed.captured("db", "order") {
		t.Fatal("nil allow without binlog flag must not open-ended capture")
	}
	on := &handler{include: compilePatterns(`db\..*`), binlogNewlyAdded: true}
	if !on.captured("db", "order") {
		t.Fatal("binlog flag did not accept a matching new table")
	}
}

func TestNewlyAddedLimitDropsLaterTables(t *testing.T) {
	h := &handler{
		include: compilePatterns(`db\..*`),
		allow:   map[string]struct{}{"db.t": {}},
	}
	if !h.captured("db", "t") || h.captured("db", "new") {
		t.Fatal("known-table limit did not drop the later table")
	}
}

func TestAdmitCreateWithAllowSet(t *testing.T) {
	h := &handler{
		include:          compilePatterns(`db\..*`),
		allow:            map[string]struct{}{"db.user": {}},
		binlogNewlyAdded: true,
	}
	if h.captured("db", "order") {
		t.Fatal("config-diff DML must stay blocked while allow is set")
	}
	if !h.admitCreate("db", "crt") {
		t.Fatal("runtime CREATE should admit when C=true")
	}
	if !h.captured("db", "crt") {
		t.Fatal("admitted CREATE table should pass captured()")
	}
	if h.captured("db", "order") {
		t.Fatal("config-diff table must remain blocked after unrelated CREATE")
	}
}

func addColumnDB() *memDB {
	db := newMemDB(nil)
	db.tables = []schema.Table{{
		Database: "db", Table: "orders",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "note", Type: "varchar(32)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}}
	return db
}

func TestFillAddColumnUsesCatalogMetadata(t *testing.T) {
	h := &handler{db: addColumnDB()}
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "db", Table: "orders",
		DDL: "ALTER TABLE `db`.`orders` ADD COLUMN `note` varchar(32) NULL",
	}
	h.fillAddColumn("db", &change)
	if len(change.AddColumns) != 1 || change.AddColumns[0] != "note" {
		t.Fatalf("AddColumns %#v", change.AddColumns)
	}
	if change.ColumnDef == nil || change.ColumnDef.Type != "varchar(32)" || !change.ColumnDef.Nullable {
		t.Fatalf("ColumnDef %#v", change.ColumnDef)
	}
}

func TestFillAddColumnWithoutCatalogLeavesDefinitionNil(t *testing.T) {
	h := &handler{}
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "db", Table: "orders",
		DDL: "ALTER TABLE db.orders ADD COLUMN note varchar(32) NULL",
	}
	h.fillAddColumn("db", &change)
	if len(change.AddColumns) != 1 || change.ColumnDef != nil {
		t.Fatalf("change %#v", change)
	}
}

func TestFillAddColumnMultiAddIsNotGuessed(t *testing.T) {
	h := &handler{db: addColumnDB()}
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "db", Table: "orders",
		DDL: "ALTER TABLE db.orders ADD COLUMN note varchar(32), ADD COLUMN extra int",
	}
	h.fillAddColumn("db", &change)
	if len(change.AddColumns) != 2 || change.ColumnDef != nil {
		t.Fatalf("change %#v", change)
	}
}

func TestOnDDLAddColumnCarriesChangeMetadata(t *testing.T) {
	ch := make(chan Commit, 1)
	h := &handler{
		ch: ch, db: addColumnDB(),
		include: compilePatterns(`db\..*`), allow: map[string]struct{}{"db.orders": {}},
	}
	query := &replication.QueryEvent{
		Schema: []byte("db"),
		Query:  []byte("ALTER TABLE `db`.`orders` ADD COLUMN `note` varchar(32) NULL"),
	}
	if err := h.OnDDL(nil, mysql.Position{}, query); err != nil {
		t.Fatal(err)
	}
	commit := <-ch
	if commit.Event == nil || commit.Event.Change == nil {
		t.Fatalf("missing change metadata: %#v", commit)
	}
	if commit.Event.Change.ColumnDef == nil || commit.Event.Change.ColumnDef.Name != "note" {
		t.Fatalf("column definition %#v", commit.Event.Change.ColumnDef)
	}
}
