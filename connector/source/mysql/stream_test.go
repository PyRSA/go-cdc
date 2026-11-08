package mysql

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/source"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
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
	if err := h.fillAddColumn("db", &change); err != nil {
		t.Fatal(err)
	}
	if len(change.AddColumns) != 1 || change.AddColumns[0] != "note" {
		t.Fatalf("AddColumns %#v", change.AddColumns)
	}
	if change.ColumnDef == nil || change.ColumnDef.Type != "varchar(32)" || !change.ColumnDef.Nullable {
		t.Fatalf("ColumnDef %#v", change.ColumnDef)
	}
}

func TestFillAddColumnWithoutCatalogFails(t *testing.T) {
	h := &handler{}
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "db", Table: "orders",
		DDL: "ALTER TABLE db.orders ADD COLUMN note varchar(32) NULL",
	}
	err := h.fillAddColumn("db", &change)
	if err == nil || !strings.Contains(err.Error(), "no catalog connection") {
		t.Fatalf("err=%v change %#v", err, change)
	}
	if len(change.AddColumns) != 1 || change.ColumnDef != nil {
		t.Fatalf("change %#v", change)
	}
}

func TestFillAddColumnDescribeFailurePropagates(t *testing.T) {
	db := newMemDB(nil)
	db.describeErr = fmt.Errorf("catalog refused")
	h := &handler{db: db}
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "db", Table: "orders",
		DDL: "ALTER TABLE db.orders ADD COLUMN note varchar(32) NULL",
	}
	err := h.fillAddColumn("db", &change)
	if err == nil || !strings.Contains(err.Error(), "catalog refused") {
		t.Fatalf("err=%v", err)
	}
	if change.ColumnDef != nil {
		t.Fatalf("ColumnDef should stay nil: %#v", change.ColumnDef)
	}
}

func TestFillAddColumnMultiAddIsNotGuessed(t *testing.T) {
	h := &handler{db: addColumnDB()}
	change := schema.Change{
		Action: schema.ActionAddColumn, Database: "db", Table: "orders",
		DDL: "ALTER TABLE db.orders ADD COLUMN note varchar(32), ADD COLUMN extra int",
	}
	if err := h.fillAddColumn("db", &change); err != nil {
		t.Fatal(err)
	}
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
	if commit.Event == nil || commit.Event.Change == nil || commit.Position != nil {
		t.Fatalf("missing change metadata: %#v", commit)
	}
	if commit.Event.Change.ColumnDef == nil || commit.Event.Change.ColumnDef.Name != "note" {
		t.Fatalf("column definition %#v", commit.Event.Change.ColumnDef)
	}
	select {
	case extra := <-ch:
		t.Fatalf("empty position must not be emitted: %#v", extra)
	default:
	}
}

func TestOnDDLEmitsStatementEndPosition(t *testing.T) {
	ch := make(chan Commit, 2)
	h := &handler{
		ch: ch, db: addColumnDB(),
		include: compilePatterns(`db\..*`), allow: map[string]struct{}{"db.orders": {}},
	}
	query := &replication.QueryEvent{
		Schema: []byte("db"),
		Query:  []byte("ALTER TABLE `db`.`orders` ADD COLUMN `note` varchar(32) NULL"),
	}
	next := mysql.Position{Name: "mysql-bin.000001", Pos: 420}
	if err := h.OnDDL(nil, next, query); err != nil {
		t.Fatal(err)
	}
	eventCommit := <-ch
	if eventCommit.Event == nil || eventCommit.Position != nil {
		t.Fatalf("event commit %#v", eventCommit)
	}
	posCommit := <-ch
	if posCommit.Event != nil || posCommit.Position == nil {
		t.Fatalf("position commit %#v", posCommit)
	}
	if posCommit.Position.File != next.Name || posCommit.Position.Pos != next.Pos || posCommit.Position.GTID != "" {
		t.Fatalf("position %#v", posCommit.Position)
	}
}

func TestOnDDLPositionKeepsTrackedGTID(t *testing.T) {
	ch := make(chan Commit, 2)
	set, err := mysql.ParseGTIDSet("mysql", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	if err != nil {
		t.Fatal(err)
	}
	h := &handler{
		ch: ch, db: addColumnDB(), trackGTID: true, gtid: set,
		include: compilePatterns(`db\..*`), allow: map[string]struct{}{"db.orders": {}},
	}
	query := &replication.QueryEvent{
		Schema: []byte("db"),
		Query:  []byte("ALTER TABLE `db`.`orders` ADD COLUMN `note` varchar(32) NULL"),
	}
	if err := h.OnDDL(nil, mysql.Position{Name: "mysql-bin.000001", Pos: 420}, query); err != nil {
		t.Fatal(err)
	}
	<-ch
	posCommit := <-ch
	if posCommit.Position == nil || posCommit.Position.GTID != set.String() {
		t.Fatalf("position %#v", posCommit.Position)
	}
}

func TestOnDDLUncapturedEmitsEndPositionOnly(t *testing.T) {
	ch := make(chan Commit, 2)
	h := &handler{ch: ch, include: compilePatterns(`other\..*`)}
	query := &replication.QueryEvent{
		Schema: []byte("db"),
		Query:  []byte("ALTER TABLE `db`.`orders` ADD COLUMN `note` varchar(32) NULL"),
	}
	if err := h.OnDDL(nil, mysql.Position{Name: "mysql-bin.000001", Pos: 88}, query); err != nil {
		t.Fatal(err)
	}
	commit := <-ch
	if commit.Event != nil || commit.Position == nil || commit.Position.Pos != 88 {
		t.Fatalf("commit %#v", commit)
	}
	select {
	case extra := <-ch:
		t.Fatalf("extra commit %#v", extra)
	default:
	}
}

func TestOnDDLCatalogErrorDoesNotEmitPosition(t *testing.T) {
	ch := make(chan Commit, 2)
	h := &handler{
		ch:      ch,
		include: compilePatterns(`db\..*`), allow: map[string]struct{}{"db.orders": {}},
	}
	query := &replication.QueryEvent{
		Schema: []byte("db"),
		Query:  []byte("ALTER TABLE `db`.`orders` ADD COLUMN `note` varchar(32) NULL"),
	}
	err := h.OnDDL(nil, mysql.Position{Name: "mysql-bin.000001", Pos: 100}, query)
	if err == nil || !strings.Contains(err.Error(), "no catalog connection") {
		t.Fatalf("err=%v", err)
	}
	select {
	case commit := <-ch:
		t.Fatalf("emitted %#v", commit)
	default:
	}
}

// TestSkippedDDLAdvancesPosition covers source.scan.skip.events. The statement
// is dropped on purpose, so no event is emitted; the position must still move,
// because a skipped DDL has no following XID and the checkpoint would otherwise
// stay on the previous transaction forever.
func TestSkippedDDLAdvancesPosition(t *testing.T) {
	ch := make(chan Commit, 2)
	h := &handler{
		ch: ch, db: addColumnDB(), skipEvents: 1,
		include: compilePatterns(`db\..*`), allow: map[string]struct{}{"db.orders": {}},
	}
	query := &replication.QueryEvent{
		Schema: []byte("db"),
		Query:  []byte("ALTER TABLE `db`.`orders` ADD COLUMN `extra` int NULL"),
	}
	if err := h.OnDDL(nil, mysql.Position{Name: "mysql-bin.000001", Pos: 900}, query); err != nil {
		t.Fatal(err)
	}
	commit := <-ch
	if commit.Event != nil {
		t.Fatalf("a skipped event must not be emitted: %#v", commit)
	}
	if commit.Position == nil || commit.Position.Pos != 900 {
		t.Fatalf("the skipped statement must still advance the position: %#v", commit)
	}
	select {
	case extra := <-ch:
		t.Fatalf("extra commit %#v", extra)
	default:
	}
}

// TestSkippedDDLKeepsTheCheckpointMoving proves the position of a skipped
// statement is the one a restart resumes from, so repeated restarts do not
// re-read the same statement.
func TestSkippedDDLKeepsTheCheckpointMoving(t *testing.T) {
	ch := make(chan Commit, 4)
	db := addColumnDB()
	db.tables[0].Columns = append(db.tables[0].Columns, schema.Column{Name: "note2", Type: "varchar(8)", Nullable: true})
	h := &handler{
		ch: ch, db: db, skipEvents: 1,
		include: compilePatterns(`db\..*`), allow: map[string]struct{}{"db.orders": {}},
	}
	skipped := &replication.QueryEvent{
		Schema: []byte("db"),
		Query:  []byte("ALTER TABLE `db`.`orders` ADD COLUMN `extra` int NULL"),
	}
	if err := h.OnDDL(nil, mysql.Position{Name: "mysql-bin.000001", Pos: 900}, skipped); err != nil {
		t.Fatal(err)
	}
	first := <-ch
	if first.Position == nil || first.Position.Pos != 900 {
		t.Fatalf("skipped statement position %#v", first.Position)
	}
	// The next statement runs normally, so its event and position follow.
	applied := &replication.QueryEvent{
		Schema: []byte("db"),
		Query:  []byte("ALTER TABLE `db`.`orders` ADD COLUMN `note2` varchar(8) NULL"),
	}
	if err := h.OnDDL(nil, mysql.Position{Name: "mysql-bin.000001", Pos: 1200}, applied); err != nil {
		t.Fatal(err)
	}
	event := <-ch
	if event.Event == nil {
		t.Fatalf("the second statement must be emitted: %#v", event)
	}
	pos := <-ch
	if pos.Position == nil || pos.Position.Pos != 1200 {
		t.Fatalf("second position %#v", pos.Position)
	}
}

// TestDDLPositionDoesNotFlushPendingRowsOfAnotherTable proves that a position
// published after a DDL cannot publish rows of a different table that are still
// buffered for a running snapshot. A DDL is implicitly committed, so no row of
// the same transaction is buffered when it arrives; anything pending belongs to
// a later statement and must wait for its own XID.
func TestDDLPositionDoesNotFlushPendingRowsOfAnotherTable(t *testing.T) {
	spill := openTestSpill(t, "")
	defer spill.Close()
	db := addColumnDB()
	stream := &memStream{events: []Commit{
		// A row for the table being snapshotted: the spill route buffers it.
		{Event: &event.Event{Op: event.OpCreate, Database: "db", Table: "pending", SourceDatabase: "db", SourceTable: "pending", After: map[string]any{"id": "1"}}},
		// A DDL on a captured table, carrying its own end position.
		{Event: &event.Event{Op: event.OpDDL, Database: "db", Table: "orders", SourceDatabase: "db", SourceTable: "orders", DDL: "ALTER TABLE `db`.`orders` ADD COLUMN `note` int NULL"}},
		{Position: &checkpoint.Position{File: "mysql-bin.000001", Pos: 1500}},
	}}
	cap := &Capture{Config: definition.Source{SchemaChange: true}, DB: db, Stream: stream, Spill: spill}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out := make(chan source.Message, 8)
	done := make(chan error, 1)
	go func() {
		done <- cap.binlogFrom(ctx, out, checkpoint.Position{File: "mysql-bin.000001", Pos: 400},
			map[string]bool{"db.pending": true, "db.orders": false})
		close(out)
	}()
	var publishedPending, publishedOrders int
	var lastPos uint32
	for msg := range out {
		if msg.Event != nil {
			switch msg.Event.Table {
			case "pending":
				publishedPending++
			case "orders":
				publishedOrders++
			}
		}
		if msg.Position != nil {
			lastPos = msg.Position.Pos
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if publishedPending != 0 {
		t.Fatalf("a buffered row was published before its own XID: %d", publishedPending)
	}
	if publishedOrders != 1 {
		t.Fatalf("the captured DDL must still be emitted: %d", publishedOrders)
	}
	// The buffered row is durable in the spill, under the XID that carried it.
	last, has, err := spill.LastPosition("db.pending")
	if err != nil {
		t.Fatal(err)
	}
	if !has || last.Pos != 1500 {
		t.Fatalf("spilled position %#v (has=%v)", last, has)
	}
	if lastPos != 1500 {
		t.Fatalf("last published position %d", lastPos)
	}
}
