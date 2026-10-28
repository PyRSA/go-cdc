package postgresql

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	dorissink "github.com/PyRSA/go-cdc/connector/sink/doris"
	kafkasink "github.com/PyRSA/go-cdc/connector/sink/kafka"
	mysqlsink "github.com/PyRSA/go-cdc/connector/sink/mysql"
	"github.com/PyRSA/go-cdc/connector/sink/stdout"
)

// fakeDescriber stands in for the MySQL catalogue handle the composition root
// injects. It records every call so a test can prove the sink asked the source
// instead of inventing a definition.
type fakeDescriber struct {
	mu    sync.Mutex
	calls []string
	table schema.Table
	err   error
}

func (f *fakeDescriber) DescribeTable(_ context.Context, database, table string) (schema.Table, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, database+"."+table)
	if f.err != nil {
		return schema.Table{}, f.err
	}
	return f.table, nil
}

// Close is not part of TableDescriber: the interface gives the sink no way to
// release the borrowed handle, so ownership cannot drift.
func (f *fakeDescriber) called() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func describedUsers() schema.Table {
	return schema.Table{
		Database: "app", Table: "users",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "note", Type: "varchar(32)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
}

func TestInjectedDescriberProvidesSourceStructure(t *testing.T) {
	describer := &fakeDescriber{table: describedUsers()}
	sink := newTestSink(&fakePool{}, 0)
	sink.SetTableDescriber(describer)

	table, err := sink.DescribeTable(context.Background(), "app", "users")
	if err != nil {
		t.Fatal(err)
	}
	if table.Table != "users" || len(table.Columns) != 2 || table.PrimaryKeys[0] != "id" {
		t.Fatalf("structure %#v", table)
	}
	if calls := describer.called(); len(calls) != 1 || calls[0] != "app.users" {
		t.Fatalf("calls %#v", calls)
	}
}

func TestDescribeTableWithoutDescriberFails(t *testing.T) {
	sink := newTestSink(&fakePool{}, 0)
	_, err := sink.DescribeTable(context.Background(), "app", "users")
	if err == nil {
		t.Fatal("a missing describer was accepted")
	}
	if !strings.Contains(err.Error(), "no source catalogue") || !strings.Contains(err.Error(), "app.users") {
		t.Fatalf("error %v", err)
	}
}

// TestDescribeTableReportsSourceFailure pins the no-fallback rule: a read that
// fails is reported with its cause, rather than being papered over with a
// definition guessed from MySQL DDL text.
func TestDescribeTableReportsSourceFailure(t *testing.T) {
	describer := &fakeDescriber{err: errors.New("context deadline exceeded")}
	sink := newTestSink(&fakePool{}, 0)
	sink.SetTableDescriber(describer)

	_, err := sink.DescribeTable(context.Background(), "app", "users")
	if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("error %v", err)
	}
	if !strings.HasPrefix(err.Error(), "sink.postgresql: describe app.users:") {
		t.Fatalf("error must name the sink, the operation and the table: %v", err)
	}
}

// TestOpenKeepsInjectedDescriber covers the composition-root order: the sink is
// opened first and the catalogue is injected afterwards, so Open must not reset
// what it was given.
func TestOpenKeepsInjectedDescriber(t *testing.T) {
	describer := &fakeDescriber{table: describedUsers()}
	p := &fakePool{}
	sink := newTestSink(p, 0)
	sink.SetTableDescriber(describer)
	if err := sink.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.pings != 1 {
		t.Fatalf("pings %d", p.pings)
	}
	if _, err := sink.DescribeTable(context.Background(), "app", "users"); err != nil {
		t.Fatalf("Open dropped the injected catalogue: %v", err)
	}
}

// TestCloseDoesNotReleaseBorrowedDescriber covers the ownership rule: the sink
// closes its own pool and leaves the source handle alone, so the capture stays
// the only owner of the connection it opened.
func TestCloseDoesNotReleaseBorrowedDescriber(t *testing.T) {
	describer := &fakeDescriber{table: describedUsers()}
	p := &fakePool{}
	sink := newTestSink(p, 0)
	sink.SetTableDescriber(describer)
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	if !p.closed {
		t.Fatal("the sink did not close its own pool")
	}
	if _, err := sink.DescribeTable(context.Background(), "app", "users"); err != nil {
		t.Fatalf("the borrowed catalogue must outlive the sink: %v", err)
	}
}

// TestNilDescriberLeavesDMLUnchanged is the PR1 compatibility guard: a process
// that never injects a catalogue still writes rows exactly as before.
func TestNilDescriberLeavesDMLUnchanged(t *testing.T) {
	p := &fakePool{}
	sink := newTestSink(p, 0)
	sink.SetTableDescriber(nil)
	if err := sink.Write([]event.Event{createEvent()}); err != nil {
		t.Fatal(err)
	}
	if p.begins != 1 || !p.txs[0].committed {
		t.Fatalf("begins %d committed %v", p.begins, p.txs[0].committed)
	}
}

// TestOtherSinksCannotTakeTheDescriber is the isolation guard: the composition
// root injects through a type assertion, so a sink that is not this one can
// never be handed the source catalogue.
func TestOtherSinksCannotTakeTheDescriber(t *testing.T) {
	type injectable interface {
		SetTableDescriber(TableDescriber)
	}
	if _, ok := any(&Sink{}).(injectable); !ok {
		t.Fatal("the PostgreSQL sink must accept the catalogue")
	}
	for _, sk := range []any{
		&stdout.Sink{},
		&mysqlsink.Sink{},
		&kafkasink.Sink{},
		&dorissink.Sink{},
	} {
		if _, ok := sk.(injectable); ok {
			t.Fatalf("%T accepts a source catalogue", sk)
		}
	}
}

// TestSetTableDescriberIsSafeWhileDescribing exercises the lock that guards the
// field: replacing the catalogue never races a read of it.
func TestSetTableDescriberIsSafeWhileDescribing(t *testing.T) {
	sink := newTestSink(&fakePool{}, 0)
	sink.SetTableDescriber(&fakeDescriber{table: describedUsers()})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if _, err := sink.DescribeTable(context.Background(), "app", "users"); err != nil {
					t.Errorf("describe: %v", err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 50; j++ {
			sink.SetTableDescriber(&fakeDescriber{table: describedUsers()})
		}
	}()
	wg.Wait()
}
