package doris

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/composer/definition"
)

// fakeResult is one scripted answer to a query.
type fakeResult struct {
	columns []string
	rows    [][]driver.Value
	err     error
}

// fakeDriver answers QueryContext from a script so the SHOW LOAD paths can be
// exercised without a Doris FE. Only the driver interfaces database/sql needs
// for a read-only query are implemented.
type fakeDriver struct {
	mu      sync.Mutex
	queries []string
	respond func(query string) fakeResult
}

func (d *fakeDriver) Open(string) (driver.Conn, error) { return &fakeConn{d: d}, nil }

func (d *fakeDriver) recorded() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.queries...)
}

type fakeConn struct{ d *fakeDriver }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}

func (c *fakeConn) Close() error { return nil }

func (c *fakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

func (c *fakeConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.d.mu.Lock()
	c.d.queries = append(c.d.queries, query)
	respond := c.d.respond
	c.d.mu.Unlock()
	result := respond(query)
	if result.err != nil {
		return nil, result.err
	}
	return &fakeRows{columns: result.columns, rows: result.rows}, nil
}

type fakeRows struct {
	columns []string
	rows    [][]driver.Value
	at      int
}

func (r *fakeRows) Columns() []string { return r.columns }
func (r *fakeRows) Close() error      { return nil }

func (r *fakeRows) Next(dest []driver.Value) error {
	if r.at >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.at])
	r.at++
	return nil
}

// fakeDriverSeq keeps each registered driver name unique: sql.Register panics on
// a duplicate name and the registry is process-wide.
var fakeDriverSeq atomic.Int64

func newFakeSQLClient(t *testing.T, respond func(query string) fakeResult) (*sqlClient, *fakeDriver) {
	t.Helper()
	d := &fakeDriver{respond: respond}
	name := fmt.Sprintf("doris-fake-%d", fakeDriverSeq.Add(1))
	sql.Register(name, d)
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open fake sql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &sqlClient{db: db}, d
}

func loadResult(rows ...[3]string) fakeResult {
	out := make([][]driver.Value, len(rows))
	for i, row := range rows {
		out[i] = []driver.Value{row[0], row[1], row[2]}
	}
	return fakeResult{columns: []string{"Label", "State", "ErrorMsg"}, rows: out}
}

func TestShowLoadIgnoresRowsForOtherLabels(t *testing.T) {
	const label = "go-cdc_orders_1700000000000000000_1"
	// A build that ignores WHERE returns another job's FINISHED row first. Only
	// the row that names this label may decide the outcome.
	client, _ := newFakeSQLClient(t, func(query string) fakeResult {
		if strings.Contains(query, "WHERE") {
			return loadResult([3]string{"other_label", "FINISHED", ""})
		}
		return loadResult(
			[3]string{"other_label", "FINISHED", ""},
			[3]string{label, "RUNNING", ""},
		)
	})

	state, err := client.showLoad(context.Background(), "db", label)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if state.State != "RUNNING" {
		t.Fatalf("state=%q, want the row for this label", state.State)
	}
}

func TestShowLoadReturnsNotFoundWhenOnlyOtherLabels(t *testing.T) {
	const label = "go-cdc_orders_1700000000000000000_2"
	client, _ := newFakeSQLClient(t, func(query string) fakeResult {
		return loadResult([3]string{"other_label", "FINISHED", ""})
	})

	_, err := client.showLoad(context.Background(), "db", label)
	if !errors.Is(err, errLoadNotFound) {
		t.Fatalf("err=%v, want errLoadNotFound", err)
	}
}

func TestShowLoadUsesExactQueryWhenLabelMatches(t *testing.T) {
	const label = "go-cdc_orders_1700000000000000000_3"
	client, fake := newFakeSQLClient(t, func(string) fakeResult {
		return loadResult([3]string{label, "FINISHED", ""})
	})

	state, err := client.showLoad(context.Background(), "db", label)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if state.State != "FINISHED" {
		t.Fatalf("state=%q", state.State)
	}
	queries := fake.recorded()
	if len(queries) != 1 {
		t.Fatalf("a matching row must not trigger the fallback scan: %#v", queries)
	}
	if !strings.Contains(queries[0], "WHERE Label = '"+label+"'") {
		t.Fatalf("query %q", queries[0])
	}
}

func TestShowLoadMatchesLabelCaseInsensitively(t *testing.T) {
	const label = "go-cdc_orders_1700000000000000000_4"
	client, _ := newFakeSQLClient(t, func(string) fakeResult {
		return loadResult([3]string{strings.ToUpper(label), "CANCELLED", "broken"})
	})

	state, err := client.showLoad(context.Background(), "db", label)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if state.State != "CANCELLED" || state.ErrorMsg != "broken" {
		t.Fatalf("state=%#v", state)
	}
}

func TestShowLoadRejectsDuplicateLabelRows(t *testing.T) {
	const label = "go-cdc_orders_1700000000000000000_6"
	cases := []struct {
		name string
		rows [][3]string
	}{
		{"finished first", [][3]string{
			{label, "FINISHED", ""},
			{label, "RUNNING", ""},
		}},
		{"running first", [][3]string{
			{label, "RUNNING", ""},
			{label, "FINISHED", ""},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Two rows for one label: neither may be reported as this load's
			// state, least of all the FINISHED row of a stale job.
			client, _ := newFakeSQLClient(t, func(string) fakeResult { return loadResult(tc.rows...) })

			_, err := client.showLoad(context.Background(), "db", label)
			if err == nil {
				t.Fatal("duplicate rows for one label must not resolve to a success")
			}
			if errors.Is(err, errLoadNotFound) {
				t.Fatalf("an ambiguous label is not a missing label: %v", err)
			}
			if !strings.Contains(err.Error(), "more than one row") {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestShowLoadPropagatesQueryErrors(t *testing.T) {
	client, _ := newFakeSQLClient(t, func(string) fakeResult {
		return fakeResult{err: errors.New("fe unavailable")}
	})

	_, err := client.showLoad(context.Background(), "db", "go-cdc_orders_1700000000000000000_5")
	if err == nil || errors.Is(err, errLoadNotFound) {
		t.Fatalf("a query failure must not look like a missing label: %v", err)
	}
}

// TestSinkWriteTreatsAmbiguousLoadStateAsFatal runs the whole recovery path
// against the production SHOW LOAD parsing: a failed Stream Load response
// queries the label, two rows share it, and the batch must stop at once instead
// of being retried or reported as loaded.
func TestSinkWriteTreatsAmbiguousLoadStateAsFatal(t *testing.T) {
	var mu sync.Mutex
	var label string
	var requests int
	client, fake := newFakeSQLClient(t, func(string) fakeResult {
		mu.Lock()
		current := label
		mu.Unlock()
		return loadResult(
			[3]string{current, "FINISHED", ""},
			[3]string{current, "RUNNING", ""},
		)
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		label = r.Header.Get("label")
		mu.Unlock()
		http.Error(w, "backend down", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	s := testSink(t, server, client, definition.Sink{Type: "doris", MaxRetries: 3})
	s.cacheTable(schemaFromDesc("db", "orders", validTarget()))

	err := s.Write([]event.Event{createEvent(1, "a")})
	if err == nil {
		t.Fatal("an ambiguous label state must fail the batch")
	}
	if errors.Is(err, errLoadNotFound) {
		t.Fatalf("an ambiguous label is not a missing label: %v", err)
	}
	if !strings.Contains(err.Error(), "more than one row") {
		t.Fatalf("err=%v", err)
	}
	if requests != 1 {
		t.Fatalf("a fatal outcome must not re-PUT, got %d requests", requests)
	}
	if queries := fake.recorded(); len(queries) != 1 {
		t.Fatalf("the exact query already matched, no fallback scan expected: %#v", queries)
	}
}
