package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/sink/elasticsearch/client"
	esv8 "github.com/PyRSA/go-cdc/connector/sink/elasticsearch/client/v8"
)

// sinkWithBulkBudget registers typesTable() on a sink whose bulk budget is
// budget bytes, so Write and bulk can be exercised with a known limit. Extra
// mutators adjust the rest of the config before the sink is built.
func sinkWithBulkBudget(t *testing.T, budget int, mutate ...func(*definition.Sink)) (*Sink, *client.Fake) {
	t.Helper()
	s, fake := newTestSink(t, func(cfg *definition.Sink) {
		cfg.AutoCreate = true
		cfg.BulkMaxBytes = budget
		for _, apply := range mutate {
			apply(cfg)
		}
	})
	if err := s.CreateTable(typesTable()); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	return s, fake
}

// opOfEncodedSize returns an index operation whose encoded NDJSON form is
// exactly size bytes, which makes a byte-boundary assertion exact instead of
// approximate. The document is the only part a caller can size; the action line
// and the two newlines are fixed by the bulk protocol.
func opOfEncodedSize(t *testing.T, id string, size int) client.BulkOperation {
	t.Helper()
	op := client.BulkOperation{Action: client.ActionIndex, Index: "shop.types", ID: id, Document: []byte(`{}`)}
	encoded, err := client.EncodeBulkOperation(op)
	if err != nil {
		t.Fatalf("encode operation: %v", err)
	}
	headerLen := len(encoded) - len(op.Document) - 1 // the action line, its newline excluded
	docLen := size - headerLen - 1                   // the document line's newline
	if docLen < 2 {
		t.Fatalf("size %d is smaller than the action line alone", size)
	}
	op.Document = []byte(`"` + strings.Repeat("x", docLen-2) + `"`)
	encoded, err = client.EncodeBulkOperation(op)
	if err != nil {
		t.Fatalf("encode sized operation: %v", err)
	}
	if len(encoded) != size {
		t.Fatalf("encoded size = %d, want %d", len(encoded), size)
	}
	return op
}

// flattenOps returns the operations of every request in send order.
func flattenOps(calls []client.BulkBatch) []client.BulkOperation {
	var out []client.BulkOperation
	for _, call := range calls {
		out = append(out, call.Ops...)
	}
	return out
}

// assertOpsInOrder checks that the operations reached the cluster exactly once,
// in the order the sink received them, with their documents unchanged.
func assertOpsInOrder(t *testing.T, got, want []client.BulkOperation) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("sent %d operation(s), want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Action != want[i].Action || got[i].Index != want[i].Index || got[i].ID != want[i].ID {
			t.Fatalf("operation %d = %s %s/%s, want %s %s/%s",
				i+1, got[i].Action, got[i].Index, got[i].ID, want[i].Action, want[i].Index, want[i].ID)
		}
		if !bytes.Equal(got[i].Document, want[i].Document) {
			t.Fatalf("operation %d (%s) document changed", i+1, got[i].ID)
		}
	}
}

// Small documents must keep the pre-existing behaviour: the sink does not
// re-batch them, so write-batch-size stays the only count limit.
func TestBulkKeepsCountBatchingForSmallDocuments(t *testing.T) {
	const budget = 5 << 20
	s, fake := sinkWithBulkBudget(t, budget)
	events := make([]event.Event, 0, 200)
	for i := 1; i <= 200; i++ {
		events = append(events, typesEvent(event.OpCreate, uint64(i)))
	}
	if err := s.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(fake.BulkCalls) != 1 {
		t.Fatalf("bulk requests = %d, want 1", len(fake.BulkCalls))
	}
	if got := len(fake.BulkCalls[0].Ops); got != len(events) {
		t.Fatalf("request carries %d operation(s), want %d", got, len(events))
	}
	if size := len(fake.BulkCalls[0].Body); size > budget {
		t.Fatalf("request body = %d bytes, over the %d-byte budget", size, budget)
	}
}

// Large documents must be split on the encoded bytes, and every byte of the
// input must still reach the cluster exactly once and in order.
func TestBulkSplitsLargeDocumentsByEncodedBytes(t *testing.T) {
	const (
		budget = 1 << 20
		docLen = 400 << 10
		count  = 10
	)
	s, fake := sinkWithBulkBudget(t, budget)
	ops := make([]client.BulkOperation, 0, count)
	for i := 0; i < count; i++ {
		ops = append(ops, opOfEncodedSize(t, fmt.Sprintf("doc-%02d", i), docLen))
	}
	if err := s.bulk(context.Background(), ops); err != nil {
		t.Fatalf("bulk: %v", err)
	}
	if len(fake.BulkCalls) < 2 {
		t.Fatalf("bulk requests = %d, want the batch split", len(fake.BulkCalls))
	}
	for i, call := range fake.BulkCalls {
		if len(call.Ops) == 0 {
			t.Fatalf("request %d is empty", i+1)
		}
		if size := len(call.Body); size > budget {
			t.Fatalf("request %d body = %d bytes, over the %d-byte budget", i+1, size, budget)
		}
	}
	assertOpsInOrder(t, flattenOps(fake.BulkCalls), ops)
}

// The budget is a ceiling on the encoded body: a body that fills it exactly is
// allowed, one byte more is not, and an operation that cannot fit fails before
// anything is sent.
func TestBulkByteBudgetBoundaries(t *testing.T) {
	const budget = 64 << 10
	cases := []struct {
		name      string
		sizes     []int
		wantCalls int
		wantErr   bool
	}{
		{"two operations fill the budget exactly", []int{budget / 2, budget / 2}, 1, false},
		{"one byte over the budget", []int{budget / 2, budget/2 + 1}, 2, false},
		{"one operation equals the budget", []int{budget}, 1, false},
		{"one operation is one byte over the budget", []int{budget + 1}, 0, true},
		{"the next operation does not fit", []int{budget / 4, 3*budget/4 + 1}, 2, false},
		{"a filled batch cannot take a tail", []int{budget / 2, budget / 2, 1 << 10}, 2, false},
		{"five operations span three requests", []int{budget / 2, budget / 2, budget / 2, budget / 2, budget / 2}, 3, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, fake := sinkWithBulkBudget(t, budget)
			ops := make([]client.BulkOperation, 0, len(tc.sizes))
			for i, size := range tc.sizes {
				ops = append(ops, opOfEncodedSize(t, fmt.Sprintf("op-%d", i), size))
			}
			err := s.bulk(context.Background(), ops)
			if tc.wantErr {
				if err == nil {
					t.Fatal("want an error for an operation over the budget")
				}
				for _, want := range []string{fmt.Sprintf("%d bytes", budget+1), fmt.Sprintf("%d-byte", budget)} {
					if !strings.Contains(err.Error(), want) {
						t.Fatalf("error %q does not report %q", err, want)
					}
				}
				if len(fake.BulkCalls) != 0 {
					t.Fatalf("an oversized operation still sent %d request(s)", len(fake.BulkCalls))
				}
				return
			}
			if err != nil {
				t.Fatalf("bulk: %v", err)
			}
			if len(fake.BulkCalls) != tc.wantCalls {
				t.Fatalf("bulk requests = %d, want %d", len(fake.BulkCalls), tc.wantCalls)
			}
			for i, call := range fake.BulkCalls {
				if len(call.Ops) == 0 {
					t.Fatalf("request %d is empty", i+1)
				}
				if size := len(call.Body); size > budget {
					t.Fatalf("request %d body = %d bytes, over the %d-byte budget", i+1, size, budget)
				}
			}
			assertOpsInOrder(t, flattenOps(fake.BulkCalls), ops)
		})
	}
}

// A 413 is permanent: the same oversized request cannot succeed, so the sink
// must report it once instead of burning its retries.
func TestBulkDoesNotRetryHTTP413(t *testing.T) {
	s, fake := sinkWithBulkBudget(t, 5<<20)
	calls := 0
	fake.BulkHook = func(client.BulkBatch) (client.BulkResult, error) {
		calls++
		return client.BulkResult{}, &client.Error{
			Op: "bulk", Status: http.StatusRequestEntityTooLarge, Reason: http.StatusText(http.StatusRequestEntityTooLarge),
		}
	}
	err := s.Write([]event.Event{typesEvent(event.OpCreate, 1)})
	if err == nil || !strings.Contains(err.Error(), "status 413") {
		t.Fatalf("error = %v, want the HTTP 413 status", err)
	}
	if !strings.Contains(err.Error(), "attempt 1/3") {
		t.Fatalf("error %q hides the attempt count", err)
	}
	if calls != 1 {
		t.Fatalf("bulk calls = %d, want 1", calls)
	}
}

// A failure after a successful request must name the request it came from, and
// the requests after it must not be sent.
func TestBulkNamesTheFailingRequest(t *testing.T) {
	const budget = 8 << 10
	s, fake := sinkWithBulkBudget(t, budget)
	ops := make([]client.BulkOperation, 0, 3)
	for i := 0; i < 3; i++ {
		ops = append(ops, opOfEncodedSize(t, fmt.Sprintf("op-%d", i), 3<<10))
	}
	calls := 0
	fake.BulkHook = func(batch client.BulkBatch) (client.BulkResult, error) {
		calls++
		if calls == 2 {
			return client.BulkResult{}, &client.Error{Op: "bulk", Status: http.StatusRequestEntityTooLarge, Reason: "too large"}
		}
		return client.BulkResult{Items: okItems(batch.Ops)}, nil
	}
	err := s.bulk(context.Background(), ops)
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "request 2/2") || !strings.Contains(err.Error(), "status 413") {
		t.Fatalf("error = %v, want the failing request and status", err)
	}
	if calls != 2 {
		t.Fatalf("bulk calls = %d, want 2", calls)
	}
}

// Retries stay inside the request that failed: only its failed items are
// resent, and the remaining requests keep their order.
func TestBulkRetriesStayWithinTheRequest(t *testing.T) {
	const budget = 8 << 10
	s, fake := sinkWithBulkBudget(t, budget)
	ops := make([]client.BulkOperation, 0, 4)
	for i := 0; i < 4; i++ {
		ops = append(ops, opOfEncodedSize(t, fmt.Sprintf("op-%d", i), 3<<10))
	}
	var attempts = map[string]int{}
	fake.BulkHook = func(batch client.BulkBatch) (client.BulkResult, error) {
		items := okItems(batch.Ops)
		for i, op := range batch.Ops {
			attempts[op.ID]++
			if attempts[op.ID] == 1 && i == len(batch.Ops)-1 {
				items[i].Status = http.StatusTooManyRequests
				items[i].ErrType = "es_rejected_execution_exception"
			}
		}
		return client.BulkResult{Items: items}, nil
	}
	if err := s.bulk(context.Background(), ops); err != nil {
		t.Fatalf("bulk: %v", err)
	}
	// Each request is retried on its own: only its failed item is resent, and
	// the next request is untouched.
	wantCalls := [][]client.BulkOperation{
		ops[0:2],
		ops[1:2],
		ops[2:4],
		ops[3:4],
	}
	if len(fake.BulkCalls) != len(wantCalls) {
		t.Fatalf("bulk calls = %d, want %d", len(fake.BulkCalls), len(wantCalls))
	}
	for i, want := range wantCalls {
		assertOpsInOrder(t, fake.BulkCalls[i].Ops, want)
		if size := len(fake.BulkCalls[i].Body); size > budget {
			t.Fatalf("request %d body = %d bytes, over the %d-byte budget", i+1, size, budget)
		}
	}
}

// newHTTPBulkSink points the real v8 adapter at an httptest server so the test
// observes the bytes that would reach Elasticsearch. Registration still runs
// against the fake; only the bulk path goes over HTTP.
func newHTTPBulkSink(t *testing.T, handler http.HandlerFunc, budget int, mutate ...func(*definition.Sink)) *Sink {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	s, _ := sinkWithBulkBudget(t, budget, mutate...)
	api, err := esv8.New(esv8.Config{Addresses: []string{server.URL}, RequestTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("v8 client: %v", err)
	}
	t.Cleanup(func() { _ = api.Close() })
	s.SetClient(api)
	return s
}

// wireOperation is one operation as it appears in the received NDJSON body.
type wireOperation struct {
	action client.BulkAction
	index  string
	id     string
	doc    []byte
}

// parseBulkBody parses a bulk request body into its operations, rejecting any
// body that is not valid newline-terminated NDJSON. It returns an error instead
// of failing a test so the httptest handler can report the problem where the
// body was received.
func parseBulkBody(body []byte) ([]wireOperation, error) {
	if len(body) == 0 {
		return nil, fmt.Errorf("empty bulk request body")
	}
	if body[len(body)-1] != '\n' {
		return nil, fmt.Errorf("bulk body does not end with a newline: %q", tail(body, 60))
	}
	lines := strings.Split(strings.TrimSuffix(string(body), "\n"), "\n")
	var out []wireOperation
	for i := 0; i < len(lines); {
		var actions map[string]struct {
			Index string `json:"_index"`
			ID    string `json:"_id"`
		}
		if err := json.Unmarshal([]byte(lines[i]), &actions); err != nil || len(actions) != 1 {
			return nil, fmt.Errorf("line %d is not a bulk action: %q (err=%v)", i+1, tail([]byte(lines[i]), 120), err)
		}
		for name, meta := range actions {
			op := wireOperation{action: client.BulkAction(name), index: meta.Index, id: meta.ID}
			i++
			if op.action == client.ActionIndex {
				if i >= len(lines) {
					return nil, fmt.Errorf("action %s/%s has no document line", op.index, op.id)
				}
				if !json.Valid([]byte(lines[i])) {
					return nil, fmt.Errorf("document line of %s/%s is not JSON", op.index, op.id)
				}
				op.doc = []byte(lines[i])
				i++
			}
			out = append(out, op)
		}
	}
	return out, nil
}

func tail(body []byte, limit int) string {
	if len(body) <= limit {
		return string(body)
	}
	return "..." + string(body[len(body)-limit:])
}

func writeBulkAccepted(w http.ResponseWriter, count int) {
	statuses := make([]int, count)
	for i := range statuses {
		statuses[i] = http.StatusCreated
	}
	writeBulkStatuses(w, statuses)
}

// writeBulkStatuses answers one item per status, so a test can reject a single
// operation and observe what the sink retries.
func writeBulkStatuses(w http.ResponseWriter, statuses []int) {
	var b strings.Builder
	b.WriteString(`{"took":1,"errors":false,"items":[`)
	for i, status := range statuses {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"index":{"status":%d}}`, status)
	}
	b.WriteString(`]}`)
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, b.String())
}

// The HTTP path must carry legal NDJSON and keep every request inside the
// budget while sending the whole input exactly once, in order.
func TestWriteSplitsRequestsOverHTTP(t *testing.T) {
	const (
		budget  = 1 << 20
		docLen  = 300 << 10
		records = 10
	)
	var mu sync.Mutex
	var sizes []int
	var received []wireOperation
	s := newHTTPBulkSink(t, func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read bulk body: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		ops, err := parseBulkBody(raw)
		if err != nil {
			t.Errorf("invalid bulk request body: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		sizes = append(sizes, len(raw))
		received = append(received, ops...)
		mu.Unlock()
		writeBulkAccepted(w, len(ops))
	}, budget)

	payload := strings.Repeat("x", docLen)
	events := make([]event.Event, 0, records)
	for i := 1; i <= records; i++ {
		ev := typesEvent(event.OpCreate, uint64(i))
		ev.After["Note"] = payload
		events = append(events, ev)
	}
	if err := s.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	meta, ok := s.metaFor("shop.types")
	if !ok {
		t.Fatal("table plan is missing")
	}
	want := make([]string, 0, records)
	for _, ev := range events {
		id, err := meta.idFor(ev.After)
		if err != nil {
			t.Fatalf("_id: %v", err)
		}
		want = append(want, id)
	}

	if len(sizes) < 2 {
		t.Fatalf("received %d request(s), want the batch split", len(sizes))
	}
	for i, size := range sizes {
		if size > budget {
			t.Fatalf("request %d body = %d bytes, over the %d-byte budget", i+1, size, budget)
		}
	}
	if len(received) != len(want) {
		t.Fatalf("received %d operation(s), want %d", len(received), len(want))
	}
	for i, op := range received {
		if op.action != client.ActionIndex || op.index != "shop.types" {
			t.Fatalf("operation %d = %s %s, want index shop.types", i+1, op.action, op.index)
		}
		if op.id != want[i] {
			t.Fatalf("operation %d _id = %q, want %q", i+1, op.id, want[i])
		}
		if !strings.Contains(string(op.doc), payload) {
			t.Fatalf("operation %d lost the large field", i+1)
		}
	}
}

// A cluster-side 413 reaches the caller with its status, and the sink does not
// repeat the same oversized request.
func TestWriteReportsHTTP413FromTheCluster(t *testing.T) {
	var posts int32
	s := newHTTPBulkSink(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&posts, 1)
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		_, _ = io.WriteString(w, `{"error":{"type":"too_large","reason":"Request Entity Too Large"},"status":413}`)
	}, 5<<20)

	err := s.Write([]event.Event{typesEvent(event.OpCreate, 1)})
	if err == nil || !strings.Contains(err.Error(), "413") {
		t.Fatalf("error = %v, want the HTTP 413 status", err)
	}
	if got := atomic.LoadInt32(&posts); got != 1 {
		t.Fatalf("bulk requests = %d, want 1", got)
	}
}

// The load-test shape that failed in CI: 50 documents of about 2 MiB each with
// a 5 MiB budget. The sink must split them instead of building one ~100 MiB
// request.
func TestWriteSplitsFiftyLargeRecords(t *testing.T) {
	const (
		budget   = 5 << 20
		records  = 50
		fieldLen = 2 << 20
	)
	s, fake := sinkWithBulkBudget(t, budget)
	payload := strings.Repeat("x", fieldLen)
	events := make([]event.Event, 0, records)
	for i := 1; i <= records; i++ {
		ev := typesEvent(event.OpCreate, uint64(i))
		ev.After["Note"] = payload
		events = append(events, ev)
	}
	if err := s.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(fake.BulkCalls) < 2 {
		t.Fatalf("%d records of %d bytes were sent in %d request(s)", records, fieldLen, len(fake.BulkCalls))
	}
	meta, ok := s.metaFor("shop.types")
	if !ok {
		t.Fatal("table plan is missing")
	}
	var sent []string
	for i, call := range fake.BulkCalls {
		if len(call.Ops) == 0 {
			t.Fatalf("request %d is empty", i+1)
		}
		if size := len(call.Body); size > budget {
			t.Fatalf("request %d body = %d bytes, over the %d-byte budget", i+1, size, budget)
		}
		for _, op := range call.Ops {
			if len(op.Document) <= fieldLen {
				t.Fatalf("document %s does not carry the %d-byte field", op.ID, fieldLen)
			}
			sent = append(sent, op.ID)
		}
	}
	if len(sent) != records {
		t.Fatalf("sent %d operation(s), want %d", len(sent), records)
	}
	seen := make(map[string]struct{}, records)
	for _, ev := range events {
		id, err := meta.idFor(ev.After)
		if err != nil {
			t.Fatalf("_id: %v", err)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != records {
		t.Fatalf("expected %d distinct _ids, got %d", records, len(seen))
	}
	for i, id := range sent {
		if _, ok := seen[id]; !ok {
			t.Fatalf("operation %d has an unexpected _id %q", i+1, id)
		}
		delete(seen, id)
	}
	if len(seen) != 0 {
		t.Fatalf("%d document(s) never reached the cluster", len(seen))
	}
}

// Every request of a split Write has its own time budget: requests that each
// finish well inside request-timeout must not starve one another, even when
// their total exceeds what a single Write-wide budget used to allow.
func TestBulkGivesEveryRequestItsOwnTimeout(t *testing.T) {
	const (
		budget     = 8 << 10
		docSize    = 5 << 10 // one operation per request
		requestLag = 120 * time.Millisecond
		timeout    = 200 * time.Millisecond
		records    = 3
	)
	var posts int32
	s := newHTTPBulkSink(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&posts, 1)
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read bulk body: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		ops, err := parseBulkBody(raw)
		if err != nil {
			t.Errorf("invalid bulk request body: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		time.Sleep(requestLag)
		writeBulkAccepted(w, len(ops))
	}, budget, func(cfg *definition.Sink) {
		cfg.RequestTimeout = timeout
		cfg.MaxRetries = 0
	})

	ops := make([]client.BulkOperation, 0, records)
	for i := 0; i < records; i++ {
		ops = append(ops, opOfEncodedSize(t, fmt.Sprintf("op-%d", i), docSize))
	}
	start := time.Now()
	if err := s.bulk(context.Background(), ops); err != nil {
		t.Fatalf("bulk: %v (every request stayed inside request-timeout)", err)
	}
	// The total time proves the requests really overran one Write-wide timeout.
	if elapsed := time.Since(start); elapsed <= timeout {
		t.Fatalf("bulk took %v, want more than the %v per-request timeout so the case is exercised", elapsed, timeout)
	}
	if got := atomic.LoadInt32(&posts); got != records {
		t.Fatalf("bulk requests = %d, want %d", got, records)
	}
}

// A retry resends only the failed operations, and the retried body still honors
// the byte budget; both are observed over real HTTP.
func TestWriteRetryStaysWithinTheByteBudget(t *testing.T) {
	const (
		budget  = 1 << 20
		docSize = 400 << 10
		records = 4
	)
	var mu sync.Mutex
	var sizes []int
	var ids []string
	s := newHTTPBulkSink(t, func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read bulk body: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		ops, err := parseBulkBody(raw)
		if err != nil {
			t.Errorf("invalid bulk request body: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		sizes = append(sizes, len(raw))
		first := len(sizes) == 1
		for _, op := range ops {
			ids = append(ids, op.id)
		}
		mu.Unlock()
		statuses := make([]int, len(ops))
		for i := range statuses {
			statuses[i] = http.StatusCreated
		}
		if first {
			statuses[0] = http.StatusTooManyRequests
		}
		writeBulkStatuses(w, statuses)
	}, budget)

	ops := make([]client.BulkOperation, 0, records)
	for i := 0; i < records; i++ {
		ops = append(ops, opOfEncodedSize(t, fmt.Sprintf("doc-%d", i), docSize))
	}
	if err := s.bulk(context.Background(), ops); err != nil {
		t.Fatalf("bulk: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(sizes) != 3 {
		t.Fatalf("bulk requests = %d, want 2 batches plus 1 retry", len(sizes))
	}
	for i, size := range sizes {
		if size > budget {
			t.Fatalf("request %d body = %d bytes, over the %d-byte budget", i+1, size, budget)
		}
	}
	want := []string{ops[0].ID, ops[1].ID, ops[0].ID, ops[2].ID, ops[3].ID}
	if len(ids) != len(want) {
		t.Fatalf("received %d operation(s), want %d", len(ids), len(want))
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("operation %d = %q, want %q", i+1, ids[i], want[i])
		}
	}
}
