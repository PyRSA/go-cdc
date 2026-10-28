package v7

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	esclient "github.com/PyRSA/go-cdc/connector/sink/elasticsearch/client"
)

// newTestClient starts a fake Elasticsearch 7 endpoint. The v7 SDK validates
// the product once per client with its own GET / request, so that call is
// answered here and never reaches the test handler.
func newTestClient(t *testing.T, handler http.HandlerFunc) (*Client, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/" {
			writeJSON(w, http.StatusOK,
				`{"name":"node","version":{"number":"7.17.24","build_flavor":"default"},"tagline":"You Know, for Search"}`)
			return
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	client, err := New(Config{Addresses: []string{server.URL}, RequestTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, server
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

func TestNewRequiresAddresses(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("want error for empty addresses")
	}
}

func TestPing(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead || r.URL.Path != "/" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		writeJSON(w, http.StatusOK, `{}`)
	})
	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

// /ping is a HEAD request, so a failing status cannot carry a response body:
// only the status is available for diagnostics.
func TestPingReportsStatus(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusUnauthorized,
			`{"error":{"type":"security_exception","reason":"missing authentication credentials"},"status":401}`)
	})
	err := client.Ping(context.Background())
	if err == nil {
		t.Fatal("want error")
	}
	var esErr *esclient.Error
	if !errors.As(err, &esErr) {
		t.Fatalf("want *client.Error, got %T", err)
	}
	if esErr.Status != http.StatusUnauthorized {
		t.Fatalf("err=%+v", esErr)
	}
	if strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "credential") {
		t.Fatalf("error leaks credentials: %v", err)
	}
}

// The SDK transport must not retry: the sink owns bounded retries. A 503 ping
// therefore reaches the cluster exactly once.
func TestSDKRetryDisabled(t *testing.T) {
	var calls int32
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		writeJSON(w, http.StatusServiceUnavailable, `{"error":{"type":"unavailable","reason":"down"},"status":503}`)
	})
	if err := client.Ping(context.Background()); err == nil {
		t.Fatal("want error")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("transport retried: %d requests, want 1", got)
	}
}

// The v7 client must not run in ES 8 compatibility mode: the sink targets an
// ES 7 cluster and no header may ask the server for the ES 8 API shape.
func TestNoCompatibilityHeader(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		for _, value := range r.Header.Values("Accept") {
			if strings.Contains(value, "compatible-with") {
				t.Errorf("Accept header requests compatibility mode: %q", value)
			}
		}
		for _, value := range r.Header.Values("Content-Type") {
			if strings.Contains(value, "compatible-with") {
				t.Errorf("Content-Type header requests compatibility mode: %q", value)
			}
		}
		writeJSON(w, http.StatusOK, `{}`)
	})
	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func TestIndexExists(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		wantExists bool
		wantErr    bool
	}{
		{"exists", http.StatusOK, true, false},
		{"missing", http.StatusNotFound, false, false},
		{"failure", http.StatusInternalServerError, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodHead || r.URL.Path != "/orders" {
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				}
				writeJSON(w, tc.status, `{}`)
			})
			exists, err := client.IndexExists(context.Background(), "orders")
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
			if exists != tc.wantExists {
				t.Fatalf("exists=%v want %v", exists, tc.wantExists)
			}
		})
	}
}

func TestCreateIndexSendsBody(t *testing.T) {
	var gotBody string
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		if r.Method != http.MethodPut || r.URL.Path != "/orders" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		writeJSON(w, http.StatusOK, `{"acknowledged":true}`)
	})
	body := json.RawMessage(`{"mappings":{"properties":{"id":{"type":"keyword"}}}}`)
	if err := client.CreateIndex(context.Background(), "orders", body); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if gotBody != string(body) {
		t.Fatalf("body=%s", gotBody)
	}
}

func TestCreateIndexReportsConflict(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusBadRequest,
			`{"error":{"type":"resource_already_exists_exception","reason":"index already exists"},"status":400}`)
	})
	err := client.CreateIndex(context.Background(), "orders", json.RawMessage(`{}`))
	if esclient.StatusOf(err) != http.StatusBadRequest {
		t.Fatalf("err=%v", err)
	}
	if !esclient.IsType(err, "resource_already_exists_exception") {
		t.Fatalf("err=%v, want the Elasticsearch error type", err)
	}
}

func TestGetMapping(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/orders/_mapping" {
			t.Errorf("path=%s", r.URL.Path)
		}
		writeJSON(w, http.StatusOK,
			`{"orders":{"mappings":{"dynamic":"strict","properties":{"id":{"type":"keyword"}}}}}`)
	})
	mappings, err := client.GetMapping(context.Background(), "orders")
	if err != nil {
		t.Fatalf("get mapping: %v", err)
	}
	if mappings["dynamic"] != "strict" {
		t.Fatalf("mappings=%v", mappings)
	}
	props, ok := mappings["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties=%T", mappings["properties"])
	}
	if _, ok := props["id"]; !ok {
		t.Fatalf("missing id property: %v", props)
	}
}

func TestBulkParsesEveryItem(t *testing.T) {
	var got string
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		got = string(raw)
		if r.URL.Path != "/_bulk" || r.Method != http.MethodPost {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		writeJSON(w, http.StatusOK, `{"took":3,"errors":true,"items":[
			{"index":{"_index":"orders","_id":"1","status":201}},
			{"delete":{"_index":"orders","_id":"2","status":200}},
			{"index":{"_index":"orders","_id":"3","status":400,"error":{"type":"document_parsing_exception","reason":"failed to parse field [amount]"}}}
		]}`)
	})
	ops := []esclient.BulkOperation{
		{Action: esclient.ActionIndex, Index: "orders", ID: "1", Document: []byte(`{"id":1}`)},
		{Action: esclient.ActionDelete, Index: "orders", ID: "2"},
		{Action: esclient.ActionIndex, Index: "orders", ID: "3", Document: []byte(`{"id":3}`)},
	}
	batch := mustBatch(t, ops)
	result, err := client.Bulk(context.Background(), batch)
	if err != nil {
		t.Fatalf("bulk: %v", err)
	}
	// The body on the wire is exactly the bytes the sink measured.
	if got != string(batch.Body) {
		t.Fatalf("sent body differs from the encoded batch:\n%s", got)
	}
	if len(result.Items) != 3 {
		t.Fatalf("items=%d", len(result.Items))
	}
	if !result.Items[0].OK() || !result.Items[1].OK() {
		t.Fatalf("items=%+v", result.Items)
	}
	bad := result.Items[2]
	if bad.OK() || bad.Status != 400 || bad.ErrType != "document_parsing_exception" || bad.ID != "3" {
		t.Fatalf("bad item=%+v", bad)
	}
	lines := strings.Split(strings.TrimSpace(got), "\n")
	if len(lines) != 5 {
		t.Fatalf("ndjson lines=%d: %q", len(lines), got)
	}
	var header map[string]map[string]string
	if err := json.Unmarshal([]byte(lines[0]), &header); err != nil {
		t.Fatalf("header %q: %v", lines[0], err)
	}
	if header["index"]["_index"] != "orders" || header["index"]["_id"] != "1" {
		t.Fatalf("header=%v", header)
	}
	if lines[1] != `{"id":1}` {
		t.Fatalf("document=%s", lines[1])
	}
	var deleteHeader map[string]map[string]string
	if err := json.Unmarshal([]byte(lines[2]), &deleteHeader); err != nil {
		t.Fatalf("delete header %q: %v", lines[2], err)
	}
	if deleteHeader["delete"]["_id"] != "2" {
		t.Fatalf("delete header=%v", deleteHeader)
	}
}

func TestBulkTopLevelFailure(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusTooManyRequests,
			`{"error":{"type":"es_rejected_execution_exception","reason":"queue full"},"status":429}`)
	})
	_, err := client.Bulk(context.Background(), mustBatch(t, []esclient.BulkOperation{
		{Action: esclient.ActionDelete, Index: "orders", ID: "1"},
	}))
	if esclient.StatusOf(err) != http.StatusTooManyRequests {
		t.Fatalf("err=%v", err)
	}
}

func TestBulkRejectsItemCountMismatch(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, `{"took":1,"errors":false,"items":[]}`)
	})
	_, err := client.Bulk(context.Background(), mustBatch(t, []esclient.BulkOperation{
		{Action: esclient.ActionDelete, Index: "orders", ID: "1"},
	}))
	if err == nil {
		t.Fatal("want error for item count mismatch")
	}
}

func TestBulkRejectsEmptyDocument(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("no request expected")
	})
	_, err := esclient.NewBulkBatch([]esclient.BulkOperation{
		{Action: esclient.ActionIndex, Index: "orders", ID: "1"},
	})
	if err == nil || !strings.Contains(err.Error(), "empty document") {
		t.Fatalf("encode error = %v, want an empty-document failure", err)
	}
	if _, err := client.Bulk(context.Background(), esclient.BulkBatch{}); err != nil {
		t.Fatalf("an empty batch must not reach the cluster: %v", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, `{}`)
	})
	if err := client.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func mustBatch(t *testing.T, ops []esclient.BulkOperation) esclient.BulkBatch {
	t.Helper()
	batch, err := esclient.NewBulkBatch(ops)
	if err != nil {
		t.Fatalf("encode batch: %v", err)
	}
	return batch
}
