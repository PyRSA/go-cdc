package doris

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/composer/definition"
)

func newTestHTTP(t *testing.T, server *httptest.Server) *httpClient {
	t.Helper()
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(base.Port())
	if err != nil {
		t.Fatal(err)
	}
	// Use the production constructor so redirect and auth handling are under test.
	client, err := newHTTPClient(definition.Sink{
		Hostname: base.Hostname(), Port: port, Username: "root", Password: "secret",
		RequestTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestStreamLoadRequest(t *testing.T) {
	var method, path, body, label, format, byLine, strict, filter, contentType, timeout string
	var authOK bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		user, pass, ok := r.BasicAuth()
		authOK = ok && user == "root" && pass == "secret"
		raw, _ := io.ReadAll(r.Body)
		body = string(raw)
		label = r.Header.Get("label")
		format = r.Header.Get("format")
		byLine = r.Header.Get("read_json_by_line")
		strict = r.Header.Get("strict_mode")
		filter = r.Header.Get("max_filter_ratio")
		timeout = r.Header.Get("timeout")
		contentType = r.Header.Get("Content-Type")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"Status":"Success","Label":"l1","NumberTotalRows":1,"NumberLoadedRows":1}`)
	}))
	defer server.Close()

	client := newTestHTTP(t, server)
	resp, err := client.streamLoad(t.Context(), "db", "orders", "l1", []byte(`{"id":1}`+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != statusSuccess || resp.Label != "l1" {
		t.Fatalf("resp %#v", resp)
	}
	if method != http.MethodPut || path != "/api/db/orders/_stream_load" {
		t.Fatalf("method=%s path=%s", method, path)
	}
	if !authOK {
		t.Fatal("basic auth missing")
	}
	if body != `{"id":1}`+"\n" {
		t.Fatalf("body %q", body)
	}
	if label != "l1" || format != "json" || byLine != "true" || strict != "true" || filter != "0" {
		t.Fatalf("headers label=%s format=%s byLine=%s strict=%s filter=%s", label, format, byLine, strict, filter)
	}
	if contentType != "application/json" {
		t.Fatalf("content-type %s", contentType)
	}
	// The Doris-side budget stays request-timeout; only the client deadline is
	// larger (request-timeout + loadTimeoutGrace).
	if timeout != "5" {
		t.Fatalf("timeout header %q want 5", timeout)
	}
}

// TestClientDeadlineOutlastsDorisTimeout pins the separation between the Doris
// Stream Load budget and the HTTP deadline: Doris answers only after its own
// timeout expires, so an equal client deadline would cancel the request exactly
// when the verdict is written.
func TestClientDeadlineOutlastsDorisTimeout(t *testing.T) {
	client, err := newHTTPClient(definition.Sink{
		Hostname: "127.0.0.1", Port: 8030, Username: "root", RequestTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if client.timeout != 30*time.Second {
		t.Fatalf("doris-side timeout=%s want 30s", client.timeout)
	}
	want := 30*time.Second + loadTimeoutGrace
	if client.client.Timeout != want {
		t.Fatalf("client deadline=%s want %s", client.client.Timeout, want)
	}
	if client.client.Timeout <= client.timeout {
		t.Fatalf("client deadline %s must exceed the Doris timeout %s", client.client.Timeout, client.timeout)
	}
}

func TestTimeoutHeaderRoundsSubSecond(t *testing.T) {
	// Truncating 1500ms would send 1s (shorter than requested) and 500ms would
	// send 0, which Doris rejects before running the load.
	cases := []struct {
		in   time.Duration
		want int
	}{
		{30 * time.Second, 30},
		{1500 * time.Millisecond, 2},
		{500 * time.Millisecond, 1},
		{0, 1},
	}
	for _, tc := range cases {
		if got := secondsRoundedUp(tc.in); got != tc.want {
			t.Fatalf("%s -> %d want %d", tc.in, got, tc.want)
		}
	}
}

func TestStreamLoadHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "backend down", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := newTestHTTP(t, server)
	_, err := client.streamLoad(t.Context(), "db", "orders", "l1", []byte("{}\n"))
	var status *httpStatusError
	if !errors.As(err, &status) || status.Code != http.StatusServiceUnavailable {
		t.Fatalf("err=%v", err)
	}
}

func TestStreamLoadInvalidJSONIsUnknown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "not json")
	}))
	defer server.Close()

	client := newTestHTTP(t, server)
	_, err := client.streamLoad(t.Context(), "db", "orders", "l1", []byte("{}\n"))
	var unknown *unknownResultError
	if !errors.As(err, &unknown) {
		t.Fatalf("err=%v", err)
	}
}

func TestStreamLoadRedirectKeepsAuthorization(t *testing.T) {
	var redirected bool
	var redirectedAuth bool
	var redirectedBody string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected = true
		user, pass, ok := r.BasicAuth()
		redirectedAuth = ok && user == "root" && pass == "secret"
		raw, _ := io.ReadAll(r.Body)
		redirectedBody = string(raw)
		_, _ = io.WriteString(w, `{"Status":"Success","Label":"l1","NumberTotalRows":0}`)
	}))
	defer target.Close()

	fe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer fe.Close()

	client := newTestHTTP(t, fe)
	resp, err := client.streamLoad(t.Context(), "db", "orders", "l1", []byte(`{"id":1}`+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != statusSuccess {
		t.Fatalf("resp %#v", resp)
	}
	if !redirected || !redirectedAuth {
		t.Fatalf("redirected=%v auth=%v", redirected, redirectedAuth)
	}
	if redirectedBody != `{"id":1}`+"\n" {
		t.Fatalf("redirected body %q", redirectedBody)
	}
}

func TestStreamLoadRedirectRejectsUnsafeScheme(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "ftp://example.invalid/upload", http.StatusTemporaryRedirect)
	}))
	defer server.Close()

	client := newTestHTTP(t, server)
	_, err := client.streamLoad(t.Context(), "db", "orders", "l1", []byte("{}\n"))
	if err == nil || !strings.Contains(err.Error(), "unsafe redirect scheme") {
		t.Fatalf("err=%v", err)
	}
}

func TestStreamLoadRedirectRejectsPublicHostAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://example.invalid/upload", http.StatusTemporaryRedirect)
	}))
	defer server.Close()

	client := newTestHTTP(t, server)
	_, err := client.streamLoad(t.Context(), "db", "orders", "l1", []byte("{}\n"))
	if err == nil || !strings.Contains(err.Error(), "refusing to forward credentials") {
		t.Fatalf("err=%v", err)
	}
}

func TestRedirectAuthAllowed(t *testing.T) {
	cases := []struct {
		fe, redirect string
		want         bool
	}{
		{"doris-fe", "doris-fe", true},
		{"Doris-FE", "doris-fe", true},
		{"doris-fe", "127.0.0.1", true},
		{"doris-fe", "10.0.0.8", true},
		{"doris-fe", "192.168.1.2", true},
		{"doris-fe", "example.com", false},
		{"doris-fe", "8.8.8.8", false},
		{"", "127.0.0.1", true},
		{"doris-fe", "", false},
	}
	for _, tc := range cases {
		if got := redirectAuthAllowed(tc.fe, tc.redirect); got != tc.want {
			t.Fatalf("fe=%q redirect=%q got %v want %v", tc.fe, tc.redirect, got, tc.want)
		}
	}
}

func TestStreamLoadStopsAfterTooManyRedirects(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, server.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer server.Close()

	client := newTestHTTP(t, server)
	_, err := client.streamLoad(t.Context(), "db", "orders", "l1", []byte("{}\n"))
	if err == nil || !strings.Contains(err.Error(), "too many redirects") {
		t.Fatalf("err=%v", err)
	}
}
