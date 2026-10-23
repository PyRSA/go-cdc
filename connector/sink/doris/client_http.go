package doris

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/PyRSA/go-cdc/composer/definition"
)

const maxResponseBytes = 1 << 20

// loadResponse is the subset of the Doris Stream Load JSON response that the
// state machine uses. Unknown Status / missing ExistingJobStatus go through
// SHOW LOAD; a Success response with bad row counts is fatal, not unknown.
type loadResponse struct {
	Status             string `json:"Status"`
	Message            string `json:"Message"`
	Label              string `json:"Label"`
	NumberTotalRows    int64  `json:"NumberTotalRows"`
	NumberLoadedRows   int64  `json:"NumberLoadedRows"`
	NumberFilteredRows int64  `json:"NumberFilteredRows"`
	ErrorURL           string `json:"ErrorURL"`
	ExistingJobStatus  string `json:"ExistingJobStatus"`
}

type httpStatusError struct {
	Code int
	Body string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("sink.doris: Stream Load HTTP %d: %s", e.Code, e.Body)
}

type unknownResultError struct {
	Err error
}

func (e *unknownResultError) Error() string {
	return "sink.doris: Stream Load result unknown: " + e.Err.Error()
}

func (e *unknownResultError) Unwrap() error { return e.Err }

// httpClient sends Stream Load requests with bounded redirects and timeouts.
type httpClient struct {
	client  *http.Client
	base    *url.URL
	user    string
	pass    string
	timeout time.Duration
}

func newHTTPClient(cfg definition.Sink) (*httpClient, error) {
	base := &url.URL{Scheme: "http", Host: net.JoinHostPort(cfg.Hostname, strconv.Itoa(cfg.Port))}
	feHost := strings.ToLower(cfg.Hostname)
	client := &http.Client{Timeout: cfg.RequestTimeout}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 {
			return errors.New("too many redirects")
		}
		if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
			return fmt.Errorf("unsafe redirect scheme %q", req.URL.Scheme)
		}
		// Go strips Authorization on a cross-host redirect. Doris FE redirects
		// Stream Load to a BE; re-attach credentials only when the Location host
		// is the FE hostname itself or a loopback/private/link-local IP. Non-IP
		// BE DNS names (for example doris-be-0) are refused — use IP Locations
		// or FE hostname equality so a public Location cannot exfiltrate auth.
		if !redirectAuthAllowed(feHost, req.URL.Hostname()) {
			return fmt.Errorf("sink.doris: refusing to forward credentials to redirect host %q", req.URL.Hostname())
		}
		req.SetBasicAuth(cfg.Username, cfg.Password)
		return nil
	}
	return &httpClient{
		client: client, base: base, user: cfg.Username, pass: cfg.Password,
		timeout: cfg.RequestTimeout,
	}, nil
}

// redirectAuthAllowed permits FE→FE (same hostname) and FE→BE when the BE
// Location host is a loopback, private, or link-local IP. Hostname-only BE
// addresses are rejected unless they equal the FE host.
func redirectAuthAllowed(feHost, redirectHost string) bool {
	redirectHost = strings.ToLower(strings.TrimSpace(redirectHost))
	feHost = strings.ToLower(strings.TrimSpace(feHost))
	if redirectHost == "" {
		return false
	}
	if feHost != "" && redirectHost == feHost {
		return true
	}
	ip := net.ParseIP(redirectHost)
	if ip == nil {
		return false
	}
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast()
}

func (c *httpClient) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	c.client.CloseIdleConnections()
	return nil
}

// streamLoad sends one JSON Lines body. A non-2xx response is an
// httpStatusError; an unparseable 2xx body is an unknownResultError so the
// caller queries the label instead of assuming success.
func (c *httpClient) streamLoad(ctx context.Context, database, table, label string, body []byte) (*loadResponse, error) {
	endpoint := *c.base
	endpoint.Path = "/api/" + database + "/" + table + "/_stream_load"
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("sink.doris: request: %w", err)
	}
	req.SetBasicAuth(c.user, c.pass)
	req.Header.Set("label", label)
	req.Header.Set("format", "json")
	req.Header.Set("read_json_by_line", "true")
	req.Header.Set("strict_mode", "true")
	req.Header.Set("max_filter_ratio", "0")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("timeout", strconv.Itoa(int(c.timeout.Seconds())))
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, &unknownResultError{Err: err}
	}
	defer resp.Body.Close()
	payload, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	// Drain the remainder so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)
	if readErr != nil {
		return nil, &unknownResultError{Err: readErr}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &httpStatusError{Code: resp.StatusCode, Body: truncate(string(payload), 512)}
	}
	var out loadResponse
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, &unknownResultError{Err: fmt.Errorf("invalid response: %w", err)}
	}
	return &out, nil
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "..."
}
