// Package v9 adapts the official go-elasticsearch v9 client to the sink's
// version-independent client interface.
//
// The requests, the typeless mappings, and the NDJSON bulk body are the same as
// in v7 and v8, so the sink business layer is shared. v9 differs in three ways
// that stay inside this adapter:
//
//   - The client is built with the functional Options API (es.New) instead of
//     the deprecated es.Config/NewClient pair.
//   - The client owns its transport: v9 exposes Close(ctx), which also closes
//     the transport it built, so the adapter keeps no transport of its own.
//   - Close is single-shot: a second call returns es.ErrAlreadyClosed, which
//     the adapter maps back to nil to keep the sink's idempotent Close contract.
package v9

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	es "github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"

	esclient "github.com/PyRSA/go-cdc/connector/sink/elasticsearch/client"
)

// DefaultRequestTimeout bounds one Elasticsearch request.
const DefaultRequestTimeout = 30 * time.Second

// Config is the adapter configuration. Credentials are never logged.
type Config struct {
	Addresses      []string
	Username       string
	Password       string
	RequestTimeout time.Duration
}

// Client is the v9 implementation of esclient.Client.
type Client struct {
	api     *es.Client
	timeout time.Duration
}

// New builds a client. Retries are disabled in the SDK on purpose: the sink
// owns bounded retries, and a retrying transport would multiply attempts. The
// compatibility mode (an older cluster behind the matching API) stays off: the
// configured version selects the client, never the other way round.
func New(cfg Config) (*Client, error) {
	if len(cfg.Addresses) == 0 {
		return nil, errors.New("elasticsearch: at least one host is required")
	}
	timeout := cfg.RequestTimeout
	if timeout <= 0 {
		timeout = DefaultRequestTimeout
	}
	options := []es.Option{
		es.WithAddresses(cfg.Addresses...),
		// The transport option is the exact v9 equivalent of the v8
		// DisableRetry flag: no internal retry and no request-body buffering.
		es.WithTransportOptions(elastictransport.WithDisableRetry()),
	}
	// Basic auth matches v7/v8: the SDK enables it when either credential is
	// set, so the adapter passes both through unchanged. Parse still requires
	// username and password to be configured together.
	if cfg.Username != "" || cfg.Password != "" {
		options = append(options, es.WithBasicAuth(cfg.Username, cfg.Password))
	}
	api, err := es.New(options...)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch: create client: %w", err)
	}
	return &Client{api: api, timeout: timeout}, nil
}

// Ping checks connectivity and credentials.
func (c *Client) Ping(ctx context.Context) error {
	ctx, cancel := c.requestContext(ctx)
	defer cancel()
	res, err := c.api.Ping(c.api.Ping.WithContext(ctx))
	if err != nil {
		return &esclient.Error{Op: "ping", Err: err}
	}
	return finish("ping", res)
}

// IndexExists issues a HEAD request; only 404 means absent.
func (c *Client) IndexExists(ctx context.Context, index string) (bool, error) {
	ctx, cancel := c.requestContext(ctx)
	defer cancel()
	res, err := c.api.Indices.Exists([]string{index}, c.api.Indices.Exists.WithContext(ctx))
	if err != nil {
		return false, &esclient.Error{Op: "indices.exists", Err: err}
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, responseError("indices.exists", res, drainBody(res.Body))
	}
}

// CreateIndex creates index from a Create Index API body.
func (c *Client) CreateIndex(ctx context.Context, index string, body json.RawMessage) error {
	ctx, cancel := c.requestContext(ctx)
	defer cancel()
	res, err := c.api.Indices.Create(
		index,
		c.api.Indices.Create.WithBody(bytes.NewReader(body)),
		c.api.Indices.Create.WithContext(ctx),
	)
	if err != nil {
		return &esclient.Error{Op: "indices.create", Err: err}
	}
	return finish("indices.create", res)
}

// GetMapping returns the mappings object of a concrete index.
func (c *Client) GetMapping(ctx context.Context, index string) (map[string]any, error) {
	ctx, cancel := c.requestContext(ctx)
	defer cancel()
	res, err := c.api.Indices.GetMapping(
		c.api.Indices.GetMapping.WithIndex(index),
		c.api.Indices.GetMapping.WithContext(ctx),
	)
	if err != nil {
		return nil, &esclient.Error{Op: "indices.get_mapping", Err: err}
	}
	if res.IsError() {
		defer res.Body.Close()
		return nil, responseError("indices.get_mapping", res, drainBody(res.Body))
	}
	body, err := readBody(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("elasticsearch: indices.get_mapping: read body: %w", err)
	}
	var payload map[string]struct {
		Mappings json.RawMessage `json:"mappings"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("elasticsearch: indices.get_mapping: decode response: %w", err)
	}
	if len(payload) != 1 {
		return nil, fmt.Errorf("elasticsearch: indices.get_mapping: expected one index, got %d", len(payload))
	}
	for _, entry := range payload {
		if len(entry.Mappings) == 0 {
			return nil, fmt.Errorf("elasticsearch: indices.get_mapping: index %q has no mappings", index)
		}
		var mappings map[string]any
		if err := json.Unmarshal(entry.Mappings, &mappings); err != nil {
			return nil, fmt.Errorf("elasticsearch: indices.get_mapping: decode mappings: %w", err)
		}
		return mappings, nil
	}
	return nil, fmt.Errorf("elasticsearch: indices.get_mapping: index %q has no mappings", index)
}

// Bulk sends one pre-encoded NDJSON request and parses every item result.
func (c *Client) Bulk(ctx context.Context, batch esclient.BulkBatch) (esclient.BulkResult, error) {
	if len(batch.Ops) == 0 {
		return esclient.BulkResult{}, nil
	}
	if len(batch.Body) == 0 {
		return esclient.BulkResult{}, fmt.Errorf(
			"elasticsearch: bulk: empty request body for %d operation(s)", len(batch.Ops))
	}
	ctx, cancel := c.requestContext(ctx)
	defer cancel()
	res, err := c.api.Bulk(bytes.NewReader(batch.Body), c.api.Bulk.WithContext(ctx))
	if err != nil {
		return esclient.BulkResult{}, &esclient.Error{Op: "bulk", Err: err}
	}
	if res.IsError() {
		defer res.Body.Close()
		return esclient.BulkResult{}, responseError("bulk", res, drainBody(res.Body))
	}
	body, err := readBody(res.Body)
	res.Body.Close()
	if err != nil {
		return esclient.BulkResult{}, fmt.Errorf("elasticsearch: bulk: read body: %w", err)
	}
	return parseBulkResponse(batch.Ops, body)
}

func parseBulkResponse(ops []esclient.BulkOperation, body []byte) (esclient.BulkResult, error) {
	var payload struct {
		Items []map[string]struct {
			Index  string `json:"_index"`
			ID     string `json:"_id"`
			Status int    `json:"status"`
			Error  *struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return esclient.BulkResult{}, fmt.Errorf("elasticsearch: bulk: decode response: %w", err)
	}
	if len(payload.Items) != len(ops) {
		return esclient.BulkResult{}, fmt.Errorf(
			"elasticsearch: bulk: response has %d items for %d operations", len(payload.Items), len(ops))
	}
	items := make([]esclient.BulkItem, 0, len(payload.Items))
	for i, raw := range payload.Items {
		if len(raw) != 1 {
			return esclient.BulkResult{}, fmt.Errorf("elasticsearch: bulk: item %d has %d results", i, len(raw))
		}
		for action, result := range raw {
			item := esclient.BulkItem{
				Action: action,
				Index:  result.Index,
				ID:     result.ID,
				Status: result.Status,
			}
			if result.Error != nil {
				item.ErrType = result.Error.Type
				item.ErrReason = result.Error.Reason
			}
			items = append(items, item)
		}
	}
	return esclient.BulkResult{Items: items}, nil
}

// Close releases the client and the transport the SDK built for it. v9 closes
// only once, so a repeated Close is reported as success to match the sink's
// idempotent contract.
func (c *Client) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	if err := c.api.Close(ctx); err != nil && !errors.Is(err, es.ErrAlreadyClosed) {
		return err
	}
	return nil
}

func (c *Client) requestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.timeout)
}

func finish(op string, res *esapi.Response) error {
	if res == nil {
		return &esclient.Error{Op: op, Reason: "empty response"}
	}
	if !res.IsError() {
		res.Body.Close()
		return nil
	}
	defer res.Body.Close()
	return responseError(op, res, drainBody(res.Body))
}

func responseError(op string, res *esapi.Response, body []byte) error {
	out := &esclient.Error{Op: op, Status: res.StatusCode}
	var payload struct {
		Error *struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &payload); err == nil && payload.Error != nil {
		out.Type = payload.Error.Type
		out.Reason = payload.Error.Reason
	}
	if out.Type == "" && out.Reason == "" {
		out.Reason = http.StatusText(res.StatusCode)
	}
	return out
}

// drainBody reads a bounded error body; the read error is not interesting when
// the request already failed.
func drainBody(body io.ReadCloser) []byte {
	data, _ := readBody(body)
	return data
}

func readBody(body io.ReadCloser) ([]byte, error) {
	return io.ReadAll(io.LimitReader(body, 1<<20))
}
