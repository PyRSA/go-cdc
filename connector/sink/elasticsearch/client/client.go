// Package client defines the Elasticsearch operations the sink needs behind a
// version-independent interface. The sink ships the official v7, v8 and v9
// adapters; the sink business layer never imports an Elasticsearch SDK.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// BulkAction is the NDJSON bulk operation for one document.
type BulkAction string

// Supported bulk actions. Update always carries the full After document, so it
// is written as an index operation and stays idempotent on replay.
const (
	ActionIndex  BulkAction = "index"
	ActionDelete BulkAction = "delete"
)

// BulkOperation is one bulk item. Document is single-line JSON for ActionIndex
// and empty for ActionDelete.
type BulkOperation struct {
	Action   BulkAction
	Index    string
	ID       string
	Document []byte
}

// BulkItem is the per-document result of a bulk request.
type BulkItem struct {
	Action    string
	Index     string
	ID        string
	Status    int
	ErrType   string
	ErrReason string
}

// OK reports whether Elasticsearch accepted the item.
func (i BulkItem) OK() bool { return i.Status >= 200 && i.Status < 300 }

// BulkResult is a parsed bulk response. Items is always one-to-one with the
// requested operations.
type BulkResult struct {
	Items []BulkItem
}

// Error is a failed Elasticsearch call. Status is 0 when the request never
// reached the cluster (connection, DNS, timeout, context).
type Error struct {
	Op     string
	Status int
	Type   string
	Reason string
	Err    error
}

func (e *Error) Error() string {
	var parts []string
	parts = append(parts, "elasticsearch "+e.Op)
	if e.Status != 0 {
		parts = append(parts, fmt.Sprintf("status %d", e.Status))
	}
	if e.Type != "" {
		parts = append(parts, e.Type)
	}
	if e.Reason != "" {
		parts = append(parts, e.Reason)
	}
	if e.Err != nil {
		parts = append(parts, e.Err.Error())
	}
	return strings.Join(parts, ": ")
}

// Unwrap exposes the transport error for errors.Is/As.
func (e *Error) Unwrap() error { return e.Err }

// StatusOf returns the HTTP status carried by err, or 0 when there is none.
func StatusOf(err error) int {
	var esErr *Error
	if errors.As(err, &esErr) {
		return esErr.Status
	}
	return 0
}

// TypeOf returns the Elasticsearch error type carried by err, or "".
func TypeOf(err error) string {
	var esErr *Error
	if errors.As(err, &esErr) {
		return esErr.Type
	}
	return ""
}

// IsType reports whether err carries one of the given Elasticsearch error
// types, for example resource_already_exists_exception.
func IsType(err error, types ...string) bool {
	actual := TypeOf(err)
	if actual == "" {
		return false
	}
	for _, want := range types {
		if actual == want {
			return true
		}
	}
	return false
}

// Client is the minimal Elasticsearch surface used by the sink.
type Client interface {
	// Ping verifies connectivity, credentials, and cluster availability.
	Ping(ctx context.Context) error
	// IndexExists reports whether the index (not alias) exists.
	IndexExists(ctx context.Context, index string) (bool, error)
	// CreateIndex creates index with a Create Index API body.
	CreateIndex(ctx context.Context, index string, body json.RawMessage) error
	// GetMapping returns the mappings object of index.
	GetMapping(ctx context.Context, index string) (map[string]any, error)
	// Bulk sends one pre-encoded batch and returns one item per operation. A
	// nil error does not mean every item succeeded.
	Bulk(ctx context.Context, batch BulkBatch) (BulkResult, error)
	// Close releases the underlying transport.
	Close() error
}
