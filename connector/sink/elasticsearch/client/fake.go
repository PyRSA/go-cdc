package client

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// CreateIndexCall records one CreateIndex invocation.
type CreateIndexCall struct {
	Index string
	Body  json.RawMessage
}

// Fake is an in-memory Client for tests. Fields are read under the mutex, so a
// test may tweak hooks before the sink starts and inspect them after.
type Fake struct {
	Mu sync.Mutex

	// Indices maps index name to its mappings object.
	Indices map[string]map[string]any
	// Created lists every successful CreateIndex call in order.
	Created []CreateIndexCall
	// BulkCalls lists every Bulk call in order, including the exact request body
	// the sink built, so tests can assert the byte budget.
	BulkCalls []BulkBatch

	PingErr    error
	CreateErr  error
	MappingErr error
	BulkErr    error
	ExistsErr  error
	CloseErr   error
	// ExistsHook replaces the default existence lookup, for race or failure tests.
	ExistsHook func(index string) (bool, error)
	// BulkHook replaces the default all-success bulk behaviour.
	BulkHook func(batch BulkBatch) (BulkResult, error)
	// PingCalls counts Ping invocations.
	PingCalls int
	Closed    bool
}

// NewFake returns an empty fake with writable index state.
func NewFake() *Fake {
	return &Fake{Indices: map[string]map[string]any{}}
}

// AddIndex pre-creates an index with mappings.
func (f *Fake) AddIndex(name string, mappings map[string]any) {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	if f.Indices == nil {
		f.Indices = map[string]map[string]any{}
	}
	f.Indices[name] = mappings
}

// Ping implements Client.
func (f *Fake) Ping(ctx context.Context) error {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	f.PingCalls++
	return f.PingErr
}

// IndexExists implements Client.
func (f *Fake) IndexExists(ctx context.Context, index string) (bool, error) {
	f.Mu.Lock()
	err := f.ExistsErr
	hook := f.ExistsHook
	f.Mu.Unlock()
	if err != nil {
		return false, err
	}
	if hook != nil {
		return hook(index)
	}
	f.Mu.Lock()
	defer f.Mu.Unlock()
	_, ok := f.Indices[index]
	return ok, nil
}

// CreateIndex implements Client. It fails when the index already exists, like
// Elasticsearch does with default settings.
func (f *Fake) CreateIndex(ctx context.Context, index string, body json.RawMessage) error {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	if f.CreateErr != nil {
		return f.CreateErr
	}
	if _, ok := f.Indices[index]; ok {
		return fmt.Errorf("fake: index %q already exists", index)
	}
	f.Indices[index] = mappingsFromCreateBody(body)
	f.Created = append(f.Created, CreateIndexCall{Index: index, Body: append(json.RawMessage(nil), body...)})
	return nil
}

// GetMapping implements Client.
func (f *Fake) GetMapping(ctx context.Context, index string) (map[string]any, error) {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	if f.MappingErr != nil {
		return nil, f.MappingErr
	}
	mappings, ok := f.Indices[index]
	if !ok {
		return nil, fmt.Errorf("fake: index %q does not exist", index)
	}
	return mappings, nil
}

// Bulk implements Client. The default succeeds for every operation.
func (f *Fake) Bulk(ctx context.Context, batch BulkBatch) (BulkResult, error) {
	f.Mu.Lock()
	f.BulkCalls = append(f.BulkCalls, batch)
	err := f.BulkErr
	hook := f.BulkHook
	f.Mu.Unlock()
	if err != nil {
		return BulkResult{}, err
	}
	if hook != nil {
		return hook(batch)
	}
	items := make([]BulkItem, 0, len(batch.Ops))
	for _, op := range batch.Ops {
		status := 201
		if op.Action == ActionDelete {
			status = 200
		}
		items = append(items, BulkItem{Action: string(op.Action), Index: op.Index, ID: op.ID, Status: status})
	}
	return BulkResult{Items: items}, nil
}

// Close implements Client.
func (f *Fake) Close() error {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	f.Closed = true
	return f.CloseErr
}

func mappingsFromCreateBody(body json.RawMessage) map[string]any {
	var doc struct {
		Mappings map[string]any `json:"mappings"`
	}
	if err := json.Unmarshal(body, &doc); err != nil || doc.Mappings == nil {
		return map[string]any{}
	}
	return doc.Mappings
}
