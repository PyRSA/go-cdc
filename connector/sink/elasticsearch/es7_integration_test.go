//go:build esintegration

// Elasticsearch 7 integration suite for the v7 client adapter.
//
// It reuses the ES 8 harness in es8_integration_test.go (same raw HTTP cluster
// helpers, same production Sink); only the expected server version and the
// configured sink.version differ. The test names start with "TestElasticsearch7"
// on purpose: the ES 8 runner filters `-run TestES`, so a machine with only an
// ES 8 node never picks these up.
//
//	docker compose up -d --wait elasticsearch7   # tests/integration
//	bash tests/integration/scripts/run.sh elasticsearch normal es7 go
//
// Verified server: Elasticsearch 7.17.x. ES 7.0-7.16 are not verified: the Go
// 7.17 client performs a product check that older 7.x servers may not satisfy.
package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	esv7 "github.com/PyRSA/go-cdc/connector/sink/elasticsearch/client/v7"
)

// newCluster7 resolves the ES 7 node and refuses to run against another major.
func newCluster7(t *testing.T) *esCluster {
	t.Helper()
	return newClusterAt(t, integrationURL7(), "7.")
}

// newSink7 opens the production sink with sink.version 7 against the ES 7 node.
func newSink7(t *testing.T, cluster *esCluster, cfg definition.Sink) *Sink {
	t.Helper()
	cfg.Version = definition.ElasticsearchVersion7
	return newSinkOn(t, cluster, cfg)
}

// assertV7Adapter proves the sink really talks through the v7 client: a
// pipeline configured for 7 must never keep the v8 adapter.
func assertV7Adapter(t *testing.T, s *Sink) {
	t.Helper()
	s.mu.Lock()
	cl := s.client
	s.mu.Unlock()
	if _, ok := cl.(*esv7.Client); !ok {
		t.Fatalf("sink client is %T, want the v7 adapter", cl)
	}
}

func es7Users(db string) schema.Table {
	return table(db, "users", []string{"id"}, col("id", "bigint"), col("name", "varchar(64)"))
}

// TestElasticsearch7IndexLifecycle covers connection, index creation, the
// typeless mapping, mapping compatibility, and auto-create semantics.
func TestElasticsearch7IndexLifecycle(t *testing.T) {
	t.Run("client connects and creates a typeless index", func(t *testing.T) {
		db := namespace(t)
		index := indexNameFor("", db, "users")
		cluster := newCluster7(t)
		s := newSink7(t, cluster, definition.Sink{AutoCreate: true})
		t.Cleanup(func() { cluster.deleteIndex(index) })
		// Open already pinged the cluster; the adapter must be the v7 one.
		assertV7Adapter(t, s)
		if err := s.CreateTable(es7Users(db)); err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		if !cluster.exists(index) {
			t.Fatalf("index %q was not created", index)
		}
		if got := fieldType(t, cluster.properties(index), "id"); got != "long" {
			t.Fatalf("id mapping = %q, want long", got)
		}
		// A typeless mapping is stored at the root: no _doc/_type wrapper.
		mappings := cluster.mapping(index)
		for name := range mappings {
			if name == "_doc" || name == "_default_" {
				t.Fatalf("index %q stored a typed mapping: %v", index, mappings)
			}
		}
		if got := cluster.count(index); got != 0 {
			t.Fatalf("fresh index count = %d, want 0", got)
		}
	})

	t.Run("auto-create false fails on a missing index", func(t *testing.T) {
		db := namespace(t)
		index := indexNameFor("", db, "users")
		cluster := newCluster7(t)
		s := newSink7(t, cluster, definition.Sink{AutoCreate: false})
		err := s.CreateTable(es7Users(db))
		if err == nil {
			t.Fatal("CreateTable: want an error for a missing index")
		}
		if !strings.Contains(err.Error(), "sink.auto-create is false") || !strings.Contains(err.Error(), index) {
			t.Fatalf("error = %v, want the index name and the auto-create hint", err)
		}
		if cluster.exists(index) {
			t.Fatalf("index %q must not be created when auto-create is false", index)
		}
	})

	t.Run("compatible existing index is accepted and untouched", func(t *testing.T) {
		db := namespace(t)
		index := indexNameFor("", db, "users")
		cluster := newCluster7(t)
		s := newSink7(t, cluster, definition.Sink{AutoCreate: false})
		t.Cleanup(func() { cluster.deleteIndex(index) })
		cluster.createIndex(index, indexBody(t, es7Users(db)))
		before := cluster.properties(index)
		if err := s.CreateTable(es7Users(db)); err != nil {
			t.Fatalf("CreateTable against a compatible index: %v", err)
		}
		if after := cluster.properties(index); !sameJSON(t, before, after) {
			t.Fatalf("mapping changed:\nbefore %v\n after %v", before, after)
		}
	})

	t.Run("incompatible existing index fails", func(t *testing.T) {
		db := namespace(t)
		index := indexNameFor("", db, "users")
		cluster := newCluster7(t)
		s := newSink7(t, cluster, definition.Sink{AutoCreate: true})
		t.Cleanup(func() { cluster.deleteIndex(index) })
		cluster.createIndex(index, map[string]any{"mappings": map[string]any{
			"properties": map[string]any{
				"id":   map[string]any{"type": "keyword"},
				"name": map[string]any{"type": "keyword"},
			},
		}})
		err := s.CreateTable(es7Users(db))
		if err == nil || !strings.Contains(err.Error(), "cannot be written to mapping type") {
			t.Fatalf("error = %v, want a mapping conflict", err)
		}
	})

	t.Run("mapping file wins for its fields and merges the rest", func(t *testing.T) {
		db := namespace(t)
		index := indexNameFor("", db, "users")
		cluster := newCluster7(t)
		s := newSink7(t, cluster, definition.Sink{
			AutoCreate: true,
			Mapping: json.RawMessage(`{"mappings":{"properties":{
				"name":{"type":"keyword","ignore_above":128},
				"extra":{"type":"keyword"}}}}`),
		})
		t.Cleanup(func() { cluster.deleteIndex(index) })
		if err := s.CreateTable(es7Users(db)); err != nil {
			t.Fatalf("CreateTable with a mapping file: %v", err)
		}
		props := cluster.properties(index)
		if got := fieldType(t, props, "extra"); got != "keyword" {
			t.Fatalf("mapping-file field extra = %q, want keyword", got)
		}
		if got, _ := fieldEntry(t, props, "name")["ignore_above"].(float64); got != 128 {
			t.Fatalf("mapping file did not override name: %v", fieldEntry(t, props, "name"))
		}
		if got := fieldType(t, props, "id"); got != "long" {
			t.Fatalf("derived field id = %q, want long", got)
		}
	})
}

// TestElasticsearch7Writes covers insert, update, delete, idempotent replay, and
// multi-document bulk requests on the v7 adapter.
func TestElasticsearch7Writes(t *testing.T) {
	accounts := func(db string) schema.Table {
		return table(db, "accounts", []string{"id"},
			col("id", "bigint"), col("name", "varchar(64)"), col("note", "varchar(64)"))
	}

	t.Run("insert update delete", func(t *testing.T) {
		db := namespace(t)
		index := indexNameFor("", db, "accounts")
		cluster := newCluster7(t)
		s := newSink7(t, cluster, definition.Sink{AutoCreate: true})
		t.Cleanup(func() { cluster.deleteIndex(index) })
		if err := s.CreateTable(accounts(db)); err != nil {
			t.Fatalf("CreateTable: %v", err)
		}

		write(t, s, change(event.OpCreate, db, "accounts", nil,
			row(t, "id", int64(1), "name", "alice", "note", "first")))
		cluster.refresh(index)
		inserted := only(t, cluster.searchIDs(index))
		if got := cluster.source(index, inserted)["name"]; got != "alice" {
			t.Fatalf("inserted name = %v, want alice", got)
		}

		write(t, s, change(event.OpUpdate, db, "accounts",
			row(t, "id", int64(1), "name", "alice", "note", "first"),
			row(t, "id", int64(1), "name", "bob", "note", nil)))
		cluster.refresh(index)
		updated := only(t, cluster.searchIDs(index))
		if updated != inserted {
			t.Fatalf("update changed the _id: %s -> %s", inserted, updated)
		}
		src := cluster.source(index, updated)
		if src["name"] != "bob" {
			t.Fatalf("updated name = %v, want bob", src["name"])
		}
		if value, ok := src["note"]; !ok || value != nil {
			t.Fatalf("updated note = %v (present %v), want an explicit null", value, ok)
		}

		write(t, s, change(event.OpDelete, db, "accounts",
			row(t, "id", int64(1), "name", "bob", "note", nil), nil))
		cluster.refresh(index)
		if got := cluster.count(index); got != 0 {
			t.Fatalf("delete left %d document(s) behind", got)
		}
		if cluster.docExists(index, updated) {
			t.Fatalf("document %s still exists after delete", updated)
		}
	})

	t.Run("one write sends several documents", func(t *testing.T) {
		db := namespace(t)
		index := indexNameFor("", db, "accounts")
		cluster := newCluster7(t)
		s := newSink7(t, cluster, definition.Sink{AutoCreate: true})
		t.Cleanup(func() { cluster.deleteIndex(index) })
		if err := s.CreateTable(accounts(db)); err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		write(t, s,
			change(event.OpCreate, db, "accounts", nil, row(t, "id", int64(1), "name", "a", "note", nil)),
			change(event.OpCreate, db, "accounts", nil, row(t, "id", int64(2), "name", "b", "note", nil)),
			change(event.OpCreate, db, "accounts", nil, row(t, "id", int64(3), "name", "c", "note", nil)),
		)
		cluster.refresh(index)
		if got := cluster.count(index); got != 3 {
			t.Fatalf("count = %d, want 3", got)
		}
	})

	t.Run("repeated index is idempotent", func(t *testing.T) {
		db := namespace(t)
		index := indexNameFor("", db, "accounts")
		cluster := newCluster7(t)
		s := newSink7(t, cluster, definition.Sink{AutoCreate: true})
		t.Cleanup(func() { cluster.deleteIndex(index) })
		if err := s.CreateTable(accounts(db)); err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		ev := change(event.OpCreate, db, "accounts", nil,
			row(t, "id", int64(2), "name", "ann", "note", nil))
		write(t, s, ev)
		write(t, s, ev) // a checkpoint rollback replays the same event
		cluster.refresh(index)
		if got := cluster.count(index); got != 1 {
			t.Fatalf("replay produced %d document(s), want 1", got)
		}
	})
}

// TestElasticsearch7BulkPartialFailure produces a real item-level rejection: a
// strict inner object mapping refuses the undeclared key while the bulk request
// itself succeeds (HTTP 200).
func TestElasticsearch7BulkPartialFailure(t *testing.T) {
	db := namespace(t)
	index := indexNameFor("", db, "docs")
	cluster := newCluster7(t)
	s := newSink7(t, cluster, definition.Sink{
		AutoCreate: true,
		Mapping:    json.RawMessage(`{"mappings":{"properties":{"meta":{"type":"object","dynamic":"strict"}}}}`),
	})
	t.Cleanup(func() { cluster.deleteIndex(index) })
	tbl := table(db, "docs", []string{"id"}, col("id", "bigint"), col("meta", "json"))
	if err := s.CreateTable(tbl); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	bad := change(event.OpCreate, db, "docs", nil, row(t, "id", int64(1), "meta", `{"a":1}`))
	err := s.Write([]event.Event{bad})
	if err == nil {
		t.Fatal("Write accepted a document Elasticsearch rejected")
	}
	var failure *bulkError
	if !errors.As(err, &failure) {
		t.Fatalf("error = %v, want a bulk item failure", err)
	}
	if len(failure.failures) != 1 {
		t.Fatalf("reported %d failure(s), want 1: %v", len(failure.failures), err)
	}
	item := failure.failures[0]
	if item.index != index {
		t.Fatalf("failure index = %q, want %q", item.index, index)
	}
	if len(item.id) != 64 {
		t.Fatalf("failure id = %q, want the 64 character document id", item.id)
	}
	if !strings.Contains(err.Error(), "strict_dynamic_mapping_exception") {
		t.Fatalf("error = %v, want the Elasticsearch item error type", err)
	}
	cluster.refresh(index)
	if got := cluster.count(index); got != 0 {
		t.Fatalf("a rejected item left %d document(s) behind", got)
	}

	// The rejection must not poison the sink for the next batch.
	good := change(event.OpCreate, db, "docs", nil, row(t, "id", int64(2), "meta", nil))
	if err := s.Write([]event.Event{good}); err != nil {
		t.Fatalf("Write after a rejected item: %v", err)
	}
	cluster.refresh(index)
	if got := cluster.count(index); got != 1 {
		t.Fatalf("count after recovery = %d, want 1", got)
	}
}

// TestElasticsearch7SchemaRecheck keeps the data-correctness fix intact on the
// v7 path: an unchanged plan still re-checks the index, and a missing index is
// reported instead of recreated.
func TestElasticsearch7SchemaRecheck(t *testing.T) {
	db := namespace(t)
	index := indexNameFor("", db, "users")
	cluster := newCluster7(t)
	s := newSink7(t, cluster, definition.Sink{AutoCreate: true})
	t.Cleanup(func() { cluster.deleteIndex(index) })

	if err := s.CreateTable(es7Users(db)); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	// Registering the same table again re-runs the index checks and creates
	// nothing new.
	if err := s.CreateTable(es7Users(db)); err != nil {
		t.Fatalf("second CreateTable: %v", err)
	}
	cluster.deleteIndex(index)
	err := s.CreateTable(es7Users(db))
	if err == nil || !strings.Contains(err.Error(), "is missing while its schema is unchanged") {
		t.Fatalf("error = %v, want the cached-schema missing-index failure", err)
	}
	if cluster.exists(index) {
		t.Fatalf("index %q was recreated from a cached schema", index)
	}
}

// TestElasticsearch7CloseAndErrorPropagation covers close semantics and the
// startup/connection error path of the v7 adapter.
func TestElasticsearch7CloseAndErrorPropagation(t *testing.T) {
	t.Run("write after close fails", func(t *testing.T) {
		db := namespace(t)
		index := indexNameFor("", db, "users")
		cluster := newCluster7(t)
		s := newSink7(t, cluster, definition.Sink{AutoCreate: true})
		t.Cleanup(func() { cluster.deleteIndex(index) })
		if err := s.CreateTable(es7Users(db)); err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
		err := s.Write([]event.Event{change(event.OpCreate, db, "users", nil,
			row(t, "id", int64(1), "name", "x"))})
		if err == nil || !strings.Contains(err.Error(), "closed") {
			t.Fatalf("error = %v, want a closed-sink failure", err)
		}
	})

	t.Run("unreachable cluster fails at open", func(t *testing.T) {
		// Port 1 is never an Elasticsearch node: the ping must fail at startup.
		s, err := New(definition.Sink{
			Type: "elasticsearch", Version: definition.ElasticsearchVersion7,
			Hosts: []string{"http://127.0.0.1:1"}, RequestTimeout: time.Second,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		if err := s.Open(context.Background()); err == nil {
			t.Fatal("Open: want a ping failure")
		}
	})

	t.Run("ping reports the configured host", func(t *testing.T) {
		s, err := New(definition.Sink{
			Type: "elasticsearch", Version: definition.ElasticsearchVersion7,
			Hosts: []string{"http://user:secret@127.0.0.1:1"}, RequestTimeout: time.Second,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		err = s.Open(context.Background())
		if err == nil {
			t.Fatal("Open: want a ping failure")
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatalf("error leaks the password: %v", err)
		}
		if !strings.Contains(err.Error(), "127.0.0.1:1") {
			t.Fatalf("error lost the configured host: %v", err)
		}
	})
}
