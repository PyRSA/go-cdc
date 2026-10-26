//go:build esintegration

// Package-level integration tests against a real Elasticsearch 8 cluster.
//
// Excluded from the default `go test ./...` run on purpose (Docker required):
//
//	docker compose up -d --wait elasticsearch   # test/integration
//	bash test/integration/scripts/run-es-integration.sh
//
// Everything here drives the production *Sink* (real v8 client, no fake) and
// verifies the cluster through plain HTTP, so a bug in the sink client cannot
// mask itself. Scenarios that cannot be produced reliably in a real cluster
// (item-level bulk rejection is produced with a strict inner object mapping,
// delete-not-found) are still exercised against the real server.
package elasticsearch

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
)

const defaultIntegrationURL = "http://127.0.0.1:19200"

func integrationURL() string {
	if raw := strings.TrimSpace(os.Getenv("ES_URL")); raw != "" {
		return raw
	}
	return defaultIntegrationURL
}

// esCluster is the raw HTTP side of the test: independent of client/v8.
type esCluster struct {
	t    *testing.T
	base string
	http *http.Client
}

func newCluster(t *testing.T) *esCluster {
	t.Helper()
	c := &esCluster{t: t, base: integrationURL(), http: &http.Client{Timeout: 60 * time.Second}}
	status, body := c.do("GET", "/", nil)
	if status != http.StatusOK {
		t.Fatalf("Elasticsearch is not reachable at %s (status %d: %s); start it with "+
			"`bash test/integration/scripts/es-up.sh`", c.base, status, body)
	}
	if !strings.HasPrefix(c.version(), "8.") {
		t.Fatalf("integration cluster is %q, these tests target Elasticsearch 8", c.version())
	}
	return c
}

func (c *esCluster) do(method, path string, body any) (int, string) {
	c.t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			c.t.Fatalf("marshal %s %s: %v", method, path, err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, c.base+path, reader)
	if err != nil {
		c.t.Fatalf("build request %s %s: %v", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		c.t.Fatalf("read %s %s: %v", method, path, err)
	}
	return resp.StatusCode, string(raw)
}

func (c *esCluster) must(method, path string, body any, want ...int) string {
	c.t.Helper()
	status, text := c.do(method, path, body)
	for _, code := range want {
		if status == code {
			return text
		}
	}
	c.t.Fatalf("%s %s -> %d (want %v): %s", method, path, status, want, text)
	return ""
}

func (c *esCluster) version() string {
	status, text := c.do("GET", "/", nil)
	if status != http.StatusOK {
		return ""
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(text), &body); err != nil {
		return ""
	}
	version, _ := body["version"].(map[string]any)
	number, _ := version["number"].(string)
	return number
}

func (c *esCluster) exists(index string) bool {
	status, _ := c.do("HEAD", "/"+index, nil)
	return status == http.StatusOK
}

func (c *esCluster) createIndex(index string, body any) {
	c.t.Helper()
	c.must("PUT", "/"+index, body, http.StatusOK)
}

func (c *esCluster) deleteIndex(index string) {
	c.t.Helper()
	status, text := c.do("DELETE", "/"+index, nil)
	if status != http.StatusOK && status != http.StatusNotFound {
		c.t.Fatalf("delete index %s -> %d: %s", index, status, text)
	}
}

func (c *esCluster) deleteByPrefix(prefix string) {
	status, text := c.do("GET", "/_cat/indices/"+prefix+"*?h=index&format=json", nil)
	if status != http.StatusOK {
		return
	}
	var rows []map[string]string
	if err := json.Unmarshal([]byte(text), &rows); err != nil {
		return
	}
	for _, row := range rows {
		if name := row["index"]; name != "" {
			c.do("DELETE", "/"+name, nil)
		}
	}
}

func (c *esCluster) mapping(index string) map[string]any {
	c.t.Helper()
	text := c.must("GET", "/"+index+"/_mapping", nil, http.StatusOK)
	var body map[string]any
	if err := json.Unmarshal([]byte(text), &body); err != nil {
		c.t.Fatalf("decode mapping %s: %v", index, err)
	}
	entry, _ := body[index].(map[string]any)
	mappings, _ := entry["mappings"].(map[string]any)
	return mappings
}

func (c *esCluster) properties(index string) map[string]any {
	c.t.Helper()
	props, _ := c.mapping(index)["properties"].(map[string]any)
	return props
}

func (c *esCluster) refresh(index string) {
	c.t.Helper()
	c.must("POST", "/"+index+"/_refresh", nil, http.StatusOK)
}

func (c *esCluster) count(index string) int {
	c.t.Helper()
	text := c.must("GET", "/"+index+"/_count", map[string]any{"query": map[string]any{"match_all": map[string]any{}}}, http.StatusOK)
	var body struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal([]byte(text), &body); err != nil {
		c.t.Fatalf("decode count %s: %v", index, err)
	}
	return body.Count
}

// searchIDs returns every document id (paged with scroll: no 10k window).
func (c *esCluster) searchIDs(index string) []string {
	c.t.Helper()
	text := c.must("POST", "/"+index+"/_search?scroll=1m", map[string]any{
		"size": 1000, "sort": []string{"_doc"}, "_source": false,
	}, http.StatusOK)
	var ids []string
	scrollID := ""
	for {
		var page struct {
			ScrollID string `json:"_scroll_id"`
			Hits     struct {
				Hits []struct {
					ID string `json:"_id"`
				} `json:"hits"`
			} `json:"hits"`
		}
		if err := json.Unmarshal([]byte(text), &page); err != nil {
			c.t.Fatalf("decode search %s: %v", index, err)
		}
		if page.ScrollID != "" {
			scrollID = page.ScrollID
		}
		if len(page.Hits.Hits) == 0 {
			break
		}
		for _, hit := range page.Hits.Hits {
			ids = append(ids, hit.ID)
		}
		if scrollID == "" {
			break
		}
		nextStatus, next := c.do("POST", "/_search/scroll", map[string]any{"scroll": "1m", "scroll_id": scrollID})
		if nextStatus != http.StatusOK {
			break
		}
		text = next
	}
	if scrollID != "" {
		c.do("DELETE", "/_search/scroll", map[string]any{"scroll_id": []string{scrollID}})
	}
	return ids
}

// source returns one document with numbers kept as json.Number text.
func (c *esCluster) source(index, id string) map[string]any {
	c.t.Helper()
	text := c.must("GET", "/"+index+"/_doc/"+id, nil, http.StatusOK)
	var body struct {
		Found  bool           `json:"found"`
		Source map[string]any `json:"_source"`
	}
	// Keep numbers as json.Number: a uint64 primary key would lose precision.
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	if err := decoder.Decode(&body); err != nil {
		c.t.Fatalf("decode _source %s/%s: %v", index, id, err)
	}
	if !body.Found {
		c.t.Fatalf("document %s/%s not found", index, id)
	}
	return body.Source
}

func (c *esCluster) docExists(index, id string) bool {
	status, _ := c.do("HEAD", "/"+index+"/_doc/"+id, nil)
	return status == http.StatusOK
}

// newSink opens the production sink against the integration cluster.
func newSink(t *testing.T, cfg definition.Sink) (*Sink, *esCluster) {
	t.Helper()
	cluster := newCluster(t)
	if len(cfg.Hosts) == 0 {
		cfg.Hosts = []string{integrationURL()}
	}
	if cfg.Type == "" {
		cfg.Type = "elasticsearch"
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s, cluster
}

// namespace returns a unique, valid index prefix for one test.
func namespace(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 4)
	if _, err := rand.Read(raw); err != nil {
		t.Fatalf("rand: %v", err)
	}
	name := strings.ToLower(t.Name())
	replacer := strings.NewReplacer("/", "-", "_", "-", " ", "-")
	return fmt.Sprintf("gocdc-it-%s-%s", replacer.Replace(name), hex.EncodeToString(raw))
}

func col(name, typ string) schema.Column {
	return schema.Column{Name: name, Type: typ, Nullable: true}
}

func table(database, name string, keys []string, columns ...schema.Column) schema.Table {
	return schema.Table{Database: database, Table: name, Columns: columns, PrimaryKeys: keys}
}

// row builds an after/before image from pairs: name, value, name, value...
func row(t *testing.T, kv ...any) map[string]any {
	t.Helper()
	if len(kv)%2 != 0 {
		t.Fatalf("row needs name/value pairs, got %d items", len(kv))
	}
	out := make(map[string]any, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		name, ok := kv[i].(string)
		if !ok {
			t.Fatalf("row key %d is not a string: %T", i, kv[i])
		}
		out[name] = kv[i+1]
	}
	return out
}

func change(op event.Op, database, name string, before, after map[string]any) event.Event {
	return event.Event{
		Op: op, Database: database, Table: name,
		SourceDatabase: "src_db", SourceTable: name,
		Before: before, After: after,
	}
}

func write(t *testing.T, s *Sink, events ...event.Event) {
	t.Helper()
	if err := s.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

func only(t *testing.T, ids []string) string {
	t.Helper()
	if len(ids) != 1 {
		t.Fatalf("want exactly 1 document, got %d: %v", len(ids), ids)
	}
	return ids[0]
}

func sameJSON(t *testing.T, left, right any) bool {
	t.Helper()
	a, err := json.Marshal(left)
	if err != nil {
		t.Fatalf("marshal left: %v", err)
	}
	b, err := json.Marshal(right)
	if err != nil {
		t.Fatalf("marshal right: %v", err)
	}
	return bytes.Equal(a, b)
}

// itTypesTable is the full supported type matrix for one row.
func itTypesTable(database, name string) schema.Table {
	return table(database, name, []string{"id"},
		col("id", "bigint unsigned"),
		col("flag", "tinyint(4)"),
		col("small_u", "smallint unsigned"),
		col("int_u", "int unsigned"),
		col("amount", "decimal(18,6)"),
		col("ratio", "float"),
		col("score", "double"),
		col("name", "varchar(64)"),
		col("long_name", "varchar(600)"),
		col("note", "text"),
		col("meta", "json"),
		col("blob", "varbinary(32)"),
		col("day", "date"),
		col("seen", "datetime(6)"),
		col("updated", "timestamp(6)"),
		col("clock", "time(6)"),
		col("status", "enum('new','done')"),
		col("tags", "set('a','b','c')"),
		col("yr", "year"),
	)
}

func itTypesRow() map[string]any {
	return map[string]any{
		"id":        uint64(18446744073709551615),
		"flag":      int64(-7),
		"small_u":   int64(65535),
		"int_u":     int64(4294967295),
		"amount":    "1234567890.123456",
		"ratio":     float32(1.5),
		"score":     2.25,
		"name":      "widget",
		"long_name": "longer-than-keyword",
		"note":      "a text value",
		"meta":      `{"b":2,"a":1}`,
		"blob":      []byte{0x00, 0xFF, 0x10},
		"day":       "2024-01-02",
		"seen":      "2024-01-02 03:04:05.123456",
		"updated":   "2024-01-02T03:04:05Z",
		"clock":     "12:34:56.123456",
		"status":    "done",
		"tags":      "a,c",
		"yr":        "2024",
	}
}

// indexBody builds the Create Index body a pre-provisioned index would hold:
// the mapping derived from the same schema.
func indexBody(t *testing.T, tbl schema.Table) map[string]any {
	t.Helper()
	props, err := deriveProperties(tbl)
	if err != nil {
		t.Fatalf("deriveProperties: %v", err)
	}
	return map[string]any{"mappings": map[string]any{"properties": props}}
}

// fieldEntry returns one field mapping from a stored index mapping.
func fieldEntry(t *testing.T, props map[string]any, name string) map[string]any {
	t.Helper()
	entry, ok := props[name].(map[string]any)
	if !ok {
		t.Fatalf("mapping has no field %q: %v", name, props)
	}
	return entry
}

func fieldType(t *testing.T, props map[string]any, name string) string {
	t.Helper()
	typ, _ := fieldEntry(t, props, name)["type"].(string)
	return typ
}

func fieldFormat(t *testing.T, props map[string]any, name string) string {
	t.Helper()
	format, _ := fieldEntry(t, props, name)["format"].(string)
	return format
}

// TestESIndexLifecycle covers creation, the auto-create default, mapping
// compatibility, the mapping file, and index-name derivation.
func TestESIndexLifecycle(t *testing.T) {
	users := func(db string) schema.Table {
		return table(db, "users", []string{"id"}, col("id", "bigint"), col("name", "varchar(64)"))
	}

	t.Run("auto-create creates the index", func(t *testing.T) {
		db := namespace(t)
		index := indexNameFor("", db, "users")
		s, cluster := newSink(t, definition.Sink{AutoCreate: true})
		t.Cleanup(func() { cluster.deleteByPrefix(db) })
		if err := s.CreateTable(users(db)); err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		if !cluster.exists(index) {
			t.Fatalf("index %q was not created", index)
		}
		if got := fieldType(t, cluster.properties(index), "id"); got != "long" {
			t.Fatalf("id mapping = %q, want long", got)
		}
		if got := cluster.count(index); got != 0 {
			t.Fatalf("fresh index count = %d, want 0", got)
		}
	})

	t.Run("auto-create false fails on a missing index", func(t *testing.T) {
		db := namespace(t)
		index := indexNameFor("", db, "users")
		s, cluster := newSink(t, definition.Sink{AutoCreate: false})
		err := s.CreateTable(users(db))
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
		s, cluster := newSink(t, definition.Sink{AutoCreate: false})
		t.Cleanup(func() { cluster.deleteIndex(index) })
		cluster.createIndex(index, indexBody(t, users(db)))
		before := cluster.properties(index)
		if err := s.CreateTable(users(db)); err != nil {
			t.Fatalf("CreateTable against a compatible index: %v", err)
		}
		if after := cluster.properties(index); !sameJSON(t, before, after) {
			t.Fatalf("mapping changed:\nbefore %v\n after %v", before, after)
		}
	})

	t.Run("existing index missing a column fails", func(t *testing.T) {
		db := namespace(t)
		index := indexNameFor("", db, "users")
		s, cluster := newSink(t, definition.Sink{AutoCreate: true})
		t.Cleanup(func() { cluster.deleteIndex(index) })
		cluster.createIndex(index, map[string]any{"mappings": map[string]any{
			"properties": map[string]any{"id": map[string]any{"type": "long"}},
		}})
		err := s.CreateTable(users(db))
		if err == nil || !strings.Contains(err.Error(), "is missing from the mapping") {
			t.Fatalf("error = %v, want a missing mapping field failure", err)
		}
	})

	t.Run("incompatible existing index fails", func(t *testing.T) {
		db := namespace(t)
		index := indexNameFor("", db, "users")
		s, cluster := newSink(t, definition.Sink{AutoCreate: true})
		t.Cleanup(func() { cluster.deleteIndex(index) })
		cluster.createIndex(index, map[string]any{"mappings": map[string]any{
			"properties": map[string]any{
				"id":   map[string]any{"type": "keyword"},
				"name": map[string]any{"type": "keyword"},
			},
		}})
		err := s.CreateTable(users(db))
		if err == nil || !strings.Contains(err.Error(), "cannot be written to mapping type") {
			t.Fatalf("error = %v, want a mapping conflict", err)
		}
	})

	t.Run("mapping file wins for its fields and merges the rest", func(t *testing.T) {
		db := namespace(t)
		index := indexNameFor("", db, "users")
		s, cluster := newSink(t, definition.Sink{
			AutoCreate: true,
			Mapping: json.RawMessage(`{"mappings":{"properties":{
				"name":{"type":"keyword","ignore_above":128},
				"extra":{"type":"keyword"}}}}`),
		})
		t.Cleanup(func() { cluster.deleteIndex(index) })
		if err := s.CreateTable(users(db)); err != nil {
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

	t.Run("derived index name is normalized and deterministic", func(t *testing.T) {
		s, cluster := newSink(t, definition.Sink{AutoCreate: true})
		const want = "shop-db.order.items-476626c2208d"
		if got := indexNameFor("", "Shop-DB", "Order.Items"); got != want {
			t.Fatalf("indexNameFor = %q, want %q", got, want)
		}
		t.Cleanup(func() { cluster.deleteIndex(want) })
		tbl := table("Shop-DB", "Order.Items", []string{"id"}, col("id", "bigint"), col("v", "varchar(16)"))
		if err := s.CreateTable(tbl); err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		write(t, s, change(event.OpCreate, "Shop-DB", "Order.Items", nil,
			row(t, "id", int64(5), "v", "x")))
		cluster.refresh(want)
		if got := cluster.count(want); got != 1 {
			t.Fatalf("count in %q = %d, want 1", want, got)
		}
	})

	t.Run("explicit index is shared by two target tables", func(t *testing.T) {
		db := namespace(t)
		index := db + "-shared"
		s, cluster := newSink(t, definition.Sink{AutoCreate: true, IndexName: index})
		t.Cleanup(func() { cluster.deleteIndex(index) })
		left := table(db, "a", []string{"id"}, col("id", "bigint"), col("v", "varchar(16)"))
		right := table(db, "b", []string{"id"}, col("id", "bigint"), col("v", "varchar(16)"))
		for _, tbl := range []schema.Table{left, right} {
			if err := s.CreateTable(tbl); err != nil {
				t.Fatalf("CreateTable %s.%s: %v", tbl.Database, tbl.Table, err)
			}
		}
		write(t, s,
			change(event.OpCreate, db, "a", nil, row(t, "id", int64(1), "v", "a")),
			change(event.OpCreate, db, "b", nil, row(t, "id", int64(1), "v", "b")),
		)
		cluster.refresh(index)
		ids := cluster.searchIDs(index)
		if len(ids) != 2 {
			t.Fatalf("shared index holds %d document(s), want 2 (one per target table): %v", len(ids), ids)
		}
		if ids[0] == ids[1] {
			t.Fatalf("both target tables produced the same _id %q", ids[0])
		}
	})

	t.Run("shared index rejects an incompatible table", func(t *testing.T) {
		db := namespace(t)
		index := db + "-shared-conflict"
		s, cluster := newSink(t, definition.Sink{AutoCreate: true, IndexName: index})
		t.Cleanup(func() { cluster.deleteIndex(index) })
		ok := table(db, "a", []string{"id"}, col("id", "bigint"), col("v", "varchar(16)"))
		bad := table(db, "b", []string{"id"}, col("id", "varchar(32)"), col("v", "varchar(16)"))
		if err := s.CreateTable(ok); err != nil {
			t.Fatalf("CreateTable %s.a: %v", db, err)
		}
		err := s.CreateTable(bad)
		if err == nil || !strings.Contains(err.Error(), "cannot be written to mapping type") {
			t.Fatalf("error = %v, want a mapping conflict on the shared index", err)
		}
	})
}

// TestESTypes writes one row covering every supported MySQL type and checks
// both the stored mapping and the stored _source.
func TestESTypes(t *testing.T) {
	db := namespace(t)
	index := indexNameFor("", db, "types")
	s, cluster := newSink(t, definition.Sink{AutoCreate: true})
	t.Cleanup(func() { cluster.deleteIndex(index) })
	if err := s.CreateTable(itTypesTable(db, "types")); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	write(t, s, change(event.OpCreate, db, "types", nil, itTypesRow()))
	cluster.refresh(index)

	id := only(t, cluster.searchIDs(index))
	if len(id) != 64 {
		t.Fatalf("_id %q is not a 64 character sha256 hex", id)
	}
	got := cluster.source(index, id)
	want := map[string]any{
		"id":        json.Number("18446744073709551615"),
		"flag":      json.Number("-7"),
		"small_u":   json.Number("65535"),
		"int_u":     json.Number("4294967295"),
		"amount":    "1234567890.123456",
		"ratio":     json.Number("1.5"),
		"score":     json.Number("2.25"),
		"name":      "widget",
		"long_name": "longer-than-keyword",
		"note":      "a text value",
		"meta":      map[string]any{"a": json.Number("1"), "b": json.Number("2")},
		"blob":      "AP8Q",
		"day":       "2024-01-02",
		"seen":      "2024-01-02 03:04:05.123456",
		"updated":   "2024-01-02 03:04:05.000000",
		"clock":     "12:34:56.123456",
		"status":    "done",
		"tags":      "a,c",
		"yr":        "2024",
	}
	if !sameJSON(t, got, want) {
		t.Fatalf("_source mismatch:\n got %v\nwant %v", got, want)
	}

	props := cluster.properties(index)
	types := map[string]string{
		"id":      "unsigned_long",
		"flag":    "byte",
		"small_u": "integer",
		"int_u":   "long",
		"amount":  "keyword",
		"ratio":   "float",
		"score":   "double",
		"name":    "keyword",
		// Above the 512 character keyword limit.
		"long_name": "text",
		"note":      "text",
		"meta":      "object",
		"blob":      "binary",
		"day":       "date",
		"seen":      "date",
		"updated":   "date",
		"clock":     "keyword",
		"status":    "keyword",
		"tags":      "keyword",
		"yr":        "date",
	}
	for name, wantType := range types {
		if gotType := fieldType(t, props, name); gotType != wantType {
			t.Errorf("mapping %s = %q, want %q", name, gotType, wantType)
		}
	}
	formats := map[string]string{
		"day":     "yyyy-MM-dd",
		"seen":    "yyyy-MM-dd HH:mm:ss.SSSSSS",
		"updated": "yyyy-MM-dd HH:mm:ss.SSSSSS",
		"yr":      "yyyy",
	}
	for name, wantFormat := range formats {
		if gotFormat := fieldFormat(t, props, name); gotFormat != wantFormat {
			t.Errorf("mapping %s format = %q, want %q", name, gotFormat, wantFormat)
		}
	}
}

// TestESUnsupportedTypeFails keeps the v1 promise that an unsupported column
// fails loudly instead of producing a lossy index.
func TestESUnsupportedTypeFails(t *testing.T) {
	db := namespace(t)
	index := indexNameFor("", db, "raw")
	s, cluster := newSink(t, definition.Sink{AutoCreate: true})
	tbl := table(db, "raw", []string{"id"}, col("id", "bigint"), col("bits", "bit(8)"))
	err := s.CreateTable(tbl)
	if err == nil || !strings.Contains(err.Error(), "bit values are raw bytes") {
		t.Fatalf("error = %v, want the bit column rejection", err)
	}
	if cluster.exists(index) {
		t.Fatalf("index %q was created for an unsupported column", index)
	}
}

// TestESWrites covers insert, update, delete, idempotent replays, composite
// keys, and one bulk spanning several indexes.
func TestESWrites(t *testing.T) {
	accounts := func(db string) schema.Table {
		return table(db, "accounts", []string{"id"},
			col("id", "bigint"), col("name", "varchar(64)"), col("note", "varchar(64)"))
	}

	t.Run("insert update delete", func(t *testing.T) {
		db := namespace(t)
		index := indexNameFor("", db, "accounts")
		s, cluster := newSink(t, definition.Sink{AutoCreate: true})
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
		if got := cluster.count(index); got != 1 {
			t.Fatalf("update changed the document count to %d", got)
		}
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

	t.Run("repeated index is idempotent", func(t *testing.T) {
		db := namespace(t)
		index := indexNameFor("", db, "accounts")
		s, cluster := newSink(t, definition.Sink{AutoCreate: true})
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

	t.Run("composite primary key is deterministic", func(t *testing.T) {
		db := namespace(t)
		index := indexNameFor("", db, "pairs")
		tbl := table(db, "pairs", []string{"tenant", "seq"},
			col("tenant", "varchar(16)"), col("seq", "int"), col("v", "varchar(32)"))
		s, cluster := newSink(t, definition.Sink{AutoCreate: true})
		t.Cleanup(func() { cluster.deleteIndex(index) })
		if err := s.CreateTable(tbl); err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		ev := change(event.OpCreate, db, "pairs", nil,
			row(t, "tenant", "acme", "seq", int64(7), "v", "a"))
		ev.Keys = []string{"tenant", "seq"}
		write(t, s, ev)
		cluster.refresh(index)
		first := only(t, cluster.searchIDs(index))

		// The event key order must not matter: the Schema order is authoritative.
		reordered := change(event.OpCreate, db, "pairs", nil,
			row(t, "tenant", "acme", "seq", int64(7), "v", "a"))
		reordered.Keys = []string{"seq", "tenant"}
		write(t, s, reordered)

		// A restarted sink (new catalog, new plan) hashes the same way.
		restarted, _ := newSink(t, definition.Sink{AutoCreate: true})
		if err := restarted.CreateTable(tbl); err != nil {
			t.Fatalf("CreateTable on the restarted sink: %v", err)
		}
		write(t, restarted, ev)

		cluster.refresh(index)
		if got := cluster.count(index); got != 1 {
			t.Fatalf("composite key replay produced %d document(s), want 1", got)
		}
		if got := only(t, cluster.searchIDs(index)); got != first {
			t.Fatalf("_id changed across replays: %s -> %s", first, got)
		}
	})

	t.Run("one bulk spans several indexes", func(t *testing.T) {
		db := namespace(t)
		leftIndex := indexNameFor("", db, "left")
		rightIndex := indexNameFor("", db, "right")
		left := table(db, "left", []string{"id"}, col("id", "bigint"), col("v", "varchar(16)"))
		right := table(db, "right", []string{"id"}, col("id", "bigint"), col("v", "varchar(16)"))
		s, cluster := newSink(t, definition.Sink{AutoCreate: true})
		t.Cleanup(func() { cluster.deleteByPrefix(db) })
		for _, tbl := range []schema.Table{left, right} {
			if err := s.CreateTable(tbl); err != nil {
				t.Fatalf("CreateTable %s.%s: %v", tbl.Database, tbl.Table, err)
			}
		}
		write(t, s,
			change(event.OpCreate, db, "left", nil, row(t, "id", int64(1), "v", "l1")),
			change(event.OpCreate, db, "right", nil, row(t, "id", int64(1), "v", "r1")),
			change(event.OpCreate, db, "left", nil, row(t, "id", int64(2), "v", "l2")),
		)
		cluster.refresh(leftIndex)
		cluster.refresh(rightIndex)
		if got := cluster.count(leftIndex); got != 2 {
			t.Fatalf("left index count = %d, want 2", got)
		}
		if got := cluster.count(rightIndex); got != 1 {
			t.Fatalf("right index count = %d, want 1", got)
		}
	})

	t.Run("delete of a missing document succeeds", func(t *testing.T) {
		db := namespace(t)
		index := indexNameFor("", db, "accounts")
		s, cluster := newSink(t, definition.Sink{AutoCreate: true})
		t.Cleanup(func() { cluster.deleteIndex(index) })
		if err := s.CreateTable(accounts(db)); err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		write(t, s, change(event.OpDelete, db, "accounts",
			row(t, "id", int64(99), "name", "ghost", "note", nil), nil))
		cluster.refresh(index)
		if got := cluster.count(index); got != 0 {
			t.Fatalf("count = %d, want 0", got)
		}
	})
}

// TestESBulkPartialFailure produces a real item-level rejection: a strict inner
// object mapping refuses the undeclared key while the bulk request succeeds.
func TestESBulkPartialFailure(t *testing.T) {
	db := namespace(t)
	index := indexNameFor("", db, "docs")
	s, cluster := newSink(t, definition.Sink{
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
