// Package elasticsearch writes routed CDC events to Elasticsearch 8 through
// the official go-elasticsearch/v8 client.
//
// v1 is strict on purpose: a table must have a primary key, an existing index
// mapping is never modified, and an event that cannot be encoded or confirmed
// fails the write so the runner cannot advance its checkpoint.
package elasticsearch

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/sink"
	"github.com/PyRSA/go-cdc/connector/sink/elasticsearch/client"
	esv8 "github.com/PyRSA/go-cdc/connector/sink/elasticsearch/client/v8"
)

// DefaultRequestTimeout bounds one Elasticsearch request when
// sink.request-timeout is not configured.
const DefaultRequestTimeout = 30 * time.Second

// Sink implements sink.Sink for Elasticsearch 8.
type Sink struct {
	cfg        definition.Sink
	timeout    time.Duration
	maxRetries int

	mu     sync.Mutex
	client client.Client
	owns   bool // the sink created client and must close it
	tables map[string]*tableMeta
	closed bool
	sleep  func(context.Context, time.Duration) error
}

// New validates the sink configuration and returns an unopened sink.
// Mapping files were read and checked by the parser; programmatic callers must
// provide cfg.Mapping themselves.
func New(cfg definition.Sink) (*Sink, error) {
	if cfg.Type != "elasticsearch" {
		return nil, fmt.Errorf("elasticsearch: sink.type must be elasticsearch, got %q", cfg.Type)
	}
	if len(cfg.Hosts) == 0 {
		return nil, errors.New("elasticsearch: sink.hosts is required")
	}
	if cfg.MaxRetries < 0 {
		return nil, errors.New("elasticsearch: sink.max-retries must be >= 0")
	}
	timeout := cfg.RequestTimeout
	if timeout <= 0 {
		timeout = DefaultRequestTimeout
	}
	return &Sink{
		cfg:        cfg,
		timeout:    timeout,
		maxRetries: cfg.MaxRetries,
		tables:     make(map[string]*tableMeta),
		sleep:      sleepContext,
	}, nil
}

// SetClient injects a client (tests) or replaces the client before Open.
func (s *Sink) SetClient(cl client.Client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.client = cl
	s.owns = false
}

// Open builds the v8 client when none was injected and pings the cluster.
func (s *Sink) Open(ctx context.Context) error {
	s.mu.Lock()
	cl := s.client
	s.mu.Unlock()
	if cl == nil {
		created, err := esv8.New(esv8.Config{
			Addresses:      append([]string(nil), s.cfg.Hosts...),
			Username:       s.cfg.Username,
			Password:       s.cfg.Password,
			RequestTimeout: s.timeout,
		})
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.client = created
		s.owns = true
		s.mu.Unlock()
		cl = created
	}
	if err := cl.Ping(ctx); err != nil {
		if s.owns {
			s.mu.Lock()
			if s.client == cl {
				s.client = nil
				s.owns = false
			}
			s.mu.Unlock()
			_ = cl.Close()
		}
		return fmt.Errorf("elasticsearch: ping %s: %w", safeAddresses(s.cfg.Hosts), err)
	}
	return nil
}

// CreateTable validates the primary key, resolves the index name, creates the
// index when allowed, and verifies the final mapping. Registering the same
// table again re-runs the checks against the current index mapping.
func (s *Sink) CreateTable(table schema.Table) error {
	if table.Database == "" || table.Table == "" {
		return errors.New("elasticsearch: create table requires a routed database and table name")
	}
	if len(table.PrimaryKeys) == 0 {
		return fmt.Errorf(
			"elasticsearch: table %s.%s has no primary key; v1 cannot build a stable _id",
			table.Database, table.Table)
	}
	if len(table.Columns) == 0 {
		return fmt.Errorf("elasticsearch: table %s.%s has no columns to map", table.Database, table.Table)
	}
	// The startup catalog and the snapshot Schema message describe the same
	// table; the cached plan already passed every check for that schema.
	if cached, ok := s.metaFor(table.Database + "." + table.Table); ok && cached.samePlan(table) {
		return nil
	}
	index := indexNameFor(s.cfg.IndexName, table.Database, table.Table)
	if err := validateIndexName(index); err != nil {
		return fmt.Errorf("elasticsearch: table %s.%s: index name %q: %v", table.Database, table.Table, index, err)
	}
	meta, err := newTableMeta(table, index)
	if err != nil {
		return err
	}
	if err := s.ensureIndex(meta); err != nil {
		return err
	}
	s.mu.Lock()
	s.tables[meta.scope] = meta
	s.mu.Unlock()
	return nil
}

// PrepareCatalog implements the optional composer.CatalogSink capability. The
// runner calls it before the first event so every routed table owns an index
// and a verified mapping, which covers restarts and binlog-only starts where
// the source emits no Schema message. The first failure stops the call.
func (s *Sink) PrepareCatalog(tables []schema.Table) error {
	for _, table := range tables {
		if err := s.CreateTable(table); err != nil {
			return err
		}
	}
	return nil
}

// ApplySchema rejects every schema change. v1 never adds, removes, or retypes
// fields, so a DDL that reaches the sink must fail instead of being ignored.
func (s *Sink) ApplySchema(change schema.Change) error {
	return fmt.Errorf(
		"elasticsearch: %s.%s: schema change %q is not supported; v1 never alters an index mapping",
		change.Database, change.Table, change.Action)
}

// Write sends every event in the batch as one bulk request and returns an error
// unless every item was acknowledged. Nothing is buffered, so a nil error means
// Elasticsearch already accepted the documents.
func (s *Sink) Write(batch []event.Event) error {
	if len(batch) == 0 {
		return nil
	}
	s.mu.Lock()
	closed := s.closed
	cl := s.client
	s.mu.Unlock()
	if closed {
		return errors.New("elasticsearch: write on closed sink")
	}
	if cl == nil {
		return errors.New("elasticsearch: not open")
	}
	ops := make([]client.BulkOperation, 0, len(batch))
	for _, item := range batch {
		op, err := s.operationFor(item)
		if err != nil {
			return err
		}
		ops = append(ops, op)
	}
	ctx, cancel := s.writeContext()
	defer cancel()
	return s.bulk(ctx, ops)
}

// Flush has nothing to drain: Write blocks until the bulk response is checked.
func (s *Sink) Flush() error { return nil }

// Close closes the client and returns its error. Write is synchronous, so
// there is never unflushed data to lose.
func (s *Sink) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	cl := s.client
	s.client = nil
	s.owns = false
	s.mu.Unlock()
	if cl == nil {
		return nil
	}
	if err := cl.Close(); err != nil {
		return fmt.Errorf("elasticsearch: close: %w", err)
	}
	return nil
}

func (s *Sink) metaFor(sinkID string) (*tableMeta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, ok := s.tables[sinkID]
	return meta, ok
}

// ensureIndex follows the catalog order: derive properties, validate the
// mapping file against the schema, check existence, create when allowed, then
// verify the mapping that Elasticsearch actually stored.
func (s *Sink) ensureIndex(meta *tableMeta) error {
	derived, err := deriveProperties(meta.table)
	if err != nil {
		return err
	}
	plan, err := buildCreatePlan(s.cfg.Mapping, derived)
	if err != nil {
		return fmt.Errorf("elasticsearch: table %s: %w", meta.scope, err)
	}
	if err := checkCompatibility(meta.index, meta.table, plan.Props, plan.Dynamic); err != nil {
		return err
	}

	ctx, cancel := s.requestContext()
	exists, err := s.client.IndexExists(ctx, meta.index)
	cancel()
	if err != nil {
		return fmt.Errorf("elasticsearch: index %q: check existence: %w", meta.index, err)
	}
	if exists {
		return s.verifyMapping(meta)
	}
	if !s.cfg.AutoCreate {
		return fmt.Errorf(
			"elasticsearch: index %q for table %s does not exist and sink.auto-create is false",
			meta.index, meta.scope)
	}
	ctx, cancel = s.requestContext()
	err = s.client.CreateIndex(ctx, meta.index, plan.Body)
	cancel()
	if err != nil && !client.IsType(err, "resource_already_exists_exception") {
		return fmt.Errorf("elasticsearch: index %q: create: %w", meta.index, err)
	}
	// A concurrent creator wins the race; the mapping check below decides.
	return s.verifyMapping(meta)
}

func (s *Sink) verifyMapping(meta *tableMeta) error {
	ctx, cancel := s.requestContext()
	mappings, err := s.client.GetMapping(ctx, meta.index)
	cancel()
	if err != nil {
		return fmt.Errorf("elasticsearch: index %q: read mapping: %w", meta.index, err)
	}
	props, rootDynamic, err := propertiesFromMappings(mappings)
	if err != nil {
		return fmt.Errorf("elasticsearch: index %q: %w", meta.index, err)
	}
	return checkCompatibility(meta.index, meta.table, props, rootDynamic)
}

func (s *Sink) requestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.timeout)
}

// writeContext bounds one Write including every retry and backoff wait.
func (s *Sink) writeContext() (context.Context, context.CancelFunc) {
	attempts := s.maxRetries + 1
	budget := time.Duration(attempts)*s.timeout + retryBudget(attempts)
	return context.WithTimeout(context.Background(), budget)
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// validateIndexName re-checks the final index name so a programmatic config
// cannot bypass the parser rules.
func validateIndexName(name string) error {
	switch name {
	case "":
		return errors.New("must not be empty")
	case ".", "..":
		return errors.New(`must not be "." or ".."`)
	}
	if len(name) > 255 {
		return errors.New("must not exceed 255 bytes")
	}
	if name != strings.ToLower(name) {
		return errors.New("must be lowercase")
	}
	if strings.HasPrefix(name, "_") || strings.HasPrefix(name, "-") || strings.HasPrefix(name, "+") {
		return fmt.Errorf("must not start with %q", name[:1])
	}
	if strings.ContainsAny(name, `\/*?"<>| ,#:`) {
		return errors.New(`must not contain a reserved character (\ / * ? " < > | space , # :)`)
	}
	return nil
}

// safeAddresses renders configured hosts without embedded credentials.
func safeAddresses(hosts []string) string {
	out := make([]string, 0, len(hosts))
	for _, host := range hosts {
		parsed, err := url.Parse(host)
		if err != nil || parsed.Host == "" {
			out = append(out, "<invalid>")
			continue
		}
		parsed.User = nil
		out = append(out, parsed.String())
	}
	return strings.Join(out, ",")
}

var _ sink.Sink = (*Sink)(nil)
