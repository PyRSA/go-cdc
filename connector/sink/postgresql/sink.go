// Package postgresql is the PostgreSQL sink: row-level DML applied in one
// transaction per batch. This release does not create tables, does not map
// source types, and rejects schema changes at startup.
package postgresql

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/sink"
)

const (
	// defaultOpTimeout bounds one transaction. Sink.Write takes no context, so
	// the deadline comes from the sink instead of the caller.
	defaultOpTimeout = 30 * time.Second
	// defaultConnectTimeout covers hand-built configs; parse.go already defaults
	// connect-timeout to 30s.
	defaultConnectTimeout = 30 * time.Second
	backoffInitial        = 100 * time.Millisecond
	backoffMax            = 2 * time.Second
	// rollbackTimeout bounds the single "rollback" round trip. A rollback is one
	// control statement rather than a batch, so it gets a fraction of the 30s
	// transaction budget and cannot hold up process exit for as long as a whole
	// transaction may take. When it expires pgx hard-closes the connection, which
	// makes the server abort the open transaction on disconnect.
	rollbackTimeout = 5 * time.Second
)

// tx is the part of pgx.Tx the sink uses.
type tx interface {
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// pool is the part of *pgxpool.Pool the sink uses. Narrowing it keeps the retry
// and rollback paths testable without a server.
type pool interface {
	Begin(ctx context.Context) (tx, error)
	Ping(ctx context.Context) error
	Close()
}

type pgPool struct {
	inner *pgxpool.Pool
}

func (p pgPool) Begin(ctx context.Context) (tx, error) {
	return p.inner.Begin(ctx)
}

func (p pgPool) Ping(ctx context.Context) error {
	return p.inner.Ping(ctx)
}

func (p pgPool) Close() {
	p.inner.Close()
}

// sleepFunc waits for d or until ctx is done.
type sleepFunc func(ctx context.Context, d time.Duration) error

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

// Sink writes row-level DML to PostgreSQL. One Write call is one transaction;
// the runner calls Write serially, so the sink needs no write lock of its own.
type Sink struct {
	cfg       definition.Sink
	builder   *Builder
	pool      pool
	opTimeout time.Duration
	sleep     sleepFunc

	baseCtx context.Context
	cancel  context.CancelFunc

	mu     sync.Mutex
	closed bool
}

// Open builds the connection pool from pipeline config. It does not connect;
// Open(ctx) does. timeZone is the source server-time-zone, applied as the
// session TimeZone so strings the source already rendered keep their meaning.
func Open(cfg definition.Sink, timeZone string) (*Sink, error) {
	poolCfg, err := poolConfig(cfg, timeZone)
	if err != nil {
		return nil, err
	}
	p, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, fmt.Errorf("sink.postgresql: %w", err)
	}
	return &Sink{
		cfg:       cfg,
		builder:   NewBuilder(cfg.Schema),
		pool:      pgPool{inner: p},
		opTimeout: defaultOpTimeout,
		sleep:     sleepContext,
		baseCtx:   context.Background(),
	}, nil
}

// Open verifies the connection and records the sink lifecycle context. The
// caller's context is the process signal context: it aborts the first ping, an
// in-flight batch, and the retry backoff, while Close stays the only way to
// stop writes for good.
func (s *Sink) Open(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.baseCtx, s.cancel = context.WithCancel(ctx)
	pingCtx, cancel := context.WithTimeout(s.baseCtx, connectTimeout(s.cfg))
	defer cancel()
	if err := s.pool.Ping(pingCtx); err != nil {
		// Release the pool and the lifecycle context so a failed Open leaves
		// nothing behind; Close stays idempotent after it.
		s.closePool()
		s.cancel()
		return fmt.Errorf("sink.postgresql: connect %s:%d/%s: %w", s.cfg.Hostname, s.cfg.Port, s.cfg.Database, err)
	}
	return nil
}

// CreateTable always fails: this release requires pre-created tables. The
// runner rejects DDL for this sink before calling it.
func (s *Sink) CreateTable(table schema.Table) error {
	return fmt.Errorf("sink.postgresql: create table is not supported in this release (table %s.%s)", table.Database, table.Table)
}

// ApplySchema always fails: this release does not apply schema changes. The
// runner rejects DDL for this sink before calling it.
func (s *Sink) ApplySchema(change schema.Change) error {
	return fmt.Errorf("sink.postgresql: schema change is not supported in this release (table %s.%s)", change.Database, change.Table)
}

// Write applies the batch in one transaction, retrying transient SQLSTATEs.
func (s *Sink) Write(batch []event.Event) error {
	if len(batch) == 0 {
		return nil
	}
	statements, err := s.statements(batch)
	if err != nil {
		return err
	}
	p := s.currentPool()
	if p == nil {
		return fmt.Errorf("sink.postgresql: write after close")
	}
	attempts := s.cfg.MaxRetries + 1
	if attempts < 1 {
		attempts = 1
	}
	ctx := s.writeContext()
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			if err := s.sleep(ctx, backoffDelay(attempt)); err != nil {
				return fmt.Errorf("sink.postgresql: retry aborted: %w", err)
			}
		}
		lastErr = s.writeOnce(ctx, p, statements)
		if lastErr == nil {
			return nil
		}
		// A shutdown that lands mid-batch cancels ctx. Report the cancellation so
		// the caller treats the run as stopped rather than failed: the transaction
		// was rolled back and the checkpoint keeps its previous position.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("sink.postgresql: write aborted by shutdown: %w (last error: %v)", ctxErr, lastErr)
		}
		if !retryable(lastErr) {
			return lastErr
		}
	}
	return lastErr
}

// writeContext returns the context of one Write call. A cancelled parent means
// the process is stopping, and the only write that can still arrive is the
// runner's shutdown drain of the pending batch. That drain must not inherit the
// cancellation, or it would fail and the checkpoint would stay behind; it is
// still bounded by the per-transaction timeout. Close drops the pool first, so
// a write after Close never reaches this point.
func (s *Sink) writeContext() context.Context {
	if s.baseCtx == nil {
		return context.Background()
	}
	if s.baseCtx.Err() != nil {
		return context.WithoutCancel(s.baseCtx)
	}
	return s.baseCtx
}

// Flush is a no-op: Write already commits one transaction per batch.
func (s *Sink) Flush() error {
	return nil
}

// Close releases the pool and cancels in-flight operations.
func (s *Sink) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	p := s.pool
	s.pool = nil
	s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
	if p != nil {
		p.Close()
	}
	return nil
}

// closePool releases the pool once and is safe after a failed Open.
func (s *Sink) closePool() {
	s.mu.Lock()
	p := s.pool
	s.pool = nil
	s.mu.Unlock()
	if p != nil {
		p.Close()
	}
}

func (s *Sink) currentPool() pool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pool
}

func (s *Sink) statements(batch []event.Event) ([]Statement, error) {
	out := make([]Statement, 0, len(batch))
	for _, item := range batch {
		rendered, err := s.builder.Statements(item)
		if err != nil {
			return nil, err
		}
		out = append(out, rendered...)
	}
	return out, nil
}

// writeOnce runs every statement of the batch inside one transaction. A failing
// statement rolls the whole transaction back so a retry cannot leave half a
// batch applied.
func (s *Sink) writeOnce(ctx context.Context, p pool, statements []Statement) error {
	opCtx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	beginTx, err := p.Begin(opCtx)
	if err != nil {
		return fmt.Errorf("sink.postgresql: begin transaction: %w", err)
	}
	batch := &pgx.Batch{}
	for _, statement := range statements {
		batch.Queue(statement.SQL, statement.Args...)
	}
	results := beginTx.SendBatch(opCtx, batch)
	var execErr error
	for i := range statements {
		if _, err := results.Exec(); err != nil && execErr == nil {
			execErr = fmt.Errorf("sink.postgresql: %s %s: %w", statements[i].Op, statements[i].Table, err)
		}
	}
	if closeErr := results.Close(); closeErr != nil && execErr == nil {
		execErr = fmt.Errorf("sink.postgresql: close batch: %w", closeErr)
	}
	if execErr != nil {
		// The rollback must run even when the operation deadline already fired.
		rollback(beginTx, opCtx)
		return execErr
	}
	if err := beginTx.Commit(opCtx); err != nil {
		rollback(beginTx, opCtx)
		return &commitError{err: fmt.Errorf("sink.postgresql: commit transaction: %w", err)}
	}
	return nil
}

// rollback abandons t with a bounded context. opCtx cannot be reused as is: it may
// already be cancelled (shutdown) or past its deadline (a slow batch).
// context.WithoutCancel alone is not enough either, because it drops the deadline
// along with the cancellation and pgx bounds a round trip only through the context
// it watches; an unbounded rollback would wait forever on a connection that stopped
// answering.
func rollback(t tx, opCtx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(opCtx), rollbackTimeout)
	defer cancel()
	_ = t.Rollback(ctx)
}

// commitError marks a failed COMMIT. The transaction may or may not be durable,
// so the sink must not retry: the job exits and replays from the last checkpoint.
type commitError struct {
	err error
}

func (e *commitError) Error() string {
	return e.err.Error()
}

func (e *commitError) Unwrap() error {
	return e.err
}

// retryable reports whether the error may be retried as a whole transaction.
// Only SQLSTATEs with a transient meaning are retried; a failed COMMIT and any
// unknown error (including client-side encode and socket errors) exit the job.
func retryable(err error) bool {
	var committed *commitError
	if errors.As(err, &committed) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001", "40P01", "57P01", "57P03":
			return true
		}
		return strings.HasPrefix(pgErr.Code, "08")
	}
	return false
}

func backoffDelay(attempt int) time.Duration {
	delay := backoffInitial
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= backoffMax {
			return backoffMax
		}
	}
	if delay > backoffMax {
		return backoffMax
	}
	return delay
}

func poolConfig(cfg definition.Sink, timeZone string) (*pgxpool.Config, error) {
	poolCfg, err := pgxpool.ParseConfig(dataSourceName(cfg))
	if err != nil {
		// The DSN embeds the password, so the parser's message is never echoed.
		return nil, fmt.Errorf("sink.postgresql: invalid connection settings for %s:%d/%s", cfg.Hostname, cfg.Port, cfg.Database)
	}
	// The runner writes serially, so one connection is enough and mirrors the
	// MySQL sink's single idle connection.
	poolCfg.MaxConns = 1
	poolCfg.MinConns = 0
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnIdleTime = 10 * time.Minute
	poolCfg.ConnConfig.ConnectTimeout = connectTimeout(cfg)
	if zone := strings.TrimSpace(timeZone); zone != "" {
		if err := validateTimeZone(zone); err != nil {
			return nil, err
		}
		poolCfg.ConnConfig.RuntimeParams["TimeZone"] = zone
	}
	return poolCfg, nil
}

// connectTimeout returns the configured connect timeout or the default, so a
// zero value never turns Ping into an immediate timeout.
func connectTimeout(cfg definition.Sink) time.Duration {
	if cfg.ConnectTimeout > 0 {
		return cfg.ConnectTimeout
	}
	return defaultConnectTimeout
}

// dataSourceName builds a libpq URL. The password is URL-encoded, so it may
// contain any character.
func dataSourceName(cfg definition.Sink) string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(cfg.Username, cfg.Password),
		Host:   net.JoinHostPort(cfg.Hostname, strconv.Itoa(cfg.Port)),
		Path:   "/" + cfg.Database,
	}
	query := url.Values{}
	query.Set("sslmode", cfg.SSLMode)
	u.RawQuery = query.Encode()
	return u.String()
}

// validateTimeZone rejects characters that no valid PostgreSQL zone name uses.
func validateTimeZone(zone string) error {
	if len(zone) > 64 {
		return fmt.Errorf("sink.postgresql: server-time-zone is longer than 64 characters")
	}
	for _, r := range zone {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '_', r == '+', r == '-', r == '/', r == ':':
		default:
			return fmt.Errorf("sink.postgresql: server-time-zone %q contains an unsupported character", zone)
		}
	}
	return nil
}

var _ sink.Sink = (*Sink)(nil)
