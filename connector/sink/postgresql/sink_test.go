package postgresql

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
)

type fakeResults struct {
	execErrs []error
	closeErr error
	execs    int
	closed   bool
}

func (r *fakeResults) Exec() (pgconn.CommandTag, error) {
	index := r.execs
	r.execs++
	if index < len(r.execErrs) && r.execErrs[index] != nil {
		return pgconn.CommandTag{}, r.execErrs[index]
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (r *fakeResults) Query() (pgx.Rows, error) { panic("unused") }

func (r *fakeResults) QueryRow() pgx.Row { panic("unused") }

func (r *fakeResults) Close() error {
	r.closed = true
	return r.closeErr
}

type fakeTx struct {
	results     *fakeResults
	commitErr   error
	rollbackErr error
	queued      int
	sqls        []string
	paramCount  int
	committed   bool
	rolledBack  bool
	// The rollback context is snapshotted at call time: the sink cancels it on
	// return, so reading its state afterwards would always report a cancellation.
	rollbackCtxErr      error
	rollbackDeadline    time.Time
	rollbackHasDeadline bool
}

func (f *fakeTx) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	f.queued = batch.Len()
	for _, item := range batch.QueuedQueries {
		f.sqls = append(f.sqls, item.SQL)
		f.paramCount += len(item.Arguments)
	}
	return f.results
}

func (f *fakeTx) Commit(context.Context) error {
	if f.commitErr != nil {
		return f.commitErr
	}
	f.committed = true
	return nil
}

func (f *fakeTx) Rollback(ctx context.Context) error {
	f.rolledBack = true
	f.rollbackCtxErr = ctx.Err()
	f.rollbackDeadline, f.rollbackHasDeadline = ctx.Deadline()
	return f.rollbackErr
}

type fakePool struct {
	pingErr     error
	beginErr    error
	rollbackErr error
	// commitErr fails the COMMIT of every transaction this pool opens, which is
	// how a test shows a commit whose result the client never learned.
	commitErr error
	// onBegin runs before the context check, so a test can cancel the lifecycle
	// context while a transaction is being opened.
	onBegin func()
	txs     []*fakeTx
	begins  int
	pings   int
	closed  bool
	// resultsFor builds the batch results of each new transaction.
	resultsFor func(attempt int) *fakeResults
}

func (f *fakePool) Begin(ctx context.Context) (tx, error) {
	f.begins++
	if f.onBegin != nil {
		f.onBegin()
	}
	// pgxpool refuses to start work on a canceled context.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.beginErr != nil {
		return nil, f.beginErr
	}
	results := &fakeResults{}
	if f.resultsFor != nil {
		results = f.resultsFor(f.begins)
	}
	handle := &fakeTx{results: results, commitErr: f.commitErr, rollbackErr: f.rollbackErr}
	f.txs = append(f.txs, handle)
	return handle, nil
}

func (f *fakePool) Ping(ctx context.Context) error {
	f.pings++
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.pingErr
}

func (f *fakePool) Close() {
	f.closed = true
}

func newTestSink(p pool, maxRetries int) *Sink {
	return newTestSinkWithCatalogue(p, maxRetries, &fakeQuerier{})
}

// newTestSinkWithCatalogue builds a sink whose target catalogue reads answer
// with the rows a test prepares, which is how a DDL test shows the table as it
// looks before and after the statement the sink runs.
func newTestSinkWithCatalogue(p pool, maxRetries int, q *fakeQuerier) *Sink {
	return &Sink{
		cfg:       definition.Sink{Schema: "public", MaxRetries: maxRetries},
		builder:   NewBuilder("public"),
		pool:      p,
		meta:      newMetadata(q),
		opTimeout: time.Second,
		sleep:     func(context.Context, time.Duration) error { return nil },
		baseCtx:   context.Background(),
		log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func pgError(code string) error {
	return &pgconn.PgError{Code: code, Message: "boom"}
}

func TestWriteEmptyBatchDoesNotOpenTransaction(t *testing.T) {
	p := &fakePool{}
	sink := newTestSink(p, 3)
	if err := sink.Write(nil); err != nil {
		t.Fatal(err)
	}
	if p.begins != 0 {
		t.Fatalf("begins %d", p.begins)
	}
}

func TestWriteCommitsOneTransaction(t *testing.T) {
	p := &fakePool{}
	sink := newTestSink(p, 3)
	batch := []event.Event{createEvent(), createEvent()}
	if err := sink.Write(batch); err != nil {
		t.Fatal(err)
	}
	if p.begins != 1 {
		t.Fatalf("begins %d", p.begins)
	}
	handle := p.txs[0]
	if !handle.committed || handle.rolledBack {
		t.Fatalf("committed=%v rolledBack=%v", handle.committed, handle.rolledBack)
	}
	if handle.queued != 2 {
		t.Fatalf("queued %d", handle.queued)
	}
	if !handle.results.closed {
		t.Fatal("batch results not closed")
	}
}

func TestWriteRollsBackAndNamesTable(t *testing.T) {
	p := &fakePool{resultsFor: func(int) *fakeResults {
		return &fakeResults{execErrs: []error{nil, pgError("23502")}}
	}}
	sink := newTestSink(p, 3)
	err := sink.Write([]event.Event{createEvent(), createEvent(), createEvent()})
	if err == nil {
		t.Fatal("expected error")
	}
	if p.begins != 1 {
		t.Fatalf("permanent error must not retry: begins %d", p.begins)
	}
	if !p.txs[0].rolledBack {
		t.Fatal("transaction not rolled back")
	}
	msg := err.Error()
	if !strings.Contains(msg, "c public.users") || !strings.Contains(msg, "23502") {
		t.Fatalf("error %q", msg)
	}
}

func TestWriteRollsBackWhenBatchCloseFails(t *testing.T) {
	p := &fakePool{resultsFor: func(int) *fakeResults {
		return &fakeResults{closeErr: errors.New("close failed")}
	}}
	sink := newTestSink(p, 0)
	if err := sink.Write([]event.Event{createEvent()}); err == nil {
		t.Fatal("expected error")
	}
	if !p.txs[0].rolledBack {
		t.Fatal("transaction not rolled back")
	}
}

// TestRollbackContextIsBoundedAndIgnoresCancellation pins the two properties the
// rollback context must have: a cancelled operation context (shutdown, or an
// expired operation deadline) must not reach the rollback, and the rollback must
// still carry a finite deadline so a connection that stopped answering cannot
// block the process exit forever.
func TestRollbackContextIsBoundedAndIgnoresCancellation(t *testing.T) {
	p := &fakePool{}
	sink := newTestSink(p, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink.baseCtx = ctx
	p.resultsFor = func(int) *fakeResults {
		// The signal arrives while the batch is on the wire, so the operation
		// context is already cancelled when the transaction is rolled back.
		cancel()
		return &fakeResults{execErrs: []error{context.Canceled}}
	}
	if err := sink.Write([]event.Event{createEvent()}); err == nil {
		t.Fatal("expected error")
	}
	handle := p.txs[0]
	if !handle.rolledBack {
		t.Fatal("transaction not rolled back")
	}
	if handle.rollbackCtxErr != nil {
		t.Fatalf("rollback context was already done: %v", handle.rollbackCtxErr)
	}
	if !handle.rollbackHasDeadline {
		t.Fatal("rollback context carries no deadline")
	}
	if remaining := time.Until(handle.rollbackDeadline); remaining <= 0 || remaining > rollbackTimeout {
		t.Fatalf("rollback deadline in %s, want (0, %s]", remaining, rollbackTimeout)
	}
}

// TestRollbackFailureKeepsTheOriginalDMLError proves a failing or expiring rollback
// does not replace the statement error the caller sees. The retry policy is a
// separate contract, covered by TestRetryPolicySurvivesRollbackFailure with a
// SQLSTATE that is actually retryable.
func TestRollbackFailureKeepsTheOriginalDMLError(t *testing.T) {
	p := &fakePool{
		rollbackErr: context.DeadlineExceeded,
		resultsFor: func(int) *fakeResults {
			return &fakeResults{execErrs: []error{pgError("23502")}}
		},
	}
	sink := newTestSink(p, 3)
	err := sink.Write([]event.Event{createEvent()})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "23502") {
		t.Fatalf("error %q lost the statement failure", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error %q reports the rollback failure", err)
	}
	if !p.txs[0].rolledBack {
		t.Fatal("transaction not rolled back")
	}
}

// TestRetryPolicySurvivesRollbackFailure pins the retry contract: the decision to
// retry comes from the statement error alone, so a batch that fails with a
// transient SQLSTATE is still retried to max-retries+1 attempts even when every
// rollback fails, and the rollback failure never reaches the caller.
func TestRetryPolicySurvivesRollbackFailure(t *testing.T) {
	rollbackFailure := errors.New("rollback aborted")
	p := &fakePool{
		rollbackErr: rollbackFailure,
		resultsFor: func(int) *fakeResults {
			return &fakeResults{execErrs: []error{pgError("40001")}}
		},
	}
	sink := newTestSink(p, 1)
	err := sink.Write([]event.Event{createEvent()})
	if err == nil {
		t.Fatal("expected error")
	}
	if p.begins != 2 {
		t.Fatalf("attempts %d, want max-retries+1", p.begins)
	}
	for i, handle := range p.txs {
		if !handle.rolledBack {
			t.Fatalf("transaction %d not rolled back", i)
		}
	}
	if !strings.Contains(err.Error(), "40001") {
		t.Fatalf("error %q lost the statement failure", err)
	}
	if strings.Contains(err.Error(), rollbackFailure.Error()) {
		t.Fatalf("error %q reports the rollback failure", err)
	}
}

func TestWriteRetriesSerializationFailureThenSucceeds(t *testing.T) {
	p := &fakePool{resultsFor: func(attempt int) *fakeResults {
		if attempt == 1 {
			return &fakeResults{execErrs: []error{pgError("40001")}}
		}
		return &fakeResults{}
	}}
	sink := newTestSink(p, 3)
	if err := sink.Write([]event.Event{createEvent()}); err != nil {
		t.Fatal(err)
	}
	if p.begins != 2 {
		t.Fatalf("begins %d", p.begins)
	}
	if !p.txs[0].rolledBack || !p.txs[1].committed {
		t.Fatalf("first rolled back=%v second committed=%v", p.txs[0].rolledBack, p.txs[1].committed)
	}
}

func TestWriteStopsAfterMaxRetries(t *testing.T) {
	p := &fakePool{resultsFor: func(int) *fakeResults {
		return &fakeResults{execErrs: []error{pgError("40P01")}}
	}}
	sink := newTestSink(p, 2)
	var delays []time.Duration
	sink.sleep = func(_ context.Context, d time.Duration) error {
		delays = append(delays, d)
		return nil
	}
	err := sink.Write([]event.Event{createEvent()})
	if err == nil {
		t.Fatal("expected error")
	}
	if p.begins != 3 {
		t.Fatalf("attempts %d, want max-retries+1", p.begins)
	}
	if len(delays) != 2 || delays[0] != backoffInitial || delays[1] != 2*backoffInitial {
		t.Fatalf("delays %#v", delays)
	}
}

func TestWriteDoesNotRetryPermanentError(t *testing.T) {
	p := &fakePool{resultsFor: func(int) *fakeResults {
		return &fakeResults{execErrs: []error{pgError("42P10")}}
	}}
	sink := newTestSink(p, 5)
	if err := sink.Write([]event.Event{createEvent()}); err == nil {
		t.Fatal("expected error")
	}
	if p.begins != 1 {
		t.Fatalf("begins %d", p.begins)
	}
}

func TestWriteDoesNotRetryCommitError(t *testing.T) {
	p := &fakePool{}
	sink := newTestSink(p, 5)
	// A commit failure must exit even when the SQLSTATE looks transient.
	sink.pool = &commitFailingPool{inner: p}
	if err := sink.Write([]event.Event{createEvent()}); err == nil {
		t.Fatal("expected error")
	}
	if p.begins != 1 {
		t.Fatalf("commit error retried: begins %d", p.begins)
	}
	if !p.txs[0].rolledBack {
		t.Fatal("transaction not rolled back")
	}
}

// commitFailingPool fails every commit with a serialization-failure SQLSTATE to
// prove commit errors are never retried.
type commitFailingPool struct {
	inner *fakePool
}

func (c *commitFailingPool) Begin(ctx context.Context) (tx, error) {
	handle, err := c.inner.Begin(ctx)
	if err != nil {
		return nil, err
	}
	fake := handle.(*fakeTx)
	fake.commitErr = pgError("40001")
	return fake, nil
}

func (c *commitFailingPool) Ping(ctx context.Context) error { return c.inner.Ping(ctx) }

func (c *commitFailingPool) Close() { c.inner.Close() }

func TestWriteStopsRetryingWhenContextCanceled(t *testing.T) {
	p := &fakePool{resultsFor: func(int) *fakeResults {
		return &fakeResults{execErrs: []error{pgError("40001")}}
	}}
	sink := newTestSink(p, 5)
	sink.sleep = func(context.Context, time.Duration) error { return context.Canceled }
	err := sink.Write([]event.Event{createEvent()})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v", err)
	}
	if p.begins != 1 {
		t.Fatalf("begins %d", p.begins)
	}
}

func TestWriteAfterCloseFails(t *testing.T) {
	p := &fakePool{}
	sink := newTestSink(p, 1)
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write([]event.Event{createEvent()}); err == nil {
		t.Fatal("expected error")
	}
	if !p.closed {
		t.Fatal("pool not closed")
	}
}

func TestOpenPingFailureReleasesPool(t *testing.T) {
	p := &fakePool{pingErr: errors.New("refused")}
	sink := newTestSink(p, 1)
	sink.cfg.ConnectTimeout = time.Second
	if err := sink.Open(context.Background()); err == nil {
		t.Fatal("expected error")
	}
	if !p.closed {
		t.Fatal("pool leaked after failed ping")
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestOpenAbortsWhenShutdownBeforeConnect keeps Ctrl-C during startup from
// waiting for the connect timeout.
func TestOpenAbortsWhenShutdownBeforeConnect(t *testing.T) {
	p := &fakePool{}
	sink := newTestSink(p, 1)
	sink.cfg.ConnectTimeout = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := sink.Open(ctx)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v", err)
	}
	if !p.closed {
		t.Fatal("pool leaked after canceled ping")
	}
}

func TestOpenPingsAndAppliesLifecycleContext(t *testing.T) {
	p := &fakePool{}
	sink := newTestSink(p, 1)
	sink.cfg.ConnectTimeout = time.Second
	if err := sink.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.pings != 1 {
		t.Fatalf("pings %d", p.pings)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// TestWriteAbortsOnCanceledLifecycleContext covers a shutdown that lands while a
// batch is in flight: the transaction fails, the failure is not retried, and the
// error still answers errors.Is(context.Canceled) so the runner reports a
// stopped run instead of a broken pipeline.
func TestWriteAbortsOnCanceledLifecycleContext(t *testing.T) {
	p := &fakePool{}
	sink := newTestSink(p, 3)
	ctx, cancel := context.WithCancel(context.Background())
	sink.baseCtx = ctx
	// The signal arrives while the transaction is being opened.
	p.onBegin = cancel
	err := sink.Write([]event.Event{createEvent()})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v", err)
	}
	if p.begins != 1 {
		t.Fatalf("cancellation was retried: begins %d", p.begins)
	}
}

// TestWriteRollsBackWhenShutdownCancelsBatch covers the signal landing after the
// transaction opened: the batch fails, the transaction rolls back, and the
// failure is neither retried nor reported as a committed write.
func TestWriteRollsBackWhenShutdownCancelsBatch(t *testing.T) {
	p := &fakePool{}
	sink := newTestSink(p, 3)
	ctx, cancel := context.WithCancel(context.Background())
	sink.baseCtx = ctx
	p.resultsFor = func(int) *fakeResults {
		// The signal arrives while the batch is on the wire.
		cancel()
		return &fakeResults{execErrs: []error{context.Canceled}}
	}
	err := sink.Write([]event.Event{createEvent(), createEvent()})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v", err)
	}
	if p.begins != 1 {
		t.Fatalf("cancellation was retried: begins %d", p.begins)
	}
	if !p.txs[0].rolledBack || p.txs[0].committed {
		t.Fatalf("rolledBack=%v committed=%v", p.txs[0].rolledBack, p.txs[0].committed)
	}
}

// TestWriteDrainsPendingBatchAfterShutdown pins the runner contract: once the
// signal has cancelled the lifecycle context, the one Write that can still
// arrive is the shutdown drain, and it must commit so the checkpoint advances.
func TestWriteDrainsPendingBatchAfterShutdown(t *testing.T) {
	p := &fakePool{}
	sink := newTestSink(p, 1)
	ctx, cancel := context.WithCancel(context.Background())
	sink.baseCtx = ctx
	cancel()
	if err := sink.Write([]event.Event{createEvent()}); err != nil {
		t.Fatalf("drain write: %v", err)
	}
	if p.begins != 1 || p.txs[0].rolledBack || !p.txs[0].committed {
		t.Fatalf("begins=%d rolledBack=%v committed=%v", p.begins, p.txs[0].rolledBack, p.txs[0].committed)
	}
}

func TestRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "serialization failure", err: pgError("40001"), want: true},
		{name: "deadlock", err: pgError("40P01"), want: true},
		{name: "admin shutdown", err: pgError("57P01"), want: true},
		{name: "cannot connect now", err: pgError("57P03"), want: true},
		{name: "connection exception", err: pgError("08006"), want: true},
		{name: "unique violation", err: pgError("23505"), want: false},
		{name: "undefined column", err: pgError("42703"), want: false},
		{name: "no conflict arbiter", err: pgError("42P10"), want: false},
		{name: "wrapped permanent", err: errors.Join(errors.New("wrap"), pgError("22P02")), want: false},
		{name: "plain error", err: errors.New("dial tcp: refused"), want: false},
		{name: "commit error", err: &commitError{err: pgError("40001")}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryable(tc.err); got != tc.want {
				t.Fatalf("retryable(%v) = %v want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestDataSourceName(t *testing.T) {
	cfg := definition.Sink{
		Hostname: "db.internal", Port: 5433, Username: "cdc",
		Password: "p@ss:w/rd", Database: "target", SSLMode: "require",
	}
	got := dataSourceName(cfg)
	want := "postgres://cdc:p%40ss%3Aw%2Frd@db.internal:5433/target?sslmode=require"
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestDataSourceNameIPv6Host(t *testing.T) {
	cfg := definition.Sink{Hostname: "::1", Port: 5432, Username: "u", Password: "p", Database: "d", SSLMode: "prefer"}
	got := dataSourceName(cfg)
	if !strings.Contains(got, "@[::1]:5432/") {
		t.Fatalf("got %s", got)
	}
}

func TestPoolConfigDefaults(t *testing.T) {
	cfg := definition.Sink{
		Hostname: "h", Port: 5432, Username: "u", Password: "p", Database: "d",
		SSLMode: "prefer", ConnectTimeout: 5 * time.Second,
	}
	poolCfg, err := poolConfig(cfg, "Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	if poolCfg.MaxConns != 1 || poolCfg.MinConns != 0 {
		t.Fatalf("conns %d/%d", poolCfg.MaxConns, poolCfg.MinConns)
	}
	if poolCfg.ConnConfig.ConnectTimeout != 5*time.Second {
		t.Fatalf("connect timeout %s", poolCfg.ConnConfig.ConnectTimeout)
	}
	// A hand-built config without connect-timeout must not expire immediately.
	zero := cfg
	zero.ConnectTimeout = 0
	zeroCfg, err := poolConfig(zero, "")
	if err != nil {
		t.Fatal(err)
	}
	if zeroCfg.ConnConfig.ConnectTimeout != defaultConnectTimeout {
		t.Fatalf("default connect timeout %s", zeroCfg.ConnConfig.ConnectTimeout)
	}
	if poolCfg.ConnConfig.RuntimeParams["TimeZone"] != "Asia/Shanghai" {
		t.Fatalf("runtime params %#v", poolCfg.ConnConfig.RuntimeParams)
	}
	if _, err := poolConfig(cfg, ""); err != nil {
		t.Fatalf("empty time zone must be skipped: %v", err)
	}
	if _, err := poolConfig(cfg, "bad zone;drop"); err == nil {
		t.Fatal("expected invalid time zone error")
	}
}

func TestValidateTimeZone(t *testing.T) {
	for _, zone := range []string{"UTC", "Asia/Shanghai", "Etc/GMT+8", "Europe/Paris", "GMT-3"} {
		if err := validateTimeZone(zone); err != nil {
			t.Fatalf("%s: %v", zone, err)
		}
	}
	for _, zone := range []string{"Asia Shanghai", "UTC;", strings.Repeat("a", 65)} {
		if err := validateTimeZone(zone); err == nil {
			t.Fatalf("%q accepted", zone)
		}
	}
}

// TestCreateTableAndApplySchemaRefuseByDefault is the PR2 default-on-safety
// contract: neither creating a table nor applying a column change happens unless
// the operator asked for it, and the refusal names the option to turn on.
func TestCreateTableAndApplySchemaRefuseByDefault(t *testing.T) {
	sink := newTestSink(&fakePool{}, 0)
	table := schema.Table{Database: "public", Table: "users",
		Columns:     []schema.Column{{Name: "id", Type: "bigint", Nullable: false}},
		PrimaryKeys: []string{"id"}}
	err := sink.CreateTable(table)
	if err == nil || !strings.Contains(err.Error(), "auto-create-table is false") {
		t.Fatalf("CreateTable without the option: %v", err)
	}
	change := schema.Change{Action: schema.ActionAddColumn, Database: "public", Table: "users",
		Column: "note", DDL: "ALTER TABLE users ADD COLUMN note text"}
	err = sink.ApplySchema(change)
	if err == nil || !strings.Contains(err.Error(), "schema changes are disabled") {
		t.Fatalf("ApplySchema with the switch off: %v", err)
	}
	if err := sink.Flush(); err != nil {
		t.Fatal(err)
	}
}

// TestCreateTableValidatesAnExistingTarget covers the case that must keep
// working when the option is off: a hand-made table the sink can write to is
// accepted, so a snapshot pipeline that only reads DML never depends on
// auto-create-table.
func TestCreateTableValidatesAnExistingTarget(t *testing.T) {
	querier := &fakeQuerier{
		columns: [][]any{columnRow("id", "bigint", true, "", false)},
		keys:    [][]any{{"users_pkey", "p", "id"}},
	}
	sink := newTestSinkWithCatalogue(&fakePool{}, 0, querier)
	table := schema.Table{Database: "public", Table: "users",
		Columns:     []schema.Column{{Name: "id", Type: "bigint", Nullable: false}},
		PrimaryKeys: []string{"id"}}
	if err := sink.CreateTable(table); err != nil {
		t.Fatal(err)
	}
	table.Columns[0].Name = "missing"
	if err := sink.CreateTable(table); err == nil {
		t.Fatal("a target that cannot hold the rows was accepted")
	}
}

// TestCreateTableWithoutSchemaNamesTarget proves the refusal is diagnosable: the
// operator sees which table is missing rather than a nil-map style failure.
func TestCreateTableWithoutSchemaNamesTarget(t *testing.T) {
	sink := newTestSink(&fakePool{}, 0)
	err := sink.CreateTable(schema.Table{Table: "users"})
	if err == nil || !strings.Contains(err.Error(), "public.users") {
		t.Fatalf("err %v", err)
	}
}
