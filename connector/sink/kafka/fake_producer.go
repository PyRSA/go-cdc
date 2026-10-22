package kafka

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// FakeProducer is a test double for the franz-go producer (unit / composer tests).
type FakeProducer struct {
	mu           sync.Mutex
	Records      []*kgo.Record
	Fail         error // returned to Produce promise
	FlushErr     error
	Flushed      bool
	Closed       bool
	LastFlushCtx context.Context

	// BlockFlushUntilCancel makes Flush wait until ctx is done (or safetyCap),
	// then return ctx.Err(). Used to verify sink flush timeouts.
	BlockFlushUntilCancel bool
	// FlushStarted is closed once when a blocking Flush enters (optional).
	FlushStarted chan struct{}
	// safetyCap bounds a blocking Flush if ctx is never cancelled (tests must not hang).
	safetyCap time.Duration
}

// Produce records the message and invokes promise with Fail (may be nil).
func (f *FakeProducer) Produce(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	f.mu.Lock()
	f.Records = append(f.Records, r)
	err := f.Fail
	f.mu.Unlock()
	if promise != nil {
		promise(r, err)
	}
}

// Flush marks Flushed, records ctx, and returns FlushErr (or ctx.Err() when blocking).
func (f *FakeProducer) Flush(ctx context.Context) error {
	f.mu.Lock()
	f.Flushed = true
	f.LastFlushCtx = ctx
	block := f.BlockFlushUntilCancel
	err := f.FlushErr
	started := f.FlushStarted
	f.FlushStarted = nil
	capDur := f.safetyCap
	f.mu.Unlock()
	if started != nil {
		close(started)
	}
	if !block {
		return err
	}
	if capDur <= 0 {
		capDur = 30 * time.Second
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(capDur):
		return errors.New("fake flush: safety cap exceeded (ctx not cancelled)")
	}
}

// Close marks Closed.
func (f *FakeProducer) Close() {
	f.mu.Lock()
	f.Closed = true
	f.mu.Unlock()
}

// AttachFake installs f as the sink producer (skips dial). For tests only.
func (s *Sink) AttachFake(f *FakeProducer) {
	s.setProducerForTest(f)
}
