package elasticsearch

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/PyRSA/go-cdc/connector/sink/elasticsearch/client"
)

const (
	// maxReportedFailures bounds how many items one error message lists.
	maxReportedFailures = 5
	// maxReasonChars bounds the Elasticsearch-supplied reason text.
	maxReasonChars = 200
	retryBaseDelay = 100 * time.Millisecond
	maxRetryDelay  = 2 * time.Second
)

// itemFailure keeps only diagnostic fields, never the document body.
type itemFailure struct {
	action  string
	index   string
	id      string
	status  int
	errType string
	reason  string
}

func (f itemFailure) String() string {
	return fmt.Sprintf("%s %s/%s status=%d type=%s reason=%q",
		f.action, f.index, f.id, f.status, f.errType, truncate(f.reason, maxReasonChars))
}

// bulkError reports failed bulk items after a bounded, read-only summary.
type bulkError struct {
	total    int
	attempts int
	failures []itemFailure
}

func (e *bulkError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "elasticsearch: bulk: %d of %d item(s) failed after %d attempt(s)",
		len(e.failures), e.total, e.attempts)
	shown := e.failures
	if len(shown) > maxReportedFailures {
		shown = shown[:maxReportedFailures]
	}
	for _, failure := range shown {
		b.WriteString("; ")
		b.WriteString(failure.String())
	}
	if extra := len(e.failures) - len(shown); extra > 0 {
		fmt.Fprintf(&b, "; and %d more", extra)
	}
	return b.String()
}

// bulk sends ops in as many requests as the byte budget requires. Batches go out
// in order and the first failure stops the call, so a rejected document leaves
// the runner checkpoint untouched and the whole Write is replayed.
//
// Every request gets its own time budget (see bulkBudget); ctx only carries
// cancellation, so splitting a Write never shortens the allowance of a request
// and no request waits forever.
func (s *Sink) bulk(ctx context.Context, ops []client.BulkOperation) error {
	batches, err := s.planBulk(ops)
	if err != nil {
		return err
	}
	for i, batch := range batches {
		batchCtx, cancel := context.WithTimeout(ctx, s.bulkBudget())
		err := s.sendBatch(batchCtx, batch)
		cancel()
		if err != nil {
			if len(batches) == 1 {
				return err
			}
			return fmt.Errorf("elasticsearch: bulk: request %d/%d: %w", i+1, len(batches), err)
		}
	}
	return nil
}

// planBulk encodes every operation once and groups the results so no request
// body exceeds the byte budget. Splitting must be decided on the encoded bytes
// (action line + document + newlines), because that is what the request carries.
// The count cap stays the runner's write-batch-size: splitting never merges
// requests, so it can only make a batch smaller. One operation over the budget
// cannot be split and fails here, before anything is sent.
func (s *Sink) planBulk(ops []client.BulkOperation) ([]client.BulkBatch, error) {
	limit := s.bulkMaxBytes
	batches := make([]client.BulkBatch, 0, 1)
	var current client.BulkBatch
	for _, op := range ops {
		encoded, err := client.EncodeBulkOperation(op)
		if err != nil {
			return nil, err
		}
		if len(encoded) > limit {
			return nil, fmt.Errorf(
				"elasticsearch: bulk: %s %s/%s needs %d bytes, over the %d-byte sink.bulk-max-bytes limit; a document cannot be split",
				op.Action, op.Index, op.ID, len(encoded), limit)
		}
		if len(current.Ops) > 0 && len(current.Body)+len(encoded) > limit {
			batches = append(batches, current)
			current = client.BulkBatch{}
		}
		current.Ops = append(current.Ops, op)
		current.Body = append(current.Body, encoded...)
	}
	if len(current.Ops) > 0 {
		batches = append(batches, current)
	}
	return batches, nil
}

// sendBatch sends one encoded batch and returns an error unless every item was
// acknowledged. Only 429, retryable 5xx, and transport failures are retried, and
// only the failed items are resent; index and delete are idempotent because the
// _id is derived from the primary key. A permanent rejection such as HTTP 413 is
// returned immediately: repeating the same oversized request cannot succeed.
func (s *Sink) sendBatch(ctx context.Context, batch client.BulkBatch) error {
	attempts := s.maxRetries + 1
	pending := batch
	var lastFailures []itemFailure
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("elasticsearch: bulk: %w", err)
		}
		if attempt > 1 {
			if err := s.sleep(ctx, retryDelay(attempt-1)); err != nil {
				return fmt.Errorf(
					"elasticsearch: bulk: wait before retry %d/%d: %w", attempt-1, s.maxRetries, err)
			}
		}
		result, err := s.client.Bulk(ctx, pending)
		if err != nil {
			if !retryableError(err) || attempt == attempts {
				return fmt.Errorf(
					"elasticsearch: bulk: %d item(s), attempt %d/%d: %w",
					len(pending.Ops), attempt, attempts, err)
			}
			continue
		}
		if len(result.Items) != len(pending.Ops) {
			return fmt.Errorf(
				"elasticsearch: bulk: response has %d item(s) for %d operation(s)",
				len(result.Items), len(pending.Ops))
		}
		retryOps, retryFailures, permanent := classifyItems(pending.Ops, result.Items)
		// Known gap, tracked as a separate data-correctness fix: when the same _id
		// appears again later in this request and that later operation already
		// succeeded, resending the failed operation makes the older value win.
		// Requests are sent serially, so duplicates split across requests are safe.
		if len(permanent) > 0 {
			// A permanent rejection cannot be retried away, and the batch as a
			// whole must fail so the runner keeps its checkpoint.
			return &bulkError{total: len(batch.Ops), attempts: attempt, failures: permanent}
		}
		if len(retryOps) == 0 {
			return nil
		}
		lastFailures = retryFailures
		if attempt == attempts {
			return &bulkError{total: len(batch.Ops), attempts: attempt, failures: retryFailures}
		}
		next, err := client.NewBulkBatch(retryOps)
		if err != nil {
			return err
		}
		pending = next
	}
	return &bulkError{total: len(batch.Ops), attempts: attempts, failures: lastFailures}
}

// classifyItems splits item results into retryable and permanent failures.
// Deleting a document that does not exist is an idempotent success.
func classifyItems(ops []client.BulkOperation, items []client.BulkItem) (retryOps []client.BulkOperation, retryFailures, permanent []itemFailure) {
	for i, op := range ops {
		item := items[i]
		if item.OK() {
			continue
		}
		if op.Action == client.ActionDelete && item.Status == http.StatusNotFound && item.ErrType == "" {
			continue
		}
		failure := itemFailure{
			action:  string(op.Action),
			index:   firstNonEmpty(item.Index, op.Index),
			id:      firstNonEmpty(item.ID, op.ID),
			status:  item.Status,
			errType: item.ErrType,
			reason:  item.ErrReason,
		}
		if retryableStatus(item.Status) {
			retryOps = append(retryOps, op)
			retryFailures = append(retryFailures, failure)
			continue
		}
		permanent = append(permanent, failure)
	}
	return retryOps, retryFailures, permanent
}

// retryableError reports whether a failed request may succeed when repeated.
// Status 0 with a transport error means the request never reached the cluster.
func retryableError(err error) bool {
	var esErr *client.Error
	if !errors.As(err, &esErr) {
		return false
	}
	if esErr.Status == 0 {
		return esErr.Err != nil
	}
	return retryableStatus(esErr.Status)
}

func retryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// retryDelay is the backoff after the given failed attempt (1-based).
func retryDelay(attempt int) time.Duration {
	delay := retryBaseDelay
	for i := 1; i < attempt && delay < maxRetryDelay; i++ {
		delay *= 2
	}
	if delay > maxRetryDelay {
		delay = maxRetryDelay
	}
	return delay
}

// retryBudget is the total backoff one request may spend across its retries.
func retryBudget(attempts int) time.Duration {
	var total time.Duration
	for attempt := 1; attempt < attempts; attempt++ {
		total += retryDelay(attempt)
	}
	return total
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// truncate shortens text to limit bytes without splitting a UTF-8 rune.
func truncate(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut] + "..."
}
