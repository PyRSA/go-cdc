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

// bulk sends ops and returns an error unless every item was acknowledged.
// Only 429, retryable 5xx, and transport failures are retried, and only the
// failed items are resent; index and delete are idempotent because the _id is
// derived from the primary key.
func (s *Sink) bulk(ctx context.Context, ops []client.BulkOperation) error {
	if len(ops) == 0 {
		return nil
	}
	attempts := s.maxRetries + 1
	pending := ops
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
					len(pending), attempt, attempts, err)
			}
			continue
		}
		if len(result.Items) != len(pending) {
			return fmt.Errorf(
				"elasticsearch: bulk: response has %d item(s) for %d operation(s)",
				len(result.Items), len(pending))
		}
		retryOps, retryFailures, permanent := classifyItems(pending, result.Items)
		if len(permanent) > 0 {
			// A permanent rejection cannot be retried away, and the batch as a
			// whole must fail so the runner keeps its checkpoint.
			return &bulkError{total: len(ops), attempts: attempt, failures: permanent}
		}
		if len(retryOps) == 0 {
			return nil
		}
		lastFailures = retryFailures
		if attempt == attempts {
			return &bulkError{total: len(ops), attempts: attempt, failures: retryFailures}
		}
		pending = retryOps
	}
	return &bulkError{total: len(ops), attempts: attempts, failures: lastFailures}
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

// retryBudget is the total backoff a Write may spend across all retries.
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
