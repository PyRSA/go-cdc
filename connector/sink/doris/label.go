package doris

import (
	"fmt"
	"hash/fnv"
	"strings"
	"sync/atomic"
	"time"
)

// labelSequence makes labels unique within one process. Cross-process
// uniqueness comes from the timestamp; it is not a replay key after a restart.
type labelSequence struct {
	value atomic.Uint64
}

// next builds a Doris label. The label is stable for one sub-batch retry and
// never contains credentials or arbitrary table text.
func (s *labelSequence) next(table string) string {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(table))
	return fmt.Sprintf("go-cdc_%x_%d_%d", hash.Sum64(), time.Now().UnixNano(), s.value.Add(1))
}

// loadStatus values from Doris 2.1.x Stream Load docs. Exact strings are covered
// by unit tests; real-cluster confirmation remains pending until CI is green.
const (
	statusSuccess          = "Success"
	statusPublishTimeout   = "Publish Timeout"
	statusLabelExists      = "Label Already Exists"
	statusFail             = "Fail"
	existingJobFinished    = "FINISHED"
	existingJobRunning     = "RUNNING"
	existingJobCancelled   = "CANCELLED"
	labelStatusUnavailable = "UNAVAILABLE"
)

type loadOutcome int

const (
	outcomeSuccess loadOutcome = iota
	outcomeRetryable
	outcomeFatal
	outcomeUnknown
	outcomeLabelExists
)

// String names the outcome for structured logs. The name is stable so operators
// can filter Stream Load records by severity class.
func (o loadOutcome) String() string {
	switch o {
	case outcomeSuccess:
		return "success"
	case outcomeRetryable:
		return "retryable"
	case outcomeFatal:
		return "fatal"
	case outcomeUnknown:
		return "unknown"
	case outcomeLabelExists:
		return "label-exists"
	default:
		return "unexpected"
	}
}

// classifyLoad turns one Stream Load response into the retry state machine.
// HTTP 200 is never sufficient on its own. wantRows is the number of JSON
// Lines sent in this sub-batch; a success response must confirm that count.
func classifyLoad(resp *loadResponse, label string, wantRows int) (loadOutcome, error) {
	if resp == nil {
		return outcomeUnknown, fmt.Errorf("sink.doris: empty Stream Load response")
	}
	if resp.Label != "" && resp.Label != label {
		return outcomeUnknown, fmt.Errorf("sink.doris: Stream Load label %q does not match %q", resp.Label, label)
	}
	switch resp.Status {
	case statusSuccess:
		if resp.NumberFilteredRows > 0 {
			return outcomeFatal, fmt.Errorf("sink.doris: label %s filtered %d rows", label, resp.NumberFilteredRows)
		}
		if wantRows > 0 && resp.NumberTotalRows != int64(wantRows) {
			return outcomeFatal, fmt.Errorf("sink.doris: label %s reported %d rows, sent %d", label, resp.NumberTotalRows, wantRows)
		}
		if resp.NumberTotalRows > 0 && resp.NumberLoadedRows != resp.NumberTotalRows {
			return outcomeFatal, fmt.Errorf("sink.doris: label %s loaded %d of %d rows",
				label, resp.NumberLoadedRows, resp.NumberTotalRows)
		}
		return outcomeSuccess, nil
	case statusPublishTimeout:
		// Doris documents Publish Timeout as an import that already completed;
		// visibility may lag. Retrying with a new label would duplicate data.
		return outcomeSuccess, nil
	case statusLabelExists:
		switch strings.ToUpper(strings.TrimSpace(resp.ExistingJobStatus)) {
		case existingJobFinished:
			return outcomeSuccess, nil
		case existingJobRunning:
			// Same label is still in flight; caller retries Stream Load with the
			// same label (or waits) rather than inventing a new one.
			return outcomeLabelExists, nil
		case existingJobCancelled:
			return outcomeFatal, fmt.Errorf("sink.doris: label %s already exists and was cancelled", label)
		case "":
			// ExistingJobStatus missing: reconcile via SHOW LOAD before retry.
			return outcomeUnknown, fmt.Errorf("sink.doris: label %s already exists without ExistingJobStatus", label)
		default:
			return outcomeFatal, fmt.Errorf("sink.doris: label %s already exists with ExistingJobStatus %q",
				label, resp.ExistingJobStatus)
		}
	case statusFail:
		return outcomeFatal, fmt.Errorf("sink.doris: label %s failed: %s %s",
			label, resp.Message, resp.ErrorURL)
	default:
		return outcomeUnknown, fmt.Errorf("sink.doris: label %s returned status %q", label, resp.Status)
	}
}

// classifyLoadState maps SHOW LOAD state onto the same outcome set.
func classifyLoadState(state loadState) (loadOutcome, error) {
	switch state.State {
	case existingJobFinished:
		return outcomeSuccess, nil
	case existingJobCancelled:
		return outcomeFatal, fmt.Errorf("sink.doris: load was cancelled: %s", state.ErrorMsg)
	case "", labelStatusUnavailable:
		return outcomeUnknown, fmt.Errorf("sink.doris: load state is unavailable")
	default:
		// RUNNING, PENDING, ETL, LOADING: the same label is still in flight.
		return outcomeRetryable, nil
	}
}

func backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := time.Duration(attempt) * 100 * time.Millisecond
	if d > 2*time.Second {
		return 2 * time.Second
	}
	return d
}

func labelAttempts(maxRetries int) int {
	if maxRetries < 0 {
		return 1
	}
	return maxRetries + 1
}
