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

// loadStatus values from the Doris 2.1 Stream Load manual. `Success` and
// `Label Already Exists` are confirmed by the 2.1.8 CI run; `Publish Timeout`
// has only been forced in unit tests so far.
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
		if resp.NumberFilteredRows != nil && *resp.NumberFilteredRows > 0 {
			return outcomeFatal, fmt.Errorf("sink.doris: label %s filtered %d rows", label, *resp.NumberFilteredRows)
		}
		// Success must account for every row. A counter the response omits is a
		// broken response, not a zero.
		if wantRows > 0 {
			if resp.NumberTotalRows == nil {
				return outcomeFatal, fmt.Errorf("sink.doris: label %s reported no NumberTotalRows, sent %d", label, wantRows)
			}
			if *resp.NumberTotalRows != int64(wantRows) {
				return outcomeFatal, fmt.Errorf("sink.doris: label %s reported %d rows, sent %d", label, *resp.NumberTotalRows, wantRows)
			}
		}
		if resp.NumberTotalRows != nil && *resp.NumberTotalRows > 0 {
			if resp.NumberLoadedRows == nil {
				return outcomeFatal, fmt.Errorf("sink.doris: label %s reported no NumberLoadedRows for %d rows", label, *resp.NumberTotalRows)
			}
			if *resp.NumberLoadedRows != *resp.NumberTotalRows {
				return outcomeFatal, fmt.Errorf("sink.doris: label %s loaded %d of %d rows",
					label, *resp.NumberLoadedRows, *resp.NumberTotalRows)
			}
		}
		return outcomeSuccess, nil
	case statusPublishTimeout:
		// Doris documents this status as an import that already completed, whose
		// data may only become visible later ("导入已经完成，但数据可能会延迟可见，
		// 无需重试"). The transaction is committed, so a new label would duplicate
		// rows and must never be used.
		//
		// A counter that is present is a fact, including an explicit 0: a load
		// that reports writing none of the sent rows is a shortfall, not an
		// unverifiable statistic. Only a counter the response omits is skipped,
		// because the 2.1 manual defines what the counters mean but does not
		// promise that every status carries them.
		if resp.NumberFilteredRows != nil && *resp.NumberFilteredRows > 0 {
			return outcomeFatal, fmt.Errorf("sink.doris: label %s filtered %d rows (status %q)",
				label, *resp.NumberFilteredRows, resp.Status)
		}
		if wantRows > 0 && resp.NumberTotalRows != nil && *resp.NumberTotalRows != int64(wantRows) {
			return outcomeFatal, fmt.Errorf("sink.doris: label %s processed %d of %d rows (status %q)",
				label, *resp.NumberTotalRows, wantRows, resp.Status)
		}
		if wantRows > 0 && resp.NumberLoadedRows != nil && *resp.NumberLoadedRows != int64(wantRows) {
			return outcomeFatal, fmt.Errorf("sink.doris: label %s loaded %d of %d rows (status %q)",
				label, *resp.NumberLoadedRows, wantRows, resp.Status)
		}
		if resp.NumberTotalRows != nil && resp.NumberLoadedRows != nil && *resp.NumberLoadedRows != *resp.NumberTotalRows {
			return outcomeFatal, fmt.Errorf("sink.doris: label %s loaded %d of %d rows (status %q)",
				label, *resp.NumberLoadedRows, *resp.NumberTotalRows, resp.Status)
		}
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
