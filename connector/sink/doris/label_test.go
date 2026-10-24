package doris

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"
)

// rowCount builds one row counter for a hand-written loadResponse. The pointer
// matters: a counter the response omits must stay nil so it cannot be mistaken
// for a reported zero.
func rowCount(value int64) *int64 { return &value }

func TestClassifyLoadSuccess(t *testing.T) {
	resp := &loadResponse{Status: statusSuccess, Label: "l1", NumberTotalRows: rowCount(3), NumberLoadedRows: rowCount(3)}
	outcome, err := classifyLoad(resp, "l1", 3)
	if err != nil || outcome != outcomeSuccess {
		t.Fatalf("outcome=%v err=%v", outcome, err)
	}
}

func TestClassifyLoadSuccessWithWrongRowCountIsFatal(t *testing.T) {
	resp := &loadResponse{Status: statusSuccess, Label: "l1", NumberTotalRows: rowCount(2), NumberLoadedRows: rowCount(2)}
	outcome, err := classifyLoad(resp, "l1", 3)
	if outcome != outcomeFatal || err == nil {
		t.Fatalf("outcome=%v err=%v", outcome, err)
	}
}

func TestClassifyLoadSuccessWithFilteredRowsIsFatal(t *testing.T) {
	resp := &loadResponse{Status: statusSuccess, Label: "l1", NumberTotalRows: rowCount(3), NumberLoadedRows: rowCount(2), NumberFilteredRows: rowCount(1)}
	outcome, err := classifyLoad(resp, "l1", 3)
	if outcome != outcomeFatal || err == nil {
		t.Fatalf("outcome=%v err=%v", outcome, err)
	}
}

func TestClassifyLoadSuccessWithMissingRowsIsFatal(t *testing.T) {
	resp := &loadResponse{Status: statusSuccess, Label: "l1", NumberTotalRows: rowCount(3), NumberLoadedRows: rowCount(2)}
	outcome, err := classifyLoad(resp, "l1", 3)
	if outcome != outcomeFatal || err == nil {
		t.Fatalf("outcome=%v err=%v", outcome, err)
	}
}

// TestClassifyLoadSuccessWithOmittedCountersIsFatal pins the Success side of the
// missing-vs-zero rule: Success must account for every row, so a counter the FE
// omits fails the job instead of being read as zero.
func TestClassifyLoadSuccessWithOmittedCountersIsFatal(t *testing.T) {
	omitted := []string{
		`{"Status":"Success","Label":"l1"}`,
		`{"Status":"Success","Label":"l1","NumberLoadedRows":3}`,
	}
	for _, body := range omitted {
		var resp loadResponse
		if err := json.Unmarshal([]byte(body), &resp); err != nil {
			t.Fatalf("decode %s: %v", body, err)
		}
		if outcome, err := classifyLoad(&resp, "l1", 3); outcome != outcomeFatal || err == nil {
			t.Fatalf("%s: outcome=%v err=%v", body, outcome, err)
		}
	}
}

// TestClassifyLoadPublishTimeout pins the Publish Timeout policy. Doris
// documents the status as an import that already completed and only may become
// visible later ("导入已经完成，但数据可能会延迟可见，无需重试"), so the load must not be
// retried under a new label and must not look like a failure.
//
// The responses are decoded from JSON on purpose: whether a counter is present
// is part of the behaviour, and a hand-built struct cannot express the
// difference between an omitted counter and a reported zero.
func TestClassifyLoadPublishTimeout(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		wantRows int
		want     loadOutcome
	}{
		{
			name:     "counters account for the batch",
			body:     `{"Status":"Publish Timeout","Label":"l1","NumberTotalRows":3,"NumberLoadedRows":3,"NumberFilteredRows":0}`,
			wantRows: 3,
			want:     outcomeSuccess,
		},
		{
			// The shape Doris may answer with when it reports no counters at all.
			name:     "no counters reported",
			body:     `{"Status":"Publish Timeout","Label":"l1"}`,
			wantRows: 3,
			want:     outcomeSuccess,
		},
		{
			// An explicit 0 is a reported shortfall, not a missing statistic.
			name:     "loaded rows explicitly zero",
			body:     `{"Status":"Publish Timeout","Label":"l1","NumberTotalRows":3,"NumberLoadedRows":0,"NumberFilteredRows":0}`,
			wantRows: 3,
			want:     outcomeFatal,
		},
		{
			name:     "total rows explicitly zero",
			body:     `{"Status":"Publish Timeout","Label":"l1","NumberTotalRows":0,"NumberLoadedRows":0}`,
			wantRows: 3,
			want:     outcomeFatal,
		},
		{
			name:     "filtered rows are never ignored",
			body:     `{"Status":"Publish Timeout","Label":"l1","NumberTotalRows":3,"NumberLoadedRows":2,"NumberFilteredRows":1}`,
			wantRows: 3,
			want:     outcomeFatal,
		},
		{
			name:     "loaded rows below the batch",
			body:     `{"Status":"Publish Timeout","Label":"l1","NumberTotalRows":3,"NumberLoadedRows":2}`,
			wantRows: 3,
			want:     outcomeFatal,
		},
		{
			name:     "total rows below the batch",
			body:     `{"Status":"Publish Timeout","Label":"l1","NumberTotalRows":2,"NumberLoadedRows":2}`,
			wantRows: 3,
			want:     outcomeFatal,
		},
		{
			name:     "loaded rows reported without a total",
			body:     `{"Status":"Publish Timeout","Label":"l1","NumberLoadedRows":2}`,
			wantRows: 3,
			want:     outcomeFatal,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var resp loadResponse
			if err := json.Unmarshal([]byte(tc.body), &resp); err != nil {
				t.Fatalf("decode %s: %v", tc.body, err)
			}
			outcome, err := classifyLoad(&resp, "l1", tc.wantRows)
			if outcome != tc.want {
				t.Fatalf("outcome=%v want %v (err=%v)", outcome, tc.want, err)
			}
			if tc.want == outcomeSuccess && err != nil {
				t.Fatalf("success must not carry an error: %v", err)
			}
			if tc.want != outcomeFatal {
				return
			}
			if err == nil {
				t.Fatal("a contradictory row count must carry an error")
			}
			// The job name and the reported figures must survive for operators.
			if !strings.Contains(err.Error(), "l1") || !strings.Contains(err.Error(), "Publish Timeout") {
				t.Fatalf("error must name the label and the status: %v", err)
			}
		})
	}
}

func TestClassifyLoadLabelAlreadyExists(t *testing.T) {
	finished := &loadResponse{Status: statusLabelExists, Label: "l1", ExistingJobStatus: existingJobFinished}
	if outcome, err := classifyLoad(finished, "l1", 0); err != nil || outcome != outcomeSuccess {
		t.Fatalf("finished: outcome=%v err=%v", outcome, err)
	}
	running := &loadResponse{Status: statusLabelExists, Label: "l1", ExistingJobStatus: existingJobRunning}
	if outcome, err := classifyLoad(running, "l1", 0); err != nil || outcome != outcomeLabelExists {
		t.Fatalf("running: outcome=%v err=%v", outcome, err)
	}
	cancelled := &loadResponse{Status: statusLabelExists, Label: "l1", ExistingJobStatus: existingJobCancelled}
	if outcome, err := classifyLoad(cancelled, "l1", 0); outcome != outcomeFatal || err == nil {
		t.Fatalf("cancelled: outcome=%v err=%v", outcome, err)
	}
	missing := &loadResponse{Status: statusLabelExists, Label: "l1"}
	if outcome, err := classifyLoad(missing, "l1", 0); outcome != outcomeUnknown || err == nil {
		t.Fatalf("missing ExistingJobStatus: outcome=%v err=%v", outcome, err)
	}
	weird := &loadResponse{Status: statusLabelExists, Label: "l1", ExistingJobStatus: "WEIRD"}
	if outcome, err := classifyLoad(weird, "l1", 0); outcome != outcomeFatal || err == nil {
		t.Fatalf("unknown ExistingJobStatus: outcome=%v err=%v", outcome, err)
	}
}

func TestClassifyLoadFailIsFatal(t *testing.T) {
	resp := &loadResponse{Status: statusFail, Label: "l1", Message: "bad row", ErrorURL: "http://err"}
	outcome, err := classifyLoad(resp, "l1", 1)
	if outcome != outcomeFatal || err == nil {
		t.Fatalf("outcome=%v err=%v", outcome, err)
	}
}

func TestClassifyLoadUnknownStatus(t *testing.T) {
	resp := &loadResponse{Status: "Something New", Label: "l1"}
	outcome, err := classifyLoad(resp, "l1", 0)
	if outcome != outcomeUnknown || err == nil {
		t.Fatalf("outcome=%v err=%v", outcome, err)
	}
}

func TestClassifyLoadLabelMismatch(t *testing.T) {
	resp := &loadResponse{Status: statusSuccess, Label: "other"}
	outcome, err := classifyLoad(resp, "l1", 0)
	if outcome != outcomeUnknown || err == nil {
		t.Fatalf("outcome=%v err=%v", outcome, err)
	}
}

func TestClassifyLoadNil(t *testing.T) {
	if outcome, err := classifyLoad(nil, "l1", 0); outcome != outcomeUnknown || err == nil {
		t.Fatalf("outcome=%v err=%v", outcome, err)
	}
}

func TestClassifyLoadState(t *testing.T) {
	cases := []struct {
		state loadState
		want  loadOutcome
	}{
		{loadState{State: existingJobFinished}, outcomeSuccess},
		{loadState{State: existingJobCancelled, ErrorMsg: "boom"}, outcomeFatal},
		{loadState{State: ""}, outcomeUnknown},
		{loadState{State: labelStatusUnavailable}, outcomeUnknown},
		{loadState{State: existingJobRunning}, outcomeRetryable},
		{loadState{State: "PENDING"}, outcomeRetryable},
		{loadState{State: "ETL"}, outcomeRetryable},
		{loadState{State: "LOADING"}, outcomeRetryable},
	}
	for _, tc := range cases {
		outcome, err := classifyLoadState(tc.state)
		if outcome != tc.want {
			t.Fatalf("%s: outcome=%v want %v", tc.state.State, outcome, tc.want)
		}
		if tc.want == outcomeFatal || tc.want == outcomeUnknown {
			if err == nil {
				t.Fatalf("%s: expected an error", tc.state.State)
			}
		}
	}
}

func TestLabelSequence(t *testing.T) {
	pattern := regexp.MustCompile(`^go-cdc_[0-9a-f]+_[0-9]+_[0-9]+$`)
	var seq labelSequence
	first := seq.next("db.orders")
	second := seq.next("db.orders")
	if first == second {
		t.Fatal("labels must be unique")
	}
	if !pattern.MatchString(first) || !pattern.MatchString(second) {
		t.Fatalf("labels %q %q", first, second)
	}
	if !validLabel(first) {
		t.Fatalf("generated label is not valid: %q", first)
	}
}

func TestValidLabel(t *testing.T) {
	for _, ok := range []string{"abc", "go-cdc_1_2_3", "A_b-9"} {
		if !validLabel(ok) {
			t.Fatalf("%q should be valid", ok)
		}
	}
	for _, bad := range []string{"", "has space", "quote'", "semi;colon", "sl/ash"} {
		if validLabel(bad) {
			t.Fatalf("%q should be invalid", bad)
		}
	}
	if validLabel(string(make([]byte, 129))) {
		t.Fatal("labels longer than 128 bytes must be rejected")
	}
}

func TestBackoffIsBounded(t *testing.T) {
	if backoff(0) != 100*time.Millisecond {
		t.Fatalf("backoff(0)=%s", backoff(0))
	}
	if backoff(100) != 2*time.Second {
		t.Fatalf("backoff must cap at 2s, got %s", backoff(100))
	}
	if labelAttempts(0) != 1 || labelAttempts(3) != 4 || labelAttempts(-1) != 1 {
		t.Fatalf("labelAttempts: %d %d %d", labelAttempts(0), labelAttempts(3), labelAttempts(-1))
	}
}
