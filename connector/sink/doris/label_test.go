package doris

import (
	"regexp"
	"testing"
	"time"
)

func TestClassifyLoadSuccess(t *testing.T) {
	resp := &loadResponse{Status: statusSuccess, Label: "l1", NumberTotalRows: 3, NumberLoadedRows: 3}
	outcome, err := classifyLoad(resp, "l1", 3)
	if err != nil || outcome != outcomeSuccess {
		t.Fatalf("outcome=%v err=%v", outcome, err)
	}
}

func TestClassifyLoadSuccessWithWrongRowCountIsFatal(t *testing.T) {
	resp := &loadResponse{Status: statusSuccess, Label: "l1", NumberTotalRows: 2, NumberLoadedRows: 2}
	outcome, err := classifyLoad(resp, "l1", 3)
	if outcome != outcomeFatal || err == nil {
		t.Fatalf("outcome=%v err=%v", outcome, err)
	}
}

func TestClassifyLoadSuccessWithFilteredRowsIsFatal(t *testing.T) {
	resp := &loadResponse{Status: statusSuccess, Label: "l1", NumberTotalRows: 3, NumberLoadedRows: 2, NumberFilteredRows: 1}
	outcome, err := classifyLoad(resp, "l1", 3)
	if outcome != outcomeFatal || err == nil {
		t.Fatalf("outcome=%v err=%v", outcome, err)
	}
}

func TestClassifyLoadSuccessWithMissingRowsIsFatal(t *testing.T) {
	resp := &loadResponse{Status: statusSuccess, Label: "l1", NumberTotalRows: 3, NumberLoadedRows: 2}
	outcome, err := classifyLoad(resp, "l1", 3)
	if outcome != outcomeFatal || err == nil {
		t.Fatalf("outcome=%v err=%v", outcome, err)
	}
}

func TestClassifyLoadPublishTimeoutIsSuccess(t *testing.T) {
	// The import already completed; a new label would duplicate data.
	resp := &loadResponse{Status: statusPublishTimeout, Label: "l1"}
	outcome, err := classifyLoad(resp, "l1", 1)
	if err != nil || outcome != outcomeSuccess {
		t.Fatalf("outcome=%v err=%v", outcome, err)
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
