package main

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

type failCloser struct {
	err    error
	closed bool
}

func (f *failCloser) Close() error {
	f.closed = true
	return f.err
}

func TestCloseLoggingReportsToStderr(t *testing.T) {
	var buf bytes.Buffer
	c := &failCloser{err: errors.New("rolling close boom")}
	code := closeLogging(c, &buf, 0)
	if !c.closed {
		t.Fatal("closer must be invoked")
	}
	if code != 1 {
		t.Fatalf("code=%d want 1", code)
	}
	if !strings.Contains(buf.String(), "rolling close boom") {
		t.Fatalf("stderr missing diagnosis: %q", buf.String())
	}
	if !strings.Contains(buf.String(), "failed to close logging") {
		t.Fatalf("stderr missing prefix: %q", buf.String())
	}
}

func TestCloseLoggingPreservesNonZeroCode(t *testing.T) {
	var buf bytes.Buffer
	code := closeLogging(&failCloser{err: errors.New("x")}, &buf, 2)
	if code != 2 {
		t.Fatalf("code=%d want 2", code)
	}
}

func TestCloseLoggingNilCloser(t *testing.T) {
	if code := closeLogging(nil, io.Discard, 0); code != 0 {
		t.Fatalf("code=%d", code)
	}
}

func TestCloseLoggingSuccess(t *testing.T) {
	var buf bytes.Buffer
	c := &failCloser{err: nil}
	if code := closeLogging(c, &buf, 0); code != 0 {
		t.Fatalf("code=%d", code)
	}
	if buf.Len() != 0 {
		t.Fatalf("unexpected stderr %q", buf.String())
	}
}
