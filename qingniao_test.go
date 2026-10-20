package qingniao

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var testMeta = Meta{Repo: "example.com/spike-target", Service: "reporter"}

func TestCaptureShipsPanicAndRethrows(t *testing.T) {
	var got Envelope
	sink := collectFunc(func(e Envelope) error { got = e; return nil })

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Capture must re-panic, not swallow")
			}
		}()
		Capture(sink, testMeta, func() {
			panic(errors.New("index out of range [3] with length 3"))
		})
	}()

	if got.Schema != Schema {
		t.Errorf("schema = %q, want %q", got.Schema, Schema)
	}
	if got.Language != "go" || got.Repo != testMeta.Repo {
		t.Errorf("envelope identity wrong: %+v", got)
	}
	if got.Title != "panic: index out of range [3] with length 3" {
		t.Errorf("title = %q", got.Title)
	}
	if !strings.Contains(got.StackTrace, "goroutine ") || !strings.Contains(got.StackTrace, "[running]") {
		t.Errorf("stack trace is not a goroutine trace: %.80s", got.StackTrace)
	}
	if !strings.Contains(got.StackTrace, "qingniao-go.TestCaptureShipsPanicAndRethrows") {
		t.Errorf("stack must name the panicking test function, got: %s", got.StackTrace)
	}
}

func TestReportDefaultsSeverityAndFillsMessage(t *testing.T) {
	var got Envelope
	sink := collectFunc(func(e Envelope) error { got = e; return nil })

	if err := Report(sink, Meta{Repo: testMeta.Repo}, "healthcheck failed", errors.New("dial tcp: refused\nextra line")); err != nil {
		t.Fatalf("Report: %v", err)
	}
	if got.Severity != "medium" {
		t.Errorf("severity = %q, want medium", got.Severity)
	}
	if got.Message != "dial tcp: refused" {
		t.Errorf("message = %q, want first line only", got.Message)
	}
}

func TestFileSinkWritesJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "signals.jsonl")
	sink := FileSink{Path: path}

	now := time.UnixMilli(1_000)
	e := PanicEnvelope(testMeta, "boom", []byte("stack"), now)
	if err := sink.Send(e); err != nil {
		t.Fatalf("send 1: %v", err)
	}
	e2 := e
	e2.Title = "second"
	if err := sink.Send(e2); err != nil {
		t.Fatalf("send 2: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 JSONL lines, got %d", len(lines))
	}
	var back Envelope
	if err := json.Unmarshal([]byte(lines[0]), &back); err != nil {
		t.Fatalf("line 0 is not JSON: %v", err)
	}
	if back.TsUnixMs != 1_000 || back.Title != "panic: boom" {
		t.Errorf("roundtrip mismatch: %+v", back)
	}
	if !strings.Contains(lines[0], `"stack_trace"`) || !strings.Contains(lines[0], `"ts_unix_ms"`) {
		t.Errorf("wire field names drifted: %.120s", lines[0])
	}
}

func TestHTTPSinkPostsEnvelope(t *testing.T) {
	var got Envelope
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink := HTTPSink{Endpoint: srv.URL}
	if err := sink.Send(PanicEnvelope(testMeta, "boom", []byte("stack"), time.UnixMilli(5))); err != nil {
		t.Fatalf("send: %v", err)
	}
	if got.Repo != testMeta.Repo || got.Schema != Schema {
		t.Errorf("server got wrong envelope: %+v", got)
	}
}

func TestHTTPSinkSurfacesHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	err := (HTTPSink{Endpoint: srv.URL}).Send(PanicEnvelope(testMeta, "x", nil, time.Now()))
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("want HTTP error surfaced, got %v", err)
	}
}

func TestHTTPSinkSendsTokenHeader(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("X-NUWA-Token")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink := HTTPSink{Endpoint: srv.URL, Token: "s3cret"}
	if err := sink.Send(PanicEnvelope(testMeta, "x", nil, time.Now())); err != nil {
		t.Fatalf("send: %v", err)
	}
	if got != "s3cret" {
		t.Errorf("X-NUWA-Token = %q, want s3cret", got)
	}
}

func TestHTTPSinkRetriesTransientFailureThenSucceeds(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink := HTTPSink{Endpoint: srv.URL, Timeout: time.Second, MaxAttempts: 2}
	if err := sink.Send(PanicEnvelope(testMeta, "x", nil, time.Now())); err != nil {
		t.Fatalf("send: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestHTTPSinkDoesNotRetryClientError(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	sink := HTTPSink{Endpoint: srv.URL, Timeout: time.Second, MaxAttempts: 3}
	if err := sink.Send(PanicEnvelope(testMeta, "x", nil, time.Now())); err == nil {
		t.Fatal("want error")
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 for a 403", calls)
	}
}

func TestHTTPSinkFallsBackToFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "fallback.jsonl")
	sink := HTTPSink{
		Endpoint:     srv.URL,
		Timeout:      100 * time.Millisecond,
		MaxAttempts:  2,
		FallbackFile: path,
	}
	if err := sink.Send(PanicEnvelope(testMeta, "fallback me", nil, time.Now())); err != nil {
		t.Fatalf("fallback should preserve the signal: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fallback: %v", err)
	}
	if !strings.Contains(string(data), "fallback me") {
		t.Fatalf("fallback file missing envelope: %s", data)
	}
}

func TestHTTPSinkTimeoutIsBounded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	start := time.Now()
	sink := HTTPSink{Endpoint: srv.URL, Timeout: 50 * time.Millisecond, MaxAttempts: 1}
	if err := sink.Send(PanicEnvelope(testMeta, "x", nil, time.Now())); err == nil {
		t.Fatal("want timeout error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("timeout was not bounded: %v", elapsed)
	}
}

// collectFunc adapts a closure into a Sink.
type collectFunc func(Envelope) error

func (f collectFunc) Send(e Envelope) error { return f(e) }
