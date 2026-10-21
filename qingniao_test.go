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
	// 兜底成功 = 信号没丢，但**没送达**：用哨兵错误区分（v0.2.1 契约）。
	if err := sink.Send(PanicEnvelope(testMeta, "fallback me", nil, time.Now())); !errors.Is(err, ErrFallbackWritten) {
		t.Fatalf("fallback must report ErrFallbackWritten: %v", err)
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

// ---- 兜底与指纹（2026-10-20 补） ----------------------------------------

func TestHTTPSinkFallbackIsSignalledNotSilentlyDelivered(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "nested", "fallback.jsonl")
	sink := HTTPSink{Endpoint: srv.URL, Timeout: 200 * time.Millisecond, MaxAttempts: 2, FallbackFile: path}
	err := sink.Send(Envelope{Schema: Schema, Repo: "example.com/test", Title: "boom"})
	if !errors.Is(err, ErrFallbackWritten) {
		t.Fatalf("want ErrFallbackWritten, got %v", err)
	}
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("read fallback: %v", readErr)
	}
	if !strings.Contains(string(data), "boom") {
		t.Fatalf("fallback file missing envelope: %s", data)
	}
}

func TestFileSinkCreatesMissingParentDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a", "b", "fallback.jsonl")
	if err := (FileSink{Path: path}).Send(Envelope{Schema: Schema, Title: "boom"}); err != nil {
		t.Fatalf("file sink: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("fallback file not created: %v", err)
	}
}

// 客户端 panic 指纹必须与山海经服务端算法一致，否则同一次崩溃会因
// "谁算的"落进两个聚合 key。golden 值与 crates/shanhaijing 的测试互钉。
func TestPanicEnvelopeFingerprintMatchesTheServerGolden(t *testing.T) {
	stack := []byte("panic: runtime error: index out of range [3] with length 3\n\n" +
		"goroutine 1 [running]:\n" +
		"example.com/spike-target.Average(...)\n" +
		"\t/app/calc.go:17 +0x8c\n" +
		"example.com/spike-target.main()\n" +
		"\t/app/calc.go:25 +0x58\n" +
		"created by testing.(*T).Run in goroutine 1\n" +
		"\tC:/Program Files/Go/src/testing/testing.go:1851 +0x3f6")
	env := PanicEnvelope(Meta{Repo: "example.com/spike-target"}, "runtime error: index out of range [3] with length 3", stack, time.Now())
	// ADR-013：repo 纳入指纹首段后的新 golden，与 shanhaijing
	// go_panic_fingerprint_matches_the_sdk_golden 互钉。
	const golden = "5cc9dae18baa7dac8582fda50dedec8d"
	if env.Fingerprint != golden {
		t.Fatalf("panic fingerprint = %s, want %s", env.Fingerprint, golden)
	}
}

func TestPanicFingerprintIsStableAcrossAddressAndLineDrift(t *testing.T) {
	base := "panic: runtime error: index out of range [3] with length 3\n\n" +
		"goroutine 1 [running]:\n" +
		"example.com/spike-target.Average(...)\n" +
		"\t/app/calc.go:17 +0x8c\n" +
		"example.com/spike-target.main()\n" +
		"\t/app/calc.go:25 +0x58"
	drifted := strings.ReplaceAll(base, "[3] with length 3", "[9] with length 9")
	drifted = strings.ReplaceAll(drifted, "calc.go:17", "calc.go:21")
	drifted = strings.ReplaceAll(drifted, "+0x8c", "+0x91")
	a := panicFingerprint("example.com/spike-target", "runtime error: index out of range [3] with length 3", base)
	b := panicFingerprint("example.com/spike-target", "runtime error: index out of range [9] with length 9", drifted)
	if a != b {
		t.Fatalf("volatile parts must not change the fingerprint: %s vs %s", a, b)
	}
}
