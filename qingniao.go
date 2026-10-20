// Package qingniao（青鸟）is the Go collector SDK for Nuwa.
//
// It reports runtime problem signals — panics first — to the 山海经 ingest.
// The SDK only captures and ships: no dedup, no cooldown, no LLM, no shell.
// All policy lives server-side (ADR-002, ADR-005).
//
// Envelope contract (docs/07 A-5 draft, schema "qingniao/v1"): the JSON is a
// superset of the ingest Signal — unknown fields are ignored server-side, so
// this wire format can grow without breaking older collectors.
package qingniao

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Schema is the wire version. Bump on breaking envelope changes.
const Schema = "qingniao/v1"

// Meta identifies the reporting service. Repo must match the repo identity
// registered in 山海经 (Signal.repo).
type Meta struct {
	Repo     string // e.g. "example.com/spike-target"
	Service  string // optional deployment/service name
	Severity string // default "high" for panics, "medium" for Report
	// Instance 标识多副本中的具体进程；空则 Reporter 自动补
	// host:pid:boot 随机串（SPEC §8）。
	Instance string
}

// Envelope is the unified, language-agnostic event envelope.
type Envelope struct {
	Schema     string `json:"schema"`
	Repo       string `json:"repo"`
	Language   string `json:"language"`
	Service    string `json:"service,omitempty"`
	Host       string `json:"host,omitempty"`
	Title      string `json:"title"`
	Severity   string `json:"severity"`
	Message    string `json:"message,omitempty"`
	StackTrace string `json:"stack_trace"`
	TsUnixMs   int64  `json:"ts_unix_ms"`
	// Fingerprint 是客户端侧归一化指纹（SPEC §4）。optional：服务端可
	// 优先采用，否则按 stack_trace 自行计算——新旧服务端都不破坏。
	Fingerprint string `json:"fingerprint,omitempty"`
	// Instance 标识多副本部署中的具体进程（host:pid:boot 随机串）。
	Instance string `json:"instance,omitempty"`
	// Attributes 承载日志 attrs 等结构化上下文。
	Attributes map[string]string `json:"attributes,omitempty"`
}

// Sink receives envelopes. Implementations must be safe for concurrent use.
type Sink interface {
	Send(e Envelope) error
}

// ErrFallbackWritten 表示投递失败、但信封已写入本地 FallbackFile：信号没丢，
// 但没有送达服务端。Reporter 用它把"已送达"与"已兜底"分开计数。
var ErrFallbackWritten = errors.New("signal written to fallback file")

// PanicEnvelope builds an envelope from a recovered panic value and a stack
// dump (runtime/debug.Stack shape — the goroutine trace format). Exported so
// hosts can pre-capture and ship later.
func PanicEnvelope(meta Meta, recovered any, stack []byte, now time.Time) Envelope {
	msg := ""
	switch v := recovered.(type) {
	case error:
		msg = v.Error()
	case string:
		msg = v
	default:
		msg = fmt.Sprintf("%v", v)
	}
	sev := meta.Severity
	if sev == "" {
		sev = "high"
	}
	host, _ := os.Hostname()
	headline := firstLine(msg)
	return Envelope{
		Schema:     Schema,
		Repo:       meta.Repo,
		Language:   "go",
		Service:    meta.Service,
		Host:       host,
		Title:      "panic: " + headline,
		Severity:   sev,
		Message:    headline,
		StackTrace: string(stack),
		TsUnixMs:   now.UnixMilli(),
		// 客户端指纹与服务端算法一致（SPEC §4）。服务端当前按自己的算法
		// 计算（信封超集原则，字段被忽略），两份值一致便于对账，也保证
		// 服务端将来"优先采用客户端指纹"时无缝切换。
		Fingerprint: panicFingerprint(headline, string(stack)),
	}
}

// Capture runs fn under a panic guard: on panic it ships the signal to sink
// and re-panics (the process still dies — Nuwa is not a substitute for crash
// handling; the collector must not swallow failures).
func Capture(sink Sink, meta Meta, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			e := PanicEnvelope(meta, r, Stack(), time.Now())
			_ = sink.Send(e) // best-effort: crashing anyway
			panic(r)
		}
	}()
	fn()
}

// Report ships a non-panic error signal manually.
func Report(sink Sink, meta Meta, title string, err error) error {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	sev := meta.Severity
	if sev == "" {
		sev = "medium"
	}
	host, _ := os.Hostname()
	return sink.Send(Envelope{
		Schema:     Schema,
		Repo:       meta.Repo,
		Language:   "go",
		Service:    meta.Service,
		Host:       host,
		Title:      title,
		Severity:   sev,
		Message:    firstLine(msg),
		StackTrace: string(Stack()),
		TsUnixMs:   time.Now().UnixMilli(),
	})
}

// Stack captures all goroutine stacks in the goroutine-trace format the
// 山海经 Go parser expects.
func Stack() []byte {
	buf := make([]byte, 64*1024)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return buf[:n]
		}
		buf = make([]byte, 2*len(buf))
	}
}

func firstLine(s string) string {
	if idx := strings.IndexByte(s, '\n'); idx >= 0 {
		return s[:idx]
	}
	return s
}

// FileSink appends one JSON object per line (JSONL) — the local/dev sink.
type FileSink struct {
	Path string
}

func (f FileSink) Send(e Envelope) error {
	line, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if err := ensureParentDir(f.Path); err != nil {
		return err
	}
	file, err := os.OpenFile(f.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	return appendAll(file, append(line, '\n'))
}

// ensureParentDir 建好落盘目录——否则"兜底"会因目录不存在而静默失败。
func ensureParentDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "" || dir == "." {
		return nil
	}
	return os.MkdirAll(dir, 0o700)
}

func appendAll(file *os.File, data []byte) error {
	_, err := file.Write(data)
	return err
}

// HTTPSink POSTs each envelope as one JSON document.
//
// A crashing process must not hang on the report path: every attempt has a
// deadline, transient failures are retried a bounded number of times, and a
// final failure can be appended to FallbackFile instead of losing the signal.
type HTTPSink struct {
	Endpoint string
	// Token is sent as the X-NUWA-Token header when the ingest server
	// requires one (NUWA_INGEST_TOKEN on the serve side). Report-only.
	Token  string
	Client *http.Client
	// Timeout bounds each attempt (default 3s).
	Timeout time.Duration
	// MaxAttempts is the total number of attempts (default 2).
	MaxAttempts int
	// FallbackFile, when set, receives the JSONL envelope if every attempt
	// fails. A successful fallback returns nil: the signal was preserved.
	FallbackFile string
}

const (
	defaultHTTPTimeout = 3 * time.Second
	defaultMaxAttempts = 2
)

func (h HTTPSink) Send(e Envelope) error {
	body, err := json.Marshal(e)
	if err != nil {
		return err
	}
	client := h.Client
	if client == nil {
		client = http.DefaultClient
	}
	timeout := h.Timeout
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}
	attempts := h.MaxAttempts
	if attempts <= 0 {
		attempts = defaultMaxAttempts
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.Endpoint, bytes.NewReader(body))
		if err != nil {
			cancel()
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "qingniao-go/"+Schema)
		if h.Token != "" {
			req.Header.Set("X-NUWA-Token", h.Token)
		}
		resp, err := client.Do(req)
		if err == nil {
			if resp.StatusCode < 300 {
				resp.Body.Close()
				cancel()
				return nil
			}
			lastErr = fmt.Errorf("ingest endpoint %s: HTTP %d", h.Endpoint, resp.StatusCode)
			retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
			resp.Body.Close()
			cancel()
			if !retryable {
				break
			}
		} else {
			lastErr = err
			cancel()
		}
		if attempt < attempts {
			time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
		}
	}

	if h.FallbackFile != "" {
		if err := appendJSONL(h.FallbackFile, body); err == nil {
			// 信号已落盘，但没有送达：用哨兵错误让调用方分开计数。
			return fmt.Errorf("%w: %v", ErrFallbackWritten, lastErr)
		} else {
			return fmt.Errorf("%w; fallback write failed: %v", lastErr, err)
		}
	}
	return lastErr
}

func appendJSONL(path string, body []byte) error {
	if err := ensureParentDir(path); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	return appendAll(file, append(body, '\n'))
}
