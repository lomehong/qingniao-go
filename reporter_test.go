package qingniao

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- fakes ---------------------------------------------------------------

type captureSink struct {
	mu    sync.Mutex
	got   []Envelope
	delay time.Duration
	fail  int // 前 fail 次返回错误（测 fallback/重试语义）
}

func (c *captureSink) Send(e Envelope) error {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail > 0 {
		c.fail--
		return errors.New("injected failure")
	}
	c.got = append(c.got, e)
	return nil
}

func (c *captureSink) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.got)
}

func (c *captureSink) last() Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.got[len(c.got)-1]
}

func newTestReporter(t *testing.T, sink Sink, mutate func(*Option)) (*Reporter, *captureSink) {
	t.Helper()
	cs := &captureSink{}
	if sink != nil {
		cs = sink.(*captureSink)
	}
	opt := Option{
		Sink:      cs,
		Meta:      Meta{Repo: "example.com/test", Service: "svc"},
		Throttle:  0, // 默认关闭，节流测试单独开
		QueueSize: 16,
		ExitFunc:  func(int) { t.Errorf("exit must not be called in this test") },
	}
	if mutate != nil {
		mutate(&opt)
	}
	return New(opt), cs
}

// ---- 异步队列与 Close flush ---------------------------------------------

func TestErrorAsyncDeliveredOnClose(t *testing.T) {
	r, cs := newTestReporter(t, nil, nil)
	r.Error("cache warmup failed", errors.New("dial timeout"))
	if cs.count() != 0 {
		t.Fatalf("async Error must not deliver synchronously, got %d", cs.count())
	}
	r.Close(context.Background())
	if cs.count() != 1 {
		t.Fatalf("Close must flush the queue, got %d", cs.count())
	}
	got := cs.last()
	if got.Title != "cache warmup failed" || got.Severity != "medium" {
		t.Fatalf("unexpected envelope: %+v", got)
	}
	if got.Fingerprint == "" || got.Instance == "" {
		t.Fatalf("fingerprint/instance must be autofilled: %+v", got)
	}
	if got.Repo != "example.com/test" || got.Service != "svc" {
		t.Fatalf("meta must be carried: %+v", got)
	}
}

func TestQueueDropOldestWhenFull(t *testing.T) {
	gate := &gateSink{gate: make(chan struct{})}
	r := New(Option{
		Sink:      gate,
		Meta:      Meta{Repo: "t"},
		QueueSize: 2,
		Throttle:  -1, // 关闭节流（负值=关）
		ExitFunc:  func(int) {},
	})
	for i := 0; i < 5; i++ {
		r.Error("e", errors.New("x"))
	}
	close(gate.gate) // 放行：E1（deliverLoop 手中）+ 队列里存活的最后 2 条
	r.Close(context.Background())
	if got := r.Stats().Dropped; got != 2 {
		t.Fatalf("queue full must drop 2 oldest, dropped=%d", got)
	}
	if got := r.Stats().Reported; got != 3 {
		t.Fatalf("in-flight 1 + surviving 2 must deliver, got %d", got)
	}
}

type gateSink struct {
	gate chan struct{} // 关闭后放行所有投递
}

func (g *gateSink) Send(e Envelope) error {
	<-g.gate
	return nil
}

// ---- 节流 ---------------------------------------------------------------

func TestThrottleSuppressesSameFingerprint(t *testing.T) {
	r, cs := newTestReporter(t, nil, func(o *Option) { o.Throttle = time.Hour })
	r.Error("same error", errors.New("boom 1"))
	r.Error("same error", errors.New("boom 2")) // 同模板 → 同指纹 → 压制
	r.Error("different error", errors.New("other"))
	r.Close(context.Background())
	if cs.count() != 2 {
		t.Fatalf("throttle must suppress the duplicate, got %d", cs.count())
	}
	if r.Stats().Dropped != 1 {
		t.Fatalf("suppressed must be counted, stats=%+v", r.Stats())
	}
}

// ---- 指纹归一化（SPEC §4）-----------------------------------------------

func TestNormalizeTemplateCollapsesDynamicValues(t *testing.T) {
	a := normalizeTemplate(`user 42 paid 3.14 USD at 10.0.0.7 req=550e8400-e29b-41d4-a716-446655440000 token=deadbeefcafe1234`)
	b := normalizeTemplate(`user 77 paid 9.99 USD at 192.168.1.1 req=123e4567-e89b-12d3-a456-426614174000 token=0123456789abcdef`)
	if a != b {
		t.Fatalf("dynamic values must collapse to one template:\nA=%s\nB=%s", a, b)
	}
	if !strings.Contains(a, "⟦N⟧") || !strings.Contains(a, "⟦UUID⟧") || !strings.Contains(a, "⟦IP⟧") || !strings.Contains(a, "⟦HEX⟧") {
		t.Fatalf("placeholders missing: %s", a)
	}
	c := normalizeTemplate(`disk full on "/var/lib/data"`)
	d := normalizeTemplate(`disk full on "/mnt/other"`)
	if c != d {
		t.Fatalf("quoted strings must collapse: %s vs %s", c, d)
	}
}

func TestLogSignalsSameTemplateShareFingerprint(t *testing.T) {
	if fingerprintOf(normalizeTemplate("conn reset peer 10.0.0.1:5432")) !=
		fingerprintOf(normalizeTemplate("conn reset peer 10.0.0.2:9999")) {
		t.Fatal("same template with different host:port must share fingerprint")
	}
}

// ---- slog Handler（信号 4：错误日志）-------------------------------------

func TestHandlerCapturesErrorLevelOnly(t *testing.T) {
	r, cs := newTestReporter(t, nil, nil)
	logger := slog.New(r.Handler(slog.LevelWarn))
	logger.Info("should not become a signal", "k", "v")
	logger.Warn("warn is at threshold", "target", "db-1")
	logger.Error("disk full", "path", "/var/data", "used_pct", 98)
	r.Close(context.Background())
	if cs.count() != 2 {
		t.Fatalf("warn+error must pass, info must not: got %d", cs.count())
	}
	got := cs.last()
	if got.Title == "" || !strings.Contains(got.Message, "disk full") {
		t.Fatalf("unexpected envelope: %+v", got)
	}
	if got.Severity != "high" {
		t.Fatalf("Error level must map to high, got %s", got.Severity)
	}
	if got.Attributes["path"] != "/var/data" {
		t.Fatalf("attrs must be carried: %+v", got.Attributes)
	}
}

// ---- Fatal（上报 + flush + exit）----------------------------------------

func TestFatalDeliversThenExits(t *testing.T) {
	exitCode := 0
	r, cs := newTestReporter(t, nil, func(o *Option) {
		o.ExitFunc = func(code int) { exitCode = code }
	})
	r.Fatal("unrecoverable", errors.New("config corrupt"))
	if cs.count() != 1 {
		t.Fatalf("fatal must deliver synchronously, got %d", cs.count())
	}
	if exitCode != 1 {
		t.Fatalf("fatal must exit 1, got %d", exitCode)
	}
}

// ---- Go（goroutine panic 安全包装）--------------------------------------

func TestGoReportsGoroutinePanicWithoutRethrow(t *testing.T) {
	r, cs := newTestReporter(t, nil, nil)
	done := make(chan struct{})
	r.goWrapped(func() {
		defer close(done)
		panic("worker exploded")
	}, false)
	<-done
	deadline := time.Now().Add(2 * time.Second)
	for cs.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	r.Close(context.Background())
	if cs.count() != 1 {
		t.Fatalf("goroutine panic must be reported, got %d", cs.count())
	}
	if !strings.Contains(cs.last().Title, "panic") {
		t.Fatalf("unexpected title: %s", cs.last().Title)
	}
}

// ---- Redact（SPEC §7）---------------------------------------------------

func TestRedactAppliesToEnvelopeFields(t *testing.T) {
	r, cs := newTestReporter(t, nil, func(o *Option) {
		o.Redact = func(s string) string { return strings.ReplaceAll(s, "secret", "[REDACTED]") }
	})
	r.Error("token secret leaked", errors.New("secret in message"))
	r.Close(context.Background())
	got := cs.last()
	if strings.Contains(got.Message, "secret") || strings.Contains(got.Title, "secret") {
		t.Fatalf("redaction failed: %+v", got)
	}
}

// ---- Check（信号 5：失败自检）-------------------------------------------

func TestCheckReportsFailureAndHonorsStop(t *testing.T) {
	r, cs := newTestReporter(t, nil, func(o *Option) { o.Throttle = 0 })
	calls := 0
	stop := r.CheckSeverity("db-ping", 10*time.Millisecond, "high", func() error {
		calls++
		return errors.New("ping timeout")
	})
	deadline := time.Now().Add(2 * time.Second)
	for cs.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	stop()
	before := cs.count()
	time.Sleep(30 * time.Millisecond)
	if cs.count() == 0 {
		t.Fatal("failed check must be reported")
	}
	if cs.count() > before {
		t.Fatal("stop must halt the check")
	}
	if calls == 0 {
		t.Fatal("check fn must have run")
	}
	if got := cs.last(); got.Severity != "high" || !strings.Contains(got.Title, "db-ping") {
		t.Fatalf("unexpected envelope: %+v", got)
	}
	r.Close(context.Background())
}

// ---- Stats 与 Close 幂等 ------------------------------------------------

func TestCloseIsIdempotentAndStatsHonest(t *testing.T) {
	r, cs := newTestReporter(t, nil, nil)
	r.Error("one", errors.New("x"))
	r.Close(context.Background())
	r.Close(context.Background()) // 幂等
	if r.Stats().Reported != 1 || r.Stats().Dropped != 0 || r.Stats().Fallback != 0 {
		t.Fatalf("stats mismatch: %+v", r.Stats())
	}
	if cs.count() != 1 {
		t.Fatalf("delivery mismatch: %d", cs.count())
	}
}
