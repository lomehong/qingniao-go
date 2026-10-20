package qingniao

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Reporter 是 SPEC v1 的五挂点采集器：panic（Capture/Go）、显式错误
// （Error/Fatal）、错误日志（Handler，log/slog）、失败自检（Check）、
// 自观测（Stats）。零配置用 NewFromEnv；显式 Option 用 New。
//
// 并发安全：所有方法可多 goroutine 同时调用。
type Reporter struct {
	sink     Sink
	meta     Meta
	redact   func(string) string
	throttle time.Duration
	queue    chan Envelope
	// done 关闭时投递循环排空队列并退出。**队列本身永不 close**——并发
	// submit 与 Close 竞争时 close(channel) 会 panic（send on closed
	// channel），违反 SPEC §6.3「SDK 自身永不 panic」。
	done     chan struct{}
	exitFunc func(int)

	mu       sync.Mutex
	lastSent map[string]time.Time

	checkStop chan struct{}
	wg        sync.WaitGroup

	closed   atomic.Bool
	reported atomic.Int64
	dropped  atomic.Int64
	fallback atomic.Int64
}

// Option 配置 Reporter。零值不可用——Sink 必须给出（或用 NewFromEnv）。
type Option struct {
	Sink Sink
	Meta Meta
	// Redact 应用于信封 title/message/attributes/stack（默认恒等）。
	Redact func(string) string
	// Throttle 是同指纹两次上报的最小间隔（默认 60s；0 = 关闭节流）。
	Throttle time.Duration
	// QueueSize 是异步投递队列容量（默认 256；满时丢最旧）。
	QueueSize int
	// ExitFunc 供 Fatal 使用（默认 os.Exit；测试注入）。
	ExitFunc func(int)
}

// New 构建五挂点采集器并启动异步投递与自检调度。用 Close 停止。
func New(opt Option) *Reporter {
	if opt.Sink == nil {
		opt.Sink = defaultSink()
	}
	if opt.Redact == nil {
		opt.Redact = func(s string) string { return s }
	}
	// 0 = 未设置（默认 60s）；负值 = 显式禁用节流。
	switch {
	case opt.Throttle < 0:
		opt.Throttle = 0
	case opt.Throttle == 0:
		opt.Throttle = 60 * time.Second
	}
	if opt.QueueSize <= 0 {
		opt.QueueSize = 256
	}
	if opt.ExitFunc == nil {
		opt.ExitFunc = os.Exit
	}
	meta := opt.Meta
	if meta.Instance == "" {
		meta.Instance = defaultInstance()
	}
	r := &Reporter{
		sink:      opt.Sink,
		meta:      meta,
		redact:    opt.Redact,
		throttle:  opt.Throttle,
		queue:     make(chan Envelope, opt.QueueSize),
		done:      make(chan struct{}),
		exitFunc:  opt.ExitFunc,
		lastSent:  map[string]time.Time{},
		checkStop: make(chan struct{}),
	}
	r.wg.Add(1)
	go r.deliverLoop()
	return r
}

// NewFromEnv 用 NUWA_INGEST_URL / NUWA_INGEST_TOKEN 推导 Sink。
func NewFromEnv(meta Meta) *Reporter {
	return New(Option{Meta: meta})
}

// defaultSink 是零配置 sink：端点缺失时直接落盘；端点存在时 HTTP 投递，
// 失败后落盘兜底（SPEC §6.2：信封不丢）。
//
// 环境变量读取顺序（通用命名空间优先，Nuwa 旧键兼容回退，SPEC §7）：
//
//	QINGNIAO_ENDPOINT → NUWA_INGEST_URL
//	QINGNIAO_TOKEN    → NUWA_INGEST_TOKEN
func defaultSink() Sink {
	fallback := defaultFallbackPath()
	url := firstNonEmpty(
		os.Getenv("QINGNIAO_ENDPOINT"),
		os.Getenv("NUWA_INGEST_URL"),
	)
	if url == "" {
		return FileSink{Path: fallback}
	}
	token := firstNonEmpty(
		os.Getenv("QINGNIAO_TOKEN"),
		os.Getenv("NUWA_INGEST_TOKEN"),
	)
	return HTTPSink{
		Endpoint:     url,
		Token:        token,
		FallbackFile: fallback,
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// defaultFallbackPath 是默认兜底文件位置（用户缓存目录下的 qingniao/）。
func defaultFallbackPath() string {
	dir, err := os.UserCacheDir()
	if err != nil || dir == "" {
		dir = os.TempDir()
	}
	return filepath.Join(dir, "qingniao", "fallback.jsonl")
}

// deliverLoop 消费异步队列：信封逐条投递（超时/重试/落盘兜底由 Sink 承担）。
// Close 时把手上的队列排空再退出（SPEC §2 的 flush）。
func (r *Reporter) deliverLoop() {
	defer r.wg.Done()
	for {
		select {
		case env := <-r.queue:
			r.deliver(env)
		case <-r.done:
			for {
				select {
				case env := <-r.queue:
					r.deliver(env)
				default:
					return
				}
			}
		}
	}
}

// deliver 投递一条并记账：送达 ingest=reported，落盘兜底=fallback，
// 既没送达也没落盘=dropped（信号丢了）。
func (r *Reporter) deliver(env Envelope) {
	err := r.sink.Send(env)
	switch {
	case err == nil:
		r.reported.Add(1)
	case errors.Is(err, ErrFallbackWritten):
		r.fallback.Add(1)
	default:
		r.dropped.Add(1)
	}
}

// submit 是异步入口：节流判定后入队（满丢最旧）。
func (r *Reporter) submit(env Envelope) {
	if r.closed.Load() {
		r.dropped.Add(1)
		return
	}
	fp := env.Fingerprint
	if r.throttle > 0 {
		now := time.Now()
		r.mu.Lock()
		if now.Sub(r.lastSent[fp]) < r.throttle {
			r.lastSent[fp] = now // 滑动窗口：压制期间持续推迟
			r.mu.Unlock()
			r.dropped.Add(1)
			return
		}
		r.lastSent[fp] = now
		r.mu.Unlock()
	}
	select {
	case r.queue <- env:
	default:
		// 满：丢最旧，保最新（最新现场最有诊断价值）。
		select {
		case <-r.queue:
			r.dropped.Add(1)
		default:
		}
		if r.closed.Load() {
			// Close 与 submit 并发：队列不会再被消费，别悄悄塞进去。
			r.dropped.Add(1)
			return
		}
		select {
		case r.queue <- env:
		default:
			r.dropped.Add(1)
		}
	}
}

// sendSync 是同步入口（panic / Fatal 路径）：进程将死，阻塞式投递。
func (r *Reporter) sendSync(env Envelope) {
	r.deliver(env)
}

// Capture 包装执行入口：panic 被捕获、同步上报后原样重抛——采集不改变
// 进程的失败语义。
func (r *Reporter) Capture(fn func()) {
	defer func() {
		if rec := recover(); rec != nil {
			env := PanicEnvelope(r.meta, rec, Stack(), time.Now())
			env.Title = clampTitle(env.Title)
			r.redactEnvelope(&env)
			r.sendSync(env)
			panic(rec)
		}
	}()
	fn()
}

// Go 在安全 goroutine 里跑 fn：panic 被同步上报后**原样重抛**（保持
// Go 的失败语义——goroutine panic 默认崩进程；吞掉它会让调用方误以为
// 没有失败）。
func (r *Reporter) Go(fn func()) {
	r.goWrapped(fn, true)
}

// goWrapped 是 Go 的实现体；rethrow=false 供测试观察上报路径
// （goroutine 的 re-panic 无法被测试进程捕获）。
func (r *Reporter) goWrapped(fn func(), rethrow bool) {
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				env := PanicEnvelope(r.meta, rec, Stack(), time.Now())
				r.redactEnvelope(&env)
				r.sendSync(env)
				if rethrow {
					panic(rec)
				}
			}
		}()
		fn()
	}()
}

// Error 显式上报一条错误（异步队列，medium）。
func (r *Reporter) Error(title string, err error) {
	r.ErrorSeverity(title, err, "medium")
}

// ErrorSeverity 显式上报并指定 severity。
func (r *Reporter) ErrorSeverity(title string, err error, severity string) {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	r.submit(r.envelope(title, msg, severity, Stack()))
}

// Fatal 同步上报 → flush → exit(1)。用于"上报后必须退出"的接入方语义。
// flush 有 2s 上限：退出路径不能被一个卡住的 Sink 拖住。
func (r *Reporter) Fatal(title string, err error) {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	r.sendSync(r.envelope(title, msg, "high", Stack()))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r.Close(ctx)
	r.exitFunc(1)
}

// Check 注册周期自检：fn 返回 error（或执行超过 interval 的视为失败——
// 由调用方在 fn 内部控制超时语义）即上报 high 信号。成功静默。
// 返回停止函数。
func (r *Reporter) Check(name string, interval time.Duration, fn func() error) func() {
	return r.CheckSeverity(name, interval, "high", fn)
}

// CheckSeverity 同 Check 并指定失败信号的 severity。
func (r *Reporter) CheckSeverity(name string, interval time.Duration, severity string, fn func() error) func() {
	if interval <= 0 || fn == nil {
		return func() {}
	}
	ticker := time.NewTicker(interval)
	stop := make(chan struct{})
	var once sync.Once
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-r.checkStop:
				return
			case <-ticker.C:
				if err := safeCheck(fn); err != nil {
					r.submit(r.envelope("healthcheck failed: "+name, err.Error(), severity, nil))
				}
			}
		}
	}()
	// 停止函数幂等：重复调用不 panic（SPEC §6.3）。
	return func() { once.Do(func() { close(stop) }) }
}

// Close 停止自检与后台投递，并等待队列排空（受 ctx 约束）。幂等。
func (r *Reporter) Close(ctx context.Context) {
	if r.closed.Swap(true) {
		return
	}
	close(r.checkStop)
	// 只关 done，不关 queue：并发 submit 往已关闭的 channel 发送会 panic。
	close(r.done)
	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// Stats 暴露 SDK 自观测计数。
type Stats struct {
	Reported int64
	Dropped  int64
	Fallback int64
}

func (r *Reporter) Stats() Stats {
	return Stats{
		Reported: r.reported.Load(),
		Dropped:  r.dropped.Load(),
		Fallback: r.fallback.Load(),
	}
}

// envelope 组装信封并应用脱敏。
func (r *Reporter) envelope(title, message, severity string, stack []byte) Envelope {
	if stack == nil {
		stack = Stack()
	}
	env := Envelope{
		Title:      clampTitle(r.redact(title)),
		Message:    r.redact(message),
		Severity:   severity,
		StackTrace: string(r.redactBytes(stack)),
	}
	template := normalizeTemplate(env.Title + " " + env.Message)
	env.Fingerprint = fingerprintOf(template)
	env.Repo = r.meta.Repo
	env.Service = r.meta.Service
	env.Instance = r.meta.Instance
	return env
}

func (r *Reporter) redactEnvelope(env *Envelope) {
	env.Title = r.redact(env.Title)
	env.Message = r.redact(env.Message)
	env.StackTrace = r.redact(env.StackTrace)
}

func (r *Reporter) redactBytes(b []byte) []byte {
	return []byte(r.redact(string(b)))
}

// safeCheck 把 check fn 的 panic 转成失败——自检自身的崩溃也是失败信号。
func safeCheck(fn func() error) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("healthcheck panicked: %v", rec)
		}
	}()
	return fn()
}

func clampTitle(t string) string {
	if len(t) > 200 {
		return t[:200]
	}
	return t
}

func defaultInstance() string {
	host, _ := os.Hostname()
	var rand [4]byte
	_, _ = cryptorand.Read(rand[:])
	return fmt.Sprintf("%s:%d:%s", host, os.Getpid(), hex.EncodeToString(rand[:]))
}

// runtimeInfo 供测试与诊断。
var _ = runtime.NumCPU
