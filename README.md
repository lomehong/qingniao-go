# qingniao-go（青鸟 · Go 采集 SDK）

Nuwa 自愈流水线的 Go 采集 SDK。行为规范见 [SPEC.md](SPEC.md)（语言无关，各语言实现共用）。

**五类信号，双通道投递**：panic（同步）、goroutine panic（同步）、显式错误（异步队列）、
错误日志（slog Handler，异步）、失败自检（异步）。指纹模板归一化 + 客户端节流 +
超时/重试/落盘兜底——崩溃路径永不挂死，高频错误永不阻塞主流程。

## 用法（v0.2）

```go
import "github.com/lomehong/qingniao-go"

q := qingniao.New(qingniao.Option{
    Meta: qingniao.Meta{Repo: "github.com/you/your-repo", Service: "api"},
    // Sink 缺省从 NUWA_INGEST_URL / NUWA_INGEST_TOKEN 推导
    // Redact: func(s string) string { ... }   // 源头脱敏（可选）
})
defer q.Close(context.Background())

// 1. panic：捕获 → 同步上报 → 原样重抛（不改变失败语义）
defer q.Capture(func() { run() })

// 2. goroutine panic：安全包装（上报后重抛）
q.Go(func() { watchTick() })

// 3. 显式错误：异步队列
q.Error("cache warmup failed", err)

// 4. 错误日志：挂进任何已有 slog.Logger（Error 级起成为信号）
logger := slog.New(q.Handler(slog.LevelWarn))

// 5. 失败自检：周期探针，失败上报 high
stop := q.Check("db-ping", 30*time.Second, ping)
defer stop()

// 不可恢复：同步上报 → flush → exit(1)
if fatal { q.Fatal("config corrupt", err) }
```

## 版本

语义化版本。v0.1 函数式 API（`Capture`/`Report`/Sinks）保留兼容。
当前：[![v0.2.0](https://img.shields.io/badge/module-v0.2.0-blue)](https://pkg.go.dev/github.com/lomehong/qingniao-go)
