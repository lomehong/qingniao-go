# qingniao-go（青鸟 · Go 采集 SDK）

Nuwa 自愈流水线的 Go 采集 SDK：捕获 panic / 错误信号 → 指纹化（归一化栈）→ JSONL 落盘或 HTTP 上报。

## 用法

```go
import "github.com/lomehong/qingniao-go"

// 崩溃捕获：记录信号后按原 panic 继续抛出（不吞 panic）
defer qingniao.Capture(qingniao.Config{
    Repo:      "github.com/you/your-repo",
    Language:  "go",
    Sink:      qingniao.HTTPSink{Endpoint: "http://nuwa-host:7321/api/v1/ingest"},
    Fingerprint: qingniao.FingerprintConfig{...},
})
```

- `HTTPSink`：每次上报带超时（默认 3s）与有界重试（默认 2 次），最终失败可落 `FallbackFile`（JSONL），崩溃路径永不挂死
- `FileSink`：直接写 JSONL
- 指纹：归一化栈符号（去地址/行号漂移），同因聚合

## 版本

语义化版本；Nuwa 服务端与采集端 schema 兼容性见 Nuwa 仓库 docs/08。
