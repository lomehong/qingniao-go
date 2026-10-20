# 青鸟采集 SDK · 语言无关规范（qingniao SPEC v1）

> 本文是青鸟各语言实现（qingniao-go / 将来的 qingniao-java / qingniao-rust …）
> 的行为规范。各语言实现 API 形态可以贴近语言惯例，但**信号类别、投递语义、
> 指纹归一化、severity 映射、失败安全**必须与本文一致——接入方换语言时
> 心智模型不变，Nuwa 服务端无感知。

## 1 · 信号类别（五类）

| # | 信号 | 触发方式 | 投递通道 | 默认 severity |
|---|------|---------|---------|--------------|
| 1 | panic / 未捕获异常 | 包装执行入口（recover 后**原样重抛**，不改变失败语义） | **同步**（进程将死） | high |
| 2 | goroutine panic | 安全 goroutine 包装（recover→上报→重抛） | 同步 | high |
| 3 | 显式错误 | 接入方在错误路径显式调用 | **异步队列** | 接入方定（默认 medium） |
| 4 | 错误日志 | 语言惯用日志面钩子（Go = `log/slog` Handler；Java = Logger Appender；Rust = log facade） | 异步队列 | 钩子级别决定（Error→medium） |
| 5 | 失败自检 | 周期探针注册：fn 返回 error 或超时即失败 | 异步队列 | high（可配） |

**明确不采集**：指标/延迟/性能剖析/告警路由——那是既有可观测性设施的域，
青鸟只消费"问题信号"（Nuwa docs/01 边界）。Go runtime fatal error
（如全局死锁 throw）不可 recover，超出任何 SDK 能力，文档须诚实声明。

## 2 · 投递语义（双通道）

- **同步通道**（panic / goroutine panic / Fatal）：进程将死或即将退出，
  上报必须在本进程死亡前完成——阻塞式尝试，超时后落 FallbackFile。
- **异步队列**（log / Check / 显式错误）：有界 ring（默认 256），后台
  投递者消费；队列满**丢最旧**并计入 dropped。高频错误绝不阻塞、绝不
  拖死主流程。`Close(timeout)` 时 flush 队列并停掉自检。

## 3 · 客户端节流

同指纹在上报后 `throttle 窗口`（默认 60s，可配 0=关）内不重复上报
（计 dropped）。**服务端的指纹去重+冷却是聚合层，不是节流的替代**——
崩溃/风暴中的进程不能被上报 IO 自身拖死。

## 4 · 指纹归一化

- panic 信号：**SDK 侧也算**（`panicFingerprint`）——归一化栈（去地址
  `+0x…`、去行号、保留符号路径），与山海经服务端的 Go 栈算法**逐字一致**；
  两侧用同一个 golden 常量互钉（`qingniao-go` 与 `crates/shanhaijing` 各有一份
  断言），任一侧改算法都会让另一侧测试失败。服务端当前按自己的算法计算
  （信封超集原则，`fingerprint` 字段被忽略）；两份值一致既便于客户端自检
  与对账，也保证服务端将来"优先采用客户端指纹"时无缝切换。
- 日志信号：**模板归一化**——连续数字→`⟦N⟧`、UUID→`⟦UUID⟧`、
  IPv4→`⟦IP⟧`、≥8 位 hex→`⟦HEX⟧`、引号内内容→`⟦S⟧`；指纹 = 归一化
  后模板的哈希。没有这步，同一错误每条日志指纹都不同，服务端聚合失效。

## 5 · severity 映射

| 信号 | 默认 | 可配 |
|------|------|------|
| panic / goroutine panic | high | 否（崩溃即高危） |
| 日志钩子 | 跟钩子级别（Error→high） | Handler 构造时定级别 |
| Check 失败 | high | 注册时指定 |
| 显式错误 | 接入方指定 | 每次 |

## 6 · 失败安全（崩溃路径三原则）

1. 每次投递有**超时**（默认 3s），有界重试（默认 2 次）；
2. 全部失败落 **FallbackFile**（JSONL），信封不丢。默认 sink 自带兜底
   路径（用户缓存目录下的 `qingniao/fallback.jsonl`）：端点缺失时直接落盘，
   端点存在时投递失败也落盘。落盘成功时 `Send` 返回哨兵错误
   `ErrFallbackWritten`——**信号没丢，但没送达**，调用方用 `errors.Is`
   区分，不要当成致命错误；
3. SDK 自身**永不 panic、永不写 stdout**（stdio 协议进程的传输通道
   不可污染）：关闭与并发上报竞争时不得 `panic: send on closed channel`
   （队列永不 close，用 done 信号停投递循环）；自检停止函数可重复调用；
   SDK 自身错误静默计数，经 `Stats()` 暴露。

`Stats()` 三个计数的语义（互斥）：

| 计数 | 含义 |
|---|---|
| `Reported` | 已送达 ingest 服务端 |
| `Fallback` | 投递失败但已写入本地兜底文件（信号在，人没看到） |
| `Dropped` | 信号既没送达也没落盘：节流抑制 / 队列满丢弃 / 投递失败且无兜底文件 |

## 7 · 安全

- `Redact` 钩子（`func(string) string`，默认恒等）应用于信封的
  title / message / attributes / stack——错误消息与日志天然携带敏感
  数据，源头脱敏是 SDK 的责任，服务端脱敏只是最后一道。
- 凭证只走 env（`NUWA_INGEST_URL` / `NUWA_INGEST_TOKEN`）或显式
  Option，不落盘、不进日志。

## 8 · 信封（qingniao/v1 超集原则）

新增字段（如 `attributes`、`instance`）永远 optional；旧服务端忽略
未知字段，不破坏。`instance`（host:pid:启动随机串）用于多副本部署时
区分"三副本同时炸"与"一台反复炸"。

## 9 · 语言无关 API 面（五挂点 + 两通道 + 三能力）

```
New(Option) → Reporter
  Capture(fn)                    # panic（同步）
  Go(fn)                         # 安全 goroutine
  Error(title, err)              # 显式错误（异步）
  Fatal(title, err)              # 上报+flush+exit —— 同步
  Logger/Handler(level)          # 错误日志钩子（异步）
  Check(name, interval, fn)      # 失败自检（异步）
  Stats()                        # reported/dropped/fallback 计数
  Close(timeout)                 # flush + 停自检
```

零配置：未显式给 Sink 时从 env 推导（`NUWA_INGEST_URL`/`NUWA_INGEST_TOKEN`）。
