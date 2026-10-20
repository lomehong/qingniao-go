package qingniao

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

// ReporterHandler 把 slog 的记录转成青鸟信号（SPEC §1 信号 4）。
// 通过 Reporter.Handler(level) 构造；挂进任何已有 slog.Logger。
// 级别低于 minLevel 的记录静默放行（Enabled=false）。
type ReporterHandler struct {
	r        *Reporter
	minLevel slog.Level
	attrs    []slog.Attr
	group    string
	mu       *sync.Mutex
}

var _ slog.Handler = (*ReporterHandler)(nil)

// Handler 返回 slog.Handler：level 及以上的记录成为信号（异步队列，
// medium；panic 级别记录映射 high）。指纹按模板归一化（SPEC §4），
// 同因日志在节流窗口内只发一条。
func (r *Reporter) Handler(minLevel slog.Level) slog.Handler {
	return &ReporterHandler{r: r, minLevel: minLevel, mu: &sync.Mutex{}}
}

func (h *ReporterHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.minLevel
}

func (h *ReporterHandler) Handle(_ context.Context, rec slog.Record) error {
	if h.r.closed.Load() {
		return nil
	}
	var b strings.Builder
	b.WriteString(rec.Message)
	attrs := make(map[string]string, len(h.attrs)+rec.NumAttrs())
	for _, a := range h.attrs {
		writeAttr(&b, attrs, "", a)
	}
	rec.Attrs(func(a slog.Attr) bool {
		writeAttr(&b, attrs, "", a)
		return true
	})
	raw := strings.TrimSpace(b.String())
	if h.group != "" {
		raw = h.group + ": " + raw
	}
	template := normalizeTemplate(raw)
	severity := "medium"
	if rec.Level >= slog.LevelError {
		severity = "high"
	}
	env := Envelope{
		Schema:      Schema,
		Repo:        h.r.meta.Repo,
		Language:    "go",
		Service:     h.r.meta.Service,
		Host:        hostName(),
		Title:       clampTitle(template),
		Severity:    severity,
		Message:     h.r.redact(rec.Message),
		TsUnixMs:    time.Now().UnixMilli(),
		Fingerprint: fingerprintOf(template),
		Instance:    h.r.meta.Instance,
		Attributes:  redactMap(h.r.redact, attrs),
	}
	if len(env.Attributes) == 0 {
		env.Attributes = nil
	}
	h.r.submit(env)
	return nil
}

func writeAttr(b *strings.Builder, attrs map[string]string, prefix string, a slog.Attr) {
	key := a.Key
	if prefix != "" {
		key = prefix + "." + a.Key
	}
	switch v := a.Value.Resolve().Any().(type) {
	case string:
		b.WriteString(" " + key + "=" + v)
		attrs[key] = v
	default:
		s := a.Value.Resolve().String()
		b.WriteString(" " + key + "=" + s)
		attrs[key] = s
	}
}

func redactMap(redact func(string) string, in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = redact(v)
	}
	return out
}

func (h *ReporterHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clone := *h
	clone.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &clone
}

func (h *ReporterHandler) WithGroup(name string) slog.Handler {
	clone := *h
	clone.group = name
	return &clone
}

func hostName() string {
	h, _ := os.Hostname()
	return h
}
