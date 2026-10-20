package qingniao

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
)

// SPEC §4：日志信号的指纹归一化。没有这一步，同一错误的每条日志
// （时间戳、计数、ID 各不相同）指纹都不同，服务端聚合失效。

var (
	uuidRe  = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	ipv4Re  = regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}\b`)
	hexRe   = regexp.MustCompile(`\b[0-9a-fA-F]{8,}\b`)
	floatRe = regexp.MustCompile(`\b\d+\.\d+\b`)
	intRe   = regexp.MustCompile(`\b\d+\b`)
	quoteRe = regexp.MustCompile(`"([^"]{2,})"|'([^']{2,})'`)
)

// normalizeTemplate 把一条日志消息归一化为稳定模板：动态值（数字、
// UUID、IP、长 hex、引号内容）替换为占位符，同因错误收敛到同一指纹。
func normalizeTemplate(s string) string {
	s = uuidRe.ReplaceAllString(s, "⟦UUID⟧")
	s = ipv4Re.ReplaceAllString(s, "⟦IP⟧")
	s = hexRe.ReplaceAllString(s, "⟦HEX⟧")
	s = floatRe.ReplaceAllString(s, "⟦N.N⟧")
	s = quoteRe.ReplaceAllString(s, `"⟦S⟧"`)
	s = intRe.ReplaceAllString(s, "⟦N⟧")
	return s
}

// fingerprintOf 返回归一化模板的短哈希（sha256 前 16 hex）。
func fingerprintOf(template string) string {
	sum := sha256.Sum256([]byte(template))
	return hex.EncodeToString(sum[:8])
}
