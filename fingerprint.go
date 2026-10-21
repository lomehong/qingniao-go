package qingniao

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"
)

// panicFingerprint 计算 panic 信号的**客户端**指纹。
//
// SPEC §4 要求 SDK 归一化 panic 栈：同一处崩溃在不同时刻（地址、行号漂移）
// 必须落到同一个 key，否则服务端聚合失效。这里逐字镜像山海经服务端的 Go
// 栈算法（`crates/shanhaijing/src/fingerprint.rs` 的 `parse_go` +
// `normalize_text`）：两侧用同一个 golden 常量互钉，任何一侧改了算法都会
// 让另一侧的测试失败——避免"同一 panic 因谁算指纹而落进两个 key"。
//
// 指纹 = sha256("repo|go|错误类|帧1|帧2|帧3")，取前 32 个 hex 字符。
// repo 是身份的一部分（ADR-013）：两个仓库恰好同栈同名时不得合并——
// 自愈流水线按仓库取 checkout、按仓库开 PR。
func panicFingerprint(repo, message, stack string) string {
	errorClass := ""
	if strings.TrimSpace(message) != "" {
		errorClass = normalizeText(message)
	} else {
		errorClass = normalizeText(firstNonEmptyLine(stack))
	}
	frames := parseGoFrames(stack)
	sum := sha256.New()
	sum.Write([]byte(repo))
	sum.Write([]byte("|"))
	sum.Write([]byte("go"))
	sum.Write([]byte("|"))
	sum.Write([]byte(errorClass))
	for i, frame := range frames {
		if i >= 3 {
			break
		}
		sum.Write([]byte("|"))
		sum.Write([]byte(frame))
	}
	return hex.EncodeToString(sum.Sum(nil))[:32]
}

// normalizeText 折叠易变部分：十六进制地址 → 0xN，十进制数字串 → N。
// 与服务端 `normalize_text` 同规则。
func normalizeText(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	i := 0
	for i < len(s) {
		if s[i] == '0' && i+1 < len(s) && s[i+1] == 'x' {
			j := i + 2
			for j < len(s) && isHexDigit(s[j]) {
				j++
			}
			if j > i+2 {
				b.WriteString("0xN")
				i = j
				continue
			}
		}
		if s[i] >= '0' && s[i] <= '9' {
			for i < len(s) && s[i] >= '0' && s[i] <= '9' {
				i++
			}
			b.WriteString("N")
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// parseGoFrames 从 goroutine trace 里取应用帧：`函数@文件`（去行号）。
// runtime./testing./reflect. 噪音帧跳过，"created by" 之后截断。
func parseGoFrames(stack string) []string {
	var frames []string
	pendingFunc := ""
	havePending := false
	for _, raw := range strings.Split(stack, "\n") {
		line := strings.TrimRightFunc(raw, unicode.IsSpace)
		trimmed := strings.TrimLeftFunc(line, unicode.IsSpace)
		if trimmed == "" || strings.HasPrefix(trimmed, "goroutine ") {
			continue
		}
		if strings.HasPrefix(trimmed, "created by ") {
			break
		}
		if strings.HasPrefix(line, "\t") {
			if strings.Contains(trimmed, ".go:") && havePending {
				fn := pendingFunc
				havePending = false
				if !isGoNoise(fn) {
					frames = append(frames, pushFrame(fn, trimmed))
				}
			}
			// 其它缩进行（调用实参）忽略。
			continue
		}
		if strings.Contains(trimmed, "(") {
			havePending = true
			pendingFunc = trimmed[:strings.Index(trimmed, "(")]
		}
	}
	return frames
}

// pushFrame 把 `pkg.Func(args)` + `/app/calc.go:17 +0x8c` 压成
// `pkg.Func@calc.go`。
func pushFrame(fn, filePart string) string {
	file := filePart
	for _, seg := range strings.FieldsFunc(filePart, func(r rune) bool {
		return r == ':' || r == ' '
	}) {
		if strings.Contains(seg, ".") {
			file = seg
			break
		}
	}
	if idx := strings.LastIndexAny(file, "/\\"); idx >= 0 {
		file = file[idx+1:]
	}
	return fn + "@" + file
}

func isGoNoise(fn string) bool {
	return strings.HasPrefix(fn, "runtime.") ||
		strings.HasPrefix(fn, "testing.") ||
		strings.HasPrefix(fn, "reflect.")
}

func firstNonEmptyLine(stack string) string {
	for _, line := range strings.Split(stack, "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
