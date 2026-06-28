package sqlbuilder

import "strings"

// hasIllegalStr 检查字符串是否包含 SQL 注入危险字符
func hasIllegalStr(val string) bool {
	// 特殊字符列表
	specialChars := []string{"\"", ";", "@", "[", "]", "{", "}", "!", "|", "&", "~", "#", "\\", "/*", "--"}
	for _, char := range specialChars {
		if strings.Contains(val, char) {
			return true
		}
	}
	return false
}

// hasIllegalStrAny 检查任意字符串是否包含 SQL 注入危险字符
func hasIllegalStrAny(vals ...string) (int, bool) {
	for i, v := range vals {
		if hasIllegalStr(v) {
			return i, true
		}
	}
	return -1, false
}

// isSafeIdentifier 检查标识符（表名、列名、别名等）是否安全
// 在 hasIllegalStr 基础上额外检查反引号，防止标识符引用逃逸
func isSafeIdentifier(val string) bool {
	return !hasIllegalStr(val) && !strings.Contains(val, "`")
}

// isSafeIdentifierAny 检查多个标识符是否全部安全
func isSafeIdentifierAny(vals ...string) bool {
	for _, v := range vals {
		if !isSafeIdentifier(v) {
			return false
		}
	}
	return true
}
