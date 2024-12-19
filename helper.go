package sqlbuilder

import "strings"

func hasIllegalStr(val string) bool {
	// 特殊字符列表
	specialChars := []string{"'", "\"", ";", "@", "[", "]", "{", "}", "!", "|", "&", "~", "#", "\\", "/*","--"}
	for _, char := range specialChars {
		if strings.Contains(val, char) {
			return true
		}
	}
	return false
}