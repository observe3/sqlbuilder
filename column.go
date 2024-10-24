package sqlbuilder

import "strings"

// Column 表示一个要查询的字段
type fcolumn struct {
	Alias  string        // 别名
	Fn     string        // 函数
	Params []interface{} // 参数
}

/**
 * 使用mysql函数
 */
func Fn(fn, alias string, params ...interface{}) *fcolumn {
	temp := []interface{}{fn, alias}
	temp = append(temp, params...)
	for _, v := range temp {
		if val, ok := v.(string); ok {
			if strings.Contains(val, "#") || strings.Contains(val, "--") || strings.Contains(val, "/*") {
				fn = ""
			}
		}
	}
	return &fcolumn{
		Alias:  alias,
		Fn:     fn,
		Params: params,
	}
}

type scolumn struct {
	TableAlias string // 表名
	Field      string // 表字段
	FieldAlias string // 字段别名
}

/**
 * 查询指定表的字段
 */
func SField(tableAlias, field, fieldAlias string) *scolumn {
	temp := []string{tableAlias, field, fieldAlias}
	for _, v := range temp {
		if strings.Contains(v, "#") || strings.Contains(v, "--") || strings.Contains(v, "/*") {
			tableAlias = ""
			fieldAlias = ""
			field = ""
		}
	}
	return &scolumn{
		TableAlias: tableAlias,
		FieldAlias: fieldAlias,
		Field:      field,
	}
}
