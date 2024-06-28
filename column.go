package sqlbuilder

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
	return &scolumn{
		TableAlias: tableAlias,
		FieldAlias: fieldAlias,
		Field:      field,
	}
}
