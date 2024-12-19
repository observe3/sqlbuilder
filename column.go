package sqlbuilder

type funCarrier struct {
	Alias  string        // 别名
	Fn     string        // 函数
	Params []interface{} // 参数
}

/**
 * 使用mysql函数
 * 避免使用本函数来拼接用户提交的数据
 * fn 表示函数名，如：sum,max,min,count,avg
 * alias 表示别名
 * params 表示函数参数
 */
func Fn(fn, alias string, params ...interface{}) *funCarrier {
	temp := []interface{}{fn, alias}
	temp = append(temp, params...)
	for _, v := range temp {
		if val, ok := v.(string); ok {
			if hasIllegalStr(val) {
				fn = ""
			}
		}
	}
	return &funCarrier{
		Alias:  alias,
		Fn:     fn,
		Params: params,
	}
}

type colCarrier struct {
	TableAlias string // 表名
	Field      string // 表字段
	FieldAlias string // 字段别名
}

/**
 * 查询指定表的字段
 * 避免使用本函数来拼接用户提交的数据
 */
func SField(tableAlias, field, fieldAlias string) *colCarrier {
	temp := []string{tableAlias, field, fieldAlias}
	for _, v := range temp {
		if hasIllegalStr(v) {
			tableAlias = ""
			fieldAlias = ""
			field = ""
		}
	}
	return &colCarrier{
		TableAlias: tableAlias,
		FieldAlias: fieldAlias,
		Field:      field,
	}
}

type literalCarrier struct {
	OriginVal string
}

/**
 * 使用原语
 * 避免使用本函数来拼接用户提交的数据
 */
func Literal(originVal string) *literalCarrier {
	if hasIllegalStr(originVal) {
		originVal = ""
	}
	return &literalCarrier{
		OriginVal: originVal,
	}
}
