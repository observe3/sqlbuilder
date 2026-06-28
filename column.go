package sqlbuilder

type funCarrier struct {
	Alias  string        // 别名
	Fn     string        // 函数
	Params []any // 参数
}

/**
 * 使用mysql函数
 * 避免使用本函数来拼接用户提交的数据
 * fn 表示函数名，如：sum,max,min,count,avg
 * alias 表示别名
 * params 表示函数参数
 */
func Fn(fn, alias string, params ...any) *funCarrier {
	// 函数名和别名是标识符，需要严格校验
	if !isSafeIdentifier(fn) || !isSafeIdentifier(alias) {
		return &funCarrier{}
	}
	// 参数值用 hasIllegalStr 校验
	for _, v := range params {
		if val, ok := v.(string); ok {
			if hasIllegalStr(val) {
				return &funCarrier{}
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
		if !isSafeIdentifier(v) {
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

// winCarrier 窗口函数载体
type winCarrier struct {
	Alias       string
	Fn          string     // sum, avg, row_number, rank, dense_rank, etc.
	Params      []any
	PartitionBy []string
	OrderBy     [][]any
}

/**
 * 使用窗口函数
 * 避免使用本函数来拼接用户提交的数据
 * fn 表示函数名，如：row_number, rank, dense_rank, sum, avg 等
 * alias 表示别名
 * params 表示函数参数
 */
func WinFn(fn, alias string, params ...any) *winCarrier {
	if !isSafeIdentifier(fn) || !isSafeIdentifier(alias) {
		return &winCarrier{}
	}
	return &winCarrier{
		Alias:  alias,
		Fn:     fn,
		Params: params,
	}
}

// Partition 设置 PARTITION BY 字段
func (w *winCarrier) Partition(fields ...string) *winCarrier {
	if !isSafeIdentifierAny(fields...) {
		return &winCarrier{}
	}
	w.PartitionBy = fields
	return w
}

// OrderByClause 设置窗口内排序
func (w *winCarrier) OrderByClause(order [][]any) *winCarrier {
	for _, v := range order {
		if len(v) >= 1 {
			if s, ok := v[0].(string); ok && hasIllegalStr(s) {
				return &winCarrier{}
			}
		}
		if len(v) >= 2 {
			if s, ok := v[1].(string); ok && hasIllegalStr(s) {
				return &winCarrier{}
			}
		}
	}
	w.OrderBy = order
	return w
}

// caseCarrier CASE WHEN 表达式载体
type caseCarrier struct {
	Alias     string
	CaseField string       // 简单 CASE 的字段
	Whens     []whenClause
	ElseVal   any
}

// whenClause WHEN ... THEN 子句
type whenClause struct {
	When any
	Then any
}

/**
 * CASE WHEN 表达式
 * alias 表示别名
 */
func CaseWhen(alias string) *caseCarrier {
	if !isSafeIdentifier(alias) {
		return &caseCarrier{}
	}
	return &caseCarrier{
		Alias: alias,
	}
}

// SimpleCase 设置简单 CASE 的字段
func (c *caseCarrier) SimpleCase(field string) *caseCarrier {
	if !isSafeIdentifier(field) {
		return &caseCarrier{}
	}
	c.CaseField = field
	return c
}

// When 添加 WHEN ... THEN 条件
func (c *caseCarrier) When(when, then any) *caseCarrier {
	c.Whens = append(c.Whens, whenClause{When: when, Then: then})
	return c
}

// Else 设置 ELSE 值
func (c *caseCarrier) Else(val any) *caseCarrier {
	c.ElseVal = val
	return c
}

// jsonFieldCarrier JSON 字段访问载体
type jsonFieldCarrier struct {
	TableAlias string
	Field      string
	Arrow      string // "->" or "->>"
	Path       string // "$.key"
}

/**
 * JSON 字段访问
 * 避免使用本函数来拼接用户提交的数据
 * tableAlias 表别名，field 字段名，arrow "->" 或 "->>"，path JSON 路径
 */
func JsonField(tableAlias, field, arrow, path string) *jsonFieldCarrier {
	for _, v := range []string{tableAlias, field, arrow, path} {
		if !isSafeIdentifier(v) {
			return &jsonFieldCarrier{}
		}
	}
	return &jsonFieldCarrier{
		TableAlias: tableAlias,
		Field:      field,
		Arrow:      arrow,
		Path:       path,
	}
}
