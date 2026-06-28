package sqlbuilder

import "fmt"

// cteDef CTE（Common Table Expression）定义
type cteDef struct {
	name       string
	columns    []string    // 可选的列别名
	definition *sqlBuilder
}

// With 添加 CTE
func (b *sqlBuilder) With(name string, def *sqlBuilder) *sqlBuilder {
	if def == nil {
		b.err = fmt.Errorf("CTE 定义不能为 nil")
		return b
	}
	if !isSafeIdentifier(name) {
		b.err = fmt.Errorf("非法的 CTE 名称: %s", name)
		return b
	}
	b.ctes = append(b.ctes, cteDef{name: name, definition: def})
	return b
}

// WithRecursive 设置 CTE 为递归模式
func (b *sqlBuilder) WithRecursive() *sqlBuilder {
	b.recursive = true
	return b
}

// WithColumns 添加带列别名的 CTE
func (b *sqlBuilder) WithColumns(name string, columns []string, def *sqlBuilder) *sqlBuilder {
	if def == nil {
		b.err = fmt.Errorf("CTE 定义不能为 nil")
		return b
	}
	if !isSafeIdentifier(name) {
		b.err = fmt.Errorf("非法的 CTE 名称: %s", name)
		return b
	}
	if !isSafeIdentifierAny(columns...) {
		b.err = fmt.Errorf("非法的 CTE 列名")
		return b
	}
	b.ctes = append(b.ctes, cteDef{name: name, columns: columns, definition: def})
	return b
}
