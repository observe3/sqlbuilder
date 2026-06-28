package sqlbuilder

import "fmt"

// unionClause UNION 子句
type unionClause struct {
	typ     string      // "union" or "union all"
	builder *sqlBuilder
}

// Union 添加 UNION 子查询
func (b *sqlBuilder) Union(sub *sqlBuilder) *sqlBuilder {
	if sub == nil {
		b.err = fmt.Errorf("Union 子查询不能为 nil")
		return b
	}
	b.unions = append(b.unions, unionClause{typ: "union", builder: sub})
	return b
}

// UnionAll 添加 UNION ALL 子查询
func (b *sqlBuilder) UnionAll(sub *sqlBuilder) *sqlBuilder {
	if sub == nil {
		b.err = fmt.Errorf("UnionAll 子查询不能为 nil")
		return b
	}
	b.unions = append(b.unions, unionClause{typ: "union all", builder: sub})
	return b
}
