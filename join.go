package sqlbuilder

import (
	"fmt"
	"strings"
)

// joinType 连接类型
type joinType uint8

const (
	innerJoin joinType = iota
	leftJoin
	rightJoin
	crossJoin
	naturalJoin
	straightJoin
	fullOuterJoin
)

func (j joinType) keyword() string {
	switch j {
	case leftJoin:
		return "left join"
	case rightJoin:
		return "right join"
	case crossJoin:
		return "cross join"
	case naturalJoin:
		return "natural join"
	case straightJoin:
		return "straight_join"
	case fullOuterJoin:
		return "left join" // FULL OUTER JOIN emulated as LEFT JOIN for MySQL
	case innerJoin:
		return "join"
	default:
		return "join"
	}
}

// onCondition 单个 ON 条件
type onCondition struct {
	leftTable  string
	leftField  string
	operator   string
	rightTable string
	rightField string
}

// joinClause 结构化连接子句
type joinClause struct {
	typ       joinType
	tableName string
	alias     string
	using     []string      // USING(f1, f2, ...)
	onConds   []onCondition // 结构化 ON 条件
	subquery  *sqlBuilder   // 子查询作为表源
}

// renderJoin 渲染单个连接子句为 SQL 片段，产生的参数值追加到 fieldValue
func (b *sqlBuilder) renderJoin(j joinClause) string {
	var sb strings.Builder

	sb.WriteString(j.typ.keyword())
	sb.WriteByte(' ')

	// 表或子查询
	if j.subquery != nil {
		q, args, err := j.subquery.BuildSelect()
		if err != nil {
			b.err = err
			return ""
		}
		b.fieldValue = append(b.fieldValue, args...)
		sb.WriteString(fmt.Sprintf("(%s)", q))
	} else {
		sb.WriteString(fmt.Sprintf("`%s`", j.tableName))
	}

	// 别名
	if j.alias != "" {
		sb.WriteString(fmt.Sprintf(" as `%s`", j.alias))
	}

	// USING
	if len(j.using) > 0 {
		quoted := make([]string, len(j.using))
		for i, f := range j.using {
			quoted[i] = fmt.Sprintf("`%s`", f)
		}
		sb.WriteString(fmt.Sprintf(" using (%s)", strings.Join(quoted, ", ")))
		return sb.String()
	}

	// ON 条件
	if len(j.onConds) > 0 {
		sb.WriteString(" on ")
		for i, c := range j.onConds {
			if i > 0 {
				sb.WriteString(" and ")
			}
			sb.WriteString(fmt.Sprintf("`%s`.`%s` %s `%s`.`%s`",
				c.leftTable, c.leftField, c.operator, c.rightTable, c.rightField))
		}
	}

	return sb.String()
}

// buildJoinClause 渲染所有连接子句
func (b *sqlBuilder) buildJoinClause() string {
	if len(b.joins) == 0 {
		return ""
	}
	var parts []string
	for _, j := range b.joins {
		parts = append(parts, b.renderJoin(j))
	}
	return strings.Join(parts, " ")
}

// Join 内连接（保持向后兼容的签名）
func (b *sqlBuilder) Join(tableName string, alias string, f1, f2 string) *sqlBuilder {
	if !isSafeIdentifierAny(tableName, alias, f1, f2) {
		b.err = fmt.Errorf("非法的 JOIN 参数")
		return b
	}
	if alias == "" {
		alias = tableName
	}
	b.joins = append(b.joins, joinClause{
		typ:       innerJoin,
		tableName: tableName,
		alias:     alias,
		onConds: []onCondition{
			{leftTable: b.alias, leftField: f1, operator: "=", rightTable: alias, rightField: f2},
		},
	})
	return b
}

// LeftJoin 左连接
func (b *sqlBuilder) LeftJoin(tableName string, alias string, f1, f2 string) *sqlBuilder {
	if !isSafeIdentifierAny(tableName, alias, f1, f2) {
		b.err = fmt.Errorf("非法的 LEFT JOIN 参数")
		return b
	}
	if alias == "" {
		alias = tableName
	}
	b.joins = append(b.joins, joinClause{
		typ:       leftJoin,
		tableName: tableName,
		alias:     alias,
		onConds: []onCondition{
			{leftTable: b.alias, leftField: f1, operator: "=", rightTable: alias, rightField: f2},
		},
	})
	return b
}

// RightJoin 右连接
func (b *sqlBuilder) RightJoin(tableName string, alias string, f1, f2 string) *sqlBuilder {
	if !isSafeIdentifierAny(tableName, alias, f1, f2) {
		b.err = fmt.Errorf("非法的 RIGHT JOIN 参数")
		return b
	}
	if alias == "" {
		alias = tableName
	}
	b.joins = append(b.joins, joinClause{
		typ:       rightJoin,
		tableName: tableName,
		alias:     alias,
		onConds: []onCondition{
			{leftTable: b.alias, leftField: f1, operator: "=", rightTable: alias, rightField: f2},
		},
	})
	return b
}

// CrossJoin 交叉连接
func (b *sqlBuilder) CrossJoin(tableName, alias string) *sqlBuilder {
	if !isSafeIdentifierAny(tableName, alias) {
		b.err = fmt.Errorf("非法的 CROSS JOIN 参数")
		return b
	}
	if alias == "" {
		alias = tableName
	}
	b.joins = append(b.joins, joinClause{
		typ:       crossJoin,
		tableName: tableName,
		alias:     alias,
	})
	return b
}

// NaturalJoin 自然连接
func (b *sqlBuilder) NaturalJoin(tableName, alias string) *sqlBuilder {
	if !isSafeIdentifierAny(tableName, alias) {
		b.err = fmt.Errorf("非法的 NATURAL JOIN 参数")
		return b
	}
	if alias == "" {
		alias = tableName
	}
	b.joins = append(b.joins, joinClause{
		typ:       naturalJoin,
		tableName: tableName,
		alias:     alias,
	})
	return b
}

// StraightJoin 强制连接顺序（MySQL）
func (b *sqlBuilder) StraightJoin(tableName, alias string) *sqlBuilder {
	if !isSafeIdentifierAny(tableName, alias) {
		b.err = fmt.Errorf("非法的 STRAIGHT_JOIN 参数")
		return b
	}
	if alias == "" {
		alias = tableName
	}
	b.joins = append(b.joins, joinClause{
		typ:       straightJoin,
		tableName: tableName,
		alias:     alias,
	})
	return b
}

// FullJoin 全外连接（MySQL 用 LEFT JOIN 模拟）
func (b *sqlBuilder) FullJoin(tableName, alias, f1, f2 string) *sqlBuilder {
	if !isSafeIdentifierAny(tableName, alias, f1, f2) {
		b.err = fmt.Errorf("非法的 FULL JOIN 参数")
		return b
	}
	if alias == "" {
		alias = tableName
	}
	b.joins = append(b.joins, joinClause{
		typ:       fullOuterJoin,
		tableName: tableName,
		alias:     alias,
		onConds: []onCondition{
			{leftTable: b.alias, leftField: f1, operator: "=", rightTable: alias, rightField: f2},
		},
	})
	return b
}

// JoinOn 内连接，支持复杂 ON 条件
// ons 每项为 []string{leftTable, leftField, operator, rightTable, rightField}
func (b *sqlBuilder) JoinOn(tableName, alias string, ons ...[]string) *sqlBuilder {
	if !isSafeIdentifierAny(tableName, alias) {
		b.err = fmt.Errorf("非法的 JOIN ON 参数")
		return b
	}
	if alias == "" {
		alias = tableName
	}
	jc := joinClause{typ: innerJoin, tableName: tableName, alias: alias}
	for _, on := range ons {
		if len(on) < 5 {
			continue
		}
		if !isSafeIdentifierAny(on...) {
			b.err = fmt.Errorf("非法的 JOIN ON 条件")
			return b
		}
		jc.onConds = append(jc.onConds, onCondition{
			leftTable: on[0], leftField: on[1], operator: on[2],
			rightTable: on[3], rightField: on[4],
		})
	}
	b.joins = append(b.joins, jc)
	return b
}

// LeftJoinOn 左连接，支持复杂 ON 条件
func (b *sqlBuilder) LeftJoinOn(tableName, alias string, ons ...[]string) *sqlBuilder {
	if !isSafeIdentifierAny(tableName, alias) {
		b.err = fmt.Errorf("非法的 LEFT JOIN ON 参数")
		return b
	}
	if alias == "" {
		alias = tableName
	}
	jc := joinClause{typ: leftJoin, tableName: tableName, alias: alias}
	for _, on := range ons {
		if len(on) < 5 {
			continue
		}
		if !isSafeIdentifierAny(on...) {
			b.err = fmt.Errorf("非法的 LEFT JOIN ON 条件")
			return b
		}
		jc.onConds = append(jc.onConds, onCondition{
			leftTable: on[0], leftField: on[1], operator: on[2],
			rightTable: on[3], rightField: on[4],
		})
	}
	b.joins = append(b.joins, jc)
	return b
}

// RightJoinOn 右连接，支持复杂 ON 条件
func (b *sqlBuilder) RightJoinOn(tableName, alias string, ons ...[]string) *sqlBuilder {
	if !isSafeIdentifierAny(tableName, alias) {
		b.err = fmt.Errorf("非法的 RIGHT JOIN ON 参数")
		return b
	}
	if alias == "" {
		alias = tableName
	}
	jc := joinClause{typ: rightJoin, tableName: tableName, alias: alias}
	for _, on := range ons {
		if len(on) < 5 {
			continue
		}
		if !isSafeIdentifierAny(on...) {
			b.err = fmt.Errorf("非法的 RIGHT JOIN ON 条件")
			return b
		}
		jc.onConds = append(jc.onConds, onCondition{
			leftTable: on[0], leftField: on[1], operator: on[2],
			rightTable: on[3], rightField: on[4],
		})
	}
	b.joins = append(b.joins, jc)
	return b
}

// JoinUsing 内连接，使用 USING 子句
func (b *sqlBuilder) JoinUsing(tableName, alias string, fields ...string) *sqlBuilder {
	allFields := append([]string{tableName, alias}, fields...)
	if !isSafeIdentifierAny(allFields...) {
		b.err = fmt.Errorf("非法的 JOIN USING 参数")
		return b
	}
	if alias == "" {
		alias = tableName
	}
	b.joins = append(b.joins, joinClause{
		typ: innerJoin, tableName: tableName, alias: alias, using: fields,
	})
	return b
}

// LeftJoinUsing 左连接，使用 USING 子句
func (b *sqlBuilder) LeftJoinUsing(tableName, alias string, fields ...string) *sqlBuilder {
	allFields := append([]string{tableName, alias}, fields...)
	if !isSafeIdentifierAny(allFields...) {
		b.err = fmt.Errorf("非法的 LEFT JOIN USING 参数")
		return b
	}
	if alias == "" {
		alias = tableName
	}
	b.joins = append(b.joins, joinClause{
		typ: leftJoin, tableName: tableName, alias: alias, using: fields,
	})
	return b
}

// RightJoinUsing 右连接，使用 USING 子句
func (b *sqlBuilder) RightJoinUsing(tableName, alias string, fields ...string) *sqlBuilder {
	allFields := append([]string{tableName, alias}, fields...)
	if !isSafeIdentifierAny(allFields...) {
		b.err = fmt.Errorf("非法的 RIGHT JOIN USING 参数")
		return b
	}
	if alias == "" {
		alias = tableName
	}
	b.joins = append(b.joins, joinClause{
		typ: rightJoin, tableName: tableName, alias: alias, using: fields,
	})
	return b
}

// JoinSub 内连接子查询
func (b *sqlBuilder) JoinSub(sub *sqlBuilder, alias, f1, f2 string) *sqlBuilder {
	if sub == nil {
		b.err = fmt.Errorf("JoinSub 子查询不能为 nil")
		return b
	}
	if !isSafeIdentifierAny(alias, f1, f2) {
		b.err = fmt.Errorf("非法的 JOIN SUB 参数")
		return b
	}
	b.joins = append(b.joins, joinClause{
		typ: innerJoin, alias: alias, subquery: sub,
		onConds: []onCondition{
			{leftTable: b.alias, leftField: f1, operator: "=", rightTable: alias, rightField: f2},
		},
	})
	return b
}

// LeftJoinSub 左连接子查询
func (b *sqlBuilder) LeftJoinSub(sub *sqlBuilder, alias, f1, f2 string) *sqlBuilder {
	if sub == nil {
		b.err = fmt.Errorf("LeftJoinSub 子查询不能为 nil")
		return b
	}
	if !isSafeIdentifierAny(alias, f1, f2) {
		b.err = fmt.Errorf("非法的 LEFT JOIN SUB 参数")
		return b
	}
	b.joins = append(b.joins, joinClause{
		typ: leftJoin, alias: alias, subquery: sub,
		onConds: []onCondition{
			{leftTable: b.alias, leftField: f1, operator: "=", rightTable: alias, rightField: f2},
		},
	})
	return b
}

// RightJoinSub 右连接子查询
func (b *sqlBuilder) RightJoinSub(sub *sqlBuilder, alias, f1, f2 string) *sqlBuilder {
	if sub == nil {
		b.err = fmt.Errorf("RightJoinSub 子查询不能为 nil")
		return b
	}
	if !isSafeIdentifierAny(alias, f1, f2) {
		b.err = fmt.Errorf("非法的 RIGHT JOIN SUB 参数")
		return b
	}
	b.joins = append(b.joins, joinClause{
		typ: rightJoin, alias: alias, subquery: sub,
		onConds: []onCondition{
			{leftTable: b.alias, leftField: f1, operator: "=", rightTable: alias, rightField: f2},
		},
	})
	return b
}
