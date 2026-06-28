package sqlbuilder

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// sqlBuilder
type sqlBuilder struct {
	// 表名
	tableName string

	// 表的别名
	alias string

	// 查询的字段
	fields []any

	// 参数的值
	fieldValue []any

	// where条件
	whr *Where

	// 排序
	orderField [][]any

	// 分组
	groupBy []string

	// having后面的条件
	hhr *Where

	// 联表
	joins []joinClause

	// sql 语句
	SqlStr string

	// 子查询
	childQuery string
	// FROM 子查询的参数（延迟到 buildFromClause 添加）
	fromArgs []any

	// 是否去重
	distinct bool

	// 分页参数
	offset   int64
	pageSize int64

	// 是否打印sql
	debugSql bool

	// 模型tag
	dbTag string

	// 当字段的值是空时，是要更新该字段
	emptyFieldMap map[string]bool
	// 当字段的值是0时，是要更新该字段
	zeroFieldMap map[string]bool

	// SQL 提示
	sqlHints []string
	// 索引提示
	indexHint string
	// 行锁
	lockClause string
	// UNION 子句
	unions []unionClause
	// CTE 定义
	ctes      []cteDef
	recursive bool
	// GROUP BY ... WITH ROLLUP
	withRollup bool
	// ON DUPLICATE KEY UPDATE
	onDuplicateUpdates map[string]any
	// 原始 SQL 片段
	customParts []string
	// 软删除字段
	softDeleteField string

	// 构建过程中的错误
	err error
}

// From 创建一个 sqlBuilder 实例
func From(tableName string, args ...any) *sqlBuilder {
	if !isSafeIdentifier(tableName) {
		return &sqlBuilder{err: fmt.Errorf("非法的表名: %s", tableName)}
	}
	builder := &sqlBuilder{
		tableName: tableName,
		alias:     tableName,
		whr: &Where{
			tableName:     tableName,
			alias:         tableName,
			groupWhere:    make([]GroupWhere, 0),
			assembleWhere: make([][]GroupWhere, 0),
		},
		hhr: &Where{
			tableName:     tableName,
			alias:         tableName,
			groupWhere:    make([]GroupWhere, 0),
			assembleWhere: make([][]GroupWhere, 0),
		},
		emptyFieldMap: make(map[string]bool),
		zeroFieldMap:  make(map[string]bool),
	}
	if len(args) > 0 {
		if val, ok := args[0].(*sqlBuilder); ok {
			childQuery, data, err := val.BuildSelect()
			if err != nil {
				builder.err = err
				return builder
			}
			builder.fromArgs = data
			builder.childQuery = childQuery
		}
	}
	return builder
}

// Table 切换表名，若未通过 As 显式设置别名则别名同步更新
func (b *sqlBuilder) Table(tableName string) *sqlBuilder {
	if !isSafeIdentifier(tableName) {
		b.err = fmt.Errorf("非法的表名: %s", tableName)
		return b
	}
	// 未显式设置别名时，别名跟随表名更新
	if b.alias == b.tableName {
		b.alias = tableName
		b.whr.SetAlias(tableName)
		b.hhr.SetAlias(tableName)
	}
	b.tableName = tableName
	b.whr.SetTableName(tableName)
	b.hhr.SetTableName(tableName)
	return b
}

// Debug 开启 SQL 打印，调试用
func (b *sqlBuilder) Debug() *sqlBuilder {
	b.debugSql = true
	return b
}

// 查询字段
func (b *sqlBuilder) Select(fields ...any) *sqlBuilder {
	for _, f := range fields {
		if s, ok := f.(string); ok {
			if !isSafeIdentifier(s) {
				b.err = fmt.Errorf("非法的查询字段: %s", s)
				return b
			}
		}
	}
	b.fields = fields
	return b
}

// Offset 设置原始偏移量（需配合 Size 或 LimitRaw 使用）
func (b *sqlBuilder) Offset(n int64) *sqlBuilder {
	if n < 0 {
		b.err = fmt.Errorf("Offset 不能为负数，收到: %d", n)
		return b
	}
	b.offset = n
	return b
}

// Size 设置每页条数（配合 Offset 使用）
func (b *sqlBuilder) Size(n int64) *sqlBuilder {
	if n <= 0 {
		b.err = fmt.Errorf("Size 必须大于 0，收到: %d", n)
		return b
	}
	b.pageSize = n
	return b
}

// Page 按页码和每页条数分页，p 从 1 开始
func (b *sqlBuilder) Page(p, num int64) *sqlBuilder {
	if p < 1 {
		b.err = fmt.Errorf("Page 页码必须 >= 1，收到: %d", p)
		return b
	}
	if num <= 0 {
		b.err = fmt.Errorf("Page 每页条数必须 > 0，收到: %d", num)
		return b
	}
	b.offset = (p - 1) * num
	b.pageSize = num
	return b
}

// Limit 限制返回条数（单参数，等价于 LIMIT n）
func (b *sqlBuilder) Limit(n int64) *sqlBuilder {
	if n <= 0 {
		b.err = fmt.Errorf("Limit 必须大于 0，收到: %d", n)
		return b
	}
	b.offset = -1
	b.pageSize = n
	return b
}

// 排序
func (b *sqlBuilder) Order(order [][]any) *sqlBuilder {
	for _, v := range order {
		if len(v) >= 1 {
			if s, ok := v[0].(string); ok && !isSafeIdentifier(s) {
				b.err = fmt.Errorf("非法的排序字段: %s", s)
				return b
			}
		}
		if len(v) >= 2 {
			if s, ok := v[1].(string); ok && !isSafeIdentifier(s) {
				b.err = fmt.Errorf("非法的排序方向: %s", s)
				return b
			}
		}
		if len(v) >= 3 {
			if s, ok := v[2].(string); ok && !isSafeIdentifier(s) {
				b.err = fmt.Errorf("非法的排序表别名: %s", s)
				return b
			}
		}
	}
	b.orderField = order
	return b
}

// 分组
func (b *sqlBuilder) Group(groupBy ...string) *sqlBuilder {
	if !isSafeIdentifierAny(groupBy...) {
		b.err = fmt.Errorf("非法的分组字段")
		return b
	}
	b.groupBy = groupBy
	return b
}

// 去重
func (b *sqlBuilder) Distinct() *sqlBuilder {
	b.distinct = true
	return b
}

// SqlNoCache 添加 SQL_NO_CACHE 提示
func (b *sqlBuilder) SqlNoCache() *sqlBuilder {
	b.sqlHints = append(b.sqlHints, "SQL_NO_CACHE")
	return b
}

// SqlCalcFoundRows 添加 SQL_CALC_FOUND_ROWS 提示
func (b *sqlBuilder) SqlCalcFoundRows() *sqlBuilder {
	b.sqlHints = append(b.sqlHints, "SQL_CALC_FOUND_ROWS")
	return b
}

// 设置数据库tag
func (b *sqlBuilder) SetDbTag(tag string) *sqlBuilder {
	if !isSafeIdentifier(tag) {
		b.err = fmt.Errorf("非法的 db tag: %s", tag)
		return b
	}
	b.dbTag = tag
	return b
}

// 设置值为0，仍要更新的字段
func (b *sqlBuilder) UpdateZeroField(zeroField ...string) *sqlBuilder {
	for _, v := range zeroField {
		b.zeroFieldMap[v] = true
	}
	return b
}

// 设置值为空时，仍要更新的字段
func (b *sqlBuilder) UpdateEmptyField(emptyField ...string) *sqlBuilder {
	for _, v := range emptyField {
		b.emptyFieldMap[v] = true
	}
	return b
}

/*
* and条件
  - WhereAnd("age", 18)
  - WhereAnd("name", "like", "张三")
  - WhereAnd("id", "=", 1, tableName)
  - WhereAnd("id", "=", 1, tableName, or)
  - WhereAnd([][]any{
    {"age", "=", 18, "user"},
    {"sex", "=", "男", "user", "or"},
    {"age", "=", 18},
    {"age", 18},
    })
  - WhereAnd([][][]any{
    [][]any{
    []any{"sex", "=", "男", "user", "or"}
    },
    [][]any{},
    })

*
*/
func (b *sqlBuilder) WhereAnd(args ...any) *sqlBuilder {
	b.where("and", args...)
	return b
}

func (b *sqlBuilder) HavingWhereAnd(args ...any) *sqlBuilder {
	b.havingWhere("and", args...)
	return b
}

func (b *sqlBuilder) HavingWhereOr(args ...any) *sqlBuilder {
	b.havingWhere("or", args...)
	return b
}

/*
* or条件
  - WhereOr("age", 18)
  - WhereOr("name", "like", "张三")
  - WhereOr("id", "=", 1, tableName)
  - WhereOr("id", "=", 1, tableName, or)
    WhereOr([][]any{
    {"age", "=", 18, "user"},
    {"sex", "=", "男", "user", "or"},
    {"age", "=", 18},
    {"age", 18},
    })

*
*/
func (b *sqlBuilder) WhereOr(args ...any) *sqlBuilder {
	b.where("or", args...)
	return b
}

/*
*
  - relation = and or
  - Where("or", "age", 18)
  - Where("or", "name", "like", "张三")
  - Where("or", "id", "=", 1, tableName)
  - Where("or", "id", "=", 1, tableName, or)
  - Where("or", [][]any{
    {"age", "=", 18, "user"},
    {"sex", "=", "男", "user", "or"},
    {"age", "=", 18},
    {"age", 18},
    })

*
*/
func (b *sqlBuilder) where(relation string, args ...any) *sqlBuilder {
	// 检查标识符参数不包含反引号
	if len(args) > 0 {
		if s, ok := args[0].(string); ok && s != "" && !isSafeIdentifier(s) {
			b.err = fmt.Errorf("非法的 WHERE 字段名: %s", s)
			return b
		}
	}
	if val, ok := argsMap[len(args)]; ok {
		groupWhere := val.ParseArgs(relation, args...)
		b.whr.assembleWhere = append(b.whr.assembleWhere, groupWhere)
	} else if len(args) > 0 {
		b.err = fmt.Errorf("不支持的 WHERE 参数数量: %d", len(args))
	}
	return b
}

// 设置having条件
func (b *sqlBuilder) havingWhere(relation string, args ...any) *sqlBuilder {
	// 检查标识符参数不包含反引号
	if len(args) > 0 {
		if s, ok := args[0].(string); ok && s != "" && !isSafeIdentifier(s) {
			b.err = fmt.Errorf("非法的 HAVING 字段名: %s", s)
			return b
		}
	}
	if val, ok := argsMap[len(args)]; ok {
		groupWhere := val.ParseArgs(relation, args...)
		b.hhr.assembleWhere = append(b.hhr.assembleWhere, groupWhere)
	} else if len(args) > 0 {
		b.err = fmt.Errorf("不支持的 HAVING 参数数量: %d", len(args))
	}
	return b
}

// WhereRaw 添加原始 WHERE 条件（绕过安全检查，慎用）
func (b *sqlBuilder) WhereRaw(condition string, args ...any) *sqlBuilder {
	b.customParts = append(b.customParts, "and ("+condition+")")
	b.fieldValue = append(b.fieldValue, args...)
	return b
}

// WhereRawOr 添加原始 OR WHERE 条件（绕过安全检查，慎用）
func (b *sqlBuilder) WhereRawOr(condition string, args ...any) *sqlBuilder {
	b.customParts = append(b.customParts, "or ("+condition+")")
	b.fieldValue = append(b.fieldValue, args...)
	return b
}

// WithRollup 为 GROUP BY 添加 WITH ROLLUP
func (b *sqlBuilder) WithRollup() *sqlBuilder {
	b.withRollup = true
	return b
}

// OnDuplicateKey 设置 ON DUPLICATE KEY UPDATE 子句（用于 INSERT）
func (b *sqlBuilder) OnDuplicateKey(updates map[string]any) *sqlBuilder {
	if err := validateMapKeys(updates); err != nil {
		b.err = err
		return b
	}
	b.onDuplicateUpdates = updates
	return b
}

// Raw 添加原始 SQL 片段（绕过所有安全检查，慎用）
func (b *sqlBuilder) Raw(sql string, args ...any) *sqlBuilder {
	b.customParts = append(b.customParts, sql)
	b.fieldValue = append(b.fieldValue, args...)
	return b
}

// 返回where条件和参数
func (b *sqlBuilder) ToString() string {
	sqlStr, args := b.whr.ParseWhere()
	b.fieldValue = append(b.fieldValue, args...)
	return sqlStr
}

// 获取字段值
func (b *sqlBuilder) GetFieldValue() []any {
	return b.fieldValue
}

// As 给表起别名
func (b *sqlBuilder) As(name string) *sqlBuilder {
	if !isSafeIdentifier(name) {
		b.err = fmt.Errorf("非法的别名: %s", name)
		return b
	}
	b.alias = name
	b.whr.SetAlias(name)
	b.hhr.SetAlias(name)
	return b
}

// ForUpdate 添加 FOR UPDATE 行锁
func (b *sqlBuilder) ForUpdate() *sqlBuilder {
	b.lockClause = "for update"
	return b
}

// LockInShareMode 添加 LOCK IN SHARE MODE
func (b *sqlBuilder) LockInShareMode() *sqlBuilder {
	b.lockClause = "lock in share mode"
	return b
}

// UseIndex 添加 USE INDEX 索引提示
func (b *sqlBuilder) UseIndex(indexes ...string) *sqlBuilder {
	quoted := make([]string, len(indexes))
	for i, idx := range indexes {
		if !isSafeIdentifier(idx) {
			return b
		}
		quoted[i] = fmt.Sprintf("`%s`", idx)
	}
	b.indexHint = fmt.Sprintf("use index (%s)", strings.Join(quoted, ", "))
	return b
}

// ForceIndex 添加 FORCE INDEX 索引提示
func (b *sqlBuilder) ForceIndex(indexes ...string) *sqlBuilder {
	quoted := make([]string, len(indexes))
	for i, idx := range indexes {
		if !isSafeIdentifier(idx) {
			return b
		}
		quoted[i] = fmt.Sprintf("`%s`", idx)
	}
	b.indexHint = fmt.Sprintf("force index (%s)", strings.Join(quoted, ", "))
	return b
}

// IgnoreIndex 添加 IGNORE INDEX 索引提示
func (b *sqlBuilder) IgnoreIndex(indexes ...string) *sqlBuilder {
	quoted := make([]string, len(indexes))
	for i, idx := range indexes {
		if !isSafeIdentifier(idx) {
			return b
		}
		quoted[i] = fmt.Sprintf("`%s`", idx)
	}
	b.indexHint = fmt.Sprintf("ignore index (%s)", strings.Join(quoted, ", "))
	return b
}

// Reset 重置 builder 全部状态，使其可完全复用
func (b *sqlBuilder) Reset() *sqlBuilder {
	b.fields = nil
	b.fieldValue = nil
	b.orderField = nil
	b.groupBy = nil
	b.joins = nil
	b.SqlStr = ""
	b.childQuery = ""
	b.distinct = false
	b.offset = 0
	b.pageSize = 0
	b.debugSql = false
	b.dbTag = ""
	b.emptyFieldMap = make(map[string]bool)
	b.zeroFieldMap = make(map[string]bool)
	b.sqlHints = nil
	b.indexHint = ""
	b.lockClause = ""
	b.unions = nil
	b.ctes = nil
	b.recursive = false
	b.withRollup = false
	b.onDuplicateUpdates = nil
	b.customParts = nil
	b.softDeleteField = ""
	b.fromArgs = nil
	b.err = nil
	// tableName/alias 保留，因为 From 时已设置，Reset 后通常复用同一表
	return b
}

// BuildSelect 构建 SELECT 查询 SQL，返回 SQL 语句和参数值列表
func (b *sqlBuilder) BuildSelect() (string, []any, error) {
	if b.err != nil {
		return "", nil, b.err
	}
	if b.alias == "" {
		b.alias = b.tableName
		b.whr.SetAlias(b.tableName)
	}

	// CTE (must accumulate args first, before SELECT)
	var ctePrefix string
	if cte := b.buildCTE(); cte != "" {
		ctePrefix = cte
	}

	// SELECT 字段
	fields, err := b.buildSelectFields()
	if err != nil {
		return "", nil, err
	}

	// FROM 子句 — 此时才把 FROM 子查询的参数加入（在 CTE 和 SELECT 之后）
	if len(b.fromArgs) > 0 {
		b.fieldValue = append(b.fieldValue, b.fromArgs...)
	}

	// FROM 子句
	from := b.buildFromClause()

	// SELECT 前缀
	selectPrefix := b.buildSelectPrefix()

	b.SqlStr = fmt.Sprintf("%s%s %s %s", ctePrefix, selectPrefix, fields, from)

	// JOIN
	if joinStr := b.buildJoinClause(); joinStr != "" {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, joinStr)
	}

	// WHERE
	whStr, whValue := b.whr.ParseWhere()
	hasWhere := whStr != ""
	if hasWhere {
		b.SqlStr = fmt.Sprintf("%s where %s", b.SqlStr, whStr)
	}
	if len(whValue) > 0 {
		b.fieldValue = append(b.fieldValue, whValue...)
	}

	// raw conditions and custom SQL fragments
	for _, cp := range b.customParts {
		if !hasWhere {
			b.SqlStr = fmt.Sprintf("%s where %s", b.SqlStr, strings.TrimPrefix(strings.TrimPrefix(cp, "and "), "or "))
			hasWhere = true
		} else {
			b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, cp)
		}
	}

	// GROUP BY
	if gb := b.buildGroupBy(); gb != "" {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, gb)
	}

	// HAVING
	hwhStr, hwhValue := b.hhr.ParseWhere()
	if hwhStr != "" {
		b.SqlStr = fmt.Sprintf("%s having %s", b.SqlStr, hwhStr)
	}
	if len(hwhValue) > 0 {
		b.fieldValue = append(b.fieldValue, hwhValue...)
	}

	// ORDER BY
	if ob := b.buildOrderBy(); ob != "" {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, ob)
	}

	// LIMIT
	if lm := b.buildLimit(); lm != "" {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, lm)
	}

	// 行锁
	if b.lockClause != "" {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, b.lockClause)
	}

	// UNION
	if un := b.buildUnions(); un != "" {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, un)
	}

	if b.debugSql {
		fmt.Println(b.SqlStr)
	}
	// 检查子构建器（CTE/JOIN/UNION）是否设置了错误
	if b.err != nil {
		return "", nil, b.err
	}
	return b.SqlStr, b.fieldValue, nil
}

// buildSelectPrefix 构建 SELECT 前缀（含 DISTINCT 和 SQL hints）
func (b *sqlBuilder) buildSelectPrefix() string {
	var sb strings.Builder
	sb.WriteString("select")
	for _, hint := range b.sqlHints {
		sb.WriteString(" " + hint)
	}
	if b.distinct {
		sb.WriteString(" distinct")
	}
	return sb.String()
}

// buildCTE 构建 WITH 子句
func (b *sqlBuilder) buildCTE() string {
	if len(b.ctes) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("with ")
	if b.recursive {
		sb.WriteString("recursive ")
	}
	for i, cte := range b.ctes {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("`%s`", cte.name))
		if len(cte.columns) > 0 {
			cols := make([]string, len(cte.columns))
			for j, col := range cte.columns {
				cols[j] = fmt.Sprintf("`%s`", col)
			}
			sb.WriteString(fmt.Sprintf(" (%s)", strings.Join(cols, ", ")))
		}
		cteSql, cteArgs, err := cte.definition.BuildSelect()
			if err != nil {
				b.err = err
				return ""
			}
		b.fieldValue = append(b.fieldValue, cteArgs...)
		sb.WriteString(fmt.Sprintf(" as (%s)", cteSql))
	}
	sb.WriteByte(' ')
	return sb.String()
}

// buildSelectFields 构建 SELECT 字段列表
func (b *sqlBuilder) buildSelectFields() (string, error) {
	var fieldsBuilder strings.Builder
	for _, v := range b.fields {
		nfield, err := b.formatSelectField(v)
		if err != nil {
			return "", err
		}
		if nfield == "" {
			continue
		}
		if fieldsBuilder.Len() > 0 {
			fieldsBuilder.WriteByte(',')
		}
		fieldsBuilder.WriteString(nfield)
	}
	fields := fieldsBuilder.String()
	if fields == "" {
		fields = fmt.Sprintf("`%s`.*", b.alias)
	}
	return fields, nil
}

// formatSelectField 格式化单个 SELECT 字段
// fieldAlias 从任意字段类型中提取别名
func (b *sqlBuilder) fieldAlias(v any) string {
	switch val := v.(type) {
	case string:
		s := val
		if strings.Contains(s, ".") {
			s = strings.Split(s, ".")[1]
		}
		return s
	case *funCarrier:
		return val.Alias
	case *winCarrier:
		return val.Alias
	case *caseCarrier:
		return val.Alias
	case *colCarrier:
		if val.FieldAlias != "" {
			return val.FieldAlias
		}
		return val.Field
	default:
		return ""
	}
}

func (b *sqlBuilder) formatSelectField(v any) (string, error) {
	switch val := v.(type) {
	case *sqlBuilder:
		if len(val.fields) == 0 || len(val.fields) > 1 {
			return "", errors.New("子查询仅需要一个字段")
		}
		childQuery, data, err := val.BuildSelect()
		if err != nil {
			return "", err
		}
		b.fieldValue = append(b.fieldValue, data...)
		fstr := b.fieldAlias(val.fields[0])
		if fstr == "" {
			return "", errors.New("子查询字段无法提取别名")
		}
		return fmt.Sprintf("(%s) as `%s`", childQuery, fstr), nil
	case *funCarrier:
		if val.Fn != "" {
			var fnpBuilder strings.Builder
			for i, vv := range val.Params {
				if i > 0 {
					fnpBuilder.WriteString(", ")
				}
				fnpBuilder.WriteString(fmt.Sprint(vv))
			}
			return fmt.Sprintf("%s(%s) as `%s`", val.Fn, fnpBuilder.String(), val.Alias), nil
		}
	case *winCarrier:
		return b.formatWinField(val), nil
	case *caseCarrier:
		return b.formatCaseField(val)
	case *colCarrier:
		if val.TableAlias == "" && val.Field == "" && val.FieldAlias == "" {
			return "", nil
		}
		if val.TableAlias != "" {
			return strings.TrimSpace(fmt.Sprintf("`%s`.`%s` %s", val.TableAlias, val.Field, val.FieldAlias)), nil
		}
		if val.Field != "" {
			return strings.TrimSpace(fmt.Sprintf("`%s` %s", val.Field, val.FieldAlias)), nil
		}
	case string:
		if strings.Contains(val, ".") {
			arr := strings.Split(val, ".")
			if len(arr) != 2 {
				return "", errors.New("错误的查询字段格式，期望 'table.field'")
			}
			return fmt.Sprintf("`%s`.`%s`", arr[0], arr[1]), nil
		}
		return fmt.Sprintf("`%s`.`%s`", b.alias, val), nil
	default:
		return "", errors.New("不支持的查询字段类型")
	}
	return "", nil
}

// buildFromClause 构建 FROM 子句
func (b *sqlBuilder) buildFromClause() string {
	target := fmt.Sprintf("`%s`", b.tableName)
	if b.childQuery != "" {
		target = fmt.Sprintf("(%s)", b.childQuery)
	}
	result := fmt.Sprintf("from %s as `%s`", target, b.alias)
	if b.indexHint != "" {
		result += " " + b.indexHint
	}
	return result
}

// buildGroupBy 构建 GROUP BY 子句
func (b *sqlBuilder) buildGroupBy() string {
	if len(b.groupBy) == 0 {
		return ""
	}
	var groupBuilder strings.Builder
	groupBuilder.WriteString("group by ")
	for i, v := range b.groupBy {
		if i > 0 {
			groupBuilder.WriteByte(',')
		}
		groupBuilder.WriteString(fmt.Sprintf("`%s`.`%s`", b.alias, v))
	}
	if b.withRollup {
		groupBuilder.WriteString(" with rollup")
	}
	return groupBuilder.String()
}

// buildOrderBy 构建 ORDER BY 子句
func (b *sqlBuilder) buildOrderBy() string {
	if len(b.orderField) == 0 {
		return ""
	}
	var orderBuilder strings.Builder
	orderBuilder.WriteString("order by ")
	added := 0
	for _, v := range b.orderField {
		if len(v) < 2 {
			continue
		}
		if added > 0 {
			orderBuilder.WriteByte(',')
		}
		if len(v) == 2 {
			orderBuilder.WriteString(fmt.Sprintf("`%s`.`%s` %s", b.alias, v[0], v[1]))
		} else if len(v) == 3 {
			orderBuilder.WriteString(fmt.Sprintf("`%s`.`%s` %s", v[2], v[0], v[1]))
		}
		added++
	}
	if added == 0 {
		return ""
	}
	return orderBuilder.String()
}

// buildLimit 构建 LIMIT 子句
func (b *sqlBuilder) buildLimit() string {
	if b.offset >= 0 && b.pageSize > 0 {
		return fmt.Sprintf("limit %d,%d", b.offset, b.pageSize)
	}
	if b.offset < 0 && b.pageSize > 0 {
		return fmt.Sprintf("limit %d", b.pageSize)
	}
	return ""
}

// buildUnions 构建 UNION 子句
func (b *sqlBuilder) buildUnions() string {
	if len(b.unions) == 0 {
		return ""
	}
	var parts []string
	for _, u := range b.unions {
		subSql, subArgs, err := u.builder.BuildSelect()
			if err != nil {
				b.err = err
				return ""
			}
		b.fieldValue = append(b.fieldValue, subArgs...)
		parts = append(parts, fmt.Sprintf("%s (%s)", u.typ, subSql))
	}
	return strings.Join(parts, " ")
}

// formatWinField 格式化窗口函数字段
func (b *sqlBuilder) formatWinField(val *winCarrier) string {
	if val.Fn == "" {
		return ""
	}
	var fnpBuilder strings.Builder
	for i, vv := range val.Params {
		if i > 0 {
			fnpBuilder.WriteString(", ")
		}
		fnpBuilder.WriteString(fmt.Sprint(vv))
	}
	fnCall := fmt.Sprintf("%s(%s)", val.Fn, fnpBuilder.String())

	var overBuilder strings.Builder
	overBuilder.WriteString("(")
	needSpace := false
	if len(val.PartitionBy) > 0 {
		overBuilder.WriteString("partition by ")
		for i, f := range val.PartitionBy {
			if i > 0 {
				overBuilder.WriteString(", ")
			}
			overBuilder.WriteString(fmt.Sprintf("`%s`.`%s`", b.alias, f))
		}
		needSpace = true
	}
	if len(val.OrderBy) > 0 {
		if needSpace {
			overBuilder.WriteByte(' ')
		}
		overBuilder.WriteString("order by ")
		for i, v := range val.OrderBy {
			if len(v) < 2 {
				continue
			}
			if i > 0 {
				overBuilder.WriteString(", ")
			}
			if len(v) == 2 {
				overBuilder.WriteString(fmt.Sprintf("`%s`.`%s` %s", b.alias, v[0], v[1]))
			} else if len(v) == 3 {
				overBuilder.WriteString(fmt.Sprintf("`%s`.`%s` %s", v[2], v[0], v[1]))
			}
		}
	}
	overBuilder.WriteByte(')')

	return fmt.Sprintf("%s over %s as `%s`", fnCall, overBuilder.String(), val.Alias)
}

// formatCaseField 格式化 CASE WHEN 字段
func (b *sqlBuilder) formatCaseField(val *caseCarrier) (string, error) {
	var sb strings.Builder
	sb.WriteString("case ")
	if val.CaseField != "" {
		sb.WriteString(fmt.Sprintf("`%s`.`%s` ", b.alias, val.CaseField))
	}
	for _, w := range val.Whens {
		switch wt := w.When.(type) {
		case string:
			if hasIllegalStr(wt) {
				return "", fmt.Errorf("非法的 CASE WHEN 条件: %s", wt)
			}
			sb.WriteString(fmt.Sprintf("when %s then ? ", wt))
		case *sqlBuilder:
			subSql, subArgs, err := wt.BuildSelect()
			if err != nil {
			return "", err
				}
			b.fieldValue = append(b.fieldValue, subArgs...)
			sb.WriteString(fmt.Sprintf("when (%s) then ? ", subSql))
		default:
			sb.WriteString("when ? then ? ")
		}
		// string/subquery when 只有 then 是占位符
		if _, isStr := w.When.(string); isStr {
			b.fieldValue = append(b.fieldValue, w.Then)
		} else if _, isSub := w.When.(*sqlBuilder); isSub {
			b.fieldValue = append(b.fieldValue, w.Then)
		} else {
			b.fieldValue = append(b.fieldValue, w.When, w.Then)
		}
	}
	if val.ElseVal != nil {
		sb.WriteString("else ? ")
		b.fieldValue = append(b.fieldValue, val.ElseVal)
	}
	sb.WriteString(fmt.Sprintf("end as `%s`", val.Alias))
	return sb.String(), nil
}

// BuildMapNamedInsert 使用 map 构建插入 SQL，使用命名参数（:key）
func (b *sqlBuilder) BuildMapNamedInsert(option map[string]any) (string, map[string]any) {
	if len(option) == 0 {
		return "", nil
	}
	if err := validateMapKeys(option); err != nil {
		b.err = err
		return "", nil
	}
	keysArr := []string{}
	valsArr := []string{}
	for k := range option {
		keysArr = append(keysArr, fmt.Sprintf("`%s`", k))
		valsArr = append(valsArr, fmt.Sprintf(":%s", k))
	}
	sqlStr := fmt.Sprintf("insert into `%s` (%s) values (%s)", b.tableName, strings.Join(keysArr, ","), strings.Join(valsArr, ","))
	return sqlStr, option
}

// BuildMapInsert 使用 map 构建插入 SQL，使用 ? 占位符
func (b *sqlBuilder) BuildMapInsert(option map[string]any) (string, []any) {
	return b.buildMapInsert("insert", option)
}

// BuildSliceMapInsert 使用 map 切片构建批量插入 SQL，使用 ? 占位符
func (b *sqlBuilder) BuildSliceMapInsert(option []map[string]any) (string, []any) {
	return b.buildSliceMapInsert("insert", option)
}

// BuildSliceMapNamedInsert 使用 map 切片构建批量插入 SQL，使用命名参数（:key）
func (b *sqlBuilder) BuildSliceMapNamedInsert(option []map[string]any) (string, []map[string]any) {
	if len(option) == 0 {
		return "", nil
	}
	if err := validateMapKeys(option[0]); err != nil {
		b.err = err
		return "", nil
	}
	keysArr := []string{}
	valsArr := []string{}
	for k := range option[0] {
		keysArr = append(keysArr, fmt.Sprintf("`%s`", k))
		valsArr = append(valsArr, fmt.Sprintf(":%s", k))
	}
	sqlStr := fmt.Sprintf("insert into `%s` (%s) values (%s)", b.tableName, strings.Join(keysArr, ","), strings.Join(valsArr, ","))
	return sqlStr, option
}

// BuildStructNamedInsert 使用结构体构建插入 SQL，使用命名参数（:tag），通过 db tag 映射字段
func (b *sqlBuilder) BuildStructNamedInsert(entity any) (string, error) {
	if b.dbTag == "" {
		b.dbTag = "db"
	}
	reflectValue := reflect.ValueOf(entity)
	if reflectValue.Kind() != reflect.Ptr || reflectValue.IsNil() {
		return "", errors.New("参数不是指针类型")
	}
	elemVal := reflectValue.Elem()
	if elemVal.Kind() != reflect.Struct {
		return "", errors.New("参数不是结构体类型")
	}
	fields := []string{}
	nameFields := []string{}

	b.recursionStructNamedEmbed(elemVal, &fields, &nameFields)

	// 构建 SQL 语句
	if len(fields) == 0 {
		return "", errors.New("没有可插入的字段")
	}
	return fmt.Sprintf("insert into `%s` (%s) values(%s)", b.tableName, strings.Join(fields, ","), strings.Join(nameFields, ",")), nil
}

func (b *sqlBuilder) recursionStructNamedEmbed(elemVal reflect.Value, fields *[]string, nameFields *[]string) {
	typ := elemVal.Type()
	for i := 0; i < elemVal.NumField(); i++ {
		if typ.Field(i).Anonymous && typ.Field(i).Type.Kind() == reflect.Struct {
			b.recursionStructNamedEmbed(elemVal.Field(i), fields, nameFields)
		}
		field := typ.Field(i)
		dbTag := field.Tag.Get(b.dbTag)
		if dbTag == "" || dbTag == "-" {
			continue
		}
		fieldVal := elemVal.Field(i)
		if !fieldVal.CanInterface() {
			continue
		}
		if ok := b.shouldSkipField(fieldVal, dbTag); ok {
			continue
		}
		*fields = append(*fields, fmt.Sprintf("`%s`", dbTag))
		*nameFields = append(*nameFields, fmt.Sprintf(":%s", dbTag))
	}
}

// BuildStructInsert 使用结构体构建插入 SQL，使用 ? 占位符，通过 db tag 映射字段
func (b *sqlBuilder) BuildStructInsert(entity any) (string, []any, error) {
	if b.dbTag == "" {
		b.dbTag = "db"
	}
	reflectValue := reflect.ValueOf(entity)
	if reflectValue.Kind() != reflect.Ptr || reflectValue.IsNil() {
		return "", nil, errors.New("参数不是指针类型")
	}
	elemVal := reflectValue.Elem()
	if elemVal.Kind() != reflect.Struct {
		return "", nil, errors.New("参数不是结构体类型")
	}

	fields := []string{}
	valsArr := []any{}
	fieldLen := 0
	b.recursionStructEmbed(elemVal, &fields, &valsArr, &fieldLen)

	placeHolder := strings.TrimRight(strings.Repeat("?,", fieldLen), ",")
	// 构建 SQL 语句
	if len(fields) == 0 {
		return "", nil, errors.New("没有可插入的字段")
	}
	return fmt.Sprintf("insert into `%s` (%s) values(%s)", b.tableName, strings.Join(fields, ","), placeHolder), valsArr, nil
}

func (b *sqlBuilder) recursionStructEmbed(elemVal reflect.Value, fields *[]string, valsArr *[]any, fieldLen *int) {
	typ := elemVal.Type()
	for i := 0; i < elemVal.NumField(); i++ {
		if typ.Field(i).Anonymous && typ.Field(i).Type.Kind() == reflect.Struct {
			b.recursionStructEmbed(elemVal.Field(i), fields, valsArr, fieldLen)
		}
		field := typ.Field(i)
		dbTag := field.Tag.Get(b.dbTag)
		if dbTag == "" || dbTag == "-" {
			continue
		}
		fieldVal := elemVal.Field(i)
		if !fieldVal.CanInterface() {
			continue
		}
		if ok := b.shouldSkipField(fieldVal, dbTag); ok {
			continue
		}

		*fields = append(*fields, fmt.Sprintf("`%s`", dbTag))
		*valsArr = append(*valsArr, fieldVal.Interface())
		*fieldLen += 1
	}
}

// BuildSliceStructInsert 使用结构体切片构建批量插入 SQL，使用 ? 占位符，通过 db tag 映射字段
func (b *sqlBuilder) BuildSliceStructInsert(entity any) (string, []any, error) {
	if b.dbTag == "" {
		b.dbTag = "db"
	}
	reflectVal := reflect.ValueOf(entity)
	if reflectVal.Kind() != reflect.Ptr || reflectVal.IsNil() {
		return "", nil, errors.New("参数不是指针类型")
	}
	elemVal := reflectVal.Elem()

	if elemVal.Kind() != reflect.Slice {
		return "", nil, errors.New("参数不是指针切片类型")
	}

	fieldValueArr := []any{}
	keysArr := []string{}
	sqlValueArr := []string{}

	sliceLenth := elemVal.Len()
	for i := 0; i < sliceLenth; i++ {
		item := elemVal.Index(i)
		if item.Kind() != reflect.Struct {
			return "", nil, errors.New("切片中的元素不是结构体类型")
		}

		placeholderArr := []string{}
		// itemType := item.Type()
		b.recursionSliceStructEmbed(item, i, &keysArr, &fieldValueArr, &placeholderArr)

		sqlValueArr = append(sqlValueArr, fmt.Sprintf("(%s)", strings.Join(placeholderArr, ",")))

	}
	if len(keysArr) == 0 {
		return "", nil, errors.New("没有可插入的字段")
	}
	insertSql := fmt.Sprintf("insert into `%s` (%s) values %s", b.tableName, strings.Join(keysArr, ","), strings.Join(sqlValueArr, ","))

	return insertSql, fieldValueArr, nil
}

func (b *sqlBuilder) recursionSliceStructEmbed(elemVal reflect.Value, i int, keysArr *[]string, fieldValueArr *[]any, placeholderArr *[]string) {
	itemType := elemVal.Type()
	for j := 0; j < elemVal.NumField(); j++ {
		if itemType.Field(j).Anonymous && itemType.Field(j).Type.Kind() == reflect.Struct {
			b.recursionSliceStructEmbed(elemVal.Field(j), i, keysArr, fieldValueArr, placeholderArr)
		}
		field := itemType.Field(j)
		dbTag := field.Tag.Get(b.dbTag)
		if dbTag == "" || dbTag == "-" {
			continue
		}
		fieldVal := elemVal.Field(j)
		if !fieldVal.CanInterface() {
			continue
		}
		if ok := b.shouldSkipField(fieldVal, dbTag); ok {
			continue
		}
		if i == 0 {
			*keysArr = append(*keysArr, fmt.Sprintf("`%s`", dbTag))
		}
		*fieldValueArr = append(*fieldValueArr, fieldVal.Interface())
		*placeholderArr = append(*placeholderArr, "?")
	}
}

// BuildSliceStructNamedInsert 使用结构体切片构建批量插入 SQL，使用命名参数（:tag），通过 db tag 映射字段
func (b *sqlBuilder) BuildSliceStructNamedInsert(entity any) (string, error) {
	if b.dbTag == "" {
		b.dbTag = "db"
	}
	reflectVal := reflect.ValueOf(entity)
	if reflectVal.Kind() != reflect.Ptr || reflectVal.IsNil() {
		return "", errors.New("参数不是指针类型")
	}
	elemVal := reflectVal.Elem()

	if elemVal.Kind() != reflect.Slice {
		return "", errors.New("参数不是指针切片类型")
	}

	keysArr := []string{}
	sliceLenth := elemVal.Len()
	var firstItem reflect.Value
	if sliceLenth == 0 {
		return "", errors.New("切片为空")
	} else {
		firstItem = elemVal.Index(0)
	}
	for i := 0; i < sliceLenth; i++ {
		item := elemVal.Index(i)
		if item.Kind() != reflect.Struct {
			return "", errors.New("切片中的元素不是结构体类型")
		}
	}
	placeholderArr := []string{}
	b.recursionSliceStructNamedEmbed(firstItem, &keysArr, &placeholderArr)

	if len(keysArr) == 0 {
		return "", errors.New("没有可插入的字段")
	}
	namedStr := fmt.Sprintf("(%s)", strings.Join(placeholderArr, ","))
	insertSql := fmt.Sprintf("insert into `%s` (%s) values %s", b.tableName, strings.Join(keysArr, ","), namedStr)

	return insertSql, nil
}

func (b *sqlBuilder) recursionSliceStructNamedEmbed(firstItem reflect.Value, keysArr *[]string, placeholderArr *[]string) {
	itemType := firstItem.Type()
	for j := 0; j < firstItem.NumField(); j++ {
		if itemType.Field(j).Anonymous && itemType.Field(j).Type.Kind() == reflect.Struct {
			b.recursionSliceStructNamedEmbed(firstItem.Field(j), keysArr, placeholderArr)
		}
		field := itemType.Field(j)
		dbTag := field.Tag.Get(b.dbTag)
		if dbTag == "" || dbTag == "-" {
			continue
		}
		fieldVal := firstItem.Field(j)
		if !fieldVal.CanInterface() {
			continue
		}
		if ok := b.shouldSkipField(fieldVal, dbTag); ok {
			continue
		}
		*keysArr = append(*keysArr, fmt.Sprintf("`%s`", dbTag))
		*placeholderArr = append(*placeholderArr, fmt.Sprintf(":%s", dbTag))
	}
}

// BuildMapUpdate 使用 map 构建更新 SQL，使用 ? 占位符，option 中值为 []any{字段名, 运算符, 值} 时表示字段运算
func (b *sqlBuilder) BuildMapUpdate(option map[string]any) (string, []any, error) {
	if err := validateMapKeys(option); err != nil {
		return "", nil, err
	}
	tableName := b.tableName
	if b.alias != "" {
		tableName = b.alias
	}
	var valsBuilder strings.Builder
	for k, v := range option {
		if valsBuilder.Len() > 0 {
			valsBuilder.WriteByte(',')
		}
		switch val := v.(type) {
		case string:
			b.fieldValue = append(b.fieldValue, val)
			valsBuilder.WriteString(fmt.Sprintf("`%s`.`%s` = ?", tableName, k))
		case int, int8, int32, int16, int64, uint, uint8, uint16, uint32, uint64,
			float32, float64:
			b.fieldValue = append(b.fieldValue, val)
			valsBuilder.WriteString(fmt.Sprintf("`%s`.`%s` = ?", tableName, k))
		case time.Time:
			b.fieldValue = append(b.fieldValue, val)
			valsBuilder.WriteString(fmt.Sprintf("`%s`.`%s` = ?", tableName, k))
		case []any:
			if len(val) < 3 {
				return "", nil, fmt.Errorf("字段 %s 的运算表达式格式错误，需要 []any{字段名, 运算符, 值}", k)
			}
			b.fieldValue = append(b.fieldValue, val[2])
			valsBuilder.WriteString(fmt.Sprintf("`%s`.`%s` = `%s`.`%s`%s?", tableName, k, tableName, val[0], val[1]))
		}
	}
	b.SqlStr = fmt.Sprintf("update `%s` as `%s` set %s", b.tableName, tableName, valsBuilder.String())

	whStr, whArgs := b.whr.ParseWhere()
	if whStr != "" {
		b.SqlStr = fmt.Sprintf("%s where %s", b.SqlStr, whStr)
	} else {
		return "", nil, errors.New("更新操作必须有一个条件")
	}
	if len(whArgs) > 0 {
		b.fieldValue = append(b.fieldValue, whArgs...)
	}

	// ORDER BY and LIMIT for UPDATE
	if ob := b.buildOrderBy(); ob != "" {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, ob)
	}
	if lm := b.buildLimit(); lm != "" {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, lm)
	}

	return b.SqlStr, b.fieldValue, nil
}

// BuildStructUpdate 使用结构体构建更新 SQL，使用 ? 占位符，通过 db tag 映射字段
func (b *sqlBuilder) BuildStructUpdate(entity any) (string, []any, error) {
	if b.dbTag == "" {
		b.dbTag = "db"
	}
	tableName := b.tableName
	if b.alias != "" {
		tableName = b.alias
	}
	reflectVal := reflect.ValueOf(entity)
	if reflectVal.Kind() != reflect.Ptr || reflectVal.IsNil() {
		return "", nil, errors.New("需要传入结构体指针")
	}
	reflectVal = reflectVal.Elem()

	var setStr string
	var fieldArr []string
	if err := b.recursionEmbedStruct(reflectVal, &fieldArr, tableName); err != nil {
		return "", nil, err
	}

	setStr = strings.Join(fieldArr, ",")
	b.SqlStr = fmt.Sprintf("update `%s` as `%s` set %s", b.tableName, tableName, setStr)

	whStr, whArgs := b.whr.ParseWhere()
	if whStr != "" {
		b.SqlStr = fmt.Sprintf("%s where %s", b.SqlStr, whStr)
	} else {
		return "", nil, errors.New("更新操作必须有一个条件")
	}
	if len(whArgs) > 0 {
		b.fieldValue = append(b.fieldValue, whArgs...)
	}

	// ORDER BY and LIMIT for UPDATE
	if ob := b.buildOrderBy(); ob != "" {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, ob)
	}
	if lm := b.buildLimit(); lm != "" {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, lm)
	}

	return b.SqlStr, b.fieldValue, nil
}

// recursionEmbedStruct 递归解析嵌套结构体的 db tag 字段用于更新

func (b *sqlBuilder) recursionEmbedStruct(reflectVal reflect.Value, fieldArr *[]string, tableName string) error {
	numFields := reflectVal.NumField()
	typElem := reflectVal.Type()
	for i := 0; i < numFields; i++ {
		field := typElem.Field(i)
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			b.recursionEmbedStruct(reflectVal.Field(i), fieldArr, tableName)
		}
		dbField := field.Tag.Get(b.dbTag)
		if dbField == "" || dbField == "-" {
			continue
		}
		fieldVal := reflectVal.Field(i)
		if !fieldVal.CanInterface() {
			continue
		}
		if ok := b.shouldSkipField(fieldVal, dbField); ok {
			continue
		}
		fial := fieldVal.Interface()
		switch tval := fial.(type) {
		case string:
			b.fieldValue = append(b.fieldValue, tval)
			*fieldArr = append(*fieldArr, fmt.Sprintf("`%s`.`%s` = ?", tableName, dbField))
		case int, int8, int32, int16, int64, uint, uint8, uint16, uint32, uint64,
			float32, float64:
			b.fieldValue = append(b.fieldValue, tval)
			*fieldArr = append(*fieldArr, fmt.Sprintf("`%s`.`%s` = ?", tableName, dbField))
		case time.Time:
			b.fieldValue = append(b.fieldValue, tval)
			*fieldArr = append(*fieldArr, fmt.Sprintf("`%s`.`%s` = ?", tableName, dbField))
		case []any:
			if len(tval) < 3 {
				return fmt.Errorf("字段 %s 的运算表达式格式错误，需要 []any{字段名, 运算符, 值}", dbField)
			}
			b.fieldValue = append(b.fieldValue, tval[2])
			*fieldArr = append(*fieldArr, fmt.Sprintf("`%s`.`%s` = `%s`.`%s`%s?", tableName, dbField, tableName, tval[0], tval[1]))
		}

	}
	return nil
}

// BuildIncrement 使用 map 构建字段累加更新 SQL（SET field = field + ?）
func (b *sqlBuilder) BuildIncrement(option map[string]any) (string, []any, error) {
	if err := validateMapKeys(option); err != nil {
		return "", nil, err
	}
	var valsBuilder strings.Builder
	tableName := b.tableName
	if b.alias != "" {
		tableName = b.alias
	}
	for k := range option {
		if valsBuilder.Len() > 0 {
			valsBuilder.WriteByte(',')
		}
		valsBuilder.WriteString(fmt.Sprintf("`%s`.`%s` = `%s`.`%s` + ?", tableName, k, tableName, k))
		b.fieldValue = append(b.fieldValue, option[k])
	}
	b.SqlStr = fmt.Sprintf("update `%s` as `%s` set %s", b.tableName, tableName, valsBuilder.String())

	whStr, whArgs := b.whr.ParseWhere()
	if whStr != "" {
		b.SqlStr = fmt.Sprintf("%s where %s", b.SqlStr, whStr)
	} else {
		return "", nil, errors.New("更新操作必须有一个条件")
	}
	if len(whArgs) > 0 {
		b.fieldValue = append(b.fieldValue, whArgs...)
	}

	// ORDER BY and LIMIT for UPDATE
	if ob := b.buildOrderBy(); ob != "" {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, ob)
	}
	if lm := b.buildLimit(); lm != "" {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, lm)
	}

	return b.SqlStr, b.fieldValue, nil
}

// BuildDecrement 使用 map 构建字段累减更新 SQL（SET field = field - ?）
func (b *sqlBuilder) BuildDecrement(option map[string]any) (string, []any, error) {
	if err := validateMapKeys(option); err != nil {
		return "", nil, err
	}
	var valsBuilder strings.Builder
	tableName := b.tableName
	if b.alias != "" {
		tableName = b.alias
	}
	for k := range option {
		if valsBuilder.Len() > 0 {
			valsBuilder.WriteByte(',')
		}
		valsBuilder.WriteString(fmt.Sprintf("`%s`.`%s` = `%s`.`%s` - ?", tableName, k, tableName, k))
		b.fieldValue = append(b.fieldValue, option[k])
	}
	b.SqlStr = fmt.Sprintf("update `%s` as `%s` set %s", b.tableName, tableName, valsBuilder.String())

	whStr, whArgs := b.whr.ParseWhere()
	if whStr != "" {
		b.SqlStr = fmt.Sprintf("%s where %s", b.SqlStr, whStr)
	} else {
		return "", nil, errors.New("更新操作必须有一个条件")
	}
	if len(whArgs) > 0 {
		b.fieldValue = append(b.fieldValue, whArgs...)
	}

	// ORDER BY and LIMIT for UPDATE
	if ob := b.buildOrderBy(); ob != "" {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, ob)
	}
	if lm := b.buildLimit(); lm != "" {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, lm)
	}

	return b.SqlStr, b.fieldValue, nil
}

// BuildDelete 构建删除 SQL，必须设置 WHERE 条件
func (b *sqlBuilder) BuildDelete() (string, []any, error) {
	tableName := b.tableName
	if b.alias != "" {
		tableName = b.alias
	}
	b.SqlStr = fmt.Sprintf("delete `%s` from `%s` as `%s`", tableName, b.tableName, tableName)

	whStr, whArgs := b.whr.ParseWhere()
	if whStr == "" || len(whArgs) == 0 {
		return "", nil, errors.New("删除操作必须有一个条件")
	}
	b.SqlStr = fmt.Sprintf("%s where %s", b.SqlStr, whStr)

	b.fieldValue = append(b.fieldValue, whArgs...)

	// ORDER BY and LIMIT for DELETE
	if ob := b.buildOrderBy(); ob != "" {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, ob)
	}
	if lm := b.buildLimit(); lm != "" {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, lm)
	}

	return b.SqlStr, b.fieldValue, nil
}

// BuildTruncate 构建 TRUNCATE TABLE SQL
func (b *sqlBuilder) BuildTruncate() (string, error) {
	return fmt.Sprintf("truncate table `%s`", b.tableName), nil
}

// BuildSoftDelete 构建软删除 SQL（UPDATE deleted_at = NOW()）
func (b *sqlBuilder) BuildSoftDelete() (string, []any, error) {
	return b.BuildMapUpdate(map[string]any{
		b.softDeleteField: time.Now(),
	})
}

// BuildSelectCount 构建 SELECT COUNT 查询（包装原查询为子查询）
func (b *sqlBuilder) BuildSelectCount() (string, []any, error) {
	innerSql, innerArgs, err := b.BuildSelect()
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("select count(*) as `_count` from (%s) as `_count`", innerSql), innerArgs, nil
}

// BuildExists 构建 SELECT EXISTS 查询
func (b *sqlBuilder) BuildExists() (string, []any, error) {
	innerSql, innerArgs, err := b.BuildSelect()
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("select exists(%s) as `_exists`", innerSql), innerArgs, nil
}

// BuildUpdateWithJoin 构建带 JOIN 的 UPDATE SQL
func (b *sqlBuilder) BuildUpdateWithJoin(option map[string]any) (string, []any, error) {
	if err := validateMapKeys(option); err != nil {
		return "", nil, err
	}
	tableName := b.tableName
	if b.alias != "" {
		tableName = b.alias
	}
	var valsBuilder strings.Builder
	for k, v := range option {
		if valsBuilder.Len() > 0 {
			valsBuilder.WriteByte(',')
		}
		switch val := v.(type) {
		case string:
			b.fieldValue = append(b.fieldValue, val)
			valsBuilder.WriteString(fmt.Sprintf("`%s`.`%s` = ?", tableName, k))
		case int, int8, int32, int16, int64, uint, uint8, uint16, uint32, uint64,
			float32, float64:
			b.fieldValue = append(b.fieldValue, val)
			valsBuilder.WriteString(fmt.Sprintf("`%s`.`%s` = ?", tableName, k))
		case time.Time:
			b.fieldValue = append(b.fieldValue, val)
			valsBuilder.WriteString(fmt.Sprintf("`%s`.`%s` = ?", tableName, k))
		case []any:
			if len(val) < 3 {
				return "", nil, fmt.Errorf("字段 %s 的运算表达式格式错误，需要 []any{字段名, 运算符, 值}", k)
			}
			b.fieldValue = append(b.fieldValue, val[2])
			valsBuilder.WriteString(fmt.Sprintf("`%s`.`%s` = `%s`.`%s`%s?", tableName, k, tableName, val[0], val[1]))
		}
	}

	updateSql := fmt.Sprintf("update `%s`", b.tableName)
	if b.alias != "" {
		updateSql += fmt.Sprintf(" as `%s`", b.alias)
	}

	if joinStr := b.buildJoinClause(); joinStr != "" {
		updateSql += " " + joinStr
	}

	updateSql += fmt.Sprintf(" set %s", valsBuilder.String())

	whStr, whArgs := b.whr.ParseWhere()
	if whStr != "" {
		updateSql = fmt.Sprintf("%s where %s", updateSql, whStr)
	} else {
		return "", nil, errors.New("更新操作必须有一个条件")
	}
	if len(whArgs) > 0 {
		b.fieldValue = append(b.fieldValue, whArgs...)
	}

	// ORDER BY and LIMIT support for UPDATE
	if ob := b.buildOrderBy(); ob != "" {
		updateSql = fmt.Sprintf("%s %s", updateSql, ob)
	}
	if lm := b.buildLimit(); lm != "" {
		updateSql = fmt.Sprintf("%s %s", updateSql, lm)
	}

	b.SqlStr = updateSql
	if b.err != nil {
		return "", nil, b.err
	}
	return b.SqlStr, b.fieldValue, nil
}

// BuildDeleteWithJoin 构建带 JOIN 的 DELETE SQL
func (b *sqlBuilder) BuildDeleteWithJoin() (string, []any, error) {
	tableName := b.tableName
	if b.alias != "" {
		tableName = b.alias
	}

	deleteSql := fmt.Sprintf("delete `%s` from `%s`", tableName, b.tableName)
	if b.alias != "" {
		deleteSql += fmt.Sprintf(" as `%s`", b.alias)
	}

	if joinStr := b.buildJoinClause(); joinStr != "" {
		deleteSql += " " + joinStr
	}

	whStr, whArgs := b.whr.ParseWhere()
	if whStr == "" || len(whArgs) == 0 {
		return "", nil, errors.New("删除操作必须有一个条件")
	}
	deleteSql = fmt.Sprintf("%s where %s", deleteSql, whStr)
	b.fieldValue = append(b.fieldValue, whArgs...)

	// ORDER BY and LIMIT for DELETE
	if ob := b.buildOrderBy(); ob != "" {
		deleteSql = fmt.Sprintf("%s %s", deleteSql, ob)
	}
	if lm := b.buildLimit(); lm != "" {
		deleteSql = fmt.Sprintf("%s %s", deleteSql, lm)
	}

	b.SqlStr = deleteSql
	if b.err != nil {
		return "", nil, b.err
	}
	return b.SqlStr, b.fieldValue, nil
}

// shouldSkipField 判断字段是否应该跳过（值为零值且未在 zeroFieldMap/emptyFieldMap 中声明需要更新时跳过）
func (b *sqlBuilder) shouldSkipField(val reflect.Value, dbField string) bool {
	// 1. 指针类型且为 nil -> 跳过
	if val.Kind() == reflect.Ptr && val.IsNil() {
		return true
	}
	jump := false
	switch val.Kind() {
	case reflect.String:
		jump = val.String() == ""
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		jump = val.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		jump = val.Uint() == 0
	case reflect.Float32, reflect.Float64:
		jump = val.Float() == 0
	default:
		// 其他类型默认不跳
		jump = false
	}
	// 等于0仍要更新
	if _, ok := b.zeroFieldMap[dbField]; ok {
		jump = false
	}
	// 等于空仍要更新
	if _, ok := b.emptyFieldMap[dbField]; ok {
		jump = false
	}
	return jump
}
