package sqlbuilder

import (
	"fmt"
	"strings"
)

// sqlBuilder
type sqlBuilder struct {
	// 表名
	tableName string

	// 表的别名
	alias string

	// 查询的字段
	fields []interface{}

	// 参数的值
	fieldValue []interface{}

	// where条件
	jwhere *Where

	// 排序
	orderField [][]interface{}

	// 联表
	joins []string

	// sql 语句
	SqlStr string

	// 子查询
	childQuery string

	// 是否去重
	distinct bool

	// 分页参数
	offset   int64
	pageSize int64

	// 是否打印sql
	debugSql bool
}

// From 用于创建一个 sqlBuilder 实例，并返回该实例的指针。
// 参数 tableName 表示表名。
// 返回值为 *sqlBuilder 指针，指向新创建的 sqlBuilder 实例。
func From(tableName string, args ...interface{}) *sqlBuilder {
	builder := &sqlBuilder{
		tableName: tableName,
		alias:     tableName,
		jwhere: &Where{
			tableName:     tableName,
			alias:         tableName,
			groupWhere:    make([]GroupWhere, 0),
			assembleWhere: make([][]GroupWhere, 0),
		},
	}
	if len(args) > 0 {
		if val, ok := args[0].(*sqlBuilder); ok {
			childQuery, data := val.BuildSelect()
			data = append(data, builder.fieldValue...)
			builder.fieldValue = data
			builder.childQuery = childQuery
		}
	}
	return builder
}

/**
 * 设置表名
 **/
func (b *sqlBuilder) Table(tableName string) *sqlBuilder {
	b.tableName = tableName
	b.jwhere.SetTableName(tableName)
	return b
}

/**
 * 开启打印sql
**/
func (b *sqlBuilder) Debug() *sqlBuilder {
	b.debugSql = true
	return b
}

// 查询字段
func (b *sqlBuilder) Select(fields ...interface{}) *sqlBuilder {
	b.fields = fields
	return b
}

// limit
func (b *sqlBuilder) Limit(p, num int64) *sqlBuilder {
	b.offset = (p - 1) * num
	b.pageSize = num
	return b
}

// 排序
func (b *sqlBuilder) Order(order [][]interface{}) *sqlBuilder {
	b.orderField = order
	return b
}

// 去重
func (b *sqlBuilder) Distinct() *sqlBuilder {
	b.distinct = true
	return b
}

/*
* and条件
  - WhereAnd("age", 18)
  - WhereAnd("name", "like", "张三")
  - WhereAnd("id", "=", 1, tableName)
  - WhereAnd("id", "=", 1, tableName, or)
  - WhereAnd([][]interface{}{
    {"age", "=", 18, "user"},
    {"sex", "=", "男", "user", "or"},
    {"age", "=", 18},
    {"age", 18},
    })
  - WhereAnd([][][]interface{}{
    [][]interface{}{
    []interface{}{"sex", "=", "男", "user", "or"}
    },
    [][]interface{}{},
    })

*
*/
func (b *sqlBuilder) WhereAnd(args ...interface{}) *sqlBuilder {
	b.where("and", args...)
	return b
}

/*
* or条件
  - WhereOr("age", 18)
  - WhereOr("name", "like", "张三")
  - WhereOr("id", "=", 1, tableName)
  - WhereOr("id", "=", 1, tableName, or)
    WhereOr([][]interface{}{
    {"age", "=", 18, "user"},
    {"sex", "=", "男", "user", "or"},
    {"age", "=", 18},
    {"age", 18},
    })

*
*/
func (b *sqlBuilder) WhereOr(args ...interface{}) *sqlBuilder {
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
  - Where("or", [][]interface{}{
    {"age", "=", 18, "user"},
    {"sex", "=", "男", "user", "or"},
    {"age", "=", 18},
    {"age", 18},
    })

*
*/
func (b *sqlBuilder) where(relation string, args ...interface{}) *sqlBuilder {
	if len(args) > 1 {
		if val, ok := args[0].(string); !ok || val == "" {
			panic("args[0] 必须是一个字符串并且不能为空")
		}
	}
	groupWhere := ArgsMap[len(args)].ParseArgs(relation, args...)
	b.jwhere.assembleWhere = append(b.jwhere.assembleWhere, groupWhere)
	return b
}

// 返回where条件和参数
func (b *sqlBuilder) ToString() string {
	sqlStr, args := b.jwhere.ParseWhere()
	b.fieldValue = append(b.fieldValue, args...)
	return sqlStr
}

// 获取字段值
func (b *sqlBuilder) GetFieldValue() []interface{} {
	return b.fieldValue
}

// 给表起别名
func (b *sqlBuilder) As(name string) *sqlBuilder {
	b.alias = name
	b.jwhere.SetAlias(name)
	return b
}

// 内连接
func (b *sqlBuilder) Join(tableName string, alias string, f1, f2 string) *sqlBuilder {
	if alias == "" {
		alias = tableName
	}
	b.joins = append(b.joins, b.buildJoin("inner", tableName, alias, f1, f2))
	return b
}

// 左连接
func (b *sqlBuilder) LeftJoin(tableName string, alias string, f1, f2 string) *sqlBuilder {
	if alias == "" {
		alias = tableName
	}
	b.joins = append(b.joins, b.buildJoin("left", tableName, alias, f1, f2))
	return b
}

// 右连接
func (b *sqlBuilder) RightJoin(tableName string, alias string, f1, f2 string) *sqlBuilder {
	if alias == "" {
		alias = tableName
	}
	b.joins = append(b.joins, b.buildJoin("right", tableName, alias, f1, f2))
	return b
}

func (b *sqlBuilder) buildJoin(jt string, tableName string, alias string, f1, f2 string) string {
	var joinStr string
	switch jt {
	case "left":
		joinStr = "left join"
	case "right":
		joinStr = "right join"
	case "inner":
		joinStr = "join"
	}
	return fmt.Sprintf("%s `%s` as `%s` on `%s`.`%s` = `%s`.`%s`", joinStr, tableName, alias, b.alias, f1, alias, f2)
}

/**
 * 构建查询sql
**/
func (b *sqlBuilder) BuildSelect() (string, []interface{}) {
	if b.alias == "" {
		b.alias = b.tableName
		b.jwhere.SetAlias(b.tableName)
	}

	var fields string
	// 拼接要查询的字段
	for _, v := range b.fields {
		var nfield string
		switch val := v.(type) {
		case *sqlBuilder:
			if len(val.fields) == 0 || len(val.fields) > 1 {
				panic("仅需要一个字段")
			}
			childQuery, data := val.BuildSelect()
			data = append(data, b.fieldValue...)
			b.fieldValue = data
			fstr := val.fields[0].(string)
			if strings.Contains(fstr, ".") {
				fstr = strings.Split(fstr, ".")[1]
			}
			nfield = fmt.Sprintf("(%s) as `%s`", childQuery, fstr)
		case *fcolumn:
			if val.Fn != "" {
				var fnp string
				for _, vv := range val.Params {
					fnp = fmt.Sprintf("%s, %v", fnp, vv)
				}
				fnp = strings.TrimLeft(fnp, ", ")
				nfield = fmt.Sprintf("%s(%s) as `%s`", val.Fn, fnp, val.Alias)
			}
		case *scolumn:
			if val.TableAlias != "" {
				nfield = fmt.Sprintf("`%s`.`%s` %s", val.TableAlias, val.Field, val.FieldAlias)
				nfield = strings.TrimSpace(nfield)
			}
		case string:
			if strings.Contains(val, ".") {
				arr := strings.Split(val, ".")
				if len(arr) != 2 {
					panic("错误的查询字段")
				}
				nfield = fmt.Sprintf("`%s`.`%s`", arr[0], arr[1])
			} else {
				nfield = fmt.Sprintf("`%s`.`%s`", b.alias, val)
			}
		default:
			panic("不支持的查询字段")
		}
		if fields != "" {
			fields = fmt.Sprintf("%s,%s", fields, nfield)
		} else {
			fields = nfield
		}
	}
	// 没有指定字段，则默认查询所有
	if fields == "" {
		fields = fmt.Sprintf("`%s`.*", b.alias)
	}
	target := fmt.Sprintf("`%s`", b.tableName)
	if b.childQuery != "" {
		target = fmt.Sprintf("(%s)", b.childQuery)
	}
	// 判断是否去重
	if b.distinct {
		b.SqlStr = fmt.Sprintf("select distinct %s from %s as `%s`", fields, target, b.alias)
	} else {
		b.SqlStr = fmt.Sprintf("select %s from %s as `%s`", fields, target, b.alias)
	}
	// 连表
	for _, v := range b.joins {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, v)
	}
	// 解析查询条件
	whStr, whValue := b.jwhere.ParseWhere()
	if whStr != "" {
		b.SqlStr = fmt.Sprintf("%s where %s", b.SqlStr, whStr)
	}

	if len(whValue) > 0 {
		b.fieldValue = append(b.fieldValue, whValue...)
	}
	// 排序
	if len(b.orderField) > 0 {
		b.SqlStr += " order by "
		var order string
		for _, v := range b.orderField {
			if len(v) < 2 {
				continue
			}
			if len(v) == 2 {
				order = fmt.Sprintf("%s,`%s`.`%s` %s", order, b.alias, v[0], v[1])
			} else if len(v) == 3 {
				order = fmt.Sprintf("%s,`%s`.`%s` %s", order, v[2], v[0], v[1])
			}
		}
		order = strings.Trim(order, ",")
		b.SqlStr += order
	}
	// 分页
	if b.offset >= 0 && b.pageSize > 0 {
		b.SqlStr = fmt.Sprintf("%s limit %d,%d", b.SqlStr, b.offset, b.pageSize)
	}
	// 只查询一条
	if b.offset < 0 && b.pageSize > 0 {
		b.SqlStr = fmt.Sprintf("%s limit %d", b.SqlStr, b.pageSize)
	}
	// 是否打印sql
	if b.debugSql {
		fmt.Println(b.SqlStr)
	}
	return b.SqlStr, b.fieldValue
}

/**
 * 构建创建sql
**/
func (b *sqlBuilder) BuildCreate(option map[string]interface{}) (string, map[string]interface{}) {
	var keys, vals string
	for k := range option {
		keys = fmt.Sprintf("%s,`%s`", keys, k)
		vals = fmt.Sprintf("%s,:%s", vals, k)
	}
	keys = strings.TrimLeft(keys, ",")
	vals = strings.TrimLeft(vals, ",")
	return fmt.Sprintf("insert into `%s` (%s) values(%s)", b.tableName, keys, vals), option
}

/**
 * 构建更新sql
**/
func (b *sqlBuilder) BuildUpdate(option map[string]interface{}) (string, []interface{}) {
	var vals string
	tableName := b.tableName
	if b.alias != "" {
		tableName = b.alias
	}
	for k := range option {
		vals = fmt.Sprintf("%s,`%s`.`%s` = ?", vals, tableName, k)
		b.fieldValue = append(b.fieldValue, option[k])
	}
	b.SqlStr = fmt.Sprintf("update `%s` as `%s` set %s", b.tableName, tableName, strings.TrimLeft(vals, ","))

	// 分组条件
	// b.groupWhere()
	whStr, whArgs := b.jwhere.ParseWhere()
	if whStr != "" {
		b.SqlStr = fmt.Sprintf("%s where %s", b.SqlStr, whStr)
	}
	if len(whArgs) > 0 {
		b.fieldValue = append(b.fieldValue, whArgs...)
	}
	return b.SqlStr, b.fieldValue
}

/**
 * 构建累加sql
**/
func (b *sqlBuilder) BuildIncrement(option map[string]interface{}) (string, []interface{}) {
	var vals string
	tableName := b.tableName
	if b.alias != "" {
		tableName = b.alias
	}
	for k := range option {
		vals = fmt.Sprintf("%s,`%s`.`%s` = `%s`.`%s` + ?", vals, tableName, k, tableName, k)
		b.fieldValue = append(b.fieldValue, option[k])
	}
	b.SqlStr = fmt.Sprintf("update `%s` as `%s` set %s", b.tableName, tableName, strings.TrimLeft(vals, ","))

	whStr, whArgs := b.jwhere.ParseWhere()
	if whStr != "" {
		b.SqlStr = fmt.Sprintf("%s where %s", b.SqlStr, whStr)
	}
	if len(whArgs) > 0 {
		b.fieldValue = append(b.fieldValue, whArgs...)
	}
	return b.SqlStr, b.fieldValue
}

/**
 * 构建累减sql
**/
func (b *sqlBuilder) BuildDecrement(option map[string]interface{}) (string, []interface{}) {
	var vals string
	tableName := b.tableName
	if b.alias != "" {
		tableName = b.alias
	}
	for k := range option {
		vals = fmt.Sprintf("%s,`%s`.`%s` = `%s`.`%s` - ?", vals, tableName, k, tableName, k)
		b.fieldValue = append(b.fieldValue, option[k])
	}
	b.SqlStr = fmt.Sprintf("update `%s` as `%s` set %s", b.tableName, tableName, strings.TrimLeft(vals, ","))

	whStr, whArgs := b.jwhere.ParseWhere()
	if whStr != "" {
		b.SqlStr = fmt.Sprintf("%s where %s", b.SqlStr, whStr)
	}
	if len(whArgs) > 0 {
		b.fieldValue = append(b.fieldValue, whArgs...)
	}
	return b.SqlStr, b.fieldValue
}

/*
 * 构建删除sql
**/
func (b *sqlBuilder) BuildDelete() (string, []interface{}) {
	tableName := b.tableName
	if b.alias != "" {
		tableName = b.alias
	}
	b.SqlStr = fmt.Sprintf("delete `%s` from `%s` as `%s`", tableName, b.tableName, tableName)

	whStr, whArgs := b.jwhere.ParseWhere()
	if whStr == "" {
		return "", nil
	}
	if whStr != "" {
		b.SqlStr = fmt.Sprintf("%s where %s", b.SqlStr, whStr)
	}
	if len(whArgs) > 0 {
		b.fieldValue = append(b.fieldValue, whArgs...)
	}
	return b.SqlStr, b.fieldValue
}
