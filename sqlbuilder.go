package sqlbuilder

import (
	"fmt"
	"strings"
)

// 条件结构体
type Wh struct {

	// 字段
	Field string

	// 条件
	Condition string

	// 值
	Value interface{}

	// 表
	TableName string

	// 关系
	Relation string
}

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

	// 查询条件
	whereMap []map[string][]Wh

	// 排序
	orderField [][]interface{}

	// 联表
	joins []string

	// sql 语句
	SqlStr string

	childQuery string

	// 分页参数
	offset   int
	pageSize int

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
		whereMap:  make([]map[string][]Wh, 0),
	}
	if len(args) > 0 {
		if val, ok := args[0].(*sqlBuilder); ok {
			childQuery, data := val.Build()
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
func (b *sqlBuilder) Limit(p, num int) *sqlBuilder {
	b.offset = (p - 1) * num
	b.pageSize = num
	return b
}

// 排序
func (b *sqlBuilder) Order(order [][]interface{}) *sqlBuilder {
	b.orderField = order
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

*
*/
func (b *sqlBuilder) WhereAnd(args ...interface{}) *sqlBuilder {
	b.Where("and", args...)
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
	b.Where("or", args...)
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
func (b *sqlBuilder) Where(relation string, args ...interface{}) *sqlBuilder {
	length := len(args)
	if length == 4 {
		b.whereMap = append(b.whereMap, map[string][]Wh{
			relation: {
				{
					Field:     args[0].(string),
					Condition: args[1].(string),
					Value:     args[2],
					TableName: args[3].(string),
				},
			},
		})
	} else if length == 3 {
		b.whereMap = append(b.whereMap, map[string][]Wh{
			relation: {
				{
					Field:     args[0].(string),
					Condition: args[1].(string),
					Value:     args[2],
				},
			},
		})
	} else if length == 2 {
		b.whereMap = append(b.whereMap, map[string][]Wh{
			relation: {
				{
					Field:     args[0].(string),
					Condition: "=",
					Value:     args[1],
				},
			},
		})
	} else if length == 1 {
		whs, ok := args[0].([][]interface{})
		if ok {
			andWh := []Wh{}
			for _, v := range whs {
				switch len(v) {
				case 5:
					andWh = append(andWh, Wh{
						Field:     v[0].(string),
						Condition: v[1].(string),
						Value:     v[2],
						TableName: v[3].(string),
						Relation:  v[4].(string),
					})
				case 4:
					andWh = append(andWh, Wh{
						Field:     v[0].(string),
						Condition: v[1].(string),
						Value:     v[2],
						TableName: v[3].(string),
					})
				case 3:
					andWh = append(andWh, Wh{
						Field:     v[0].(string),
						Condition: v[1].(string),
						Value:     v[2],
					})
				case 2:
					andWh = append(andWh, Wh{
						Field:     v[0].(string),
						Condition: "=",
						Value:     v[1],
					})
				}
			}
			b.whereMap = append(b.whereMap, map[string][]Wh{
				relation: andWh,
			})
		}
	}
	return b
}

/**
 * 构建sql
**/
func (b *sqlBuilder) Build() (string, []interface{}) {
	if b.alias == "" {
		b.alias = b.tableName
	}

	var fields string
	// 拼接要查询的字段
	for k, v := range b.fields {
		if val, ok := v.(*sqlBuilder); ok {
			if len(val.fields) == 0 {
				// 没有查询字段
				panic("no query field")
			}
			field, data := val.Build()
			data = append(data, b.fieldValue...)
			b.fieldValue = data
			fstr := val.fields[0].(string)
			if strings.Contains(fstr, ".") {
				fstr = strings.Split(fstr, ".")[1]
			}
			b.fields[k] = fmt.Sprintf("(%s) as %s", field, fstr)
		}
		if val, ok := v.(string); ok {
			b.fields[k] = fmt.Sprintf("`%s`.`%s`", b.alias, val)
		}
		if fields != "" {
			fields = fmt.Sprintf("%s,%s", fields, b.fields[k])
		} else {
			fields = fmt.Sprintf("%s", b.fields[k])
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
	b.SqlStr = fmt.Sprintf("select %s from %s as `%s`", fields, target, b.alias)
	// 连表
	for _, v := range b.joins {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, v)
	}
	b.SqlStr = fmt.Sprintf("%s where 1 = 1", b.SqlStr)
	if len(b.whereMap) > 0 {
		for _, v := range b.whereMap {
			// 判断是and 还是 or
			for key, vv := range v {
				if key != "and" && key != "or" {
					continue
				}
				// 遍历and 条件
				var wh string
				if len(vv) == 1 {
					wh = fmt.Sprintf(" %s ", key)
				} else if len(vv) > 1 {
					wh = fmt.Sprintf(" %s (", key)
				}
				for k, w := range vv {
					if k > 0 {
						relation := "and"
						if w.Relation != "" {
							relation = w.Relation
						}
						wh += fmt.Sprintf(" %s ", relation)
					}
					tableName := b.tableName
					if b.alias != "" {
						tableName = b.alias
					}
					if w.TableName != "" {
						tableName = w.TableName
					}

					condition, value := fvalue(w.Condition)
					if sval, ok := w.Value.([]interface{}); ok {
						if strings.Contains(condition, "in") {
							var valStr string
							for _, v := range sval {
								valStr += fmt.Sprintf("%v,", v)
							}
							valStr = strings.TrimRight(valStr, ",")
							b.fieldValue = append(b.fieldValue, valStr)
						} else if strings.Contains(condition, "between") {
							if len(sval) == 2 {
								b.fieldValue = append(b.fieldValue, sval...)
							}
						}
					} else if builder, ok := w.Value.(*sqlBuilder); ok {
						buildQuery, args := builder.Build()
						b.fieldValue = append(b.fieldValue, args...)
						value = fmt.Sprintf("(%v)", buildQuery)
					} else {
						if w.Value != "" {
							b.fieldValue = append(b.fieldValue, w.Value)
						}
					}
					wh = fmt.Sprintf("%s`%s`.`%s` %s %s", wh, tableName, w.Field, condition, value)
				}
				if len(vv) > 1 {
					wh += ")"
				}
				b.SqlStr += wh
			}

		}
	}
	// 排序
	if len(b.orderField) > 0 {
		b.SqlStr += " order by "
		var order string
		for _, v := range b.orderField {
			if len(v) < 2 {
				continue
			}
			order = fmt.Sprintf("%s,`%s`.`%s` %s", order, b.alias, v[0], v[1])
		}
		order = strings.Trim(order, ",")
		b.SqlStr += order
	}
	// 分页
	if b.offset >= 0 && b.pageSize > 0 {
		b.SqlStr = fmt.Sprintf("%s limit %d,%d", b.SqlStr, b.offset, b.pageSize)
	}
	if b.offset < 0 && b.pageSize > 0 {
		b.SqlStr = fmt.Sprintf("%s limit %d", b.SqlStr, b.pageSize)
	}
	// 是否打印sql
	if b.debugSql {
		fmt.Println(b.SqlStr)
	}
	return b.SqlStr, b.fieldValue
}

// 获取字段值
func (b *sqlBuilder) GetFieldValue() []interface{} {
	return b.fieldValue
}

// 给表起别名
func (b *sqlBuilder) As(name string) *sqlBuilder {
	b.alias = name
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

func fvalue(c string) (string, string) {
	var condition, value string
	c = strings.ToLower(c)
	switch c {
	case "like", "not like":
		value = "CONCAT('%',?,'%')"
		condition = c
	case "start with", "not start with":
		value = "CONCAT(?,'%')"
		if c == "start with" {
			condition = "like"
		} else if c == "not start with" {
			condition = "not like"
		}
	case "end with", "not end with":
		value = "CONCAT('%',?)"
		if c == "end with" {
			condition = "like"
		} else if c == "not end with" {
			condition = "not like"
		}
	case "=", "<>", "!=", ">", ">=", "<", "<=":
		value = "?"
		condition = c
	case "in",
		"not in":
		value = "CONCAT('(',?,')')"
		condition = c
	case "is null":
		value = ""
		condition = "is null"
	case "is not null":
		value = ""
		condition = "is not null"
	case "is empty":
		value = ""
		condition = "= ''"
	case "is not empty":
		value = ""
		condition = "<> ''"
	case "between":
		value = "and ?"
		condition = "between ?"
	case "not between":
		value = "and ?"
		condition = "not between ?"
	default:
		value = "?"
		condition = c
	}
	return condition, value
}
