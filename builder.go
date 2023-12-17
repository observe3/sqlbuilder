package sqlbuilder

import (
	"fmt"
	"strings"
)

/**
 * sql sqlBuilder
**/

// where条件
type Wh struct {
	// 字段
	Field string
	// 条件
	Condition string
	// 值
	Value interface{}
	// 表
	TableName string
}
type sqlBuilder struct {
	// 表名
	tableName string

	// 别名
	alias string

	// 查询的字段
	fields []string

	// 查询的字段值
	fieldValue []interface{}

	// 查询条件
	whereMap []map[string][]Wh

	// 排序
	orderField [][]interface{}

	joins []string

	// sql 语句
	SqlStr   string
	offset   int
	pageSize int

	// 是否打印sql
	debugSql bool
}

func From(name string) *sqlBuilder {
	return &sqlBuilder{
		tableName: name,
		whereMap:  make([]map[string][]Wh, 0),
		alias:     name,
		debugSql:  false,
	}
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
func (b *sqlBuilder) Select(fields ...string) *sqlBuilder {
	b.fields = fields
	return b
}

// limit分页
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

/**
 *  and查询条件
 **/
func (b *sqlBuilder) WhereAnd(wh []Wh) *sqlBuilder {
	b.whereMap = append(b.whereMap, map[string][]Wh{
		"and": wh,
	})
	return b
}

/**
 *  or查询条件
 **/
func (b *sqlBuilder) WhereOr(wh []Wh) *sqlBuilder {
	b.whereMap = append(b.whereMap, map[string][]Wh{
		"or": wh,
	})
	return b
}

/**
 *  构建sql
**/
func (b *sqlBuilder) Build() (string, []interface{}) {
	// 没有指定字段就查询所有
	if len(b.fields) == 0 {
		b.fields = []string{"`" + b.alias + "`" + ".*"}
	}
	// 拼接要查询的字段
	field := strings.Join(b.fields, ",")
	// 去掉右边的逗号
	if field[len(field)-1:] == "," {
		field = field[:len(field)-1]
	}
	b.SqlStr = fmt.Sprintf("select %s from `%s` as `%s`", field, b.tableName, b.alias)
	// 连表
	for _, v := range b.joins {
		b.SqlStr = fmt.Sprintf("%s %s", b.SqlStr, v)
	}
	b.SqlStr = fmt.Sprintf("%s where 1 = 1", b.SqlStr)
	if len(b.whereMap) > 0 {
		for _, v := range b.whereMap {
			// 判断是and 还是 or
			for key, vv := range v {
				if key == "and" {
					// 遍历and 条件
					wh := " and ("
					for k, w := range vv {
						if k > 0 {
							wh += " and "
						}
						sval, ok := w.Value.([]interface{})
						var valStr string
						if ok {
							for _, v := range sval {
								valStr += fmt.Sprintf("%v,", v)
							}
						} else {
							valStr = fmt.Sprintf("%v", w.Value)
						}
						b.fieldValue = append(b.fieldValue, valStr)
						tableName := b.tableName
						if w.TableName != "" {
							tableName = w.TableName
						}
						wh = fmt.Sprintf("%s`%s`.`%s` %s %s", wh, tableName, w.Field, w.Condition, fvalue(w.Condition))
					}
					wh += ")"
					b.SqlStr += wh
				} else if key == "or" {
					// 遍历or 条件
					wh := " or ("
					for k, w := range vv {
						if k > 0 {
							wh += " and "
						}
						sval, ok := w.Value.([]interface{})
						var valStr string
						if ok {
							for _, v := range sval {
								valStr += fmt.Sprintf("%v,", v)
							}
						} else {
							valStr = fmt.Sprintf("%v", w.Value)
						}
						b.fieldValue = append(b.fieldValue, valStr)
						tableName := b.tableName
						if w.TableName != "" {
							tableName = w.TableName
						}
						wh = fmt.Sprintf("%s`%s`.`%s` %s %s", wh, tableName, w.Field, w.Condition, fvalue(w.Condition))
					}
					wh += ")"
					b.SqlStr += wh
				} else {
					continue
				}
			}

		}
	}
	// 排序
	if len(b.orderField) > 0 {
		b.SqlStr += " order by "
		var order string
		for _, v := range b.orderField {
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

// 连表
func (b *sqlBuilder) Join(tableName string, alias string, f1, f2 string) *sqlBuilder {
	str := fmt.Sprintf("join `%s`", tableName)
	if alias != "" {
		str = fmt.Sprintf("%s as `%s`", str, alias)
	} else {
		alias = tableName
	}
	str = fmt.Sprintf("%s on `%s`.`%s` = `%s`.`%s`", str, b.alias, f1, alias, f2)
	b.joins = append(b.joins, str)
	return b
}

func fvalue(c string) string {
	var tmp string
	switch c {
	case "like":
		tmp = "CONCAT(?,'%')"
	case "=", "<>", "!=", ">", ">=", "<", "<=":
		tmp = "?"
	case "in",
		"IN",
		"NOT IN",
		"not in":
		tmp = "CONCAT('(',?,')')"
	}
	return tmp
}
