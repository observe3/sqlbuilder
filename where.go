package sqlbuilder

import (
	"strings"
)

// 条件结构体
type Condition struct {

	// 字段
	Field string

	FieldType int64

	// 条件
	Condition string

	// 值
	Value interface{}

	// 表
	TableName string

	// 关系
	Relation string
}

type GroupWhere struct {
	Relation  string
	Condition []Condition
}

type Where struct {

	// 查询条件
	groupWhere    []GroupWhere
	assembleWhere [][]GroupWhere

	// 表名
	tableName string

	// 表的别名
	alias string

	ogroup bool // 是否用了分组
}

// 实现where接口
func (r *Where) ParseWhere() (string, []interface{}) {
	var fieldValue []interface{}
	var whStr strings.Builder
	for _, bigGroup := range r.assembleWhere {
		for j, v := range bigGroup {
			for k, w := range v.Condition {
				tableName := r.tableName
				if r.alias != "" {
					tableName = r.alias
				}
				if w.TableName != "" {
					tableName = w.TableName
				}
				operater, placeholder, result := r.parseOperater(w)
				if operater == "" || placeholder == "" {
					continue
				}
				fieldValue = append(fieldValue, result...)
				var firstRelation string
				if w.Relation != "" {
					firstRelation = w.Relation
				} else {
					firstRelation = v.Relation
				}
				if whStr.Len() > 0 {
					whStr.WriteString(" " + firstRelation)
				}
				if len(bigGroup) > 1 && j == 0 && k == 0 {
					whStr.WriteString(" (")
				}
				if len(v.Condition) > 1 && k == 0 {
					whStr.WriteString(" " + "(")
				}
				joinStr := ""
				if whStr.Len() > 0 {
					joinStr += " "
				}
				// normal field
				if w.FieldType == 1 {
					whStr.WriteString(joinStr + "`" + tableName + "`.`" + w.Field + "` " + operater + " " + placeholder)
				} else if w.FieldType == 2 {
					// special
					whStr.WriteString(joinStr + w.Field + " " + operater + " " + placeholder)
				}
				if len(v.Condition) > 1 && k == len(v.Condition)-1 {
					whStr.WriteString(" )")
				}
				if len(bigGroup) > 1 && j == len(bigGroup)-1 && k == len(v.Condition)-1 {
					whStr.WriteString(")")
				}
			}
		}
	}
	if whStr.String() == "" {
		fieldValue = nil
	}
	// 重置
	r.groupWhere = nil
	r.assembleWhere = nil
	return whStr.String(), fieldValue
}

func (r *Where) SetGroupWhere(groupWhere []GroupWhere) {
	r.groupWhere = append(r.groupWhere, groupWhere...)
}

// 设置表名
func (r *Where) SetTableName(tableName string) {
	r.tableName = tableName
}

// 设置表别名
func (r *Where) SetAlias(alias string) {
	r.alias = alias
}

func (r *Where) SetGroup() {
	r.ogroup = true
}

func (r *Where) parseOperater(w Condition) (string, string, []interface{}) {
	if val, ok := SymbolMap[strings.ToLower(w.Condition)]; ok {
		return val.Operate(w)
	}
	return "", "", nil
}
