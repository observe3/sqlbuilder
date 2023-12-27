package sqlbuilder

import (
	"fmt"
	"testing"
)

func TestSqlBuilder(t *testing.T) {

	sql, args := From("admin").As("a").WhereAnd("name", "张三").WhereOr([][]interface{}{
		{"age", ">", 18},
		{"age", "<", 20},
	}).Limit(0, 1).Build()
	fmt.Println(sql, args)
	fmt.Println("")

	sql, args = From("admin").As("a").Join("department", "b", "department_id", "id").WhereAnd("id", "=", 23, "b").Limit(0, 1).Build()
	fmt.Println(sql, args)
	fmt.Println("")

	sql, args = From("admin").As("a").WhereAnd([][]interface{}{
		{"name", "=", "张三"},
		{"name", "like", "李四", "a", "or"},
	}).Limit(0, 1).Build()
	fmt.Println(sql, args)
	fmt.Println("")

	sql, args = From("admin").As("a").Join("department", "b", "department_id", "id").WhereAnd([][]interface{}{
		{"sex", "男"},
		{"name", "=", "张三"},
		{"id", "in", From("user_age").As("h").Select("id").WhereAnd("age", ">", 23), "a", "and"},
		{"d_name", "=", "研发部", "b"},
	}).Limit(0, 1).Build()
	fmt.Println(sql, args)
	fmt.Println("")

	sql, args = From("admin").As("a").Select("id", "name", From("vip_user").As("c").Select("sex").WhereAnd("level", 3).Limit(0, 1)).WhereAnd([][]interface{}{
		{"age", "in", []interface{}{18, 19, 20}},
		{"name", "between", []interface{}{2, 20}},
	}).Limit(0, 1).Build()
	fmt.Println(sql, args)
}
