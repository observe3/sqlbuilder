package sqlbuilder

import (
	"fmt"
	"testing"
)

type Person struct {
	Id           int    `db:"id"`
	Name         string `db:"name"`
	Age          int    `db:"age"`
	Sex          string `db:"sex"`
	DepartmentId int    `db:"department_id"`
}

func (Person) TableName() string {
	return "person"
}

// 获得id字段
func (Person) GetIdField() string {
	return "id"
}

// 获得name字段
func (Person) GetNameField() string {
	return "name"
}

// 获得年龄字段
func (Person) GetAgeField() string {
	return "age"
}

func TestSqlBuilder(t *testing.T) {
	p1 := Person{
		Id:           1,
		Name:         "张三",
		Age:          18,
		Sex:          "男",
		DepartmentId: 1,
	}
	sql, args := From(p1.TableName()).Select(Fn("count", "total", "*")).As("a").
		WhereAnd(p1.GetNameField(), "=", "张三", "g").
		WhereAnd("age", ">=", "43", "a", "or").
		WhereAnd([][]interface{}{
			{p1.GetAgeField(), ">", 18, "b"},
			{p1.GetAgeField(), "<", 20, "b"},
		}).
		Order([][]interface{}{
			{"id", "desc"},
		}).
		Group("d_id", "a_id").
		Limit(0, 1).BuildSelect()
	fmt.Println(sql, args)
	fmt.Println("")

	sql, args = From("admin").As("a").Join("department", "b", "department_id", "id").WhereAnd("id", "=", 23, "b").Limit(0, 1).BuildSelect()
	fmt.Println(sql, args)
	fmt.Println("")

	sql, args = From("admin").As("a").WhereAnd([][]interface{}{
		{"name", "=", "张三"},
		{"name", "like", "李四", "d", "or"},
	}).Limit(0, 1).BuildSelect()
	fmt.Println(sql, args)
	fmt.Println("")

	sql, args = From("admin").As("a").Join("department", "b", "department_id", "id").WhereAnd([][]interface{}{
		{"sex", "男"},
		{"name", "=", "张三"},
		{"id", "in", From("user_age").As("h").Select("id").WhereAnd("age", ">", 23), "", "and"},
		{"d_name", "=", "研发部", "b"},
	}).Limit(0, 1).BuildSelect()
	fmt.Println(sql, args)
	fmt.Println("")

	sql, args = From("admin").As("a").Select("id", "name", From("vip_user").As("c").Select("sex").WhereAnd("level", 3).Limit(0, 1)).WhereAnd([][]interface{}{
		{"age", "in", []interface{}{18, 19, 20}},
		{"name", "between", []interface{}{2, 20}},
		{"gui", "not between", []string{"a", "b"}},
		{"", ""},
	}).Limit(0, 1).BuildSelect()
	fmt.Println(sql, args)
	fmt.Println("")

	sql, _ = From("admin").BuildCreate(map[string]any{
		"name": "张三",
		"age":  18,
	})
	fmt.Println(sql)
	fmt.Println("")

	sql, a := From("admin").WhereAnd("id", 1).BuildUpdate(map[string]any{
		"name": "张三",
		"age":  18,
	})
	fmt.Println(sql, a)
	fmt.Println("")

	sql, args = From("admin").As("a").
		WhereOr("name", "like", "张三", "r").WhereOr("age", ">", 18).
		WhereAnd([][][]interface{}{
			{
				[]interface{}{"id", ">", 1},
				[]interface{}{"id", "<", 10},
				[]interface{}{"id", "=", 11, "a", "or"},
			},
			{
				[]interface{}{"nation", "=", "中国", "a", "or"},
				[]interface{}{"city", "=", "北京", "a"},
			},
			{
				[]interface{}{"fff", "=", "332", "n", "or"},
				[]interface{}{"sex", "男"},
			},
		}).
		WhereAnd([][]interface{}{
			{"level", ">", 1, "a", "or"},
			{"level", "<", 10, "a", "or"},
		}).
		WhereAnd("id", "=", 1, "a").
		BuildDelete()
	fmt.Println(sql, args)
	fmt.Println("")

	sql, args = From("produt").As("a").
		WhereAnd("id", 2).
		BuildDecrement(map[string]any{
			"num": 1,
		})
	fmt.Println(sql, args)
	fmt.Println("")

	sql, args = From("produt").As("a").
		Select("classify_name", Fn("SUM", "total_sum", "bill_sum")).
		WhereAnd("id", 2).
		Group("classify_id").
		WhereAnd("in_num", "<", SField("", "num", "")).
		WhereAnd("in_sum", "<", Literal("sum+u_sum")).
		BuildSelect()
	fmt.Println(sql, args)

	var wh [][][]interface{}
	wh = append(wh, [][]interface{}{
		{"name", "like", "张三"},
		{"id", "in", From("user_age").WhereAnd("age", ">", 233)},
	})
	sql, args = From("admin").As("a").WhereAnd(wh).BuildSelect()
	fmt.Println(sql, args)
	fmt.Println("")

	// 子查询
	sql, args = From("product_static", From("product_static").Select("id", "name").WhereAnd("name", "start with", "苹果")).Select(Fn("count", "total", "*")).BuildSelect()
	fmt.Println(sql, args)
	fmt.Println("")

	// 累加、减、乘、除某个字段
	// action: + - * /
	sql, args = From("inventory").As("a").WhereAnd("id", 1).BuildUpdate(map[string]interface{}{
		"num":  []any{"u_num", "+", 20},
		"name": "张三",
	})
	fmt.Println(sql, args)
	fmt.Println("")

	// having
	sql, args = From("produt").As("a").
		Select("classify_id", Fn("SUM", "total_sum", "bill_sum")).
		WhereAnd("id", 2).
		Group("classify_id").
		WhereAnd([][]any{
			{Literal("`a`.`in_num`+`a`.`a_num`"), "<", Literal("num+sum")},
		}).
		HavingWhereAnd([][]interface{}{
			{"id", "=", 23},
			{"age", ">", 23},
		}).
		HavingWhereAnd(Fn("sum", "total_sum", "sum"), ">", 423).
		BuildSelect()
	fmt.Println(sql, args)
	fmt.Println("")
}
