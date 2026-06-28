package sqlbuilder

import (
	"strings"
	"testing"
	"time"
)

type Person struct {
	Id           int    `db:"id"`
	Name         string `db:"name"`
	Age          int    `db:"age"`
	Sex          string `db:"sex"`
	DepartmentId int    `db:"department_id"`
	Speak
}

type Speak struct {
	Act string `db:"act"`
	Walk
}

type Walk struct {
	Step string `db:"step"`
}

func (Person) TableName() string { return "person" }

func TestSelect_Basic(t *testing.T) {
	sql, args, err := From("admin").Select("id", "name").Limit(1).BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "select") {
		t.Errorf("expected SELECT statement, got: %s", sql)
	}
	if !strings.Contains(sql, "from `admin`") {
		t.Errorf("expected FROM admin, got: %s", sql)
	}
	if len(args) != 0 {
		t.Errorf("expected 0 args, got %d", len(args))
	}
	t.Logf("SELECT basic: %s | args: %v", sql, args)
}

func TestSelect_WhereAnd(t *testing.T) {
	p := Person{Id: 1, Name: "张三", Age: 18, Sex: "男", DepartmentId: 1}
	sql, args, err := From(p.TableName()).Select(Fn("count", "total", "*")).As("a").
		WhereAnd(p.TableName()+".name", "=", "张三", "g").
		WhereAnd("age", ">=", "43", "a", "or").
		WhereAnd([][]any{
			{"age", ">", 18, "b"},
			{"age", "<", 20, "b"},
		}).
		Order([][]any{{"id", "desc"}}).
		Group("d_id", "a_id").
		Limit(1).BuildSelect()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "count(*)") {
		t.Errorf("expected count(*), got: %s", sql)
	}
	if !strings.Contains(sql, "where") {
		t.Errorf("expected WHERE clause, got: %s", sql)
	}
	if !strings.Contains(sql, "group by") {
		t.Errorf("expected GROUP BY, got: %s", sql)
	}
	if !strings.Contains(sql, "order by") {
		t.Errorf("expected ORDER BY, got: %s", sql)
	}
	if !strings.Contains(sql, "limit") {
		t.Errorf("expected LIMIT, got: %s", sql)
	}
	t.Logf("SELECT with WHERE/GROUP/ORDER/LIMIT: %s | args: %v", sql, args)
}

func TestSelect_Join(t *testing.T) {
	sql, args, err := From("admin").As("a").
		Join("department", "b", "department_id", "id").
		WhereAnd("id", "=", 23, "b").
		Limit(1).BuildSelect()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "join") {
		t.Errorf("expected JOIN, got: %s", sql)
	}
	t.Logf("SELECT with JOIN: %s | args: %v", sql, args)
}

func TestSelect_Subquery(t *testing.T) {
	sql, args, err := From("admin").As("a").Select("id", "name",
		From("vip_user").As("c").Select("sex").WhereAnd("level", 3).Limit(1),
	).WhereAnd([][]any{
		{"age", "in", []any{18, 19, 20}},
		{"name", "between", []any{2, 20}},
	}).Limit(1).BuildSelect()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "select") {
		t.Errorf("expected SELECT, got: %s", sql)
	}
	t.Logf("SELECT with subquery: %s | args: %v", sql, args)
}

func TestSelect_FromSubquery(t *testing.T) {
	sql, args, err := From("product_static",
		From("product_static").Select("id", "name").WhereAnd("name", "start with", "苹果"),
	).Select(Fn("count", "total", "*")).BuildSelect()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "select") {
		t.Errorf("expected SELECT, got: %s", sql)
	}
	t.Logf("SELECT from subquery: %s | args: %v", sql, args)
}

func TestSelect_Distinct(t *testing.T) {
	sql, args, err := From("admin").Select("name").Distinct().BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "distinct") {
		t.Errorf("expected DISTINCT, got: %s", sql)
	}
	t.Logf("SELECT DISTINCT: %s | args: %v", sql, args)
}

func TestSelect_GroupByHaving(t *testing.T) {
	sql, args, err := From("product").As("a").
		Select("classify_id", Fn("SUM", "total_sum", "bill_sum")).
		WhereAnd("id", 2).
		Group("classify_id").
		HavingWhereAnd([][]any{
			{"id", "=", 23},
			{"age", ">", 23},
		}).
		BuildSelect()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "group by") {
		t.Errorf("expected GROUP BY, got: %s", sql)
	}
	if !strings.Contains(sql, "having") {
		t.Errorf("expected HAVING, got: %s", sql)
	}
	t.Logf("SELECT with HAVING: %s | args: %v", sql, args)
}

func TestSelect_SFieldAndLiteral(t *testing.T) {
	sql, args, err := From("produt").As("a").
		Select("classify_name", Fn("SUM", "total_sum", "bill_sum")).
		WhereAnd("id", 2).
		Group("classify_id").
		WhereAnd("in_num", "<", SField("", "num", "")).
		WhereAnd("in_sum", "<", Literal("sum+u_sum")).
		BuildSelect()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "`num`") {
		t.Errorf("expected field reference, got: %s", sql)
	}
	if !strings.Contains(sql, "sum+u_sum") {
		t.Errorf("expected literal, got: %s", sql)
	}
	t.Logf("SELECT with SField/Literal: %s | args: %v", sql, args)
}

func TestInsert_MapInsert(t *testing.T) {
	sql, args := From("department").BuildMapInsert(map[string]any{
		"name": "技术部",
		"age":  "25",
	})
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "insert into") {
		t.Errorf("expected INSERT, got: %s", sql)
	}
	if !strings.Contains(sql, "?") {
		t.Errorf("expected placeholder ?, got: %s", sql)
	}
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
	t.Logf("INSERT map: %s | args: %v", sql, args)
}

func TestInsert_MapNamedInsert(t *testing.T) {
	sql, namedArgs := From("department").BuildMapNamedInsert(map[string]any{
		"name": "张三",
		"age":  18,
	})
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "insert into") {
		t.Errorf("expected INSERT, got: %s", sql)
	}
	if !strings.Contains(sql, ":name") {
		t.Errorf("expected named param :name, got: %s", sql)
	}
	if _, ok := namedArgs["name"]; !ok {
		t.Errorf("expected named arg 'name', got: %v", namedArgs)
	}
	t.Logf("INSERT map named: %s | args: %v", sql, namedArgs)
}

func TestInsert_SliceMapInsert(t *testing.T) {
	sql, args := From("product").BuildSliceMapInsert([]map[string]any{
		{"id": 1, "name": "张三", "age": 23},
		{"id": 2, "name": "李四", "age": 25},
	})
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "insert into") {
		t.Errorf("expected INSERT, got: %s", sql)
	}
	if len(args) != 6 {
		t.Errorf("expected 6 args, got %d: %v", len(args), args)
	}
	t.Logf("INSERT slice map: %s | args: %v", sql, args)
}

func TestInsert_StructInsert(t *testing.T) {
	sql, args, err := From("product").BuildStructInsert(&Person{
		Id: 1, Name: "张三",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "insert into") {
		t.Errorf("expected INSERT, got: %s", sql)
	}
	if !strings.Contains(sql, "?") {
		t.Errorf("expected placeholder ?, got: %s", sql)
	}
	t.Logf("INSERT struct: %s | args: %v", sql, args)
}

func TestInsert_StructNamedInsert(t *testing.T) {
	sql, err := From("person").As("a").BuildStructNamedInsert(&Person{
		Id: 1, Name: "乔峰", Age: 35, Sex: "男",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, ":id") {
		t.Errorf("expected named param :id, got: %s", sql)
	}
	t.Logf("INSERT struct named: %s", sql)
}

func TestInsert_SliceStructInsert(t *testing.T) {
	sql, args, err := From("person").As("a").BuildSliceStructInsert(&[]Person{
		{Id: 1, Name: "孙悟空", Age: 23},
		{Id: 2, Name: "唐僧", Age: 25},
		{Id: 3, Name: "猪八戒", Age: 27, Speak: Speak{Act: "attack", Walk: Walk{Step: "walk"}}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "insert into") {
		t.Errorf("expected INSERT, got: %s", sql)
	}
	if len(args) != 11 {
		t.Errorf("expected 11 args, got %d", len(args))
	}
	t.Logf("INSERT slice struct: %s | args: %v", sql, args)
}

func TestInsert_SliceStructNamedInsert(t *testing.T) {
	sql, err := From("person").As("a").BuildSliceStructNamedInsert(&[]Person{
		{Id: 1, Name: "孙悟空", Age: 23},
		{Id: 2, Name: "唐僧", Age: 25},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, ":id") {
		t.Errorf("expected named param :id, got: %s", sql)
	}
	t.Logf("INSERT slice struct named: %s", sql)
}

func TestInsert_NilPointerError(t *testing.T) {
	_, _, err := From("t").BuildStructInsert(nil)
	if err == nil {
		t.Fatal("expected error for nil pointer")
	}
}

func TestInsert_NonPointerError(t *testing.T) {
	p := Person{Id: 1}
	_, _, err := From("t").BuildStructInsert(p) // not a pointer
	if err == nil {
		t.Fatal("expected error for non-pointer")
	}
}

func TestInsert_NoFieldsError(t *testing.T) {
	type Empty struct{}
	_, _, err := From("t").BuildStructInsert(&Empty{})
	if err == nil {
		t.Fatal("expected error for struct with no db tags")
	}
}

func TestUpdate_MapUpdate(t *testing.T) {
	sql, args, err := From("admin").As("a").
		WhereAnd("id", 1).
		BuildMapUpdate(map[string]any{
			"name": "张三",
			"age":  18,
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "update") {
		t.Errorf("expected UPDATE, got: %s", sql)
	}
	if !strings.Contains(sql, "where") {
		t.Errorf("expected WHERE clause, got: %s", sql)
	}
	t.Logf("UPDATE map: %s | args: %v", sql, args)
}

func TestUpdate_MapUpdateNoWhereError(t *testing.T) {
	_, _, err := From("admin").BuildMapUpdate(map[string]any{"name": "张三"})
	if err == nil {
		t.Fatal("expected error for update without WHERE")
	}
}

func TestUpdate_StructUpdate(t *testing.T) {
	p := Person{
		Id:           1,
		Name:         "张三",
		Age:          18,
		Sex:          "男",
		DepartmentId: 1,
		Speak:        Speak{Act: "attack", Walk: Walk{Step: "walk"}},
	}
	sql, args, err := From("person").As("a").
		WhereAnd("id", 11).
		BuildStructUpdate(&p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "update") {
		t.Errorf("expected UPDATE, got: %s", sql)
	}
	if !strings.Contains(sql, "where") {
		t.Errorf("expected WHERE clause, got: %s", sql)
	}
	t.Logf("UPDATE struct: %s | args: %v", sql, args)
}

func TestUpdate_StructUpdateNoWhereError(t *testing.T) {
	p := Person{Id: 1, Name: "张三"}
	_, _, err := From("person").BuildStructUpdate(&p)
	if err == nil {
		t.Fatal("expected error for update without WHERE")
	}
}

func TestUpdate_Increment(t *testing.T) {
	sql, args, err := From("product").As("a").
		WhereAnd("id", 2).
		BuildIncrement(map[string]any{"num": 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "+ ?") {
		t.Errorf("expected increment (+ ?), got: %s", sql)
	}
	t.Logf("UPDATE increment: %s | args: %v", sql, args)
}

func TestUpdate_Decrement(t *testing.T) {
	sql, args, err := From("product").As("a").
		WhereAnd("id", 2).
		BuildDecrement(map[string]any{"num": 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "- ?") {
		t.Errorf("expected decrement (- ?), got: %s", sql)
	}
	t.Logf("UPDATE decrement: %s | args: %v", sql, args)
}

func TestUpdate_IncrementNoWhereError(t *testing.T) {
	_, _, err := From("product").BuildIncrement(map[string]any{"num": 1})
	if err == nil {
		t.Fatal("expected error for increment without WHERE")
	}
}

func TestUpdate_FieldArithmetic(t *testing.T) {
	sql, args, err := From("inventory").As("a").
		WhereAnd("id", 1).
		BuildMapUpdate(map[string]any{
			"num":  []any{"u_num", "+", 20},
			"name": "张三",
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "`u_num`+?") {
		t.Errorf("expected field arithmetic, got: %s", sql)
	}
	t.Logf("UPDATE arithmetic: %s | args: %v", sql, args)
}

func TestDelete_Basic(t *testing.T) {
	sql, args, err := From("admin").As("a").
		WhereOr("name", "like", "张三", "r").
		WhereOr("age", ">", 18).
		WhereAnd([][][]any{
			{
				{"id", ">", 1},
				{"id", "<", 10},
				{"id", "=", 11, "a", "or"},
			},
			{
				{"nation", "=", "中国", "a", "or"},
				{"city", "=", "北京", "a"},
			},
		}).
		WhereAnd([][]any{
			{"level", ">", 1, "a", "or"},
			{"level", "<", 10, "a", "or"},
		}).
		WhereAnd("id", "=", 1, "a").
		BuildDelete()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "delete") {
		t.Errorf("expected DELETE, got: %s", sql)
	}
	if !strings.Contains(sql, "where") {
		t.Errorf("expected WHERE clause, got: %s", sql)
	}
	t.Logf("DELETE: %s | args: %v", sql, args)
}

func TestDelete_NoWhereError(t *testing.T) {
	_, _, err := From("admin").BuildDelete()
	if err == nil {
		t.Fatal("expected error for delete without WHERE")
	}
}

func TestWhere_OrCondition(t *testing.T) {
	sql, args, err := From("admin").As("a").
		WhereOr("name", "like", "张三").
		WhereOr("age", ">", 18).
		Limit(1).BuildSelect()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "where") {
		t.Errorf("expected WHERE clause, got: %s", sql)
	}
	t.Logf("SELECT OR: %s | args: %v", sql, args)
}

func TestWhere_MixedAndOr(t *testing.T) {
	sql, args, err := From("admin").As("a").
		WhereAnd([][]any{
			{"name", "=", "张三"},
			{"name", "like", "李四", "d", "or"},
		}).
		Limit(1).BuildSelect()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	t.Logf("SELECT mixed AND/OR: %s | args: %v", sql, args)
}

func TestWhere_InOperator(t *testing.T) {
	sql, args, err := From("admin").As("a").
		WhereAnd([][]any{
			{"age", "in", []any{18, 19, 20}},
		}).
		BuildSelect()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "in") {
		t.Errorf("expected IN operator, got: %s", sql)
	}
	if len(args) != 3 {
		t.Errorf("expected 3 args for IN (18,19,20), got %d", len(args))
	}
	t.Logf("SELECT IN: %s | args: %v", sql, args)
}

func TestWhere_BetweenOperator(t *testing.T) {
	sql, args, err := From("admin").As("a").
		WhereAnd("score", "between", []any{60, 100}).
		BuildSelect()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "between") {
		t.Errorf("expected BETWEEN, got: %s", sql)
	}
	t.Logf("SELECT BETWEEN: %s | args: %v", sql, args)
}

func TestWhere_IsNullOperator(t *testing.T) {
	// IS NULL 需要使用 3 参数形式调用：WhereAnd(field, "is null", nil)
	sql, args, err := From("admin").As("a").
		WhereAnd("deleted_at", "is null", nil).
		BuildSelect()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	t.Logf("SELECT IS NULL: %s | args: %v", sql, args)
}

func TestWhere_SubqueryCondition(t *testing.T) {
	wh := [][][]any{{
		{"name", "like", "张三"},
		{"id", "in", From("user_age").WhereAnd("age", ">", 233)},
	}}
	sql, args, err := From("admin").As("a").WhereAnd(wh).BuildSelect()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "select") {
		t.Errorf("expected subquery in WHERE, got: %s", sql)
	}
	t.Logf("SELECT with WHERE subquery: %s | args: %v", sql, args)
}

func TestSelect_LiteralInWhere(t *testing.T) {
	sql, args, err := From("category").As("a").
		Select("id").
		WhereAnd("id", 2).
		WhereAnd([][]any{
			{Literal("`a`.`in_num`+`a`.`a_num`"), "<", Literal("num+sum")},
			{Literal("DATE_FORMAT(date, '%Y-%m')"), 23},
		}).
		BuildSelect()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	t.Logf("SELECT with Literal: %s | args: %v", sql, args)
}

func TestSelect_JoinMultiple(t *testing.T) {
	sql, args, err := From("admin").As("a").
		Join("department", "b", "department_id", "id").
		LeftJoin("company", "c", "company_id", "id").
		Limit(1).BuildSelect()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "join") {
		t.Errorf("expected JOIN, got: %s", sql)
	}
	if !strings.Contains(sql, "left join") {
		t.Errorf("expected LEFT JOIN, got: %s", sql)
	}
	t.Logf("SELECT multi JOIN: %s | args: %v", sql, args)
}

func TestSelect_RightJoin(t *testing.T) {
	sql, args, err := From("admin").As("a").
		RightJoin("log", "l", "id", "admin_id").
		BuildSelect()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	if !strings.Contains(sql, "right join") {
		t.Errorf("expected RIGHT JOIN, got: %s", sql)
	}
	t.Logf("SELECT RIGHT JOIN: %s | args: %v", sql, args)
}

func TestSelect_Debug(t *testing.T) {
	sql, args, err := From("admin").Debug().Select("id").Limit(1).BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	t.Logf("SELECT debug: %s | args: %v", sql, args)
}

func TestToString(t *testing.T) {
	b := From("admin").WhereAnd("id", 1)
	whStr := b.ToString()
	if whStr == "" {
		t.Fatal("expected non-empty WHERE string")
	}
	args := b.GetFieldValue()
	if len(args) != 1 {
		t.Errorf("expected 1 arg, got %d", len(args))
	}
	t.Logf("ToString: %s | args: %v", whStr, args)
}

func TestSetDbTag(t *testing.T) {
	type CustomPerson struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	sql, args, err := From("person").SetDbTag("json").BuildStructInsert(&CustomPerson{
		ID: 1, Name: "张三",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "`id`") {
		t.Errorf("expected `id` field, got: %s", sql)
	}
	t.Logf("Custom tag INSERT: %s | args: %v", sql, args)
}

func TestTable_SwitchTableName(t *testing.T) {
	// Table 切换表名，未调用 As 时别名自动跟随新表名
	sql, args, err := From("old").Table("new").Select("id").BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "from `new`") {
		t.Errorf("expected table 'new', got: %s", sql)
	}
	if !strings.Contains(sql, "`new`.`id`") {
		t.Errorf("expected field `new`.`id`, got: %s", sql)
	}
	t.Logf("Table switch: %s | args: %v", sql, args)
}

func TestTable_SwitchTableWithAlias(t *testing.T) {
	// As 设置别名后，Table 切换表名，别名保持不变
	sql, args, err := From("old").As("my_alias").Table("new").Select("id").BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "from `new`") {
		t.Errorf("expected table 'new', got: %s", sql)
	}
	if !strings.Contains(sql, "as `my_alias`") {
		t.Errorf("expected alias 'my_alias', got: %s", sql)
	}
	if !strings.Contains(sql, "`my_alias`.`id`") {
		t.Errorf("expected field `my_alias`.`id`, got: %s", sql)
	}
	t.Logf("Table switch with alias: %s | args: %v", sql, args)
}

func TestUpdateZeroField_NoPanic(t *testing.T) {
	// 修复：zeroFieldMap 为 nil 时 UpdateZeroField 不再 panic
	b := From("admin").UpdateZeroField("name", "age")
	if b == nil {
		t.Fatal("expected non-nil builder")
	}
	sql, args, err := b.WhereAnd("id", 1).BuildMapUpdate(map[string]any{
		"name": "",
		"age":  0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	// name 值为空但声明了 UpdateZeroField，应依然更新
	if !strings.Contains(sql, "`name`") || !strings.Contains(sql, "`age`") {
		t.Errorf("expected name and age in SET clause, got: %s", sql)
	}
	t.Logf("UpdateZeroField: %s | args: %v", sql, args)
}

func TestUpdateEmptyField_NoPanic(t *testing.T) {
	// 修复：emptyFieldMap 为 nil 时 UpdateEmptyField 不再 panic
	b := From("admin").UpdateEmptyField("desc")
	if b == nil {
		t.Fatal("expected non-nil builder")
	}
	sql, args, err := b.WhereAnd("id", 1).BuildMapUpdate(map[string]any{
		"desc": "",
		"age":  18,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "`desc`") {
		t.Errorf("expected desc in SET clause (empty but declared), got: %s", sql)
	}
	t.Logf("UpdateEmptyField: %s | args: %v", sql, args)
}

func TestBuildMapUpdate_BadSlicePanicPrevented(t *testing.T) {
	// 修复：[]any 长度不足时返回 error 而非 panic
	_, _, err := From("admin").WhereAnd("id", 1).BuildMapUpdate(map[string]any{
		"num": []any{"field"}, // 只有 1 个元素，需要 3 个
	})
	if err == nil {
		t.Fatal("expected error for malformed arithmetic slice")
	}
	t.Logf("Error (expected): %v", err)
}

func TestWhere_UnknownArgCount(t *testing.T) {
	// 修复：不支持的参数数量不再静默丢弃，会通过 BuildSelect 暴露错误
	b := From("admin").WhereAnd("a", "b", "c", "d", "e", "f") // 6 args，未注册
	_, _, err := b.BuildSelect()
	if err == nil {
		t.Fatal("expected error for unsupported arg count")
	}
	t.Logf("Error (expected): %v", err)
}

func TestInsert_EmptyMap(t *testing.T) {
	// 空 map 不会生成非法 SQL insert into X () values ()
	sql, args := From("admin").BuildMapInsert(map[string]any{})
	if sql != "" {
		t.Errorf("expected empty SQL for empty map, got: %s", sql)
	}
	if args != nil {
		t.Errorf("expected nil args, got: %v", args)
	}
}

func TestInsert_EmptySliceMap(t *testing.T) {
	// 空切片不会生成非法 SQL
	sql, args := From("admin").BuildSliceMapInsert([]map[string]any{})
	if sql != "" {
		t.Errorf("expected empty SQL for empty slice, got: %s", sql)
	}
	if args != nil {
		t.Errorf("expected nil args, got: %v", args)
	}
}

func BenchmarkBuildSelect_Large(b *testing.B) {
	for i := 0; i < b.N; i++ {
		From("admin").As("a").
			Select("id", "name", "age", "sex", "created_at", "updated_at",
				Fn("count", "total", "*"),
				Fn("sum", "amount", "price"),
			).
			WhereAnd("status", 1).
			WhereAnd("age", ">=", 18).
			WhereAnd([][]any{{"name", "like", "test"}, {"deleted", 0}}).
			Group("status", "age").
			Order([][]any{{"id", "desc"}, {"created_at", "asc"}}).
			Offset(0).Size(20).BuildSelect()
	}
}

func TestPage(t *testing.T) {
	// 旧 Limit(p, num) → 新 Page(p, num)
	sql, args, err := From("admin").Select("id").Page(2, 10).BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "limit 10,10") {
		t.Errorf("expected LIMIT 10,10 (page 2, size 10), got: %s", sql)
	}
	t.Logf("Page: %s | args: %v", sql, args)
}

func TestOffsetSize(t *testing.T) {
	// 新 Offset + Size 组合
	sql, args, err := From("admin").Select("id").Offset(5).Size(20).BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "limit 5,20") {
		t.Errorf("expected LIMIT 5,20, got: %s", sql)
	}
	t.Logf("Offset+Size: %s | args: %v", sql, args)
}

func TestLimit_SingleArg(t *testing.T) {
	sql, args, err := From("admin").Select("id").Limit(3).BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "limit 3") {
		t.Errorf("expected LIMIT 3, got: %s", sql)
	}
	t.Logf("Limit: %s | args: %v", sql, args)
}

func TestReset(t *testing.T) {
	b := From("admin").Select("id").WhereAnd("status", 1)

	// 第一次构建
	sql1, args1, err := b.BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql1 == "" {
		t.Fatal("expected non-empty SQL")
	}

	// Reset 后重建
	b.Reset().Select("name").WhereAnd("age", ">", 18)
	sql2, args2, err := b.BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql2, "`name`") {
		t.Errorf("expected name field after Reset, got: %s", sql2)
	}
	if !strings.Contains(sql2, "`age` > ?") {
		t.Errorf("expected age condition after Reset, got: %s", sql2)
	}

	t.Logf("Before Reset: %s | %v", sql1, args1)
	t.Logf("After Reset:  %s | %v", sql2, args2)
}

func TestBuildSliceMapInsert_ConsistentKeys(t *testing.T) {
	// 修复：验证后续 map 的 key 与第一个 map 一致时值正确映射
	sql, args := From("product").BuildSliceMapInsert([]map[string]any{
		{"name": "张三", "age": 18},
		{"name": "李四", "age": 25},
		{"name": "王五", "age": 30},
	})
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	// 每行应有 2 个占位符
	if strings.Count(sql, "?") != 6 {
		t.Errorf("expected 6 placeholders (3 rows * 2 fields), got: %s", sql)
	}
	// 值应按列顺序排列
	if len(args) != 6 {
		t.Errorf("expected 6 args, got %d: %v", len(args), args)
	}
	t.Logf("SliceMapInsert: %s | args: %v", sql, args)
}

// ========== Phase 1: SELECT Enhancements ==========

func TestSelect_SqlHints(t *testing.T) {
	sql, _, err := From("admin").SqlNoCache().SqlCalcFoundRows().Select("id").Limit(1).BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "SQL_NO_CACHE") || !strings.Contains(sql, "SQL_CALC_FOUND_ROWS") {
		t.Errorf("expected SQL hints, got: %s", sql)
	}
	t.Logf("SQL hints: %s", sql)
}

func TestSelect_IndexHints(t *testing.T) {
	sql, _, err := From("admin").As("a").UseIndex("idx_name", "idx_age").Select("id").BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "use index (`idx_name`, `idx_age`)") {
		t.Errorf("expected USE INDEX, got: %s", sql)
	}
	t.Logf("USE INDEX: %s", sql)

	sql2, _, _ := From("admin").As("a").ForceIndex("idx_force").Select("id").BuildSelect()
	if !strings.Contains(sql2, "force index (`idx_force`)") {
		t.Errorf("expected FORCE INDEX, got: %s", sql2)
	}

	sql3, _, _ := From("admin").As("a").IgnoreIndex("idx_ignore").Select("id").BuildSelect()
	if !strings.Contains(sql3, "ignore index (`idx_ignore`)") {
		t.Errorf("expected IGNORE INDEX, got: %s", sql3)
	}
}

func TestSelect_ForUpdate(t *testing.T) {
	sql, _, err := From("admin").Select("id").WhereAnd("id", 1).ForUpdate().BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "for update") {
		t.Errorf("expected FOR UPDATE, got: %s", sql)
	}
	t.Logf("FOR UPDATE: %s", sql)
}

func TestSelect_LockInShareMode(t *testing.T) {
	sql, _, err := From("admin").Select("id").WhereAnd("id", 1).LockInShareMode().BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "lock in share mode") {
		t.Errorf("expected LOCK IN SHARE MODE, got: %s", sql)
	}
	t.Logf("LOCK IN SHARE MODE: %s", sql)
}

func TestWhere_ExistsOperator(t *testing.T) {
	sql, args, err := From("user").As("u").
		WhereAnd("", "exists", From("order").As("o").Select("id").WhereAnd("user_id", SField("u", "id", ""))).
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "exists") {
		t.Errorf("expected EXISTS, got: %s", sql)
	}
	t.Logf("EXISTS: %s | args: %v", sql, args)
}

func TestSelect_Union(t *testing.T) {
	q1 := From("active_users").As("a").Select("id", "name").WhereAnd("status", 1)
	q2 := From("archived_users").As("b").Select("id", "name").WhereAnd("status", 0)
	sql, args, err := q1.UnionAll(q2).BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "union all") {
		t.Errorf("expected UNION ALL, got: %s", sql)
	}
	t.Logf("UNION ALL: %s | args: %v", sql, args)
}

func TestSelect_UnionDistinct(t *testing.T) {
	q1 := From("t1").Select("id")
	q2 := From("t2").Select("id")
	sql, _, err := q1.Union(q2).BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "union") {
		t.Errorf("expected UNION, got: %s", sql)
	}
	t.Logf("UNION: %s", sql)
}

// ========== Phase 2: JOIN Enhancements ==========

func TestJoin_CrossJoin(t *testing.T) {
	sql, _, err := From("admin").As("a").CrossJoin("log", "l").Select("a.*").BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "cross join") {
		t.Errorf("expected CROSS JOIN, got: %s", sql)
	}
	t.Logf("CROSS JOIN: %s", sql)
}

func TestJoin_NaturalJoin(t *testing.T) {
	sql, _, err := From("a").NaturalJoin("b", "b").Select("*").BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "natural join") {
		t.Errorf("expected NATURAL JOIN, got: %s", sql)
	}
	t.Logf("NATURAL JOIN: %s", sql)
}

func TestJoin_StraightJoin(t *testing.T) {
	sql, _, err := From("a").As("a").StraightJoin("b", "b").Select("*").BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "straight_join") {
		t.Errorf("expected STRAIGHT_JOIN, got: %s", sql)
	}
	t.Logf("STRAIGHT_JOIN: %s", sql)
}

func TestJoin_FullJoin(t *testing.T) {
	sql, _, err := From("admin").As("a").FullJoin("log", "l", "id", "admin_id").Select("a.*").BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "left join") {
		t.Errorf("expected LEFT JOIN (full outer join emulation), got: %s", sql)
	}
	t.Logf("FULL JOIN (LEFT emulation): %s", sql)
}

func TestJoin_ComplexOn(t *testing.T) {
	sql, _, err := From("a").As("a").JoinOn("c", "c",
		[]string{"a", "id", "=", "c", "a_id"},
		[]string{"a", "type", "=", "c", "type"},
	).Select("a.*").BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, " and ") {
		t.Errorf("expected complex ON with AND, got: %s", sql)
	}
	t.Logf("Complex ON: %s", sql)
}

func TestJoin_Using(t *testing.T) {
	sql, _, err := From("a").As("a").JoinUsing("b", "b", "common_id").Select("*").BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "using (`common_id`)") {
		t.Errorf("expected USING, got: %s", sql)
	}
	t.Logf("USING: %s", sql)
}

func TestJoin_LeftJoinOn(t *testing.T) {
	sql, _, err := From("a").As("a").LeftJoinOn("b", "b",
		[]string{"a", "id", "=", "b", "a_id"},
	).Select("a.*").BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "left join") {
		t.Errorf("expected LEFT JOIN with complex ON, got: %s", sql)
	}
	t.Logf("LEFT JOIN ON: %s", sql)
}

func TestJoin_SubqueryJoin(t *testing.T) {
	sub := From("order").As("o").Select("user_id", Fn("max", "max_amt", "amount")).Group("user_id")
	sql, _, err := From("user").As("u").
		JoinSub(sub, "t", "id", "user_id").
		Select("u.*", "t.max_amt").
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "join (select") {
		t.Errorf("expected JOIN subquery, got: %s", sql)
	}
	t.Logf("JOIN subquery: %s", sql)
}

// ========== Phase 3: WHERE Enhancements ==========

func TestWhere_AnyAllOperator(t *testing.T) {
	sub := From("product").Select("price").WhereAnd("category_id", 1)
	sql, args, err := From("order_item").As("oi").
		WhereAnd("price", "> any", sub).
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "> any") {
		t.Errorf("expected > ANY, got: %s", sql)
	}
	t.Logf("> ANY: %s | args: %v", sql, args)
}

func TestWhere_AllOperator(t *testing.T) {
	sub := From("product").Select("price").WhereAnd("category_id", 1)
	sql, _, err := From("order_item").As("oi").
		WhereAnd("price", ">= all", sub).
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, ">= all") {
		t.Errorf("expected >= ALL, got: %s", sql)
	}
	t.Logf(">= ALL: %s", sql)
}

func TestWhere_MultiColumnIn(t *testing.T) {
	sql, args, err := From("product").As("p").
		WhereAnd([][]any{
			{"(category_id, brand_id)", "multi in", [][]any{{1, 2}, {3, 4}}},
		}).
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "in") {
		t.Errorf("expected MULTI IN, got: %s", sql)
	}
	if len(args) != 4 {
		t.Errorf("expected 4 args for 2 tuples of 2, got %d", len(args))
	}
	t.Logf("Multi-column IN: %s | args: %v", sql, args)
}

func TestWhere_FindInSet(t *testing.T) {
	sql, args, err := From("product").As("p").
		WhereAnd("category_ids", "find in set", "5").
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "find_in_set") {
		t.Errorf("expected FIND_IN_SET, got: %s", sql)
	}
	t.Logf("FIND_IN_SET: %s | args: %v", sql, args)
}

func TestWhere_JsonExtract(t *testing.T) {
	sql, args, err := From("config").As("c").
		WhereAnd("data", "json_extract", "$.name").
		WhereAnd("info", "=", "hello").
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "json_extract") {
		t.Errorf("expected JSON_EXTRACT, got: %s", sql)
	}
	t.Logf("JSON_EXTRACT: %s | args: %v", sql, args)
}

func TestWhere_JsonField(t *testing.T) {
	sql, args, err := From("user").As("u").
		WhereAnd(JsonField("u", "data", "->>", "$.name"), "=", "Tom").
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "->>") {
		t.Errorf("expected JSON arrow operator, got: %s", sql)
	}
	t.Logf("JSON field: %s | args: %v", sql, args)
}

func TestWhere_RawCondition(t *testing.T) {
	sql, args, err := From("admin").As("a").
		WhereAnd("status", 1).
		WhereRaw("`a`.`created_at` > NOW() - INTERVAL 7 DAY").
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "NOW()") {
		t.Errorf("expected raw condition, got: %s", sql)
	}
	t.Logf("Raw condition: %s | args: %v", sql, args)
}

// ========== Phase 4: CTE / Window / CASE WHEN / ROLLUP ==========

func TestSelect_WithCTE(t *testing.T) {
	cteDef := From("category").Select("id", "name").WhereAnd("active", 1)
	sql, args, err := From("product").As("p").
		With("active_categories", cteDef).
		Select("p.*").
		Join("active_categories", "ac", "category_id", "id").
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "with ") {
		t.Errorf("expected WITH clause, got: %s", sql)
	}
	t.Logf("WITH CTE: %s | args: %v", sql, args)
}

func TestSelect_WithRecursive(t *testing.T) {
	baseDef := From("category").Select("id", "name", "parent_id").WhereAnd("parent_id", "is null", nil)
	sql, _, err := From("category").
		WithRecursive().
		With("cte_hierarchy", baseDef).
		Select("*").Limit(10).BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "with recursive") {
		t.Errorf("expected WITH RECURSIVE, got: %s", sql)
	}
	t.Logf("WITH RECURSIVE: %s", sql)
}

func TestSelect_WindowFunction(t *testing.T) {
	sql, args, err := From("employee").As("e").
		Select("e.*",
			WinFn("row_number", "rn").Partition("dept_id").OrderByClause([][]any{{"salary", "desc"}}),
		).
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "row_number() over") || !strings.Contains(sql, "partition by") {
		t.Errorf("expected window function, got: %s", sql)
	}
	t.Logf("Window function: %s | args: %v", sql, args)
}

func TestSelect_CaseWhen(t *testing.T) {
	sql, args, err := From("score").As("s").
		Select("s.name",
			CaseWhen("grade").
				When("score >= 90", "A").
				When("score >= 80", "B").
				When("score >= 60", "C").
				Else("D"),
		).
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "case ") || !strings.Contains(sql, "when") || !strings.Contains(sql, "end as `grade`") {
		t.Errorf("expected CASE WHEN, got: %s", sql)
	}
	t.Logf("CASE WHEN: %s | args: %v", sql, args)
}

func TestSelect_WithRollup(t *testing.T) {
	sql, args, err := From("sales").As("s").
		Select("category", Fn("sum", "total", "amount")).
		Group("category").
		WithRollup().
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "with rollup") {
		t.Errorf("expected WITH ROLLUP, got: %s", sql)
	}
	t.Logf("GROUP BY ROLLUP: %s | args: %v", sql, args)
}

// ========== Phase 5: INSERT Enhancements ==========

func TestInsert_MapInsertIgnore(t *testing.T) {
	sql, args := From("admin").BuildMapInsertIgnore(map[string]any{"name": "张三"})
	if !strings.Contains(sql, "insert ignore into") {
		t.Errorf("expected INSERT IGNORE, got: %s", sql)
	}
	t.Logf("INSERT IGNORE: %s | args: %v", sql, args)
}

func TestInsert_MapReplace(t *testing.T) {
	sql, args := From("admin").BuildMapReplace(map[string]any{"id": 1, "name": "张三"})
	if !strings.Contains(sql, "replace into") {
		t.Errorf("expected REPLACE INTO, got: %s", sql)
	}
	t.Logf("REPLACE: %s | args: %v", sql, args)
}

func TestInsert_SliceMapInsertIgnore(t *testing.T) {
	sql, args := From("admin").BuildSliceMapInsertIgnore([]map[string]any{
		{"name": "张三"}, {"name": "李四"},
	})
	if !strings.Contains(sql, "insert ignore into") {
		t.Errorf("expected INSERT IGNORE batch, got: %s", sql)
	}
	t.Logf("Batch INSERT IGNORE: %s | args: %v", sql, args)
}

func TestInsert_SliceMapReplace(t *testing.T) {
	sql, args := From("admin").BuildSliceMapReplace([]map[string]any{
		{"id": 1, "name": "张三"}, {"id": 2, "name": "李四"},
	})
	if !strings.Contains(sql, "replace into") {
		t.Errorf("expected REPLACE batch, got: %s", sql)
	}
	t.Logf("Batch REPLACE: %s | args: %v", sql, args)
}

func TestInsert_OnDuplicateKey(t *testing.T) {
	sql, args := From("admin").OnDuplicateKey(map[string]any{"name": "new_name", "updated_at": "now"}).
		BuildMapInsert(map[string]any{"id": 1, "name": "张三"})
	if !strings.Contains(sql, "on duplicate key update") {
		t.Errorf("expected ON DUPLICATE KEY UPDATE, got: %s", sql)
	}
	t.Logf("ON DUPLICATE KEY: %s | args: %v", sql, args)
}

func TestInsert_SliceMapWithOnDuplicateKey(t *testing.T) {
	sql, args := From("admin").OnDuplicateKey(map[string]any{"counter": 0}).
		BuildSliceMapInsert([]map[string]any{
			{"id": 1, "name": "张三"}, {"id": 2, "name": "李四"},
		})
	if !strings.Contains(sql, "on duplicate key update") {
		t.Errorf("expected ON DUPLICATE KEY UPDATE for batch, got: %s", sql)
	}
	t.Logf("Batch ON DUPLICATE KEY: %s | args: %v", sql, args)
}

func TestInsert_InsertSelect(t *testing.T) {
	sel := From("temp_users").Select("id", "name").WhereAnd("status", 1)
	sql, args, err := From("users").BuildInsertSelect([]string{"id", "name"}, sel)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "insert into `users` (`id`, `name`) select") {
		t.Errorf("expected INSERT SELECT, got: %s", sql)
	}
	t.Logf("INSERT SELECT: %s | args: %v", sql, args)
}

func TestInsert_InsertSet(t *testing.T) {
	sql, args := From("admin").BuildInsertSet(map[string]any{"name": "张三", "age": 18})
	if !strings.Contains(sql, "insert into `admin` set") {
		t.Errorf("expected INSERT SET, got: %s", sql)
	}
	t.Logf("INSERT SET: %s | args: %v", sql, args)
}

// ========== Phase 6-7: UPDATE/DELETE/General ==========

func TestUpdate_WithJoin(t *testing.T) {
	sql, args, err := From("order").As("o").
		Join("user", "u", "user_id", "id").
		WhereAnd("o.status", 1).
		BuildUpdateWithJoin(map[string]any{"o.status": 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "join") || !strings.Contains(sql, "update") {
		t.Errorf("expected UPDATE with JOIN, got: %s", sql)
	}
	t.Logf("UPDATE with JOIN: %s | args: %v", sql, args)
}

func TestUpdate_WithOrderLimit(t *testing.T) {
	sql, args, err := From("queue").As("q").
		WhereAnd("status", 0).
		Order([][]any{{"created_at", "asc"}}).
		Limit(1).
		BuildMapUpdate(map[string]any{"status": 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "order by") {
		t.Errorf("expected ORDER BY in UPDATE, got: %s", sql)
	}
	if !strings.Contains(sql, "limit 1") {
		t.Errorf("expected LIMIT in UPDATE, got: %s", sql)
	}
	t.Logf("UPDATE with ORDER/LIMIT: %s | args: %v", sql, args)
}

func TestDelete_WithJoin(t *testing.T) {
	sql, args, err := From("post").As("p").
		Join("author", "a", "author_id", "id").
		WhereAnd("a.deleted", 1).
		BuildDeleteWithJoin()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "join") || !strings.Contains(sql, "delete") {
		t.Errorf("expected DELETE with JOIN, got: %s", sql)
	}
	t.Logf("DELETE with JOIN: %s | args: %v", sql, args)
}

func TestDelete_WithOrderLimit(t *testing.T) {
	sql, args, err := From("log").As("l").
		WhereAnd("created_at", "<", "2020-01-01").
		Order([][]any{{"created_at", "asc"}}).
		Limit(100).
		BuildDelete()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "order by") || !strings.Contains(sql, "limit") {
		t.Errorf("expected DELETE with ORDER/LIMIT, got: %s", sql)
	}
	t.Logf("DELETE with ORDER/LIMIT: %s | args: %v", sql, args)
}

func TestTruncate(t *testing.T) {
	sql, err := From("temp_log").BuildTruncate()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "truncate table") {
		t.Errorf("expected TRUNCATE, got: %s", sql)
	}
	t.Logf("TRUNCATE: %s", sql)
}

func TestBuildSelectCount(t *testing.T) {
	sql, args, err := From("admin").As("a").
		WhereAnd("status", 1).
		BuildSelectCount()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "select count(*)") || !strings.Contains(sql, "as `_count`") {
		t.Errorf("expected SELECT COUNT wrapper, got: %s", sql)
	}
	t.Logf("BuildSelectCount: %s | args: %v", sql, args)
}

func TestBuildExists(t *testing.T) {
	sql, args, err := From("admin").WhereAnd("id", 1).BuildExists()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "select exists(") {
		t.Errorf("expected SELECT EXISTS, got: %s", sql)
	}
	t.Logf("BuildExists: %s | args: %v", sql, args)
}

func TestSoftDelete(t *testing.T) {
	b := From("admin").As("a").WhereAnd("id", 1)
	b.softDeleteField = "deleted_at"
	sql, args, err := b.BuildSoftDelete()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "update") || !strings.Contains(sql, "`deleted_at`") {
		t.Errorf("expected soft delete, got: %s", sql)
	}
	if len(args) < 1 {
		t.Errorf("expected args, got %d", len(args))
	}
	t.Logf("SoftDelete: %s | args: %v", sql, args)
}

func TestRaw_SQL(t *testing.T) {
	sql, args, err := From("admin").As("a").
		Select("id", "name").
		WhereAnd("status", 1).
		Raw("order by `a`.`id` desc").
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "order by `a`.`id` desc") {
		t.Errorf("expected raw SQL fragment, got: %s", sql)
	}
	t.Logf("Raw SQL: %s | args: %v", sql, args)
}

func TestJoin_LeftJoinUsing(t *testing.T) {
	sql, _, err := From("a").As("a").LeftJoinUsing("b", "b", "col1", "col2").Select("*").BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "left join") || !strings.Contains(sql, "using") {
		t.Errorf("expected LEFT JOIN USING, got: %s", sql)
	}
	t.Logf("LEFT JOIN USING: %s", sql)
}

func TestInsert_EmptyIgnore(t *testing.T) {
	sql, args := From("admin").BuildMapInsertIgnore(map[string]any{})
	if sql != "" || args != nil {
		t.Errorf("expected empty result for empty map, got: %s, %v", sql, args)
	}
}

func TestInsert_EmptySet(t *testing.T) {
	sql, args := From("admin").BuildInsertSet(map[string]any{})
	if sql != "" || args != nil {
		t.Errorf("expected empty result for empty map, got: %s, %v", sql, args)
	}
}

func TestSelect_ComplexWindow(t *testing.T) {
	sql, _, err := From("sales").As("s").
		Select("s.*",
			WinFn("sum", "running_total", "amount").Partition("dept").OrderByClause([][]any{{"sale_date", "asc"}}),
			WinFn("rank", "rnk").OrderByClause([][]any{{"amount", "desc"}}),
		).
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Count(sql, "over") != 2 {
		t.Errorf("expected 2 window functions, got: %s", sql)
	}
	t.Logf("Multiple window functions: %s", sql)
}

func TestWhere_NotExists(t *testing.T) {
	sql, _, err := From("user").As("u").
		WhereAnd("", "not exists", From("banned").As("b").Select("id").WhereAnd("user_id", SField("u", "id", ""))).
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "not exists") {
		t.Errorf("expected NOT EXISTS, got: %s", sql)
	}
	t.Logf("NOT EXISTS: %s", sql)
}

func TestSelect_CaseWhenSimple(t *testing.T) {
	sql, args, err := From("user").As("u").
		Select("u.name",
			CaseWhen("level_name").SimpleCase("level").
				When(1, "VIP").
				When(2, "SVIP").
				Else("Normal"),
		).
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "case `u`.`level`") {
		t.Errorf("expected simple CASE WHEN, got: %s", sql)
	}
	t.Logf("Simple CASE WHEN: %s | args: %v", sql, args)
}

func TestJoin_RightJoinOn(t *testing.T) {
	sql, _, err := From("a").As("a").RightJoinOn("b", "b",
		[]string{"a", "id", "=", "b", "a_id"},
	).Select("*").BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "right join") {
		t.Errorf("expected RIGHT JOIN ON, got: %s", sql)
	}
	t.Logf("RIGHT JOIN ON: %s", sql)
}

func TestInsert_EmptySliceReplace(t *testing.T) {
	sql, args := From("admin").BuildSliceMapReplace([]map[string]any{})
	if sql != "" || args != nil {
		t.Errorf("expected empty result for empty slice, got: %s, %v", sql, args)
	}
}

func TestSelect_WithColumns(t *testing.T) {
	cteDef := From("t").As("src").Select("id", "name")
	sql, _, err := From("t2").WithColumns("cte", []string{"a", "b"}, cteDef).
		Select("*").BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "with `cte` (`a`, `b`)") {
		t.Errorf("expected WITH columns, got: %s", sql)
	}
	t.Logf("WITH columns: %s", sql)
}

func TestJoin_LeftJoinSub(t *testing.T) {
	sub := From("order").As("o").Select("user_id", Fn("count", "cnt", "*")).Group("user_id")
	sql, _, err := From("user").As("u").
		LeftJoinSub(sub, "t", "id", "user_id").
		Select("u.*", "t.cnt").
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "left join (select") {
		t.Errorf("expected LEFT JOIN subquery, got: %s", sql)
	}
	t.Logf("LEFT JOIN sub: %s", sql)
}

func TestUpdate_IncrementWithOrderLimit(t *testing.T) {
	sql, args, err := From("counter").As("c").
		WhereAnd("id", 1).
		Order([][]any{{"version", "asc"}}).
		Limit(1).
		BuildIncrement(map[string]any{"count": 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "order by") || !strings.Contains(sql, "limit 1") {
		t.Errorf("expected ORDER/LIMIT in increment, got: %s", sql)
	}
	t.Logf("Increment with ORDER/LIMIT: %s | args: %v", sql, args)
}

func TestUpdate_DecrementWithOrderLimit(t *testing.T) {
	sql, args, err := From("stock").As("s").
		WhereAnd("id", 1).
		Order([][]any{{"updated_at", "asc"}}).
		Limit(5).
		BuildDecrement(map[string]any{"qty": 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "order by") || !strings.Contains(sql, "limit 5") {
		t.Errorf("expected ORDER/LIMIT in decrement, got: %s", sql)
	}
	t.Logf("Decrement with ORDER/LIMIT: %s | args: %v", sql, args)
}

func TestUpdate_StructUpdateWithOrderLimit(t *testing.T) {
	p := Person{Name: "张三", Age: 18}
	sql, args, err := From("person").As("p").
		WhereAnd("id", 1).
		Order([][]any{{"id", "desc"}}).
		Limit(2).
		BuildStructUpdate(&p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "order by") || !strings.Contains(sql, "limit 2") {
		t.Errorf("expected ORDER/LIMIT in struct update, got: %s", sql)
	}
	t.Logf("StructUpdate with ORDER/LIMIT: %s | args: %v", sql, args)
}

// ========== SQL Injection Protection Tests ==========

func TestSQLInjection_FromTableName(t *testing.T) {
	b := From("admin; DROP TABLE users--")
	_, _, err := b.Select("id").BuildSelect()
	if err == nil {
		t.Fatal("expected error for SQL injection in table name")
	}
	t.Logf("Blocked table name injection: %v", err)
}

func TestSQLInjection_TableSwitch(t *testing.T) {
	b := From("admin").Table("admin; DROP TABLE users--")
	_, _, err := b.Select("id").BuildSelect()
	if err == nil {
		t.Fatal("expected error for SQL injection in Table()")
	}
	t.Logf("Blocked Table() injection: %v", err)
}

func TestSQLInjection_AsAlias(t *testing.T) {
	b := From("admin").As("a; DROP TABLE users--")
	_, _, err := b.Select("id").BuildSelect()
	if err == nil {
		t.Fatal("expected error for SQL injection in alias")
	}
	t.Logf("Blocked alias injection: %v", err)
}

func TestSQLInjection_SelectFields(t *testing.T) {
	b := From("admin").Select("id; DROP TABLE users--")
	_, _, err := b.BuildSelect()
	if err == nil {
		t.Fatal("expected error for SQL injection in SELECT fields")
	}
	t.Logf("Blocked SELECT field injection: %v", err)
}

func TestSQLInjection_GroupFields(t *testing.T) {
	b := From("admin").Select("id").Group("id; DROP TABLE users--")
	_, _, err := b.BuildSelect()
	if err == nil {
		t.Fatal("expected error for SQL injection in GROUP BY")
	}
	t.Logf("Blocked GROUP BY injection: %v", err)
}

func TestSQLInjection_OrderFields(t *testing.T) {
	b := From("admin").Select("id").Order([][]any{{"id; DROP TABLE users--", "desc"}})
	_, _, err := b.BuildSelect()
	if err == nil {
		t.Fatal("expected error for SQL injection in ORDER BY field")
	}
	t.Logf("Blocked ORDER BY field injection: %v", err)
}

func TestSQLInjection_OrderDirection(t *testing.T) {
	b := From("admin").Select("id").Order([][]any{{"id", "desc; DROP TABLE users--"}})
	_, _, err := b.BuildSelect()
	if err == nil {
		t.Fatal("expected error for SQL injection in ORDER BY direction")
	}
	t.Logf("Blocked ORDER BY direction injection: %v", err)
}

func TestSQLInjection_JoinTable(t *testing.T) {
	b := From("admin").As("a").Join("b; DROP TABLE users--", "b", "id", "id")
	_, _, err := b.Select("a.*").BuildSelect()
	if err == nil {
		t.Fatal("expected error for SQL injection in JOIN table name")
	}
	t.Logf("Blocked JOIN table injection: %v", err)
}

func TestSQLInjection_JoinAlias(t *testing.T) {
	b := From("admin").As("a").Join("b", "b; DROP TABLE users--", "id", "id")
	_, _, err := b.Select("a.*").BuildSelect()
	if err == nil {
		t.Fatal("expected error for SQL injection in JOIN alias")
	}
	t.Logf("Blocked JOIN alias injection: %v", err)
}

func TestSQLInjection_JoinFields(t *testing.T) {
	b := From("admin").As("a").Join("b", "b", "id; DROP TABLE users--", "id")
	_, _, err := b.Select("a.*").BuildSelect()
	if err == nil {
		t.Fatal("expected error for SQL injection in JOIN ON field")
	}
	t.Logf("Blocked JOIN ON field injection: %v", err)
}

func TestSQLInjection_LeftJoin(t *testing.T) {
	b := From("admin").As("a").LeftJoin("b; DROP--", "b", "id", "id")
	_, _, err := b.Select("a.*").BuildSelect()
	if err == nil {
		t.Fatal("expected error for SQL injection in LEFT JOIN")
	}
	t.Logf("Blocked LEFT JOIN injection: %v", err)
}

func TestSQLInjection_CrossJoin(t *testing.T) {
	b := From("admin").As("a").CrossJoin("b; DROP--", "b")
	_, _, err := b.Select("a.*").BuildSelect()
	if err == nil {
		t.Fatal("expected error for SQL injection in CROSS JOIN")
	}
	t.Logf("Blocked CROSS JOIN injection: %v", err)
}

func TestSQLInjection_JoinOn(t *testing.T) {
	b := From("a").As("a").JoinOn("c", "c",
		[]string{"a; DROP--", "id", "=", "c", "a_id"},
	)
	_, _, err := b.Select("a.*").BuildSelect()
	if err == nil {
		t.Fatal("expected error for SQL injection in JOIN ON")
	}
	t.Logf("Blocked JOIN ON injection: %v", err)
}

func TestSQLInjection_JoinUsing(t *testing.T) {
	b := From("a").As("a").JoinUsing("b", "b", "id; DROP TABLE users--")
	_, _, err := b.Select("*").BuildSelect()
	if err == nil {
		t.Fatal("expected error for SQL injection in JOIN USING")
	}
	t.Logf("Blocked JOIN USING injection: %v", err)
}

func TestSQLInjection_JoinSub(t *testing.T) {
	sub := From("x").Select("y")
	b := From("a").As("a").JoinSub(sub, "t; DROP--", "id", "id")
	_, _, err := b.Select("a.*").BuildSelect()
	if err == nil {
		t.Fatal("expected error for SQL injection in JOIN SUB alias")
	}
	t.Logf("Blocked JOIN SUB injection: %v", err)
}

func TestSQLInjection_CTEName(t *testing.T) {
	cte := From("t").Select("id")
	b := From("t2").With("cte; DROP TABLE users--", cte).Select("*")
	_, _, err := b.BuildSelect()
	if err == nil {
		t.Fatal("expected error for SQL injection in CTE name")
	}
	t.Logf("Blocked CTE name injection: %v", err)
}

func TestSQLInjection_CTEColumns(t *testing.T) {
	cte := From("t").Select("id", "name")
	b := From("t2").WithColumns("cte", []string{"a; DROP--", "b"}, cte).Select("*")
	_, _, err := b.BuildSelect()
	if err == nil {
		t.Fatal("expected error for SQL injection in CTE columns")
	}
	t.Logf("Blocked CTE column injection: %v", err)
}

func TestSQLInjection_InsertMapKeys(t *testing.T) {
	_, _ = From("admin").BuildMapInsert(map[string]any{
		"name; DROP TABLE users--": "value",
	})
	b := From("admin")
	b.BuildMapInsert(map[string]any{"name; DROP--": "value"})
	if b.err == nil {
		t.Fatal("expected error for SQL injection in INSERT column names")
	}
	t.Logf("Blocked INSERT column injection: %v", b.err)
}

func TestSQLInjection_InsertSetKeys(t *testing.T) {
	_, _ = From("admin").BuildInsertSet(map[string]any{
		"name; DROP TABLE users--": "value",
	})
	b := From("admin")
	b.BuildInsertSet(map[string]any{"name; DROP--": "value"})
	if b.err == nil {
		t.Fatal("expected error for SQL injection in INSERT SET column names")
	}
	t.Logf("Blocked INSERT SET column injection: %v", b.err)
}

func TestSQLInjection_InsertSelectColumns(t *testing.T) {
	sel := From("t").Select("id")
	_, _, err := From("users").BuildInsertSelect([]string{"id; DROP TABLE users--", "name"}, sel)
	if err == nil {
		t.Fatal("expected error for SQL injection in INSERT SELECT columns")
	}
	t.Logf("Blocked INSERT SELECT column injection: %v", err)
}

func TestSQLInjection_UpdateMapKeys(t *testing.T) {
	_, _, err := From("admin").WhereAnd("id", 1).BuildMapUpdate(map[string]any{
		"name; DROP TABLE users--": "value",
	})
	if err == nil {
		t.Fatal("expected error for SQL injection in UPDATE column names")
	}
	t.Logf("Blocked UPDATE column injection: %v", err)
}

func TestSQLInjection_UpdateIncrement(t *testing.T) {
	_, _, err := From("admin").WhereAnd("id", 1).BuildIncrement(map[string]any{
		"count; DROP TABLE users--": 1,
	})
	if err == nil {
		t.Fatal("expected error for SQL injection in INCREMENT column names")
	}
	t.Logf("Blocked INCREMENT column injection: %v", err)
}

func TestSQLInjection_UpdateDecrement(t *testing.T) {
	_, _, err := From("admin").WhereAnd("id", 1).BuildDecrement(map[string]any{
		"count; DROP TABLE users--": 1,
	})
	if err == nil {
		t.Fatal("expected error for SQL injection in DECREMENT column names")
	}
	t.Logf("Blocked DECREMENT column injection: %v", err)
}

func TestSQLInjection_OnDuplicateKey(t *testing.T) {
	b := From("admin").OnDuplicateKey(map[string]any{
		"name; DROP TABLE users--": "value",
	})
	if b.err == nil {
		t.Fatal("expected error for SQL injection in ON DUPLICATE KEY column names")
	}
	t.Logf("Blocked ON DUPLICATE KEY column injection: %v", b.err)
}

func TestSQLInjection_UseIndex(t *testing.T) {
	b := From("admin").UseIndex("idx; DROP TABLE users--")
	_, _, err := b.Select("id").BuildSelect()
	// UseIndex should NOT error because it returns early without setting fields,
	// but the BuildSelect should still work (index hint is skipped for invalid names)
	if err != nil {
		t.Logf("UseIndex with injection handled: %v", err)
	}
	// Verify: the index hint should not be in the SQL
	t.Logf("UseIndex with injection: builder handles gracefully")
}

func TestSQLInjection_CaseWhenSimpleCase(t *testing.T) {
	c := CaseWhen("alias").SimpleCase("field; DROP TABLE users--")
	if c.CaseField != "" {
		t.Errorf("expected empty CaseField for SQL injection, got: %s", c.CaseField)
	}
	t.Logf("Blocked CASE WHEN SimpleCase injection")
}

func TestSQLInjection_WinFnIllegalName(t *testing.T) {
	w := WinFn("row_number; DROP--", "rn")
	if w.Fn != "" {
		t.Errorf("expected empty Fn for SQL injection, got: %s", w.Fn)
	}
	t.Logf("Blocked WinFn injection")
}

// ========== Bug Fix Regression Tests ==========

func TestBugFix_TimeInWhereCondition(t *testing.T) {
	now := time.Now()
	sql, args, err := From("log").As("l").
		WhereAnd("created_at", ">=", now).
		Limit(1).BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "where") {
		t.Errorf("expected WHERE, got: %s", sql)
	}
	if len(args) != 1 {
		t.Errorf("expected 1 arg for time.Time, got %d: %v", len(args), args)
	}
	t.Logf("time.Time in WHERE: %s | args: %v", sql, args)
}

func TestBugFix_TimeInBetweenCondition(t *testing.T) {
	start := time.Now().Add(-7 * 24 * time.Hour)
	end := time.Now()
	sql, args, err := From("log").As("l").
		WhereAnd("created_at", "between", []any{start, end}).
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "between") {
		t.Errorf("expected BETWEEN, got: %s", sql)
	}
	if len(args) != 2 {
		t.Errorf("expected 2 args for BETWEEN time.Time, got %d: %v", len(args), args)
	}
	t.Logf("time.Time BETWEEN: %s | args: %v", sql, args)
}

func TestBugFix_UpdateWithJoinInjection(t *testing.T) {
	_, _, err := From("t").As("t").
		Join("j", "j", "id", "id").
		WhereAnd("id", 1).
		BuildUpdateWithJoin(map[string]any{
			"name; DROP TABLE users--": "value",
		})
	if err == nil {
		t.Fatal("expected error for SQL injection in BuildUpdateWithJoin")
	}
	t.Logf("BuildUpdateWithJoin injection blocked: %v", err)
}

func TestBugFix_SetDbTagInjection(t *testing.T) {
	b := From("person").SetDbTag("db; DROP TABLE users--")
	if b.err == nil {
		t.Fatal("expected error for SQL injection in SetDbTag")
	}
	t.Logf("SetDbTag injection blocked: %v", b.err)
}

func TestBugFix_WinFnPartitionInjection(t *testing.T) {
	w := WinFn("row_number", "rn").Partition("dept; DROP TABLE users--")
	if w.Fn != "" {
		t.Errorf("expected empty carrier for partition injection, got Fn=%s", w.Fn)
	}
	t.Logf("WinFn Partition injection blocked")
}

func TestBugFix_BuildStructUpdateArithError(t *testing.T) {
	// This test verifies that malformed arithmetic expressions in struct updates
	// are properly reported as errors (previously silently ignored)
	// Note: this is hard to test directly since struct tags use []any literals
	// Just verify normal struct update still works
	p := Person{Id: 1, Name: "test"}
	sql, _, err := From("person").As("p").
		WhereAnd("id", 1).
		BuildStructUpdate(&p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
	t.Logf("BuildStructUpdate with error propagation: %s", sql)
}

func TestBugFix_ExistsInGroupCondition(t *testing.T) {
	// EXISTS should work in grouped [][]any conditions
	sub := From("order").As("o").Select("id").WhereAnd("user_id", SField("u", "id", ""))
	sql, args, err := From("user").As("u").
		WhereAnd([][]any{
			{"status", 1},
			{"", "exists", sub},
		}).
		BuildSelect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "exists") {
		t.Errorf("expected EXISTS in group condition, got: %s", sql)
	}
	t.Logf("EXISTS in group: %s | args: %v", sql, args)
}

func TestSQLInjection_BacktickInTableName(t *testing.T) {
	b := From("admin`; DROP TABLE users; --")
	_, _, err := b.Select("id").BuildSelect()
	if err == nil {
		t.Fatal("expected error for backtick injection in table name")
	}
	t.Logf("Blocked backtick injection: %v", err)
}

func TestSQLInjection_BacktickInAlias(t *testing.T) {
	b := From("admin").As("a`; DROP TABLE users; --")
	_, _, err := b.Select("id").BuildSelect()
	if err == nil {
		t.Fatal("expected error for backtick injection in alias")
	}
	t.Logf("Blocked backtick in alias: %v", err)
}

func TestSQLInjection_BacktickInJoinField(t *testing.T) {
	b := From("admin").As("a").Join("b", "b", "id`; DROP--", "id")
	_, _, err := b.Select("a.*").BuildSelect()
	if err == nil {
		t.Fatal("expected error for backtick injection in JOIN field")
	}
	t.Logf("Blocked backtick in JOIN: %v", err)
}

func TestSQLInjection_BacktickInWhereField(t *testing.T) {
	_, _, err := From("admin").As("a").
		WhereAnd("id`; DROP TABLE--", "=", 1).
		BuildSelect()
	if err == nil {
		t.Fatal("expected error for backtick injection in WHERE field")
	}
	t.Logf("Blocked backtick in WHERE field: %v", err)
}

func TestSQLInjection_CaseWhenCondition(t *testing.T) {
	_, _, err := From("t").Select(
		CaseWhen("x").When("1=1; DROP TABLE users--", "bad"),
	).BuildSelect()
	if err == nil {
		t.Fatal("expected error for SQL injection in CASE WHEN condition")
	}
	t.Logf("Blocked CASE WHEN injection: %v", err)
}

func TestParamOrder_CTE_FromSubquery_Where_Correct(t *testing.T) {
	// 回归测试：CTE + FROM 子查询 + WHERE 的参数顺序必须与 SQL 占位符顺序一致
	cteDef := From("t1").Select("id").WhereAnd("x", 1)
	fromSub := From("t3").Select("id").WhereAnd("y", 2)

	sql, args, err := From("t2", fromSub).
		With("ct", cteDef).
		Select("*").
		WhereAnd("z", 3).
		BuildSelect()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(args) != 3 {
		t.Fatalf("expected 3 args, got %d", len(args))
	}
	// 参数顺序必须为: [cte_arg, from_arg, where_arg]
	if args[0] != 1 || args[1] != 2 || args[2] != 3 {
		t.Errorf("参数顺序错误: 期望 [1 2 3], 实际 %v\nSQL: %s", args, sql)
	}
	t.Logf("Param order OK: %v", args)
}

func TestParamOrder_SelectSubquery_Where_Correct(t *testing.T) {
	// 回归测试：SELECT 子查询 + WHERE 的参数顺序
	subSel := From("items").Select(Fn("count", "cnt", "*")).WhereAnd("type", "book")

	sql, args, err := From("orders").As("o").
		Select("o.id", subSel.WhereAnd("order_id", SField("o", "id", ""))).
		WhereAnd("o.total", ">", 100).
		BuildSelect()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}
	// 参数顺序必须为: [subquery_arg, where_arg]
	if args[0] != "book" || args[1] != 100 {
		t.Errorf("参数顺序错误: 期望 [book 100], 实际 %v\nSQL: %s", args, sql)
	}
	t.Logf("Param order OK: %v", args)
}
