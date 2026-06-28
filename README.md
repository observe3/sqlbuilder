# sqlbuilder

SQL 构造器，配合 sqlx 使用。支持链式调用，生成参数化 SQL，有效防止 SQL 注入。

## 快速开始

```go
import "github.com/Ifkl/sqlbuilder"

// 基本查询
sql, args, _ := sqlbuilder.From("user").
    As("u").
    Select("id", "name", "age").
    WhereAnd("status", 1).
    Order([][]any{{"id", "desc"}}).
    Limit(10).
    BuildSelect()

// INSERT
sql, args := sqlbuilder.From("user").BuildMapInsert(map[string]any{
    "name": "张三",
    "age":  25,
})
```

## 完整 API 支持

### SELECT 查询
| 方法 | 说明 |
|------|------|
| `Select(fields...)` | 查询字段，支持 string、`Fn()`、`SField()`、`WinFn()`、`CaseWhen()`、子查询 `*sqlBuilder` |
| `Distinct()` | SELECT DISTINCT |
| `SqlNoCache()` | SQL_NO_CACHE 提示 |
| `SqlCalcFoundRows()` | SQL_CALC_FOUND_ROWS 提示 |
| `From(table, subquery?)` | 表名或子查询 |
| `As(alias)` | 表别名 |
| `Table(name)` | 切换表名 |
| `SetDbTag(tag)` | 设置结构体 db tag 名 |
| `Debug()` | 打印 SQL |

### JOIN 连接
| 方法 | 说明 |
|------|------|
| `Join(table, alias, f1, f2)` | INNER JOIN |
| `LeftJoin(table, alias, f1, f2)` | LEFT JOIN |
| `RightJoin(table, alias, f1, f2)` | RIGHT JOIN |
| `CrossJoin(table, alias)` | CROSS JOIN |
| `NaturalJoin(table, alias)` | NATURAL JOIN |
| `StraightJoin(table, alias)` | STRAIGHT_JOIN (MySQL) |
| `FullJoin(table, alias, f1, f2)` | FULL OUTER JOIN (LEFT JOIN 模拟) |
| `JoinOn(table, alias, ons...)` | INNER JOIN 复杂 ON 条件 |
| `LeftJoinOn(table, alias, ons...)` | LEFT JOIN 复杂 ON 条件 |
| `RightJoinOn(table, alias, ons...)` | RIGHT JOIN 复杂 ON 条件 |
| `JoinUsing(table, alias, fields...)` | INNER JOIN ... USING |
| `LeftJoinUsing(table, alias, fields...)` | LEFT JOIN ... USING |
| `RightJoinUsing(table, alias, fields...)` | RIGHT JOIN ... USING |
| `JoinSub(sub, alias, f1, f2)` | JOIN 子查询 |
| `LeftJoinSub(sub, alias, f1, f2)` | LEFT JOIN 子查询 |
| `RightJoinSub(sub, alias, f1, f2)` | RIGHT JOIN 子查询 |

### WHERE 条件
| 操作符 | 说明 |
|--------|------|
| `=` `!=` `>` `>=` `<` `<=` | 比较运算 |
| `in` `not in` | 集合运算（支持切片和子查询） |
| `between` `not between` | 范围运算 |
| `like` `not like` | 模糊匹配（自动加 %） |
| `start with` `not start with` | 前缀匹配 |
| `end with` `not end with` | 后缀匹配 |
| `is null` `is not null` | 空值判断 |
| `is empty` `is not empty` | 空字符串判断 |
| `exists` `not exists` | 存在性子查询 |
| `> any` `>= all` `= some` 等 | ANY/ALL/SOME 子查询（18种组合） |
| `multi in` `multi not in` | 多列 IN |
| `find in set` | FIND_IN_SET (MySQL) |
| `json_extract` | JSON_EXTRACT (MySQL) |
| `whereRaw(cond, args...)` | 原始条件（慎用） |

**条件格式：**

```go
// 2 参数：等值判断
WhereAnd("status", 1)                    // status = ?

// 3 参数：指定操作符
WhereAnd("age", ">=", 18)                // age >= ?
WhereAnd("name", "like", "张三")          // name like ?
WhereAnd("id", "in", []any{1,2,3})      // id in (?,?,?)

// 4 参数：指定表别名
WhereAnd("id", "=", 1, "u")             // `u`.`id` = ?

// 5 参数：指定表别名和关系(and/or)
WhereAnd("name", "like", "张三", "u", "or") // or `u`.`name` like ?

// WHERE 子查询
WhereAnd("id", "in", From("t2").Select("id"))
WhereAnd("", "exists", From("t2").Select("id").WhereAnd(...))

// EXISTS 关联子查询
WhereAnd("", "exists", 
    From("order").As("o").Select("id").
        WhereAnd("user_id", SField("u", "id", "")))

// 分组条件
WhereAnd([][]any{
    {"status", 1},
    {"age", ">=", 18, "u", "or"},
})
WhereAnd([][][]any{...})  // 嵌套分组
```

### GROUP BY / HAVING / ORDER BY / LIMIT
| 方法 | 说明 |
|------|------|
| `Group(fields...)` | GROUP BY |
| `WithRollup()` | GROUP BY ... WITH ROLLUP |
| `HavingWhereAnd(...)` | HAVING AND |
| `HavingWhereOr(...)` | HAVING OR |
| `Order([][]any{{field, dir}, ...})` | ORDER BY，支持三元素 `{table, field, dir}` |
| `Offset(n)` | 偏移量（配合 Size） |
| `Size(n)` | 每页条数（配合 Offset） |
| `Page(p, n)` | 分页（p 从 1 开始） |
| `Limit(n)` | LIMIT n |
| `ForUpdate()` | FOR UPDATE 行锁 |
| `LockInShareMode()` | LOCK IN SHARE MODE |

### 索引提示
| 方法 | 说明 |
|------|------|
| `UseIndex(names...)` | USE INDEX |
| `ForceIndex(names...)` | FORCE INDEX |
| `IgnoreIndex(names...)` | IGNORE INDEX |

### 高级查询

**UNION:**
```go
q1.Union(q2).BuildSelect()     // UNION
q1.UnionAll(q2).BuildSelect()  // UNION ALL
```

**CTE (WITH):**
```go
cte := From("active_users").Select("id", "name")
From("orders").As("o").
    With("au", cte).
    WithRecursive().  // WITH RECURSIVE
    WithColumns("cte", []string{"a","b"}, def).
    Join("au", "au", "user_id", "id").
    BuildSelect()
```

**窗口函数:**
```go
WinFn("row_number", "rn").Partition("dept_id").OrderByClause([][]any{{"salary", "desc"}})
WinFn("sum", "total", "amount").Partition("dept").OrderByClause(...)
```

**CASE WHEN:**
```go
CaseWhen("grade").
    When("score >= 90", "A").
    When("score >= 80", "B").
    Else("D")

// 简单 CASE
CaseWhen("level_name").SimpleCase("level").
    When(1, "VIP").When(2, "SVIP").Else("Normal")
```

**列辅助函数:**
```go
Fn("count", "total", "*")           // COUNT(*) as `total`
Fn("sum", "amount", "price")        // SUM(price) as `amount`
SField("u", "id", "uid")            // `u`.`id` uid
Literal("NOW()")                    // 原语
JsonField("u", "data", "->>", "$.name")  // `u`.`data`->>'$.name'
```

### INSERT
| 方法 | 说明 |
|------|------|
| `BuildMapInsert(map)` | INSERT ... VALUES (?) |
| `BuildMapNamedInsert(map)` | INSERT ... VALUES (:key) |
| `BuildSliceMapInsert([]map)` | 批量 INSERT |
| `BuildSliceMapNamedInsert([]map)` | 批量命名参数 INSERT |
| `BuildStructInsert(&struct)` | 结构体 INSERT |
| `BuildStructNamedInsert(&struct)` | 结构体命名参数 INSERT |
| `BuildSliceStructInsert(&[]struct)` | 批量结构体 INSERT |
| `BuildSliceStructNamedInsert(&[]struct)` | 批量结构体命名参数 INSERT |
| `BuildMapInsertIgnore(map)` | INSERT IGNORE |
| `BuildSliceMapInsertIgnore([]map)` | 批量 INSERT IGNORE |
| `BuildMapReplace(map)` | REPLACE INTO |
| `BuildSliceMapReplace([]map)` | 批量 REPLACE |
| `BuildInsertSet(map)` | INSERT ... SET (MySQL) |
| `BuildInsertSelect(cols, builder)` | INSERT ... SELECT |
| `OnDuplicateKey(map)` | ON DUPLICATE KEY UPDATE |

### UPDATE
| 方法 | 说明 |
|------|------|
| `BuildMapUpdate(map)` | UPDATE SET（支持 `[]any{field, op, val}` 字段运算） |
| `BuildStructUpdate(&struct)` | 结构体 UPDATE |
| `BuildIncrement(map)` | 字段累加（field = field + ?） |
| `BuildDecrement(map)` | 字段累减（field = field - ?） |
| `BuildUpdateWithJoin(map)` | 带 JOIN 的 UPDATE |
| `BuildSoftDelete()` | 软删除（需设置 `softDeleteField`） |

> UPDATE 支持 `.Order()` + `.Limit()` / `.Page()` 组合

### DELETE / TRUNCATE
| 方法 | 说明 |
|------|------|
| `BuildDelete()` | DELETE（必须有 WHERE） |
| `BuildDeleteWithJoin()` | 带 JOIN 的 DELETE |
| `BuildTruncate()` | TRUNCATE TABLE |

> DELETE 支持 `.Order()` + `.Limit()` / `.Page()` 组合

### 工具方法
| 方法 | 说明 |
|------|------|
| `BuildSelectCount()` | 包装为 SELECT COUNT(*) |
| `BuildExists()` | 包装为 SELECT EXISTS() |
| `ToString()` | 获取 WHERE 字符串和参数 |
| `GetFieldValue()` | 获取参数值列表 |
| `Reset()` | 重置 builder 以复用 |
| `UpdateZeroField(fields...)` | 值为 0 时仍更新 |
| `UpdateEmptyField(fields...)` | 值为空时仍更新 |
| `Raw(sql, args...)` | 追加原始 SQL（慎用） |
