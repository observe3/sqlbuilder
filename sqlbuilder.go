package sqlbuilder

import (
	"errors"
	"fmt"
	"reflect"
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

	// 分组
	groupBy []string

	// having后面的条件
	hwhere *Where

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

	// 模型tag
	dbTag string

	// 当字段的值是空时，是要更新该字段
	emptyFieldMap map[string]bool
	// 当字段的值是0时，是要更新该字段
	zeroFieldMap map[string]bool
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
		hwhere: &Where{
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

// 分组
func (b *sqlBuilder) Group(groupBy ...string) *sqlBuilder {
	b.groupBy = groupBy
	return b
}

// 去重
func (b *sqlBuilder) Distinct() *sqlBuilder {
	b.distinct = true
	return b
}

// 设置数据库tag
func (b *sqlBuilder) SetDbTag(tag string) *sqlBuilder {
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

func (b *sqlBuilder) HavingWhereAnd(args ...interface{}) *sqlBuilder {
	b.havingWhere("and", args...)
	return b
}

func (b *sqlBuilder) HavingWhereOr(args ...interface{}) *sqlBuilder {
	b.havingWhere("or", args...)
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
	if val, ok := argsMap[len(args)]; ok {
		groupWhere := val.ParseArgs(relation, args...)
		b.jwhere.assembleWhere = append(b.jwhere.assembleWhere, groupWhere)
	}
	return b
}

// 设置having条件
func (b *sqlBuilder) havingWhere(relation string, args ...interface{}) *sqlBuilder {
	if val, ok := argsMap[len(args)]; ok {
		groupWhere := val.ParseArgs(relation, args...)
		b.hwhere.assembleWhere = append(b.hwhere.assembleWhere, groupWhere)
	}
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
		case *funCarrier:
			if val.Fn != "" {
				var fnp string
				for _, vv := range val.Params {
					fnp = fmt.Sprintf("%s, %v", fnp, vv)
				}
				fnp = strings.TrimLeft(fnp, ", ")
				nfield = fmt.Sprintf("%s(%s) as `%s`", val.Fn, fnp, val.Alias)
			}
		case *colCarrier:
			if val.TableAlias == "" && val.Field == "" && val.FieldAlias == "" {
				continue
			}
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
	// 分组
	if len(b.groupBy) > 0 {
		b.SqlStr += " group by "
		var group string
		for _, v := range b.groupBy {
			group = fmt.Sprintf("%s,`%s`.`%s`", group, b.alias, v)
		}
		group = strings.Trim(group, ",")
		b.SqlStr += group
	}
	// 过滤
	hwhStr, hwhValue := b.hwhere.ParseWhere()
	if hwhStr != "" {
		b.SqlStr = fmt.Sprintf("%s having %s", b.SqlStr, hwhStr)
	}
	if len(hwhValue) > 0 {
		b.fieldValue = append(b.fieldValue, hwhValue...)
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
 * 使用map构建插入sql
 * 命名参数
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
 * 使用map构建插入sql
 * 占位符
**/
func (b *sqlBuilder) BuildPCreate(option *[]map[string]interface{}) (string, []any) {
	var (
		fieldValue  []any
		keysArr     []string
		sqlValueArr []string
	)
	for k, v := range *option {
		placeholderArr := []string{}
		for kk, vv := range v {
			if k == 0 {
				keysArr = append(keysArr, fmt.Sprintf("`%s`", kk))
			}
			fieldValue = append(fieldValue, vv)
			placeholderArr = append(placeholderArr, "?")
		}
		sqlValueArr = append(sqlValueArr, fmt.Sprintf("(%s)", strings.Join(placeholderArr, ",")))
	}
	insertSql := fmt.Sprintf("insert into `%s` (%s) values %s", b.tableName, strings.Join(keysArr, ","), strings.Join(sqlValueArr, ","))

	return insertSql, fieldValue
}

/**
 * 使用结构体构建插入sql
 * 命名参数
**/
func (b *sqlBuilder) BuildSCreate(entity any) (string, error) {
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
	typ := elemVal.Type()

	var (
		fields    []string
		nameField []string
	)
	for i := 0; i < elemVal.NumField(); i++ {
		field := typ.Field(i)
		dbTag := field.Tag.Get(b.dbTag)
		if dbTag == "" || dbTag == "-" {
			continue
		}
		fieldVal := elemVal.Field(i)
		if !fieldVal.CanInterface() {
			continue
		}
		fields = append(fields, fmt.Sprintf("`%s`", dbTag))
		nameField = append(nameField, fmt.Sprintf(":%s", dbTag))
	}
	// 构建 SQL 语句
	if len(fields) == 0 {
		return "", errors.New("没有可插入的字段")
	}
	return fmt.Sprintf("insert into `%s` (%s) values(%s)", b.tableName, strings.Join(fields, ","), strings.Join(nameField, ",")), nil
}

/**
 * 使用结构体构建插入sql
 * 占位符
**/
func (b *sqlBuilder) BuildSPCreate(entity any) (string, []any, error) {
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
		itemType := item.Type()
		for j := 0; j < item.NumField(); j++ {
			field := itemType.Field(j)
			dbTag := field.Tag.Get(b.dbTag)
			if dbTag == "" || dbTag == "-" {
				continue
			}
			fieldVal := item.Field(j)
			if !fieldVal.CanInterface() {
				continue
			}
			if i == 0 {
				keysArr = append(keysArr, fmt.Sprintf("`%s`", dbTag))
			}
			fieldValueArr = append(fieldValueArr, fieldVal.Interface())
			placeholderArr = append(placeholderArr, "?")
		}
		sqlValueArr = append(sqlValueArr, fmt.Sprintf("(%s)", strings.Join(placeholderArr, ",")))

	}
	if len(keysArr) == 0 {
		return "", nil, errors.New("没有可插入的字段")
	}
	insertSql := fmt.Sprintf("insert into `%s` (%s) values %s", b.tableName, strings.Join(keysArr, ","), strings.Join(sqlValueArr, ","))

	return insertSql, fieldValueArr, nil
}

/**
 * 使用map构建更新sql
 * 占位符
**/
func (b *sqlBuilder) BuildPUpdate(option map[string]interface{}) (string, []interface{}) {
	var vals string
	tableName := b.tableName
	if b.alias != "" {
		tableName = b.alias
	}
	for k, v := range option {
		switch val := v.(type) {
		case string:
			b.fieldValue = append(b.fieldValue, val)
			vals = fmt.Sprintf("%s,`%s`.`%s` = ?", vals, tableName, k)
		case int, int8, int32, int16, int64, uint, uint8, uint16, uint32, uint64,
			float32, float64:
			b.fieldValue = append(b.fieldValue, val)
			vals = fmt.Sprintf("%s,`%s`.`%s` = ?", vals, tableName, k)
		case []any:
			b.fieldValue = append(b.fieldValue, val[2])
			vals = fmt.Sprintf("%s,`%s`.`%s` = `%s`.`%s`%s?", vals, tableName, k, tableName, val[0], val[1])
		}
	}
	b.SqlStr = fmt.Sprintf("update `%s` as `%s` set %s", b.tableName, tableName, strings.TrimLeft(vals, ","))

	// 分组条件
	// b.groupWhere()
	whStr, whArgs := b.jwhere.ParseWhere()
	if whStr != "" {
		b.SqlStr = fmt.Sprintf("%s where %s", b.SqlStr, whStr)
	} else {
		panic("必须有一个条件")
	}
	if len(whArgs) > 0 {
		b.fieldValue = append(b.fieldValue, whArgs...)
	}
	return b.SqlStr, b.fieldValue
}

/**
* 使用结构体构建更新sql
* 占位符
 */
func (b *sqlBuilder) BuildSPUpdate(entity any) (string, []any, error) {
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
	typElem := reflectVal.Type()

	setStr := ""
	for i := 0; i < reflectVal.NumField(); i++ {
		field := typElem.Field(i)
		dbField := field.Tag.Get(b.dbTag)
		if dbField == "" || dbField == "-" {
			continue
		}
		fieldVal := reflectVal.Field(i)
		if !fieldVal.CanInterface() {
			continue
		}
		fial := fieldVal.Interface()
		jump := false
		if fial == nil || fial == "" || fial == 0 {
			jump = true
		}
		// 等于0仍要更新
		if _, ok := b.zeroFieldMap[dbField]; ok {
			jump = false
		}
		// 等于空仍要更新
		if _, ok := b.emptyFieldMap[dbField]; ok {
			jump = false
		}
		if jump {
			continue
		}
		switch tval := fial.(type) {
		case string:
			b.fieldValue = append(b.fieldValue, tval)
			setStr = fmt.Sprintf("%s,`%s`.`%s` = ?", setStr, tableName, dbField)
		case int, int8, int32, int16, int64, uint, uint8, uint16, uint32, uint64,
			float32, float64:
			b.fieldValue = append(b.fieldValue, tval)
			setStr = fmt.Sprintf("%s,`%s`.`%s` = ?", setStr, tableName, dbField)
		case []any:
			b.fieldValue = append(b.fieldValue, tval[2])
			setStr = fmt.Sprintf("%s,`%s`.`%s` = `%s`.`%s`%s?", setStr, tableName, dbField, tableName, tval[0], tval[1])
		}
	}
	setStr = strings.TrimLeft(setStr, ",")
	b.SqlStr = fmt.Sprintf("update `%s` as `%s` set %s", b.tableName, tableName, setStr)

	whStr, whArgs := b.jwhere.ParseWhere()
	if whStr != "" {
		b.SqlStr = fmt.Sprintf("%s where %s", b.SqlStr, whStr)
	} else {
		panic("必须有一个条件")
	}
	if len(whArgs) > 0 {
		b.fieldValue = append(b.fieldValue, whArgs...)
	}
	return b.SqlStr, b.fieldValue, nil
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
	} else {
		panic("必须有一个条件")
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
	} else {
		panic("必须有一个条件")
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
	} else {
		panic("必须有一个条件")
	}
	if len(whArgs) > 0 {
		b.fieldValue = append(b.fieldValue, whArgs...)
	}
	return b.SqlStr, b.fieldValue
}
