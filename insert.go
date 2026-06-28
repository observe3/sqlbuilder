package sqlbuilder

import (
	"fmt"
	"strings"
)

// validateMapKeys 校验 map 的 key（列名）是否安全
func validateMapKeys(option map[string]any) error {
	for k := range option {
		if !isSafeIdentifier(k) {
			return fmt.Errorf("非法的列名: %s", k)
		}
	}
	return nil
}

// buildMapInsert 内部 insert 辅助方法，prefix 支持 "insert", "insert ignore", "replace"
func (b *sqlBuilder) buildMapInsert(prefix string, option map[string]any) (string, []any) {
	if len(option) == 0 {
		return "", nil
	}
	if err := validateMapKeys(option); err != nil {
		b.err = err
		return "", nil
	}
	keysArr := []string{}
	valsArr := []any{}
	placeArr := []string{}
	for k, v := range option {
		keysArr = append(keysArr, fmt.Sprintf("`%s`", k))
		valsArr = append(valsArr, v)
		placeArr = append(placeArr, "?")
	}
	sqlStr := fmt.Sprintf("%s into `%s` (%s) values (%s)", prefix, b.tableName, strings.Join(keysArr, ","), strings.Join(placeArr, ","))

	// ON DUPLICATE KEY UPDATE
	if len(b.onDuplicateUpdates) > 0 {
		var dupParts []string
		for k, v := range b.onDuplicateUpdates {
			dupParts = append(dupParts, fmt.Sprintf("`%s` = ?", k))
			valsArr = append(valsArr, v)
		}
		sqlStr = fmt.Sprintf("%s on duplicate key update %s", sqlStr, strings.Join(dupParts, ", "))
	}

	return sqlStr, valsArr
}

// buildSliceMapInsert 内部批量 insert 辅助方法
func (b *sqlBuilder) buildSliceMapInsert(prefix string, option []map[string]any) (string, []any) {
	if len(option) == 0 {
		return "", nil
	}
	if err := validateMapKeys(option[0]); err != nil {
		b.err = err
		return "", nil
	}
	first := option[0]
	keys := make([]string, 0, len(first))
	for k := range first {
		keys = append(keys, k)
	}

	var (
		fieldValue  []any
		sqlValueArr []string
	)
	keysArr := make([]string, len(keys))
	for i, k := range keys {
		keysArr[i] = fmt.Sprintf("`%s`", k)
	}

	for _, row := range option {
		placeholders := make([]string, len(keys))
		for i, k := range keys {
			fieldValue = append(fieldValue, row[k])
			placeholders[i] = "?"
		}
		sqlValueArr = append(sqlValueArr, fmt.Sprintf("(%s)", strings.Join(placeholders, ",")))
	}
	insertSql := fmt.Sprintf("%s into `%s` (%s) values %s", prefix, b.tableName, strings.Join(keysArr, ","), strings.Join(sqlValueArr, ","))

	// ON DUPLICATE KEY UPDATE
	if len(b.onDuplicateUpdates) > 0 {
		var dupParts []string
		for k, v := range b.onDuplicateUpdates {
			dupParts = append(dupParts, fmt.Sprintf("`%s` = ?", k))
			fieldValue = append(fieldValue, v)
		}
		insertSql = fmt.Sprintf("%s on duplicate key update %s", insertSql, strings.Join(dupParts, ", "))
	}

	return insertSql, fieldValue
}

// BuildMapInsertIgnore 使用 map 构建 INSERT IGNORE SQL
func (b *sqlBuilder) BuildMapInsertIgnore(option map[string]any) (string, []any) {
	return b.buildMapInsert("insert ignore", option)
}

// BuildMapReplace 使用 map 构建 REPLACE INTO SQL
func (b *sqlBuilder) BuildMapReplace(option map[string]any) (string, []any) {
	return b.buildMapInsert("replace", option)
}

// BuildSliceMapInsertIgnore 使用 map 切片构建批量 INSERT IGNORE SQL
func (b *sqlBuilder) BuildSliceMapInsertIgnore(option []map[string]any) (string, []any) {
	return b.buildSliceMapInsert("insert ignore", option)
}

// BuildSliceMapReplace 使用 map 切片构建批量 REPLACE INTO SQL
func (b *sqlBuilder) BuildSliceMapReplace(option []map[string]any) (string, []any) {
	return b.buildSliceMapInsert("replace", option)
}

// BuildInsertSet 使用 map 构建 INSERT ... SET SQL（MySQL）
func (b *sqlBuilder) BuildInsertSet(option map[string]any) (string, []any) {
	if len(option) == 0 {
		return "", nil
	}
	if err := validateMapKeys(option); err != nil {
		b.err = err
		return "", nil
	}
	var setParts []string
	var vals []any
	for k, v := range option {
		setParts = append(setParts, fmt.Sprintf("`%s` = ?", k))
		vals = append(vals, v)
	}
	return fmt.Sprintf("insert into `%s` set %s", b.tableName, strings.Join(setParts, ", ")), vals
}

// BuildInsertSelect 构建 INSERT ... SELECT SQL
func (b *sqlBuilder) BuildInsertSelect(columns []string, selectBuilder *sqlBuilder) (string, []any, error) {
	if selectBuilder == nil {
		return "", nil, fmt.Errorf("INSERT SELECT 子查询不能为 nil")
	}
	if !isSafeIdentifierAny(columns...) {
		return "", nil, fmt.Errorf("非法的 INSERT SELECT 列名")
	}
	selectSql, selectArgs, err := selectBuilder.BuildSelect()
	if err != nil {
		return "", nil, err
	}
	cols := make([]string, len(columns))
	for i, col := range columns {
		cols[i] = fmt.Sprintf("`%s`", col)
	}
	sql := fmt.Sprintf("insert into `%s` (%s) %s", b.tableName, strings.Join(cols, ", "), selectSql)
	return sql, selectArgs, nil
}
