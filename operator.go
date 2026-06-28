package sqlbuilder

import (
	"fmt"
	"strings"
	"time"
)

// IOperator 运算符接口
type IOperator interface {
	Operate(w Condition) (string, string, []any)
}

var symbolMap map[string]IOperator

func init() {
	symbolMap = map[string]IOperator{
		LIKE:          &likeOp{Symbol: "like"},
		NLIKE:         &likeOp{Symbol: "not like"},
		START_WITH:    &startWithOp{Symbol: "like"},
		NSTART_WITH:   &startWithOp{Symbol: "not like"},
		END_WITH:      &endWithOp{Symbol: "like"},
		NEND_WITH:     &endWithOp{Symbol: "not like"},
		IN:            &inOp{Symbol: "in"},
		NIN:           &inOp{Symbol: "not in"},
		IS_NULL:       &isNullOp{Symbol: "is null"},
		IS_NOT_NULL:   &isNullOp{Symbol: "is not null"},
		IS_EMPTY:      &isEmptyOp{Symbol: "=''"},
		IS_NOT_EMPTY:  &isEmptyOp{Symbol: "!=''"},
		EQUAL:         &equalOp{Symbol: "="},
		NEQUAL:        &equalOp{Symbol: "!="},
		GT:            &gtOp{Symbol: ">"},
		GTE:           &gtOp{Symbol: ">="},
		LT:            &ltOp{Symbol: "<"},
		LTE:           &ltOp{Symbol: "<="},
		BETWEEN:       &betweenOp{Symbol: "between"},
		NBETWEEN:      &betweenOp{Symbol: "not between"},
		EXISTS:        &existsOp{Symbol: "exists"},
		NOT_EXISTS:    &existsOp{Symbol: "not exists"},
		FIND_IN_SET:   &findInSetOp{},
		JSON_EXTRACT:  &jsonExtractOp{},
		MULTI_IN:      &multiInOp{Symbol: "in"},
		MULTI_NIN:     &multiInOp{Symbol: "not in"},
	}

	// 注册 ANY/ALL/SOME 复合操作符
	ops := []string{">", ">=", "<", "<=", "=", "!="}
	modifiers := []string{"any", "all", "some"}
	for _, op := range ops {
		for _, mod := range modifiers {
			key := op + " " + mod
			symbolMap[key] = &anyAllOp{op: op, symbol: mod}
		}
	}
}

func RegisterSymbol(operator string, obj IOperator) {
	symbolMap[operator] = obj
}

type likeOp struct {
	Symbol string
}

func (r *likeOp) Operate(w Condition) (string, string, []any) {
	placeholder, result := handleCondition(w)
	return strings.ToLower(r.Symbol), placeholder, result
}

type startWithOp struct {
	Symbol string
}

func (r *startWithOp) Operate(w Condition) (string, string, []any) {
	placeholder, result := handleCondition(w)
	return strings.ToLower(r.Symbol), placeholder, result
}

type endWithOp struct {
	Symbol string
}

func (r *endWithOp) Operate(w Condition) (string, string, []any) {
	placeholder, result := handleCondition(w)
	return strings.ToLower(r.Symbol), placeholder, result
}

type inOp struct {
	Symbol string
}

func (r *inOp) Operate(w Condition) (string, string, []any) {
	var length int
	var result []any
	var placeholder string
	switch val := w.Value.(type) {
	case []string:
		length = len(val)
		for _, v := range val {
			result = append(result, v)
		}
	case []int64:
		length = len(val)
		for _, v := range val {
			result = append(result, v)
		}
	case []any:
		length = len(val)
		result = append(result, val...)
	case *sqlBuilder:
		buildQuery, args, err := val.BuildSelect()
		if err != nil {
			return "", "", nil
		}
		result = append(result, args...)
		placeholder = buildQuery

	}
	if length > 0 {
		placeholder = strings.TrimRight(strings.Repeat("?,", length), ",")
	}
	placeholder = fmt.Sprintf("(%s)", placeholder)
	return strings.ToLower(r.Symbol), placeholder, result
}

type isNullOp struct {
	Symbol string
}

func (r *isNullOp) Operate(w Condition) (string, string, []any) {
	var result []any
	return strings.ToLower(r.Symbol), "", result
}

type isEmptyOp struct {
	Symbol string
}

func (r *isEmptyOp) Operate(w Condition) (string, string, []any) {
	var result []any
	return strings.ToLower(r.Symbol), "", result
}

type equalOp struct {
	Symbol string
}

func (r *equalOp) Operate(w Condition) (string, string, []any) {
	placeholder, result := handleCondition(w)
	return strings.ToLower(r.Symbol), placeholder, result
}

type gtOp struct {
	Symbol string
}

func (r *gtOp) Operate(w Condition) (string, string, []any) {
	placeholder, result := handleCondition(w)
	return strings.ToLower(r.Symbol), placeholder, result
}

type ltOp struct {
	Symbol string
}

func (r *ltOp) Operate(w Condition) (string, string, []any) {
	placeholder, result := handleCondition(w)
	return strings.ToLower(r.Symbol), placeholder, result
}

type betweenOp struct {
	Symbol string
}

func (r *betweenOp) Operate(w Condition) (string, string, []any) {
	var result []any
	var length int
	switch val := w.Value.(type) {
	case []string:
		length = len(val)
		for _, v := range val {
			result = append(result, v)
		}
	case []int64:
		length = len(val)
		for _, v := range val {
			result = append(result, v)
		}
	case []any:
		length = len(val)
		result = append(result, val...)
	}
	if length == 2 {
		return strings.ToLower(r.Symbol), "? and ?", result
	} else {
		return "", "", result
	}
}

func handleCondition(w Condition) (string, []any) {
	var result []any
	var placeholder string = "?"
	switch val := w.Value.(type) {
	case *sqlBuilder:
		buildQuery, args, err := val.BuildSelect()
		if err != nil {
			return "?", nil
		}
		result = append(result, args...)
		placeholder = fmt.Sprintf("(%v)", buildQuery)
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		result = append(result, val)
	case string:
		if w.Condition == NLIKE || w.Condition == LIKE {
			val = "%" + val + "%"
		} else if w.Condition == START_WITH || w.Condition == NSTART_WITH {
			val = val + "%"
		} else if w.Condition == END_WITH || w.Condition == NEND_WITH {
			val = "%" + val
		}
		result = append(result, val)
	case float32, float64:
		result = append(result, val)
	case bool:
		result = append(result, val)
	case time.Time:
		result = append(result, val)
	case *colCarrier:
		if val.Field != "" && val.TableAlias != "" {
			placeholder = fmt.Sprintf("`%s`.`%s`", val.TableAlias, val.Field)
		} else if val.Field != "" {
			placeholder = fmt.Sprintf("`%s`", val.Field)
		}
	case *literalCarrier:
		if val.OriginVal != "" {
			placeholder = val.OriginVal
		}
	case *jsonFieldCarrier:
		if val.Field != "" {
			if val.TableAlias != "" {
				placeholder = fmt.Sprintf("`%s`.`%s`%s'%s'", val.TableAlias, val.Field, val.Arrow, val.Path)
			} else {
				placeholder = fmt.Sprintf("`%s`%s'%s'", val.Field, val.Arrow, val.Path)
			}
		}
	}
	return placeholder, result
}

// existsOp EXISTS / NOT EXISTS 操作符
type existsOp struct {
	Symbol string
}

func (r *existsOp) Operate(w Condition) (string, string, []any) {
	val, ok := w.Value.(*sqlBuilder)
	if !ok {
		return "", "", nil
	}
	buildQuery, args, err := val.BuildSelect()
	if err != nil {
		return "", "", nil
	}
	return strings.ToLower(r.Symbol), fmt.Sprintf("(%v)", buildQuery), args
}

// anyAllOp ANY / ALL / SOME 复合操作符
type anyAllOp struct {
	op     string
	symbol string
}

func (r *anyAllOp) Operate(w Condition) (string, string, []any) {
	val, ok := w.Value.(*sqlBuilder)
	if !ok {
		return "", "", nil
	}
	buildQuery, args, err := val.BuildSelect()
	if err != nil {
		return "", "", nil
	}
	return fmt.Sprintf("%s %s", r.op, r.symbol), fmt.Sprintf("(%v)", buildQuery), args
}

// multiInOp 多列 IN 操作符
type multiInOp struct {
	Symbol string
}

func (r *multiInOp) Operate(w Condition) (string, string, []any) {
	tuples, ok := w.Value.([][]any)
	if !ok {
		return "", "", nil
	}
	var parts []string
	var result []any
	for _, tuple := range tuples {
		result = append(result, tuple...)
		parts = append(parts, "("+strings.TrimRight(strings.Repeat("?,", len(tuple)), ",")+")")
	}
	return strings.ToLower(r.Symbol), "(" + strings.Join(parts, ", ") + ")", result
}

// findInSetOp FIND_IN_SET 操作符（MySQL）
type findInSetOp struct{}

func (r *findInSetOp) Operate(w Condition) (string, string, []any) {
	placeholder, result := handleCondition(w)
	tableName := w.TableName
	if tableName == "" {
		return "", "", nil
	}
	return "find_in_set", fmt.Sprintf("(%s, `%s`.`%s`)", placeholder, tableName, w.Field), result
}

// jsonExtractOp JSON_EXTRACT 操作符（MySQL）
type jsonExtractOp struct{}

func (r *jsonExtractOp) Operate(w Condition) (string, string, []any) {
	val, ok := w.Value.(string)
	if !ok {
		return "", "", nil
	}
	tableName := w.TableName
	if tableName == "" {
		return "", "", nil
	}
	return "json_extract", fmt.Sprintf("(`%s`.`%s`, '%s')", tableName, w.Field, val), nil
}
