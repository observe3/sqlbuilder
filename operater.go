package sqlbuilder

import (
	"fmt"
	"strings"
)

// 运算符号接口
type IOperater interface {
	Operate(w Condition) (string, string, []interface{})
}

var symbolMap map[string]IOperater

func init() {
	symbolMap = make(map[string]IOperater)
	symbolMap = map[string]IOperater{
		LIKE:         &likeOp{Symbol: "like"},
		NLIKE:        &likeOp{Symbol: "not like"},
		START_WITH:   &startWithOp{Symbol: "like"},
		NSTART_WITH:  &startWithOp{Symbol: "not like"},
		END_WITH:     &endWithOp{Symbol: "like"},
		NEND_WITH:    &endWithOp{Symbol: "not like"},
		IN:           &inOp{Symbol: "in"},
		NIN:          &inOp{Symbol: "not in"},
		IS_NULL:      &isNullOp{Symbol: "is null"},
		IS_NOT_NULL:  &isNullOp{Symbol: "is not null"},
		IS_EMPTY:     &isEmptyOp{Symbol: "=''"},
		IS_NOT_EMPTY: &isEmptyOp{Symbol: "!=''"},
		EQUAL:        &equalOp{Symbol: "="},
		NEQUAL:       &equalOp{Symbol: "!="},
		GT:           &gtOp{Symbol: ">"},
		GTE:          &gtOp{Symbol: ">="},
		LT:           &ltOp{Symbol: "<"},
		LTE:          &ltOp{Symbol: "<="},
		BETWEEN:      &betweenOp{Symbol: "between"},
		NBETWEEN:     &betweenOp{Symbol: "not between"},
	}
}

func RegisterSymbol(operater string, obj IOperater) {
	symbolMap[operater] = obj
}

type likeOp struct {
	Symbol string
}

func (r *likeOp) Operate(w Condition) (string, string, []interface{}) {
	placeholder, result := handleCondition(w)
	return strings.ToLower(r.Symbol), placeholder, result
}

type startWithOp struct {
	Symbol string
}

func (r *startWithOp) Operate(w Condition) (string, string, []interface{}) {
	placeholder, result := handleCondition(w)
	return strings.ToLower(r.Symbol), placeholder, result
}

type endWithOp struct {
	Symbol string
}

func (r *endWithOp) Operate(w Condition) (string, string, []interface{}) {
	placeholder, result := handleCondition(w)
	return strings.ToLower(r.Symbol), placeholder, result
}

type inOp struct {
	Symbol string
}

func (r *inOp) Operate(w Condition) (string, string, []interface{}) {
	var length int
	var result []interface{}
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
	case []interface{}:
		length = len(val)
		result = append(result, val...)
	case *sqlBuilder:
		buildQuery, args := val.BuildSelect()
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

func (r *isNullOp) Operate(w Condition) (string, string, []interface{}) {
	var result []interface{}
	return strings.ToLower(r.Symbol), "", result
}

type isEmptyOp struct {
	Symbol string
}

func (r *isEmptyOp) Operate(w Condition) (string, string, []interface{}) {
	var result []interface{}
	return strings.ToLower(r.Symbol), "", result
}

type equalOp struct {
	Symbol string
}

func (r *equalOp) Operate(w Condition) (string, string, []interface{}) {
	placeholder, result := handleCondition(w)
	return strings.ToLower(r.Symbol), placeholder, result
}

type gtOp struct {
	Symbol string
}

func (r *gtOp) Operate(w Condition) (string, string, []interface{}) {
	placeholder, result := handleCondition(w)
	return strings.ToLower(r.Symbol), placeholder, result
}

type ltOp struct {
	Symbol string
}

func (r *ltOp) Operate(w Condition) (string, string, []interface{}) {
	placeholder, result := handleCondition(w)
	return strings.ToLower(r.Symbol), placeholder, result
}

type betweenOp struct {
	Symbol string
}

func (r *betweenOp) Operate(w Condition) (string, string, []interface{}) {
	var result []interface{}
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
	case []interface{}:
		length = len(val)
		result = append(result, val...)
	}
	if length == 2 {
		return strings.ToLower(r.Symbol), "? and ?", result
	} else {
		return "", "", result
	}
}

func handleCondition(w Condition) (string, []interface{}) {
	var result []interface{}
	var placeholder string = "?"
	switch val := w.Value.(type) {
	case *sqlBuilder:
		buildQuery, args := val.BuildSelect()
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
	}
	return placeholder, result
}
