package sqlbuilder

import (
	"fmt"
	"strings"
)

// 运算符号接口
type IOperater interface {
	Operate(w Condition) (string, string, []interface{})
}

var SymbolMap map[string]IOperater

func init() {
	SymbolMap = make(map[string]IOperater)
	SymbolMap = map[string]IOperater{
		LIKE:         &LikeOp{Symbol: "like"},
		NLIKE:        &LikeOp{Symbol: "not like"},
		START_WITH:   &StartWithOp{Symbol: "like"},
		NSTART_WITH:  &StartWithOp{Symbol: "not like"},
		END_WITH:     &EndWithOp{Symbol: "like"},
		NEND_WITH:    &EndWithOp{Symbol: "not like"},
		IN:           &InOp{Symbol: "in"},
		NIN:          &InOp{Symbol: "not in"},
		IS_NULL:      &IsNullOp{Symbol: "is null"},
		IS_NOT_NULL:  &IsNullOp{Symbol: "is not null"},
		IS_EMPTY:     &IsEmptyOp{Symbol: "=''"},
		IS_NOT_EMPTY: &IsEmptyOp{Symbol: "!=''"},
		EQUAL:        &EqualOp{Symbol: "="},
		NEQUAL:       &EqualOp{Symbol: "!="},
		GT:           &GtOp{Symbol: ">"},
		GTE:          &GtOp{Symbol: ">="},
		LT:           &LtOp{Symbol: "<"},
		LTE:          &LtOp{Symbol: "<="},
		BETWEEN:      &BetweenOp{Symbol: "between"},
		NBETWEEN:     &BetweenOp{Symbol: "not between"},
	}
}

func RegisterSymbol(operater string, obj IOperater) {
	SymbolMap[operater] = obj
}

type LikeOp struct {
	Symbol string
}

func (r *LikeOp) Operate(w Condition) (string, string, []interface{}) {
	placeholder, result := handleCondition(w)
	return strings.ToLower(r.Symbol), placeholder, result
}

type StartWithOp struct {
	Symbol string
}

func (r *StartWithOp) Operate(w Condition) (string, string, []interface{}) {
	placeholder, result := handleCondition(w)
	return strings.ToLower(r.Symbol), placeholder, result
}

type EndWithOp struct {
	Symbol string
}

func (r *EndWithOp) Operate(w Condition) (string, string, []interface{}) {
	placeholder, result := handleCondition(w)
	return strings.ToLower(r.Symbol), placeholder, result
}

type InOp struct {
	Symbol string
}

func (r *InOp) Operate(w Condition) (string, string, []interface{}) {
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

type IsNullOp struct {
	Symbol string
}

func (r *IsNullOp) Operate(w Condition) (string, string, []interface{}) {
	var result []interface{}
	return strings.ToLower(r.Symbol), "", result
}

type IsEmptyOp struct {
	Symbol string
}

func (r *IsEmptyOp) Operate(w Condition) (string, string, []interface{}) {
	var result []interface{}
	return strings.ToLower(r.Symbol), "", result
}

type EqualOp struct {
	Symbol string
}

func (r *EqualOp) Operate(w Condition) (string, string, []interface{}) {
	placeholder, result := handleCondition(w)
	return strings.ToLower(r.Symbol), placeholder, result
}

type GtOp struct {
	Symbol string
}

func (r *GtOp) Operate(w Condition) (string, string, []interface{}) {
	placeholder, result := handleCondition(w)
	return strings.ToLower(r.Symbol), placeholder, result
}

type LtOp struct {
	Symbol string
}

func (r *LtOp) Operate(w Condition) (string, string, []interface{}) {
	placeholder, result := handleCondition(w)
	return strings.ToLower(r.Symbol), placeholder, result
}

type BetweenOp struct {
	Symbol string
}

func (r *BetweenOp) Operate(w Condition) (string, string, []interface{}) {
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
