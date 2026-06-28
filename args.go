package sqlbuilder

import (
	"fmt"
	"strings"
)

type IArgser interface {
	ParseArgs(relation string, args ...any) []GroupWhere
}

var argsMap map[int]IArgser

func init() {
	argsMap = make(map[int]IArgser)
	argsMap[1] = &oneArgs{}
	argsMap[2] = &twoArgs{}
	argsMap[3] = &threeArgs{}
	argsMap[4] = &fourArgs{}
	argsMap[5] = &fiveArgs{}
}

/**
 * 注册参数处理函数
 */
func RegisterArgsHandle(n int, obj IArgser) {
	argsMap[n] = obj
}

type oneArgs struct {
}

func (r *oneArgs) ParseArgs(relation string, args ...any) []GroupWhere {
	groupWhere := make([]GroupWhere, 0)
	switch whs := args[0].(type) {
	case [][]any:
		andWh := []Condition{}

		for _, v := range whs {
			if v[0] == "" {
				_, isSub := v[len(v)-1].(*sqlBuilder)
				if !isSub {
					continue
				}
			}
			groupCondition(&andWh, v)

		}
		groupWhere = append(groupWhere, GroupWhere{
			Condition: andWh,
			Relation:  relation,
		})
	case [][][]any:
		// 遍历每一组条件
		for _, v := range whs {
			var gwh []Condition
			for _, vv := range v {
					if vv[0] == "" {
						_, isSub := vv[len(vv)-1].(*sqlBuilder)
						if !isSub {
							continue
						}
					}
				groupCondition(&gwh, vv)
			}
			groupWhere = append(groupWhere, GroupWhere{
				Relation:  relation,
				Condition: gwh,
			})
		}
	}
	return groupWhere
}
func groupCondition(andWh *[]Condition, v []any) string {
	var relation string
	nfield, ftype := parseAggregation(v...)
	if nfield == "" && ftype == 0 {
		return ""
	}
	switch len(v) {
	case 5:
		*andWh = append(*andWh, Condition{
			Field:     nfield,
			Condition: v[1].(string),
			Value:     v[2],
			TableName: v[3].(string),
			Relation:  v[4].(string),
			FieldType: ftype,
		})
		relation = v[4].(string)
	case 4:
		*andWh = append(*andWh, Condition{
			Field:     nfield,
			Condition: v[1].(string),
			Value:     v[2],
			TableName: v[3].(string),
			FieldType: ftype,
		})
	case 3:
		*andWh = append(*andWh, Condition{
			Field:     nfield,
			Condition: v[1].(string),
			Value:     v[2],
			FieldType: ftype,
		})
	case 2:
		*andWh = append(*andWh, Condition{
			Field:     nfield,
			Condition: "=",
			Value:     v[1],
			FieldType: ftype,
		})
	}
	return relation
}

type twoArgs struct {
}

func (r *twoArgs) ParseArgs(relation string, args ...any) []GroupWhere {
	groupWhere := make([]GroupWhere, 0)
	nfield, ftype := parseAggregation(args...)
	if nfield == "" && ftype == 0 {
		return groupWhere
	}
	groupWhere = append(groupWhere, GroupWhere{
		Relation: relation,
		Condition: []Condition{
			{
				Field:     nfield,
				Condition: "=",
				Value:     args[1],
				FieldType: ftype,
			},
		},
	})
	return groupWhere
}

type threeArgs struct {
}

func (r *threeArgs) ParseArgs(relation string, args ...any) []GroupWhere {
	groupWhere := make([]GroupWhere, 0)
	nfield, ftype := parseAggregation(args...)
	if nfield == "" && ftype == 0 {
		return groupWhere
	}
	groupWhere = append(groupWhere, GroupWhere{
		Relation: relation,
		Condition: []Condition{
			{
				Field:     nfield,
				Condition: args[1].(string),
				Value:     args[2],
				FieldType: ftype,
			},
		},
	})
	return groupWhere
}

type fourArgs struct {
}

func (r *fourArgs) ParseArgs(relation string, args ...any) []GroupWhere {
	groupWhere := make([]GroupWhere, 0)
	nfield, ftype := parseAggregation(args...)
	if nfield == "" && ftype == 0 {
		return groupWhere
	}
	groupWhere = append(groupWhere, GroupWhere{
		Relation: relation,
		Condition: []Condition{
			{
				Field:     nfield,
				Condition: args[1].(string),
				Value:     args[2],
				TableName: args[3].(string),
				FieldType: ftype,
			},
		},
	})
	return groupWhere
}

type fiveArgs struct {
}

func (r *fiveArgs) ParseArgs(relation string, args ...any) []GroupWhere {
	groupWhere := make([]GroupWhere, 0)
	if val, ok := args[4].(string); ok {
		relation = val
	}
	nfield, ftype := parseAggregation(args...)
	if nfield == "" && ftype == 0 {
		return groupWhere
	}
	groupWhere = append(groupWhere, GroupWhere{
		Relation: relation,
		Condition: []Condition{
			{
				Field:     nfield,
				Condition: args[1].(string),
				Value:     args[2],
				TableName: args[3].(string),
				Relation:  args[4].(string),
				FieldType: ftype,
			},
		},
	})
	return groupWhere
}

func parseAggregation(args ...any) (string, int64) {
	for _, v := range args {
		if val, ok := v.(string); ok {
			if hasIllegalStr(val) {
				return "", 0
			}
		}
	}
	var nfield string
	var ftype int64
	switch args[0].(type) {
	case string:
		nfield = args[0].(string)
		if nfield == "" {
			ftype = 3 // bare operator (EXISTS, NOT EXISTS, etc.)
		} else {
			ftype = 1
		}
		// FIND_IN_SET 和 JSON_EXTRACT 需要 bare operator 模式
		if len(args) >= 2 {
			if cond, ok := args[1].(string); ok {
				lc := strings.ToLower(cond)
				if lc == FIND_IN_SET || lc == JSON_EXTRACT {
					ftype = 3
				}
			}
		}
	case *funCarrier:
		val := args[0].(*funCarrier)
		if val.Fn != "" {
			var fnp string
			for _, vv := range val.Params {
				fnp = fmt.Sprintf("%s, %v", fnp, vv)
			}
			fnp = strings.TrimLeft(fnp, ", ")
			nfield = fmt.Sprintf("%s(%s)", val.Fn, fnp)
		}
		ftype = 2
	case *literalCarrier:
		val := args[0].(*literalCarrier)
		nfield = val.OriginVal
		ftype = 2
	case *jsonFieldCarrier:
		val := args[0].(*jsonFieldCarrier)
		if val.Field != "" {
			if val.TableAlias != "" {
				nfield = fmt.Sprintf("`%s`.`%s`%s'%s'", val.TableAlias, val.Field, val.Arrow, val.Path)
			} else {
				nfield = fmt.Sprintf("`%s`%s'%s'", val.Field, val.Arrow, val.Path)
			}
		}
		ftype = 2
	case *sqlBuilder:
		nfield = ""
		ftype = 3 // bare operator, used by EXISTS
	default:
		nfield = ""
		ftype = 0
	}
	return nfield, ftype
}
