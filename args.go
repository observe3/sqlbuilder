package sqlbuilder

import (
	"fmt"
	"strings"
)

type IArgser interface {
	ParseArgs(relation string, args ...interface{}) []GroupWhere
}

var ArgsMap map[int]IArgser

func init() {
	ArgsMap = make(map[int]IArgser)
	ArgsMap[1] = &OneArgs{}
	ArgsMap[2] = &TwoArgs{}
	ArgsMap[3] = &ThreeArgs{}
	ArgsMap[4] = &FourArgs{}
	ArgsMap[5] = &FiveArgs{}
}

type OneArgs struct {
}

func (r *OneArgs) ParseArgs(relation string, args ...interface{}) []GroupWhere {
	groupWhere := make([]GroupWhere, 0)
	switch whs := args[0].(type) {
	case [][]interface{}:
		andWh := []Condition{}

		for _, v := range whs {
			if v[0] == "" {
				continue
			}
			groupCondition(&andWh, v)

		}
		groupWhere = append(groupWhere, GroupWhere{
			Condition: andWh,
			Relation:  relation,
		})
	case [][][]interface{}:
		// 遍历每一组条件
		for _, v := range whs {
			var gwh []Condition
			for _, vv := range v {
				if vv[0] == "" {
					continue
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
func groupCondition(andWh *[]Condition, v []interface{}) string {
	var relation string
	nfield, ftype := parseAggregation(v...)
	if nfield == "" {
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

type TwoArgs struct {
}

func (r *TwoArgs) ParseArgs(relation string, args ...interface{}) []GroupWhere {
	groupWhere := make([]GroupWhere, 0)
	nfield, ftype := parseAggregation(args...)
	if nfield == "" {
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

type ThreeArgs struct {
}

func (r *ThreeArgs) ParseArgs(relation string, args ...interface{}) []GroupWhere {
	groupWhere := make([]GroupWhere, 0)
	nfield, ftype := parseAggregation(args...)
	if nfield == "" {
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

type FourArgs struct {
}

func (r *FourArgs) ParseArgs(relation string, args ...interface{}) []GroupWhere {
	groupWhere := make([]GroupWhere, 0)
	nfield, ftype := parseAggregation(args...)
	if nfield == "" {
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

type FiveArgs struct {
}

func (r *FiveArgs) ParseArgs(relation string, args ...interface{}) []GroupWhere {
	groupWhere := make([]GroupWhere, 0)
	if val, ok := args[4].(string); ok {
		relation = val
	}
	nfield, ftype := parseAggregation(args...)
	if nfield == "" {
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

func parseAggregation(args ...interface{}) (string, int64) {
	for _, v := range args {
		if val, ok := v.(string); ok {
			if strings.Contains(val, "#") || strings.Contains(val, "--") || strings.Contains(val, "/*") {
				return "", 0
			}
		}
	}
	var nfield string
	var ftype int64
	if firstField, ok := args[0].(string); ok {
		nfield = firstField
		ftype = 1
	} else if val, ok := args[0].(*fcolumn); ok {
		if val.Fn != "" {
			var fnp string
			plen := len(val.Params)
			if plen == 1 {
				fnp = fmt.Sprintf("%s, `%v`", fnp, val.Params[0])
			} else {
				for _, vv := range val.Params {
					fnp = fmt.Sprintf("%s, %v", fnp, vv)
				}
			}
			fnp = strings.TrimLeft(fnp, ", ")
			nfield = fmt.Sprintf("%s(%s)", val.Fn, fnp)
		}
		ftype = 2
	}
	return nfield, ftype
}
