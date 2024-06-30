package sqlbuilder

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
	switch len(v) {
	case 5:
		*andWh = append(*andWh, Condition{
			Field:     v[0].(string),
			Condition: v[1].(string),
			Value:     v[2],
			TableName: v[3].(string),
			Relation:  v[4].(string),
		})
		relation = v[4].(string)
	case 4:
		*andWh = append(*andWh, Condition{
			Field:     v[0].(string),
			Condition: v[1].(string),
			Value:     v[2],
			TableName: v[3].(string),
		})
	case 3:
		*andWh = append(*andWh, Condition{
			Field:     v[0].(string),
			Condition: v[1].(string),
			Value:     v[2],
		})
	case 2:
		*andWh = append(*andWh, Condition{
			Field:     v[0].(string),
			Condition: "=",
			Value:     v[1],
		})
	}
	return relation
}

type TwoArgs struct {
}

func (r *TwoArgs) ParseArgs(relation string, args ...interface{}) []GroupWhere {
	groupWhere := make([]GroupWhere, 0)
	groupWhere = append(groupWhere, GroupWhere{
		Relation: relation,
		Condition: []Condition{
			{
				Field:     args[0].(string),
				Condition: "=",
				Value:     args[1],
			},
		},
	})
	return groupWhere
}

type ThreeArgs struct {
}

func (r *ThreeArgs) ParseArgs(relation string, args ...interface{}) []GroupWhere {
	groupWhere := make([]GroupWhere, 0)
	groupWhere = append(groupWhere, GroupWhere{
		Relation: relation,
		Condition: []Condition{
			{
				Field:     args[0].(string),
				Condition: args[1].(string),
				Value:     args[2],
			},
		},
	})
	return groupWhere
}

type FourArgs struct {
}

func (r *FourArgs) ParseArgs(relation string, args ...interface{}) []GroupWhere {
	groupWhere := make([]GroupWhere, 0)
	groupWhere = append(groupWhere, GroupWhere{
		Relation: relation,
		Condition: []Condition{
			{
				Field:     args[0].(string),
				Condition: args[1].(string),
				Value:     args[2],
				TableName: args[3].(string),
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
	groupWhere = append(groupWhere, GroupWhere{
		Relation: relation,
		Condition: []Condition{
			{
				Field:     args[0].(string),
				Condition: args[1].(string),
				Value:     args[2],
				TableName: args[3].(string),
				Relation:  args[4].(string),
			},
		},
	})
	return groupWhere
}
