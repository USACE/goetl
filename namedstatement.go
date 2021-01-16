package dbetl

import (
	"fmt"
	"strings"
)

type NamedStatement struct {
	dbfields map[string]int
	namedSql string
	ParamSql string
	paramMap []string
}

func NewNamedStatement(sql string, data interface{}) NamedStatement {
	ns := NamedStatement{}
	ns.parameterizeSql(sql, data)
	return ns
}

func (ns *NamedStatement) ParamArray(data interface{}) []interface{} {
	vals := ValsAsInterfaceArray(data)
	var params []interface{}
	for _, v := range ns.paramMap {
		params = append(params, vals[ns.dbfields[v]])
	}
	return params
}

func (ns *NamedStatement) parameterizeSql(sql string, data interface{}) {
	ns.namedSql = sql
	ns.dbfields = TagAsPositionMap("db", data)
	var params []string
	var d = []rune{',', ' ', ')'} //delimiters
	var sqlbuilder strings.Builder
	var fieldBuilder strings.Builder
	i := 1
	fieldExtraction := false
	for _, c := range ns.namedSql {
		if !fieldExtraction && c == ':' {
			fieldExtraction = true
		} else if fieldExtraction && (contains(d, c)) {
			sqlbuilder.WriteString(fmt.Sprintf("$%d%c", i, c))
			params = append(params, fieldBuilder.String())
			fieldBuilder.Reset()
			fieldExtraction = false
			i++
		} else if fieldExtraction {
			fieldBuilder.WriteRune(c)
		} else {
			sqlbuilder.WriteRune(c)
		}
	}
	if fieldExtraction {
		sqlbuilder.WriteString(fmt.Sprintf("$%d", i))
		params = append(params, fieldBuilder.String())
	}
	ns.ParamSql = sqlbuilder.String()
	ns.paramMap = params
}

func contains(list []rune, r rune) bool {
	for _, v := range list {
		if v == r {
			return true
		}
	}
	return false
}
