package dbetl

import (
	"fmt"
	"strings"
)

type ParameterTemplateFunction func(field string, i int) string

var namedStatements map[string]NamedStatement = make(map[string]NamedStatement) //cached named statements.

type NamedStatement struct {
	templateFunction ParameterTemplateFunction
	dbfields         map[string]int
	namedSql         string
	ParamSql         string
	paramMap         []string
}

func NewNamedStatement(templateFunction ParameterTemplateFunction, sql string, data interface{}) NamedStatement {
	h := getHash(sql)
	ns, ok := namedStatements[h]
	if !ok {
		ns := NamedStatement{}
		ns.templateFunction = templateFunction
		ns.parameterizeSql(sql, data)
		namedStatements[h] = ns
	}
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
			//sqlbuilder.WriteString(fmt.Sprintf("$%d%c", i, c))
			f := fieldBuilder.String()
			sqlbuilder.WriteString(ns.templateFunction(f, i))
			sqlbuilder.WriteRune(c)
			params = append(params, f)
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
