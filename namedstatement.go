package goetl

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
)

var namedStatements map[string]NamedStatement = make(map[string]NamedStatement) //cached named statements.

type NamedStatement struct {
	templateFunction BindParamTemplateFunction
	dbfields         map[string]int
	namedSql         string
	ParamSql         string
	paramMap         []string
}

func NewNamedStatement(driver string, templateFunction BindParamTemplateFunction, table *Table) (NamedStatement, error) {
	if table.InsertSql == "" {
		sql, err := NamedInsertSql(table.Name, reflect.TypeOf(table.Fields).Elem())
		if err != nil {
			return NamedStatement{}, err
		}
		table.InsertSql = sql
	}

	h := getHash(driver + table.InsertSql)
	ns, ok := namedStatements[h]
	if !ok {
		ns = NamedStatement{}
		ns.templateFunction = templateFunction
		ns.parameterizeSql(table.InsertSql, table.Fields)
		namedStatements[h] = ns
	}
	return ns, nil
}

func (ns *NamedStatement) ParamArray(data interface{}) []interface{} {
	vals := ValsAsInterfaceArray(data)
	var params []interface{}
	for _, v := range ns.paramMap {
		val := vals[ns.dbfields[v]]
		params = append(params, val)
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

func getHash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}
