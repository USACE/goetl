package dbetl

import "reflect"

func TagAndVals(tag string, data interface{}) ([]string, []interface{}) {
	val := reflect.ValueOf(data).Elem()
	typ := reflect.TypeOf(data).Elem()
	fieldNum := val.NumField()
	tags := make([]string, fieldNum)
	ia := make([]interface{}, fieldNum)
	for i := 0; i < fieldNum; i++ {
		if tagval, ok := typ.Field(i).Tag.Lookup(tag); ok {
			tags[i] = tagval
		}
		ia[i] = val.Field(i).Addr().Interface()
	}
	return tags, ia
}

func TagAsPositionMap(tag string, data interface{}) map[string]int {
	tagmap := make(map[string]int)
	typ := reflect.TypeOf(data).Elem()
	fieldNum := typ.NumField()
	for i := 0; i < fieldNum; i++ {
		if tagval, ok := typ.Field(i).Tag.Lookup(tag); ok {
			tagmap[tagval] = i
		}
	}
	return tagmap
}

func TagAsStringArray(tag string, data interface{}) []string {
	typ := reflect.TypeOf(data).Elem()
	fieldNum := typ.NumField()
	tags := make([]string, fieldNum)
	for i := 0; i < fieldNum; i++ {
		if tagval, ok := typ.Field(i).Tag.Lookup(tag); ok {
			tags[i] = tagval
		}
	}
	return tags
}

func ValsAsInterfaceArray(data interface{}) []interface{} {
	val := reflect.ValueOf(data).Elem()
	fieldNum := val.NumField()
	ia := make([]interface{}, fieldNum)
	for i := 0; i < fieldNum; i++ {
		valField := val.Field(i)
		ia[i] = valField.Addr().Interface()
	}
	return ia
}
