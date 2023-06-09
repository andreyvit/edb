package edb

import (
	"fmt"
	"reflect"
	"sync"
)

var typeInfoCache sync.Map

type structInfo struct {
	keyField reflect.StructField
}

func (si *structInfo) keyValue(rowVal reflect.Value) reflect.Value {
	return rowVal.Elem().FieldByIndex(si.keyField.Index)
}

func reflectType(typ reflect.Type) *structInfo {
	if v, ok := typeInfoCache.Load(typ); ok {
		return v.(*structInfo)
	}
	info := reflectTypeWithoutCache(typ)
	actual, _ := typeInfoCache.LoadOrStore(typ, info)
	return actual.(*structInfo)
}

func reflectTypeWithoutCache(typ reflect.Type) *structInfo {
	if typ.Kind() != reflect.Ptr {
		panic(fmt.Errorf("%v not a pointer", typ))
	}
	typ = typ.Elem()
	if typ.Kind() != reflect.Struct {
		panic(fmt.Errorf("%v not a struct", typ))
	}
	if typ.NumField() == 0 {
		panic(fmt.Errorf("%v is an empty struct", typ))
	}
	keyField := typ.Field(0)
	if !keyField.IsExported() {
		panic(fmt.Errorf("key field %v.%s must be exported", typ, keyField.Name))
	}

	info := &structInfo{
		keyField: keyField,
	}
	return info
}
