package util

import (
	"fmt"
	"reflect"
)

func StructFields(m interface{}) (string, []reflect.StructField) {
	typ := reflect.TypeOf(m)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		fmt.Printf("%v type can't have attributes inspected\n", typ.Kind())
		return typ.Name(), nil
	}
	var sfl []reflect.StructField
	for i := 0; i < typ.NumField(); i++ {
		sf := typ.Field(i)
		if !sf.Anonymous {
			sfl = append(sfl, sf)
		}
	}
	return typ.Name(), sfl
}

var (
	types = []reflect.Kind{
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,

		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,

		reflect.Float32,
		reflect.Float64,

		reflect.Complex64,
		reflect.Complex128,

		reflect.Bool,
		reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Array,
		reflect.Map,
		reflect.Slice,
		reflect.Struct,

		reflect.String,

		reflect.Ptr,
		reflect.Uintptr,
		reflect.UnsafePointer,
	}
)

func InspectStructV(val reflect.Value) {
	if val.Kind() == reflect.Interface && !val.IsNil() {
		elm := val.Elem()
		if elm.Kind() == reflect.Ptr && !elm.IsNil() && elm.Elem().Kind() == reflect.Ptr {
			val = elm
		}
	}
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	for i := 0; i < val.NumField(); i++ {
		valueField := val.Field(i)
		typeField := val.Type().Field(i)
		address := "not-addressable"

		if valueField.Kind() == reflect.Interface && !valueField.IsNil() {
			elm := valueField.Elem()
			if elm.Kind() == reflect.Ptr && !elm.IsNil() && elm.Elem().Kind() == reflect.Ptr {
				valueField = elm
			}
		}

		if valueField.Kind() == reflect.Ptr {
			valueField = valueField.Elem()

		}
		if valueField.CanAddr() {
			address = fmt.Sprintf("0x%X", valueField.Addr().Pointer())
		}

		fmt.Printf("Field Name: %s,\t Field Value: %v,\t Address: %v\t, Field type: %v\t, Field kind: %v\n", typeField.Name,
			valueField.Interface(), address, typeField.Type, valueField.Kind())

		if valueField.Kind() == reflect.Struct {
			InspectStructV(valueField)
		}
	}
}

func InspectStruct(v interface{}) {
	InspectStructV(reflect.ValueOf(v))
}
