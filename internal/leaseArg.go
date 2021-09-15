package internal

import (
	"math"
	"reflect"
	"strconv"
)

var _ Unpacker = new(LeaseArg)

type LeaseArg struct {
	Name  string
	Value interface{}
}

func (arg *LeaseArg) Unpack() []interface{} {
	var arr []interface{}

	var (
		name  = arg.Name
		value = arg.Value
	)

	if value != nil {
		var rv reflect.Value
		rv = reflect.ValueOf(value)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			value = strconv.FormatInt(rv.Int(), 10)
		case reflect.Float32, reflect.Float64:
			if math.IsNaN(rv.Float()) {
				break
			}
			value = strconv.FormatFloat(rv.Float(), 'f', 5, 64)
		case reflect.String:
			value = rv.String()
		case reflect.Invalid:
			break
		}

		arr = append(arr, name)
		arr = append(arr, value)
	}
	return arr
}
