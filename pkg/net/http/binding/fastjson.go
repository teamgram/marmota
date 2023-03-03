package binding

import (
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/valyala/fastjson"
)

type fastjsonBinding struct{}

func (fastjsonBinding) Name() string {
	return "fastjson"
}

func (fastjsonBinding) Bind(req *http.Request, obj interface{}) error {
	readBodyF := func() ([]byte, error) {
		defer req.Body.Close()
		return io.ReadAll(req.Body)
	}

	b, err := readBodyF()
	if err != nil {
		return errors.WithStack(err)
	}

	var (
		v *fastjson.Value
	)

	v, err = fastjson.ParseBytes(b)
	if err != nil {
		return errors.WithStack(err)
	}

	if err = mapFastjson(obj, v); err != nil {
		return err
	}
	return validate(obj)
}

func mapFastjson(ptr interface{}, v *fastjson.Value) error {
	sinfo2 := scache.get(reflect.TypeOf(ptr))
	val := reflect.ValueOf(ptr).Elem()
	for i, fd := range sinfo2.field {
		typeField := fd.tp
		structField := val.Field(i)
		if !structField.CanSet() {
			continue
		}

		structFieldKind := structField.Kind()
		inputFieldName := fd.name
		if inputFieldName == "" {
			inputFieldName = typeField.Name

			// if "form" tag is nil, we inspect if the field is a struct.
			// this would not make sense for JSON parsing but it does for a form
			// since data is flatten
			if structFieldKind == reflect.Struct {
				err := mapFastjson(structField.Addr().Interface(), v)
				if err != nil {
					return err
				}
				continue
			}
		}
		exists := v.Exists(inputFieldName)
		if !exists {
			fmt.Println("exists: ", inputFieldName)
			// Set the field as default value when the input value is not exist
			if fd.hasDefault {
				structField.Set(fd.defaultValue)
			}
			continue
		}
		inputValue := strings.Trim(v.Get(inputFieldName).String(), "\"")
		// Set the field as default value when the input value is empty
		if fd.hasDefault && inputValue == "" {
			structField.Set(fd.defaultValue)
			continue
		}
		if _, isTime := structField.Interface().(time.Time); isTime {
			if err := setTimeField(inputValue, typeField, structField); err != nil {
				return err
			}
			continue
		}
		if err := setWithProperType(typeField.Type.Kind(), []string{inputValue}, structField, fd.option); err != nil {
			return err
		}
	}
	return nil
}
