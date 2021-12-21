package questdb

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

func structToFieldSlice(fieldPrefix, colPrefix string, ty reflect.Type, val reflect.Value) ([]*field, error) {
	if ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	fields := []*field{}

	for i := 0; i < ty.NumField(); i++ {
		fieldType := ty.Field(i)
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
		}
		fieldValue := reflect.ValueOf(nil)
		if val.IsValid() {
			fieldValue = val.Field(i)
		}

		fieldName := fieldPrefix + fieldType.Name

		tagStr := fieldType.Tag.Get(tagName)
		tagProps := strings.Split(tagStr, ";")

		if len(tagProps) < 2 {
			return nil, fmt.Errorf("%s: invalid tag length (expected 2 to 3 semicolon delimited items but got %d)", fieldName, len(tagProps))
		}

		columnName := colPrefix + tagProps[0]
		columnType := tagProps[1]

		f := &field{
			name:            fieldName,
			qdbName:         columnName,
			qdbType:         QuestDBType(columnType),
			typ:             fieldType.Type,
			value:           fieldValue,
			valueSerialized: "",
			tagOptions:      tagOptions{},
		}

		if len(tagProps) > 2 {
			if err := ensureOptionsAreValid(tagProps[2:]); err != nil {
				return nil, fmt.Errorf("%s: invalid tag: %w", fieldName, err)
			}
			opts, err := makeTagOptions(f, tagProps[2:])
			if err != nil {
				return nil, fmt.Errorf("%s: %w", fieldName, err)
			}
			f.tagOptions = opts
		}

		if columnType != "embedded" && !isValidAndSupportedQuestDBType(f.qdbType) {
			return nil, fmt.Errorf("%s: unsupported qdb type %s", fieldName, f.qdbType)
		}

		if columnType == "embedded" && f.tagOptions.embeddedPrefix == "" {
			return nil, fmt.Errorf("%s: 'embeddedPrefix' is required if type is embedded", fieldName)
		}

		if columnType == "embedded" {
			embeddedFields, err := structToFieldSlice(f.name+".", f.tagOptions.embeddedPrefix, f.typ, f.value)
			if err != nil {
				return nil, err
			}
			fields = append(fields, embeddedFields...)
			continue
		}

		fields = append(fields, f)
	}

	return fields, nil
}

// isSerializableType takes a v interface{} and returns a bool which represents
// whether or not v can be serialized into Influx line protocol message value.
func IsSerializableType(v interface{}) bool {
	switch v.(type) {
	case bool, int8, uint8, int16, uint16, int32, uint32, int64, uint64, int, uint,
		float32, float64, string, []byte, time.Time:
		return true
	default:
		return false
	}
}
