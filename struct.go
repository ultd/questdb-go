package questdb

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
)

const tagName = "qdb"

type QuestDBType string

var (
	// boolean (true or false)
	Boolean QuestDBType = "boolean"
	// 8 bit signed integer (-128 to 127)
	Byte QuestDBType = "byte"
	// 16-bit signed integer (-32768 to 32767)
	Short QuestDBType = "short"
	// 16-bit unicode charachter
	Char QuestDBType = "char"
	// 32-bit signed integer (0x80000000 to 0x7fffffff)
	Int QuestDBType = "int"
	// 32-bit float (float32 - single precision IEEE 754)
	Float QuestDBType = "float"
	// variable length string (see QuestDB docs)
	Symbol QuestDBType = "symbol"
	// variable length string
	String QuestDBType = "string"
	// 64-bit signed integer (0x8000000000000000L to 0x7fffffffffffffffL)
	Long QuestDBType = "long"
	// 64-bit signed offset in milliseconds from Unix Epoch
	Date QuestDBType = "date"
	// 64-bit signed offset in microseconds from Unix Epoch
	Timestamp QuestDBType = "timestamp"
	// 64-bit float (float64 - double precision IEEE 754)
	Double QuestDBType = "double"
	// byte array
	// 		unsupported
	Binary QuestDBType = "binary"
	// 256-bit unsigned integer
	//		unsupported
	Long256 QuestDBType = "long256"
	// Geohash
	// 		unsupported
	Geohash QuestDBType = "geohash"
)

func toString(v interface{}, qdbType QuestDBType) (string, error) {
	switch qdbType {
	case Boolean:
		switch val := v.(type) {
		case bool:
			return fmt.Sprintf("%t", val), nil
		}
	case Byte:
		switch val := v.(type) {
		case int8:
			return fmt.Sprintf("%d", val), nil
		}
	case Short:
		switch val := v.(type) {
		case uint8, int8, int16:
			return fmt.Sprintf("%d", val), nil
		}
	case Char:
		switch val := v.(type) {
		case rune:
			return fmt.Sprintf("%c", val), nil
		}
	case Int:
		switch val := v.(type) {
		case uint8, int8, uint16, int16, int32:
			return fmt.Sprintf("%d", val), nil
		}
	case Float:
		switch val := v.(type) {
		case float32:
			return fmt.Sprintf("%f", val), nil
		}
	case Symbol:
		switch val := v.(type) {
		case string:
			return val, nil
		}
	case String:
		switch val := v.(type) {
		case string:
			return fmt.Sprintf("\"%s\"", val), nil
		}
	case Long:
		switch val := v.(type) {
		case uint8, int8, uint16, int16, uint32, int32, int64, int:
			return fmt.Sprintf("%di", val), nil
		}
	case Date:
		switch val := v.(type) {
		case int64:
			return fmt.Sprintf("%d", val), nil
		case time.Time:
			return fmt.Sprintf("%d", val.UnixMilli()), nil
		}
	case Timestamp:
		switch val := v.(type) {
		case int64:
			return fmt.Sprintf("%d", val), nil
		case time.Time:
			return fmt.Sprintf("%d", val.UnixMicro()), nil
		}
	case Double:
		switch val := v.(type) {
		case float32, float64:
			return fmt.Sprintf("%f", val), nil
		}
	}
	return "", fmt.Errorf("type %T is not compatible %s", v, qdbType)
}

// var questDBTypeToReflectKind = map[QuestDBType]reflect.Kind{
// 	Boolean:   reflect.Bool,
// 	Byte:      reflect.Int8,
// 	Short:     reflect.Int16,
// 	Char:      reflect.Int32,
// 	Int:       reflect.Int32,
// 	Float:     reflect.Float32,
// 	Symbol:    reflect.String,
// 	String:    reflect.String,
// 	Long:      reflect.Int64,
// 	Date:      reflect.Int64,
// 	Timestamp: reflect.Int64,
// 	Double:    reflect.Float64,
// 	Binary:    reflect.Array,
// }

// var reflectKindToDefaultQuestDBType = map[reflect.Kind]QuestDBType{
// 	reflect.Bool:    Boolean,
// 	reflect.Int8:    Byte,
// 	reflect.Int16:   Short,
// 	reflect.Int32:   Int,
// 	reflect.Float32: Float,
// 	reflect.String:  String,
// 	reflect.Int64:   Long,
// 	reflect.Float64: Double,
// 	reflect.Array:   Binary,
// }

var supportedTypes = []string{
	string(Boolean),
	string(Byte),
	string(Short),
	string(Char),
	string(Int),
	string(Float),
	string(Symbol),
	string(String),
	string(Long),
	string(Date),
	string(Timestamp),
	string(Double),
	string(Binary),
	string(Long256),
}

type TableNamer interface {
	TableName() string
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

func StructToLine(a interface{}) (*Line, error) {
	ty := reflect.TypeOf(a)
	val := reflect.ValueOf(a)

	if ty.Kind() == reflect.Ptr {
		val = val.Elem()
		ty = reflect.TypeOf(val.Interface())
	}

	if ty.Kind() != reflect.Struct {
		return nil, fmt.Errorf("only structs allowed")
	}

	tableName := fmt.Sprintf("%ss", toSnakeCase(ty.Name()))
	symbols := map[string]string{}
	columns := map[string]string{}
	now := time.Now()

	aTableNamer, ok := a.(TableNamer)
	if ok {
		tableName = aTableNamer.TableName()
	}

	for i := 0; i < ty.NumField(); i++ {
		field := ty.Field(i)
		val := val.Field(i)

		if field.Type.Kind() == reflect.Ptr {
			if val.IsNil() {
				continue
			}
			val = val.Elem()
		}

		col, err := fieldToColumn(field, val.Interface())
		if err != nil {
			return nil, fmt.Errorf("%s: %w", field.Name, err)
		}

		if col.qdbType == Symbol {
			symbols[col.name] = col.valueStr
		} else {
			columns[col.name] = col.valueStr
		}
	}

	l := NewLine(tableName, symbols, columns, now)

	return l, nil
}

type column struct {
	name     string `qdb:"name,long"`
	qdbType  QuestDBType
	value    interface{}
	valueStr string
}

func fieldToColumn(field reflect.StructField, value interface{}) (*column, error) {
	tagStr := field.Tag.Get(tagName)
	tagProps := strings.Split(tagStr, ",")

	if len(tagProps) != 2 {
		return nil, fmt.Errorf("invalid tag length (expected 2 comma delimited items but got %d)", len(tagProps))
	}

	col := &column{}

	qdbColumnName := tagProps[0]
	qdbColumnType := tagProps[1]
	if !isValidQuestDBType(qdbColumnType) {
		return col, fmt.Errorf("unsupported tag type %s", qdbColumnType)
	}

	col.name = qdbColumnName
	col.qdbType = QuestDBType(qdbColumnType)
	col.value = value

	// valStr, err := marshalIntoLineFormat(qdbColumnName, value, col.qdbType)
	valStr, err := toString(col.value, col.qdbType)
	if err != nil {
		return nil, fmt.Errorf("could not marshal type %t into questdb type %s: %w", value, col.qdbType, err)
	}
	col.valueStr = valStr

	return col, nil
}

// func marshalIntoLineFormat(columnName string, value interface{}, qdbType QuestDBType) (string, error) {
// 	outStr := ""
// 	switch val := value.(type) {
// 	case bool:
// 		if qdbType != Boolean {
// 			return "", fmt.Errorf("type in tag is not boolean")
// 		}
// 		if val {
// 			outStr = "t"
// 		} else {
// 			outStr = "f"
// 		}
// 	case string:
// 		if qdbType != String && qdbType != Symbol {
// 			return "", fmt.Errorf("type in tag is not Symbol or String")
// 		}

// 		if qdbType == String {
// 			outStr = fmt.Sprintf("\"%s\"", val)
// 		} else {
// 			outStr = val
// 		}
// 	case byte:
// 		if qdbType != Byte {
// 			return "", fmt.Errorf("type in tag is not Byte")
// 		}

// 		outStr = fmt.Sprintf("%d", val)
// 	case int16:
// 		if qdbType != Short {
// 			return "", fmt.Errorf("type in tag is not Short")
// 		}

// 		outStr = fmt.Sprintf("%d", val)
// 	case int32:
// 		if qdbType != Int {
// 			return "", fmt.Errorf("type in tag is not Int")
// 		}

// 		outStr = fmt.Sprintf("%d", val)
// 	case int64:
// 		if qdbType != Long {
// 			return "", fmt.Errorf("type in tag is not Long")
// 		}

// 		outStr = fmt.Sprintf("%di", val)
// 	case float32:
// 		if qdbType != Float {
// 			return "", fmt.Errorf("type in tag is not Float")
// 		}

// 		outStr = fmt.Sprintf("%f", val)

// 	case float64:
// 		if qdbType != Double {
// 			return "", fmt.Errorf("type in tag is not Double")
// 		}

// 		outStr = fmt.Sprintf("%f", val)
// 	case time.Time:
// 		if qdbType != Date && qdbType != Timestamp {
// 			return "", fmt.Errorf("type in tag is not Date or Timestamp")
// 		}

// 		if qdbType == Date {
// 			outStr = fmt.Sprintf("%d", val.UnixMilli())
// 		} else {
// 			outStr = fmt.Sprintf("%d", val.UnixMicro())
// 		}
// 	default:
// 		return "", fmt.Errorf("type of field is not supported")
// 	}

// 	return outStr, nil
// }

func isValidQuestDBType(str string) bool {
	for _, kind := range supportedTypes {
		if str == kind {
			return true
		}
	}
	return false
}
