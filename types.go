package questdb

import (
	"encoding/base64"
	"fmt"
	"time"
)

// QuestDBType is string which represents a type in the QuestDb world
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
	Binary QuestDBType = "binary"
	// 256-bit unsigned integer
	//		unsupported
	Long256 QuestDBType = "long256"
	// Geohash
	// 		unsupported
	Geohash QuestDBType = "geohash"
)

// serializeValue func takes a value interface{} and a QuestDBType and returns the
// serialized string of that value according to the provided QuestDBType.
func serializeValue(v interface{}, qdbType QuestDBType) (string, error) {
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
			return fmt.Sprintf("%di", val), nil
		}
	case Char:
		switch val := v.(type) {
		case rune:
			return fmt.Sprintf("%c", val), nil
		}
	case Int:
		switch val := v.(type) {
		case uint8, int8, uint16, int16, int32:
			return fmt.Sprintf("%di", val), nil
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
			return fmt.Sprintf("%dt", val), nil
		case time.Time:
			return fmt.Sprintf("%dt", val.UnixMicro()), nil
		}
	case Double:
		switch val := v.(type) {
		case float32, float64:
			return fmt.Sprintf("%f", val), nil
		}
	case Binary:
		switch val := v.(type) {
		case Bytes:
			return fmt.Sprintf("\"%s\"", base64.StdEncoding.EncodeToString(val)), nil
		case string:
			return fmt.Sprintf("\"%s\"", base64.StdEncoding.EncodeToString([]byte(val))), nil
		case []byte:
			return fmt.Sprintf("\"%s\"", base64.StdEncoding.EncodeToString(val)), nil
		}
	}
	return "", fmt.Errorf("type %T is not compatible with %s", v, qdbType)
}

var supportedQDBTypes = []QuestDBType{
	Boolean,
	Byte,
	Short,
	Char,
	Int,
	Float,
	Symbol,
	String,
	Long,
	Date,
	Timestamp,
	Double,
	Binary,
	Long256,
}

// TableNamer is an interface which has a single method, TableName, which
// returns a string representing the struct's table name in QuestDB.
type TableNamer interface {
	TableName() string
}

// isValidAndSupportedQuestDBType func takes a str string and returns a bool representing
// whether or not str is a valid and supported QuestDBType.
func isValidAndSupportedQuestDBType(str QuestDBType) bool {
	for _, kind := range supportedQDBTypes {
		if str == kind {
			return true
		}
	}
	return false
}
