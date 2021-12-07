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
	Boolean   QuestDBType = "boolean"
	Byte      QuestDBType = "byte"
	Short     QuestDBType = "short"
	Char      QuestDBType = "char"
	Int       QuestDBType = "int"
	Float     QuestDBType = "float"
	Symbol    QuestDBType = "symbol"
	String    QuestDBType = "string"
	Long      QuestDBType = "long"
	Date      QuestDBType = "date"
	Timestamp QuestDBType = "timestamp"
	Double    QuestDBType = "double"
	Binary    QuestDBType = "binary"
	Long256   QuestDBType = "long256"
)

var supportedKinds = []string{
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
	t := reflect.TypeOf(a)

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("only structs allowed")
	}

	tableName := fmt.Sprintf("%ss", toSnakeCase(t.Name()))
	symbols := map[string]string{}
	columns := map[string]string{}
	now := time.Now()

	aTableNamer, ok := a.(TableNamer)
	if ok {
		tableName = aTableNamer.TableName()
	}

	fmt.Printf("%s", tableName)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get(tagName)

		fmt.Printf("%d. %v (%v), tag: '%v'\n", i+1, field.Name, field.Type.Name(), tag)
	}

	fmt.Println("Type:", t.Name())
	fmt.Println("Kind:", t.Kind())

	l := NewLine(tableName, symbols, columns, now)

	return l, nil
}

type tag struct {
	fieldName string
	qdb_type  QuestDBType
}

func tagStringToTag(tagStr string) (tag, error) {
	t := tag{}
	tagProps := strings.Split(tagStr, ",")
	if len(tagProps) == 0 {
		return t, nil
	}
	if len(tagProps) == 1 {
		t.fieldName = tagProps[0]
		return t, nil
	}
	if len(tagProps) == 2 {
		if !isValidQuestDBType(tagProps[1]) {
			return t, fmt.Errorf("unsupported type")
		}
		t.fieldName = tagProps[0]
		t.qdb_type = QuestDBType(tagProps[1])
		return t, nil
	}
	return t, fmt.Errorf("invalid 'qdb' struct tag")
}

func isValidQuestDBType(str string) bool {
	for _, kind := range supportedKinds {
		if str == kind {
			return true
		}
	}
	return false
}
