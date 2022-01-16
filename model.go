package questdb

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
)

// Model represents a struct's model
type Model struct {
	tableName          string
	fields             []*field
	indexFields        []*field
	typ                reflect.Type
	val                reflect.Value
	designatedTS       *field
	createTableOptions *CreateTableOptions
}

// field struct represents a field within a valid qdb tagged struct
type field struct {
	isZero          bool
	name            string
	qdbName         string
	qdbType         QuestDBType
	typ             reflect.Type
	value           reflect.Value
	valueSerialized string
	tagOptions      tagOptions
}

// PartitionOption is a string which is used in CreateTableOptions struct
// for specifying the partition by strategy
type PartitionOption string

const (
	None  PartitionOption = "NONE"
	Year  PartitionOption = "YEAR"
	Month PartitionOption = "MONTH"
	Day   PartitionOption = "DAY"
)

// CreateTableOptions struct is a struct which specifies options for creating
// a QuestDB table
type CreateTableOptions struct {
	PartitionBy        PartitionOption
	MaxUncommittedRows int
	CommitLag          string
}

// String func prints out the CreateTableOptions in string format which would be appended
// to the sql create table statement
func (c *CreateTableOptions) String() string {
	out := ""
	if c.PartitionBy != "" {
		out += fmt.Sprintf("PARTITION BY %s ", c.PartitionBy)
	}

	if c.MaxUncommittedRows != 0 {
		out += fmt.Sprintf("WITH maxUncommittedRows=%d ", c.MaxUncommittedRows)
	}

	if c.CommitLag != "" {
		if c.MaxUncommittedRows != 0 {
			out += ", "
		}
		out += fmt.Sprintf("commitLag=%s ", c.CommitLag)
	}
	return out
}

// CreateTableOptioner is an interface which has a single method
// CreateTableOptions which returns the CreateTableOptions struct.
// This is used when specifying specific options for creating a table
// in QuestDB world.
type CreateTableOptioner interface {
	CreateTableOptions() CreateTableOptions
}

// NewModel func takes a struct and returns the Model representation of
// that struct or an optional error
func NewModel(a interface{}) (*Model, error) {
	ty := reflect.TypeOf(a)
	val := reflect.ValueOf(a)

	if ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	if ty.Kind() != reflect.Struct {
		return nil, fmt.Errorf("only structs allowed")
	}

	tableName := fmt.Sprintf("%ss", toSnakeCase(ty.Name()))

	aTableNamer, ok := a.(TableNamer)
	if ok {
		tableName = aTableNamer.TableName()
	}

	m := &Model{
		typ:       ty,
		val:       val,
		tableName: tableName,
	}

	aCreateTableOptioner, ok := a.(CreateTableOptioner)
	if ok {
		opts := aCreateTableOptioner.CreateTableOptions()
		m.createTableOptions = &opts
	}

	fields, err := structToFieldSlice("", "", ty, val)
	if err != nil {
		return nil, fmt.Errorf("could not parse field: %w", err)
	}

	for _, field := range fields {
		if field.tagOptions.designatedTS {
			if m.designatedTS != nil {
				return nil, fmt.Errorf("multiple designated timestamp fields found")
			}
			field2 := field
			m.designatedTS = field2
		}
		if field.tagOptions.index {
			m.indexFields = append(m.indexFields, field)
		}
	}

	m.fields = fields

	if err := m.serialize(); err != nil {
		return nil, err
	}

	return m, nil
}

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
		// skip fields that are marked to ignore
		if tagStr == "-" {
			continue
		}
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

func (m *Model) serialize() error {
	for _, field := range m.fields {

		fieldValue := field.value
		// if fieldValue kind is pointer, get its underlying poited to value
		if fieldValue.Kind() == reflect.Ptr {
			fieldValue = fieldValue.Elem()
		}

		if !fieldValue.IsValid() || fieldValue.IsZero() {
			field.isZero = true
		}

		if field.isZero && !field.tagOptions.commitZeroValue {
			continue
		}

		valStr, err := serializeValue(fieldValue.Interface(), field.qdbType)
		if err != nil {
			return fmt.Errorf("%s: %w", field.name, err)
		}

		field.valueSerialized = valStr
	}
	return nil
}

// Columns func will take return all the model's fields in column format
// i.e. "column_1, column_2, column_3, ..."
func (m *Model) Columns() string {
	out := ""
	for i, field := range m.fields {
		out += field.qdbName
		if i != len(m.fields)-1 {
			out += ", "
		}
	}
	return out
}

// ScanInto func is a helper function which takes a *sql.Row and a dest (an valid qdb model struct)
// and scans the row values into dest. This will typically be used in conjunction with a select statement
// which has used (Model).Columns() to specify the columns for selecting.
func ScanInto(row *sql.Row, dest interface{}) (err error) {
	m, err := NewModel(dest)
	if err != nil {
		return fmt.Errorf("could not make model from dest: %w", err)
	}
	return row.Scan(m.destinations()...)
}

func (m *Model) destinations() []interface{} {
	addrs := []interface{}{}
	for _, field := range m.fields {
		if !field.value.IsValid() {
			fmt.Println(field.name)
		}
		v := field.value.Addr().Interface()
		qdbScanner, ok := v.(Scanner)
		if ok {
			v = newIntermediate(qdbScanner)
		}
		addrs = append(addrs, v)
	}
	return addrs
}

// CreateTableIfNotExistStatement func returns the sql create table statement for
// the Model
func (m *Model) CreateTableIfNotExistStatement() string {
	out := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" ( `, m.tableName)

	// add each qdb column to the create table statement's column definition
	for i, field := range m.fields {
		qdbType := field.qdbType
		// currently encoding binary as base64 encoded string
		if qdbType == Binary || qdbType == JSON {
			qdbType = String
		}
		out += fmt.Sprintf("\"%s\" %s", field.qdbName, qdbType)
		if i != len(m.fields)-1 {
			out += ", "
		}
	}

	// add default designated timestamp field
	if m.designatedTS == nil {
		out += ", \"timestamp\" timestamp"
	}
	out += " ) "

	// if index fields, add them to statement
	indexFieldsLen := len(m.indexFields)
	if indexFieldsLen > 0 {
		out += ", "
		for i, field := range m.indexFields {
			out += fmt.Sprintf("index(%s)", field.qdbName)
			if i != indexFieldsLen-1 {
				out += ", "
			} else {
				out += " "
			}
		}
	}

	// if designatedTS is specified, add to statement, else use default designated TS field
	if m.designatedTS == nil {
		out += "timestamp(timestamp) "
	} else {
		out += fmt.Sprintf("timestamp(%s) ", m.designatedTS.qdbName)
	}

	// if some create table options exists, add them to statement
	if m.createTableOptions != nil {
		out += m.createTableOptions.String()
	}

	// end statement
	out += ";"

	return out
}

func (m *Model) buildSymbols() string {
	if len(m.fields) == 0 {
		return ""
	}

	fields := []*field{}

	for _, field := range m.fields {
		if field.qdbType == Symbol && (!field.isZero || (field.isZero && field.tagOptions.commitZeroValue)) {
			fields = append(fields, field)
		}
	}

	out := ""
	n := 0
	for _, field := range fields {
		out += fmt.Sprintf("%s=%s", field.qdbName, field.valueSerialized)
		if n != len(fields)-1 {
			out += ","
		}
		n++
	}

	return out
}

func (m *Model) buildColumns() string {
	if len(m.fields) == 0 {
		return ""
	}

	fields := []*field{}

	for _, field := range m.fields {
		if field.qdbType != Symbol && (!field.isZero || (field.isZero && field.tagOptions.commitZeroValue)) {
			fields = append(fields, field)
		}
	}

	out := ""
	n := 0
	// decrement one from n counter if designated field is not nil
	if m.designatedTS != nil {
		n = 1
	}
	for _, field := range fields {
		// skip including this in columns field as it will be included in the timestamp section of
		// line message:
		// 			 <table name>,<symbols,...> <columns,...> <timestamp>
		// 												here ----^
		if field.tagOptions.designatedTS {
			continue
		}
		out += fmt.Sprintf("%s=%s", field.qdbName, field.valueSerialized)
		if n != len(fields)-1 {
			out += ","
		}
		n++
	}

	return out
}

func (m *Model) buildTimestamp() string {
	if m.designatedTS != nil && m.designatedTS.value.IsValid() {
		designatedTSTime, ok := m.designatedTS.value.Interface().(time.Time)
		if ok {
			if !designatedTSTime.IsZero() {
				return fmt.Sprintf("%d", designatedTSTime.UnixNano())
			}
		}
	}
	return ""
}

// MarshalLine func marshals Model's underlying struct values into Influx Line Protocol
// message serialization format to be written to the QuestDB ILP port for ingestion.
func (m *Model) MarshalLine() (msg []byte) {
	m.serialize()
	symbolsString := m.buildSymbols()
	columnsString := m.buildColumns()
	timestampString := m.buildTimestamp()

	outString := m.tableName

	if symbolsString != "" {
		outString += fmt.Sprintf(",%s", symbolsString)
	}

	if columnsString != "" {
		outString += fmt.Sprintf(" %s", columnsString)
	}

	if timestampString != "" {
		outString += fmt.Sprintf(" %s", timestampString)
	}

	outString += "\n"

	return []byte(outString)
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

// toSnakeCase func takes a string and returns it's snake case form
func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}
