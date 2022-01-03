package questdb

// SerializableValue is a value that is one of the following types:
//
//  int
//  uint
//  int16
//  uint16
//  int32
//  uint32
//  int64
//  float64
//  float32
//  bool
//  string
//  time.Time
//  Bytes
//
type Value interface{}

// QBDValuer is the interface providing the QDBValue method.
//
// Types implementing QBDValuer interface are able to convert
// themselves to a questdb Value.
type QBDValuer interface {
	// QDBValue returns a questdb Value.
	// QDBValue must not panic.
	QDBValue() Value
}

// Scanner interface has one method QDBScan which scans a value from
// QuestDB into the type implementing Scanner
type Scanner interface {
	QDBScan(src interface{}) error
}

// intermediate struct is a struct which implements the sql.Scanner interface. It will hold a
// Scanner (v) which will provide the ability to scan a QuestDB value into v's underlying type.
// This struct acts as as proxy to v.
type intermediate struct {
	v Scanner
}

// newIntermediate func returns *intermediate given a Scanner
func newIntermediate(v Scanner) *intermediate {
	return &intermediate{
		v: v,
	}
}

// Scan func is implementation of the sql.Scanner's Scan method which proxies the Scan call to
// intermediate's (v) underlying Scanner QDBScan method.
func (i *intermediate) Scan(src interface{}) error {
	return i.v.QDBScan(src)
}
