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

// QBDValuer is the interface providing the Value method.
//
// Types implementing Valuer interface are able to convert
// themselves to a questdb Value.
type QBDValuer interface {
	// QDBValue returns a questdb Value.
	// QDBValue must not panic.
	QDBValue() Value
}
