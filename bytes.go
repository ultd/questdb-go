package questdb

import (
	"database/sql/driver"
	"encoding/base64"
	"fmt"
)

// Bytes is an alias for []byte. It is used to provide a []byte type that can
// serialized into questdb Value by implementing the QDBValuer interface.
//
// As it stands, QuestDB cannot ingest BYTEA type via Postgres Wire Protocol
// nor can byte slice(s) be serialized into an Influx Line Protocol message. The current work around is
// to convert byte slice(s) to a base64 encoded string and save that to QuestDB. Once QuestDB supports
// byte slice(s) natively via Influx Line Protocol or Postgres Wire protocol, this type will just
// act as an alias to []byte type and the base64 encoding behaviour will be removed.
type Bytes []byte

// Value func implements the driver.Valuer interface
func (b Bytes) Value() (driver.Value, error) {
	b64Str := base64.StdEncoding.EncodeToString(b)
	return b64Str, nil
}

// Value func implements the QDBValuer interface
func (b Bytes) QDBValue() (Value, error) {
	b64Str := base64.StdEncoding.EncodeToString(b)
	return b64Str, nil
}

// Scan func implements the sql.Scanner interface
func (b *Bytes) Scan(src interface{}) error {
	switch val := src.(type) {
	case string:
		by, err := base64.StdEncoding.DecodeString(val)
		if err != nil {
			return fmt.Errorf("could not base64 decode src: %w", err)
		}
		*b = by
		return nil
	case []byte:
		*b = val
		return nil
	default:
		return fmt.Errorf("%T cannot be scanned into Bytes", val)
	}
}
