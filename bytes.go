package questdb

import (
	"database/sql/driver"
	"encoding/base64"
	"fmt"
)

type Bytes []byte

func (b Bytes) Value() (driver.Value, error) {
	b64Str := base64.StdEncoding.EncodeToString(b)
	return b64Str, nil
}

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
