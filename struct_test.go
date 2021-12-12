package questdb

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestStructToLine(t *testing.T) {

	type TestTable struct {
		Name        string `qdb:"name,string"`
		AccountUUID string `qdb:"account_uuid,symbol"`
		Age         int32  `qdb:"age,int"`
	}

	t.Run("should successfully create a Line from an arbitrary struct", func(t *testing.T) {

		a := TestTable{
			Name:        "Ahmad Abbasi",
			AccountUUID: uuid.New().String(),
			Age:         29,
		}
		l, err := StructToLine(a)
		assert.Nil(t, err)
		assert.NotNil(t, l)
		t.Log(l.String())

	})
}
