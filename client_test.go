package questdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	t.Run("should return a client and no error if passed valid config", func(t *testing.T) {
		client, err := New(Config{})
		assert.Nil(t, err)
		assert.NotNil(t, client)
	})
}

func TestClient_Connect(t *testing.T) {
	t.Run("should successfully connect given proper config", func(t *testing.T) {
		client := Default()

		err := client.Connect()

		assert.Nil(t, err)
	})
}

func TestClient_Close(t *testing.T) {
	t.Run("should successfully close client", func(t *testing.T) {
		client := Default()

		err := client.Connect()

		assert.Nil(t, err)

		err = client.Close()

		assert.Nil(t, err)
	})
}

func TestClientWriteDataThenRead(t *testing.T) {
	client := Default()

	err := client.Connect()
	assert.Nil(t, err)

	now := time.Now()
	err = client.WriteMessage([]byte(fmt.Sprintf("table_abc,symbol_a=abcd1234 col_a=42323532i,col_b=f,ts=%dt %d\n", now.UnixMicro(), now.UnixNano())))
	assert.Nil(t, err)

	row := client.DB().QueryRowContext(context.Background(), "SELECT col_a FROM table_abc WHERE symbol_a = 'abcd1234'")

	err = row.Err()
	assert.Nil(t, err)

	someInt := 0

	err = row.Scan(&someInt)
	assert.Nil(t, err)

	assert.Equal(t, 42323532, someInt)

}
