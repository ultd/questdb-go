package questdb

import (
	"testing"

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
