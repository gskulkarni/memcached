package server

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDataStore(t *testing.T) {

	ds := newDataStore()

	err := ds.Set("k1", []byte("value1"))
	assert.NoError(t, err)

	v, found := ds.Get("k1")
	assert.True(t, found)
	assert.Equal(t, v, []byte("value1"))

	// TODO(Sunil): add more tests here
}
