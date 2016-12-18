package server

import (
  "github.com/stretchr/testify/assert"
  "testing"
)

func TestDataStore(t *testing.T) {

  ds := newDataStore()

  err := ds.set("k1", []byte("value1"), 0, 0, 0)
  assert.NoError(t, err)

  v, found := ds.get("k1")
  assert.True(t, found)
  assert.Equal(t, v.value, []byte("value1"))

  // TODO(Sunil): add more tests here
}
