package server

import (
  "fmt"
  "sync"
)

// This file implement key value data store functionality.

// item wraps the value for a key with additional data like expiry,
// user specified flags and CAS identifier.
type item struct {
  value  []byte
  expiry uint32
  flags  uint32
  cas    uint64
}

// dataStore implements key value store.
type dataStore struct {
  mu sync.RWMutex
  kv map[string]*item
}

func newDataStore() *dataStore {
  return &dataStore{
    kv: make(map[string]*item),
  }
}

// get retrieves item for a given key k.
func (ds *dataStore) get(k string) (*item, bool) {
  ds.mu.RLock()
  defer ds.mu.RUnlock()

  // TODO(sunil): implement expiry
  item, found := ds.kv[k]
  return item, found
}

// set stores value v for key k.
// flags and expiry attributes are also saved.
// if cas is non-zero, then value is updated only if cas matches with
// the item's cas otherwise an error is returned.
func (ds *dataStore) set(k string, v []byte,
  flags uint32, expiry uint32, cas uint64) error {

  ds.mu.Lock()
  defer ds.mu.Unlock()

  it, found := ds.kv[k]
  if found {
    // if key is found
    if cas != 0 {
      if it.cas != cas {
        // cas does not match, so we can't do this operation
        return fmt.Errorf("cas mismatch")
      }
    }
    it.cas++
    it.flags = flags
    if expiry > 0 {
      it.expiry = expiry
    }
    it.value = v
  } else {
    if cas != 0 {
      return fmt.Errorf("key does not exist")
    }
    it = &item{
      value:  v,
      flags:  flags,
      expiry: expiry,
      cas:    1,
    }
  }
  ds.kv[k] = it
  return nil
}
