package server

import (
  "fmt"
  "sync"
)

type item struct {
  value  []byte
  expiry uint32
  flags  uint32
  cas    uint64
}

type dataStore struct {
  mu sync.RWMutex
  kv map[string]*item
}

func newDataStore() *dataStore {
  return &dataStore{
    kv: make(map[string]*item),
  }
}

func (ds *dataStore) Get(k string) (*item, bool) {
  ds.mu.RLock()
  defer ds.mu.RUnlock()

  // TODO(sunil): implement expiry
  item, found := ds.kv[k]
  return item, found
}

func (ds *dataStore) Set(k string, v []byte,
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
