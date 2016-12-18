package server

import (
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

  // TODO(sunil): implement cas
  // implement other field as optional params
  ds.kv[k] = &item{
    value:  v,
    flags:  flags,
    expiry: expiry,
    cas:    cas,
  }
  return nil
}
