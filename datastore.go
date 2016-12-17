package main

import (
  "sync"
)

type dataStore struct {
  mu sync.RWMutex
  kv map[string][]byte
}

func NewDataStore() *dataStore {
  return &dataStore{
    kv: make(map[string][]byte),
  }
}

func (ds *dataStore) Get(k string) ([]byte, bool) {
  ds.mu.RLock()
  defer ds.mu.RUnlock()

  v, found := ds.kv[k]
  return v, found
}

func (ds *dataStore) Set(k string, v []byte) error {
  ds.mu.Lock()
  defer ds.mu.Unlock()

  ds.kv[k] = v
  return nil
}
