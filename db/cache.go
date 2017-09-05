package db

import (
)

type CacheHandle uint64
type CacheEntryDeleter func(key []byte, value uintptr)

const (
  NullCacheHandle = CacheHandle(0)
)

type Cache interface {

  Insert(key []byte, value uintptr, charge int, deleter CacheEntryDeleter) CacheHandle
  Lookup(key []byte) CacheHandle
  Release(handle CacheHandle)
  Value(handle CacheHandle) uintptr
  Erase(key []byte)
  NewId() uint64
  Prune()
  TotalCharge() int
}

