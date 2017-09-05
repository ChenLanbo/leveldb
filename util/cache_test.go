package util

import (
  "bytes"
  "testing"
  "unsafe"

  "github.com/chenlanbo/leveldb/db"
)

func testCacheDeleter(key []byte, value uintptr) {
}

func encodeCacheValue(v *[]byte) uintptr {
  return uintptr(unsafe.Pointer(v))
}

func decodeCacheValue(v uintptr) *[]byte {
  return (*[]byte)(unsafe.Pointer(v))
}

func TestHitAndMiss(t *testing.T) {
  cache := NewLRUCache(128)
  hitKey := []byte("123")
  missKey := []byte("456")
  value := []byte("123")

  h := cache.Insert(hitKey, encodeCacheValue(&value), 1, testCacheDeleter)
  defer cache.Release(h)

  if bytes.Compare(value, *decodeCacheValue(cache.Value(h))) != 0 {
    t.Error("Value doesn't match.")
  }

  h1 := cache.Lookup(missKey)
  if h1 != db.NullCacheHandle {
    t.Error("Should return null cache handle for non-exist key.")
  }
}

func TestNewId(t *testing.T) {
  cache := NewLRUCache(128)
  id1 := cache.NewId()
  id2 := cache.NewId()
  if id1 == id2 {
    t.Error("New id should not match.")
  }
}
