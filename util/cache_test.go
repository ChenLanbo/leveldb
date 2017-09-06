package util

import (
  "bytes"
  "fmt"
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
  value1 := []byte("123")
  value2 := []byte("124")

  h1 := cache.Insert([]byte("123"), encodeCacheValue(&value1), 1, testCacheDeleter)
  defer cache.Release(h1)

  if bytes.Compare(value1, *decodeCacheValue(cache.Value(h1))) != 0 {
    t.Error("Value doesn't match.")
  }

  // Inspect the underlying struct.
  hh := (*lruCacheHandle)(unsafe.Pointer(uintptr(h1)))
  if hh.refs != 2 {
    t.Error("")
  }

  h2 := cache.Lookup([]byte("123"))
  defer cache.Release(h2)
  if hh.refs != 3 {
    t.Error("")
  }

  // Insert the same key with a different value.
  h3 := cache.Insert([]byte("123"), encodeCacheValue(&value2), 1, testCacheDeleter)
  if bytes.Compare(value2, *decodeCacheValue(cache.Value(h3))) != 0 {
    t.Error("Value doesn't match.")
  }
  if hh.refs != 2 {
    t.Error("")
  }

  // A cache miss.
  hNull := cache.Lookup([]byte("456"))
  if hNull != db.NullCacheHandle {
    t.Error("Should return null cache handle for non-exist key.")
  }

  // Add a new cache entry, release and then lookup.
  h4 := cache.Insert([]byte("100"), encodeCacheValue(&value2), 1, testCacheDeleter)
  cache.Release(h4)

  h4 = cache.Lookup([]byte("100"))
  defer cache.Release(h4)
  hh = (*lruCacheHandle)(unsafe.Pointer(uintptr(h4)))
  if hh.refs != 2 || !hh.inCache {
    t.Error("")
  }
}

func TestFrequentEviction(t *testing.T) {
  cache := NewLRUCache(48)
  v1 := []byte("101")
  v2 := []byte("201")
  v3 := []byte("301")
  h1 := cache.Insert([]byte("100"), encodeCacheValue(&v1), 1, testCacheDeleter)
  cache.Release(h1)
  h2 := cache.Insert([]byte("200"), encodeCacheValue(&v2), 1, testCacheDeleter)
  cache.Release(h2)
  h3 := cache.Insert([]byte("300"), encodeCacheValue(&v3), 1, testCacheDeleter)
  defer cache.Release(h3)

  // Frequent used entry should be kept.
  for i := 1000; i < 2000; i++ {
    v := []byte(fmt.Sprint(i))
    h := cache.Insert([]byte(fmt.Sprint(i)), encodeCacheValue(&v), 1, testCacheDeleter)
    cache.Release(h)
    h2 = cache.Lookup([]byte("200"))
    if h2 == db.NullCacheHandle {
      panic("")
    }
    if bytes.Compare([]byte("201"), *decodeCacheValue(cache.Value(h2))) != 0 {
      t.Error("")
    }
    cache.Release(h2)
  }

  h2 = cache.Lookup([]byte("200"))
  defer cache.Release(h2)
  if bytes.Compare([]byte("201"), *decodeCacheValue(cache.Value(h2))) != 0 {
    t.Error("")
  }

  h1 = cache.Lookup([]byte("100"))
  if h1 != db.NullCacheHandle {
    t.Error("")
  }
}

func TestErase(t *testing.T) {
  cache := NewLRUCache(16)
  v1 := []byte("101")
  h1 := cache.Insert([]byte("100"), encodeCacheValue(&v1), 1, testCacheDeleter)

  cache.Erase([]byte("100"))
  h2 := cache.Lookup([]byte("100"))
  if h2 != db.NullCacheHandle {
    t.Error("")
  }

  cache.Release(h1)
  hh := (*lruCacheHandle)(unsafe.Pointer(uintptr(h1)))
  if hh.refs != 0 || hh.inCache {
    t.Error("")
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
