package util

import (
  "bytes"
  "container/list"
  "sync"
  "unsafe"

  "github.com/chenlanbo/leveldb/db"
)

const (
  numShardBits = 4
  numShards = 1 << numShardBits
)

type lruCacheHandle struct {
  key []byte
  value uintptr
  hash uint32
  refs uint32
  charge int
  inCache bool
  next *lruCacheHandle
  prev *lruCacheHandle
  deleter db.CacheEntryDeleter
}

// Simple hash table for lru cache handle
type handleTable map[uint32]*list.List

func newHandleTable() handleTable {
  t := make(map[uint32]*list.List, 0)
  return t
}

func (t handleTable) Lookup(key []byte, hash uint32) *lruCacheHandle {
  l, prs := t[hash]
  if !prs {
    return nil
  }

  for e := l.Front(); e != nil; e = e.Next() {
    h := e.Value.(*lruCacheHandle)
    if bytes.Compare(h.key, key) == 0 {
      return h
    }
  }

  return nil
}

func (t handleTable) Insert(handle *lruCacheHandle) *lruCacheHandle {
  l, prs := t[handle.hash]
  if !prs {
    t[handle.hash] = list.New()
  }
  l = t[handle.hash]
  var pe *list.Element = nil
  for e := l.Front(); e != nil; e = e.Next() {
    h := e.Value.(*lruCacheHandle)
    if bytes.Compare(h.key, handle.key) == 0 {
      pe = e
      break
    }
  }
  l.PushFront(handle)
  if pe != nil {
    l.Remove(pe)
    return pe.Value.(*lruCacheHandle)
  }
  return nil
}

func (t handleTable) Remove(key []byte, hash uint32) *lruCacheHandle {
  l, prs := t[hash]
  if !prs {
    return nil
  }

  var entry *list.Element = nil
  for e := l.Front(); e != nil; e = e.Next() {
    h := e.Value.(*lruCacheHandle)
    if bytes.Compare(h.key, key) == 0 {
      entry = e
      break
    }
  }
  l.Remove(entry)
  return entry.Value.(*lruCacheHandle)
}


type lruCache struct {
  mu sync.Mutex

  usage int
  capacity int
  lru lruCacheHandle
  inUse lruCacheHandle
  table handleTable
}

func newLruCache(capacity int) *lruCache {
  cache := &lruCache{}
  cache.SetCapacity(capacity)
  cache.table = newHandleTable()
  cache.lru.next = &cache.lru
  cache.lru.prev = &cache.lru
  cache.inUse.next = &cache.inUse
  cache.inUse.prev = &cache.inUse

  return cache
}

func (cache *lruCache) SetCapacity(capacity int) {
  cache.capacity = capacity
}

func (cache *lruCache) Insert(key []byte, hash uint32, value uintptr, charge int, deleter db.CacheEntryDeleter) db.CacheHandle {
  cache.mu.Lock()
  defer cache.mu.Unlock()

  e := &lruCacheHandle{}
  e.key = make([]byte, len(key))
  copy(e.key, key)
  e.value = value
  e.hash = hash
  e.refs = 1  // For the returned handle
  e.charge = charge
  e.inCache = false
  e.next = nil
  e.prev = nil
  e.deleter = deleter

  if cache.capacity > 0 {
    e.refs++  // For the cache's reference
    e.inCache = true
    cache.LRUAppend(&cache.inUse, e)
    cache.usage += charge
    cache.FinishErase(cache.table.Insert(e))
  }

  for cache.usage > cache.capacity && cache.lru.next != &cache.lru {
    old := cache.lru.next
    cache.FinishErase(cache.table.Remove(old.key, old.hash))
  }

  return db.CacheHandle(uintptr(unsafe.Pointer(e)))
}

func (cache *lruCache) Lookup(key []byte, hash uint32) db.CacheHandle {
  cache.mu.Lock()
  defer cache.mu.Unlock()
  e := cache.table.Lookup(key, hash)
  if e != nil {
    cache.Ref(e)
  }
  if e == nil {
    return db.CacheHandle(0)
  }
  return db.CacheHandle(uintptr(unsafe.Pointer(e)))
}

func (cache *lruCache) Release(handle db.CacheHandle) {
  cache.mu.Lock()
  defer cache.mu.Unlock()
  e := (*lruCacheHandle)(unsafe.Pointer(uintptr(handle)))
  cache.Unref(e)
}

func (cache *lruCache) Erase(key []byte, hash uint32) {
  cache.mu.Lock()
  defer cache.mu.Unlock()
  cache.FinishErase(cache.table.Remove(key, hash))
}

func (cache *lruCache) Prune() {
  cache.mu.Lock()
  defer cache.mu.Unlock()

  for cache.lru.next != &cache.lru {
    e := cache.lru.next
    if e.refs != 1 {
      panic("")
    }
    cache.FinishErase(cache.table.Remove(e.key, e.hash))
  }
}

func (cache *lruCache) TotalCharge() int {
  cache.mu.Lock()
  defer cache.mu.Unlock()

  return cache.usage
}

// Private methods
func (cache *lruCache) Ref(handle *lruCacheHandle) {
  if handle.refs == 1 && handle.inCache {
    cache.LRURemove(handle)
    cache.LRUAppend(&cache.inUse, handle)
  }
  handle.refs++
}

func (cache *lruCache) Unref(handle *lruCacheHandle) {
  if handle.refs == 0 {
    panic("")
  }
  handle.refs--
  if handle.refs == 0 {
    if handle.inCache {
      panic("")
    }
    handle.deleter(handle.key, handle.value)
  } else if handle.inCache && handle.refs == 1 {
    cache.LRURemove(handle)
    cache.LRUAppend(&cache.lru, handle)
  }
}

func (cache *lruCache) LRURemove(handle *lruCacheHandle) {
  handle.next.prev = handle.prev
  handle.prev.next = handle.next
}

func (cache *lruCache) LRUAppend(l, handle *lruCacheHandle) {
  handle.next = l
  handle.prev = l.prev
  handle.prev.next = handle
  handle.next.prev = handle
}

func (cache *lruCache) FinishErase(handle *lruCacheHandle) bool {
  if handle != nil {
    if !handle.inCache {
      panic("")
    }
    cache.LRURemove(handle)
    handle.inCache = false
    cache.usage -= handle.charge
    cache.Unref(handle)
  }

  return handle != nil
}

type sharedLruCache struct {
  mu sync.Mutex

  shard [numShards]*lruCache
  lastId uint64
}

func NewLRUCache(capacity int) db.Cache {
  cache := &sharedLruCache{}
  perShardCapacity := (capacity + numShards - 1) / numShards
  for i := 0 ; i < numShards; i++ {
    cache.shard[i] = newLruCache(perShardCapacity)
  }
  cache.lastId = 0
  return cache
}

func (cache *sharedLruCache) Insert(key []byte, value uintptr, charge int, deleter db.CacheEntryDeleter) db.CacheHandle {
  h := cache.HashKey(key)
  return cache.shard[cache.Shard(h)].Insert(key, h, value, charge, deleter)
}

func (cache *sharedLruCache) Lookup(key []byte) db.CacheHandle {
  h := cache.HashKey(key)
  return cache.shard[cache.Shard(h)].Lookup(key, h)
}

func (cache *sharedLruCache) Release(handle db.CacheHandle) {
  e := (*lruCacheHandle)(unsafe.Pointer(uintptr(handle)))
  cache.shard[cache.Shard(e.hash)].Release(handle)
}

func (cache *sharedLruCache) Erase(key []byte ) {
  h := cache.HashKey(key)
  cache.shard[cache.Shard(h)].Erase(key, h)
}

func (cache *sharedLruCache) Value(handle db.CacheHandle) uintptr {
  e := (*lruCacheHandle)(unsafe.Pointer(uintptr(handle)))
  return e.value
}

func (cache *sharedLruCache) NewId() uint64 {
  cache.mu.Lock()
  defer cache.mu.Unlock()
  cache.lastId++
  return cache.lastId
}

func (cache *sharedLruCache) Prune() {
  for i := 0; i < numShards; i++ {
    cache.shard[i].Prune()
  }
}

func (cache *sharedLruCache) TotalCharge() int {
  total := 0
  for i := 0; i < numShards; i++ {
    total += cache.shard[i].TotalCharge()
  }
  return total
}

func (cache *sharedLruCache) HashKey(key []byte) uint32 {
  return Hash(key, 0xbc9f1d34)
}

func (cache *sharedLruCache) Shard(hash uint32) int {
  return int(hash >> (32 - numShardBits))
}
