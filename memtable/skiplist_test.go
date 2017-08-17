package memtable

import (
  "math/rand"
  "testing"

  "github.com/chenlanbo/leveldb"
  "github.com/chenlanbo/leveldb/util"
)

func TestInsertAndContains(t *testing.T) {
  s := NewSkipList(leveldb.DefaultComparator, util.NewArena())

  for i := 0; i < 100; i++ {
    key := make([]byte, 128)
    rand.Read(key)
    s.Insert(key)
    if !s.Contains(key) {
      t.Error("Skip list doesn't contain key just inserted.")
    }
  }
}

func TestIteratorSeekToFirst(t *testing.T) {
  s := NewSkipList(leveldb.DefaultComparator, util.NewArena())
  iter := s.Iterator()

  iter.SeekToFirst()
  if iter.Valid() {
    t.Error("Iterator should not be valid if the skiplist is empty.")
  }

  keys := []string{"a", "b", "c", "d", "e"}
  for _, key := range(keys) {
    s.Insert([]byte(key))
  }

  iter.SeekToFirst()
  if leveldb.DefaultComparator.Compare(iter.Key(), []byte("a")) != 0 {
    t.Error("Skip list is not ordered.")
  }
}

func TestIteratorSeekToLast(t *testing.T) {
  s := NewSkipList(leveldb.DefaultComparator, util.NewArena())
  iter := s.Iterator()

  iter.SeekToLast()
  if iter.Valid() {
    t.Error("Iterator should not be valid if the skiplist is empty.")
  }

  keys := []string{"a", "b", "c", "d", "e"}
  for _, key := range(keys) {
    s.Insert([]byte(key))
  }

  iter.SeekToLast()
  if leveldb.DefaultComparator.Compare(iter.Key(), []byte("e")) != 0 {
    t.Error("Skip list is not ordered.")
  }
}

func TestIteratorSeek(t *testing.T) {
  s := NewSkipList(leveldb.DefaultComparator, util.NewArena())
  iter := s.Iterator()

  keys := []string{"c", "e", "f", "d", "b"}
  for _, key := range(keys) {
    s.Insert([]byte(key))
  }

  iter.Seek([]byte("c"))
  if leveldb.DefaultComparator.Compare(iter.Key(), []byte("c")) != 0 {
    t.Error("Skiplist is not ordered.")
  }

  iter.Seek([]byte("g"))
  if iter.Valid() {
    t.Error("Is greater than the biggest key in the skiplist.")
  }

  iter.Seek([]byte("a"))
  if !iter.Valid() {
    t.Error("It's OK to be smaller than the smallest key in the skiplist.")
  }
}

func TestIteratorPrevNext(t *testing.T) {
  s := NewSkipList(leveldb.DefaultComparator, util.NewArena())
  iter := s.Iterator()

  keys := []string{"c", "e", "a", "d", "b"}
  for _, key := range(keys) {
    s.Insert([]byte(key))
  }

  iter.SeekToFirst()
  iter.Next()
  if leveldb.DefaultComparator.Compare(iter.Key(), []byte("b")) != 0 {
    t.Error("Skip list is not ordered.")
  }
  iter.Prev()
  if leveldb.DefaultComparator.Compare(iter.Key(), []byte("a")) != 0 {
    t.Error("Skip list is not ordered.")
  }
  iter.Next()
  if leveldb.DefaultComparator.Compare(iter.Key(), []byte("b")) != 0 {
    t.Error("Skip list is not ordered.")
  }

  iter.SeekToLast()
  iter.Prev()
  if leveldb.DefaultComparator.Compare(iter.Key(), []byte("d")) != 0 {
    t.Error("Skip list is not ordered.")
  }
}
