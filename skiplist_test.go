package leveldb

import (
  "fmt"
  "math/rand"
  "testing"
)

func TestInsertAndContains(t *testing.T) {
  s := NewSkipList(DefaultComparator, NewArena())

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
  s := NewSkipList(DefaultComparator, NewArena())
  iter := s.NewIterator()

  iter.SeekToFirst()
  if iter.Valid() {
    t.Error("Iterator should not be valid if the skiplist is empty.")
  }

  keys := []string{"a", "b", "c", "d", "e"}
  for _, key := range(keys) {
    s.Insert([]byte(key))
  }

  iter.SeekToFirst()
  if DefaultComparator.Compare(iter.Key(), []byte("a")) != 0 {
    t.Error("Skip list is not ordered.")
  }
}

func TestIteratorSeekToLast(t *testing.T) {
  s := NewSkipList(DefaultComparator, NewArena())
  iter := s.NewIterator()

  iter.SeekToLast()
  if iter.Valid() {
    t.Error("Iterator should not be valid if the skiplist is empty.")
  }

  keys := []string{"a", "b", "c", "d", "e"}
  for _, key := range(keys) {
    s.Insert([]byte(key))
  }

  iter.SeekToLast()
  if DefaultComparator.Compare(iter.Key(), []byte("e")) != 0 {
    t.Error("Skip list is not ordered.")
  }
}

func TestIteratorSeek(t *testing.T) {
  s := NewSkipList(DefaultComparator, NewArena())
  iter := s.NewIterator()

  keys := []string{"c", "e", "f", "d", "b"}
  for _, key := range(keys) {
    s.Insert([]byte(key))
  }

  iter.Seek([]byte("c"))
  if DefaultComparator.Compare(iter.Key(), []byte("c")) != 0 {
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
  s := NewSkipList(DefaultComparator, NewArena())
  iter := s.NewIterator()

  keys := []string{"c", "e", "a", "d", "b"}
  for _, key := range(keys) {
    s.Insert([]byte(key))
  }

  iter.SeekToFirst()
  iter.Next()
  if DefaultComparator.Compare(iter.Key(), []byte("b")) != 0 {
    t.Error("Skip list is not ordered.")
  }
  iter.Prev()
  if DefaultComparator.Compare(iter.Key(), []byte("a")) != 0 {
    t.Error("Skip list is not ordered.")
  }
  iter.Next()
  if DefaultComparator.Compare(iter.Key(), []byte("b")) != 0 {
    t.Error("Skip list is not ordered.")
  }

  iter.SeekToLast()
  iter.Prev()
  if DefaultComparator.Compare(iter.Key(), []byte("d")) != 0 {
    t.Error("Skip list is not ordered.")
  }
}

func TestIteratorComplexOperations(t *testing.T) {
  s := NewSkipList(DefaultComparator, NewArena())

  n := 2048
  for i := 0; i < n; i++ {
    s.Insert([]byte(fmt.Sprint(i)))
  }

  iter := s.NewIterator()
  cnt := 0
  var prevKey []byte = nil

  iter.SeekToFirst()
  for iter.Valid() {
    cnt++
    if prevKey != nil && DefaultComparator.Compare(prevKey, iter.Key()) >= 0 {
      t.Error("")
    }
    prevKey = iter.Key()
    iter.Next()
  }
  if cnt != n {
    t.Error("")
  }

  cnt = 0
  prevKey = nil
  iter.SeekToLast()
  for iter.Valid() {
    cnt++
    if prevKey != nil && DefaultComparator.Compare(prevKey, iter.Key()) <= 0 {
      t.Error("")
    }
    prevKey = iter.Key()
    iter.Prev()
  }
  if cnt != n {
    t.Error("")
  }
}
