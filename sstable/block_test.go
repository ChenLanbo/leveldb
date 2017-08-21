package sstable

import (
  "bytes"
  "fmt"
  "math/rand"
  "testing"

  "github.com/chenlanbo/leveldb/db"
  "github.com/chenlanbo/leveldb/memtable"
  "github.com/chenlanbo/leveldb/util"
)

func TestSimpleBlockBuilder(t *testing.T) {
  builder := NewBlockBuilder(defaultOptions())

  keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
  for _, key := range(keys) {
    builder.Add(key, key)
  }

  _ = builder.Finish()
}

func TestSimpleBlockIterator(t *testing.T) {
  builder := NewBlockBuilder(defaultOptions())

  keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
  for _, key := range(keys) {
    builder.Add(key, key)
  }

  b := NewBlock(builder.Finish())
  iter := b.NewIterator(defaultOptions().Comparator)
  if iter.Valid() {
    t.Error("Iterator should not be valid after creation.")
  }

  iter.SeekToFirst()
  if bytes.Compare(iter.Key(), []byte("a")) != 0 {
    t.Error("")
  }

  iter.Next()
  if bytes.Compare(iter.Key(), []byte("b")) != 0 {
    t.Error("")
  }

  iter.Next()
  if bytes.Compare(iter.Key(), []byte("c")) != 0 {
    t.Error("")
  }

  iter.Seek([]byte("b"))
  if bytes.Compare(iter.Key(), []byte("b")) != 0 {
    t.Error("")
  }
}

func TestComplexBlockIterator(t *testing.T) {
  n := 2048
  s := memtable.NewSkipList(db.DefaultComparator, util.NewArena())
  for i := 0; i < n; i++ {
    s.Insert([]byte(fmt.Sprint(i)))
  }

  builder := NewBlockBuilder(defaultOptions())

  sIter := memtable.NewSkipListIterator(s)
  sIter.SeekToFirst()

  for i := 0; i < n; i++ {
    builder.Add(sIter.Key(), sIter.Key())
    sIter.Next()
  }

  b := NewBlock(builder.Finish())
  iter := b.NewIterator(defaultOptions().Comparator)

  for i := 0; i < n; i++ {
    k := []byte(fmt.Sprint(rand.Intn(n)))
    iter.Seek(k)
    if bytes.Compare(iter.Key(), k) != 0 {
      t.Error("")
    }
    if bytes.Compare(iter.Value(), k) != 0 {
      t.Error("")
    }
  }
}

