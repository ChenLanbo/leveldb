package sstable

import (
  "bytes"
  "testing"

  "github.com/chenlanbo/leveldb"
)

func TestSimpleBlockBuilder(t *testing.T) {
  builder := NewBlockBuilder(leveldb.DefaultOptions)

  keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
  for _, key := range(keys) {
    builder.Add(key, key)
  }

  _ = builder.Finish()
}

func TestSimpleBlockIterator(t *testing.T) {
  builder := NewBlockBuilder(leveldb.DefaultOptions)

  keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
  for _, key := range(keys) {
    builder.Add(key, key)
  }

  b := NewBlock(builder.Finish())
  iter := b.NewIterator(leveldb.DefaultOptions.Comparator)
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
