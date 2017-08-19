package sstable

import (
  "bytes"
  "testing"

  "github.com/chenlanbo/leveldb/util"
)

func TestEmptyFilterBlock(t *testing.T) {
  builder := NewFilterBlockBuilder(util.NewBloomFilter(6))

  block := builder.Finish()
  if !bytes.Equal([]byte{0x00, 0x00, 0x00, 0x00, 0x0b}, block) {
    t.Error("Invalid empty filter block.")
  }

  reader := NewFilterBlockReader(util.NewBloomFilter(6), block)
  if !reader.MayContain(0, []byte("foo")) {
    t.Error("Empty filter block should return true.")
  }
  if !reader.MayContain(10000, []byte("foo")) {
    t.Error("Empty filter block should return true.")
  }
}

func TestFilterBlockSingleChunk(t *testing.T) {
  builder := NewFilterBlockBuilder(util.NewBloomFilter(6))

  builder.StartBlock(100)
  builder.AddKey([]byte("foo"))
  builder.AddKey([]byte("bar"))
  builder.AddKey([]byte("baz"))
  builder.StartBlock(200)
  builder.AddKey([]byte("baz"))
  builder.StartBlock(300)
  builder.AddKey([]byte("hello"))

  block := builder.Finish()
  reader := NewFilterBlockReader(util.NewBloomFilter(6), block)

  if !reader.MayContain(100, []byte("foo")) {
    t.Error("Filter block should contain.")
  }
  if !reader.MayContain(100, []byte("bar")) {
    t.Error("Filter block should contain.")
  }
  if !reader.MayContain(100, []byte("baz")) {
    t.Error("Filter block should contain.")
  }
  if reader.MayContain(100, []byte("missing")) {
    t.Error("Filter block should not contain.")
  }
}

func TestFilterBlockMultiChunk(t *testing.T) {
  builder := NewFilterBlockBuilder(util.NewBloomFilter(6))

  // First filter
  builder.StartBlock(0)
  builder.AddKey([]byte("foo"))
  builder.StartBlock(2000)
  builder.AddKey([]byte("bar"))

  // Second filter
  builder.StartBlock(3100)
  builder.AddKey([]byte("baz"))

  // Third filter empty

  // Last filter
  builder.StartBlock(9000)
  builder.AddKey([]byte("baz"))
  builder.AddKey([]byte("hello"))

  block := builder.Finish()
  reader := NewFilterBlockReader(util.NewBloomFilter(6), block)

  // Test first filter
  if !reader.MayContain(0, []byte("foo")) {
    t.Error("Filter block should contain.")
  }
  if !reader.MayContain(2000, []byte("bar")) {
    t.Error("Filter block should contain.")
  }
  if reader.MayContain(0, []byte("baz")) {
    t.Error("Filter block should not contain.")
  }
  if reader.MayContain(2000, []byte("hello")) {
    t.Error("Filter block should not contain.")
  }
}
