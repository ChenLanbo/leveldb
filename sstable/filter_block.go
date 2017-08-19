package sstable

import (
  "encoding/binary"

  "github.com/chenlanbo/leveldb"
)

const (
  FilterBaseLg = 11
  FilterBase = 1 << FilterBaseLg
)

type FilterBlockBuilder struct {
  policy leveldb.FilterPolicy
  keys []byte
  start []int
  tmpKeys [][]byte
  filterOffsets []uint32
  result []byte
}

func NewFilterBlockBuilder(policy leveldb.FilterPolicy) *FilterBlockBuilder {
  builder := &FilterBlockBuilder{}
  builder.policy = policy
  builder.keys = make([]byte, 0)
  builder.start = make([]int, 0)
  builder.tmpKeys = nil
  builder.filterOffsets = make([]uint32, 0)
  builder.result = make([]byte, 0)
  return builder
}

func (b *FilterBlockBuilder) StartBlock(blockOffset uint64) {
  filterIndex := blockOffset / FilterBase
  if int(filterIndex) < len(b.filterOffsets) {
    panic("")
  }
  for int(filterIndex) > len(b.filterOffsets) {
    b.generateFilter()
  }
}

func (b *FilterBlockBuilder) AddKey(key []byte) {
  b.start = append(b.start, len(b.keys))
  for _, c := range(key) {
    b.keys = append(b.keys, c)
  }
}

func (b *FilterBlockBuilder) Finish() []byte {
  if len(b.start) != 0 {
    b.generateFilter()
  }
  buf := make([]byte, 4)
  arrayOffset := len(b.result)
  for _, offset := range(b.filterOffsets) {
    binary.LittleEndian.PutUint32(buf, uint32(offset))
    for _, c := range(buf) {
      b.result = append(b.result, c)
    }
  }
  binary.LittleEndian.PutUint32(buf, uint32(arrayOffset))
  for _, c := range(buf) {
    b.result = append(b.result, c)
  }
  b.result = append(b.result, byte(FilterBaseLg))
  return b.result
}

func (b *FilterBlockBuilder) generateFilter() {
  numKeys := len(b.start)
  if numKeys == 0 {
    b.filterOffsets = append(b.filterOffsets, 0)
  }

  b.start = append(b.start, len(b.keys))
  b.tmpKeys = make([][]byte, numKeys)
  for i := 0; i < numKeys; i++ {
    b.tmpKeys[i] = b.keys[b.start[i]:b.start[i+1]]
  }

  b.filterOffsets = append(b.filterOffsets, uint32(len(b.result)))
  out := b.policy.CreateFilter(b.tmpKeys)
  for _, c := range(out) {
    b.result = append(b.result, c)
  }

  b.tmpKeys = nil
  b.keys = b.keys[0:0]
  b.start = b.start[0:0]
}

type FilterBlockReader struct {
  policy leveldb.FilterPolicy
  data []byte
  baseLg byte
  offset int
  num int
}

func NewFilterBlockReader(policy leveldb.FilterPolicy, data []byte) *FilterBlockReader {
  reader := &FilterBlockReader{}
  reader.policy = policy
  if len(data) > 5 {
    reader.data = data
    reader.baseLg = data[len(data) - 1]
    reader.offset = int(binary.LittleEndian.Uint32(data[len(data) - 5:len(data) - 1]))
    reader.num = (len(data) - 5 - reader.offset) / 4
  }

  return reader
}

func (r *FilterBlockReader) MayContain(blockOffset uint64, key []byte) bool {
  filterIndex := blockOffset >> r.baseLg
  if int(filterIndex) >= r.num {
    return true
  }

  x := r.offset + int(filterIndex) * 4
  start := binary.LittleEndian.Uint32(r.data[x:x+4])
  limit := binary.LittleEndian.Uint32(r.data[x+4:x+8])

  if start <= limit && int(limit) <= r.offset {
    return r.policy.MayContain(r.data[start:limit], key)
  } else if start == limit {
    return false
  }

  return false
}
