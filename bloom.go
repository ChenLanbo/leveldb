package leveldb

import (
)

// FilterPolicy is an algorithm for probabilistically encoding a set of keys.
type FilterPolicy interface {
  Name() string
  CreateFilter(keys [][]byte) []byte
  MayContain(filter, key []byte) bool
}

type BloomFilter struct {
  bitsPerKey int
  k int
}

func NewBloomFilter(bitsPerKey int) FilterPolicy {
  filter := BloomFilter{}
  filter.bitsPerKey = bitsPerKey
  filter.k = int(float64(bitsPerKey) * 0.69)
  return filter
}

func (BloomFilter) Name() string {
  return "leveldb.BuiltinBloomFilter2"
}

func (f BloomFilter) CreateFilter(keys [][]byte) []byte {
  bits := f.bitsPerKey * len(keys)
  if bits < 64 {
    bits = 64
  }
  bits = (bits + 7 / 8) * 8

  out := make([]byte, (bits / 8) + 1)
  out[len(out) - 1] = uint8(f.k)

  for i := 0; i < len(keys); i++ {
    h := Hash(keys[i], 0xbc9f1d34)
    delta := (h >> 17) | (h << 15)
    for j := 0; j < f.k; j++ {
      pos := h % uint32(bits)
      out[int(pos) / 8] |= (1 << (pos % 8))
      h += delta
    }
  }

  return out
}

func (b BloomFilter) MayContain(filter, key []byte) bool {
  if len(filter) < 2 {
    return false
  }

  bits := (len(filter) - 1) * 8
  kk := int(filter[len(filter) - 1])
  if kk > 30 {
    // Reserve for potentially new encoding, consider it a match.
    return true
  }

  h := Hash(key, 0xbc9f1d34)
  delta := (h >> 17) | (h << 15)
  for i := 0; i < kk; i++ {
    pos := h % uint32(bits)
    if (filter[int(pos)/8] & (1 << (pos % 8))) == 0 {
      return false
    }
    h += delta
  }
  return true
}
