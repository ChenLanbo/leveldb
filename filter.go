package leveldb

import (
)

// FilterPolicy is an algorithm for probabilistically encoding a set of keys.
type FilterPolicy interface {
  Name() string
  CreateFilter(keys [][]byte) []byte
  MayContain(filter, key []byte) bool
}
