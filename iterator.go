package leveldb

import (
)

// Iterator interface
type Iterator interface {
  Valid() bool
  SeekToFirst()
  SeekToLast()
  Seek(key []byte)
  Next()
  Prev()
  Key() []byte
  Value() []byte
}
