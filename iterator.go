package leveldb

import (
)

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
