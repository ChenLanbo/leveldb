package sstable

import (
  "github.com/chenlanbo/leveldb/db"
)

type blockReader func(*db.ReadOptions, []byte) db.Iterator

// Two level iterator
type tableIterator struct {
  indexIter db.Iterator
  dataIter db.Iterator
  reader blockReader
  options db.ReadOptions
}

func newTableIterator(indexIter db.Iterator, reader blockReader, options *db.ReadOptions) db.Iterator {
  iter := &tableIterator{}
  iter.indexIter = indexIter
  iter.dataIter = nil
  iter.reader = reader
  iter.options = *options

  return iter
}

func (iter *tableIterator) Valid() bool {
  return iter.dataIter != nil && iter.dataIter.Valid()
}

func (iter *tableIterator) SeekToFirst() {
  iter.indexIter.SeekToFirst()
  iter.initDataBlock()
  if iter.dataIter != nil {
    iter.dataIter.SeekToFirst()
  }
  iter.skipEmptyDataBlocksForward()
}

func (iter *tableIterator) SeekToLast() {
}

func (iter *tableIterator) Seek(key []byte) {
}

func (iter *tableIterator) Next() {
  if !iter.Valid() {
    panic("")
  }
  iter.dataIter.Next()
  iter.skipEmptyDataBlocksForward()
}

func (iter *tableIterator) Prev() {
}

func (iter *tableIterator) Key() []byte {
  if !iter.Valid() {
    panic("")
  }
  return iter.dataIter.Key()
}

func (iter *tableIterator) Value() []byte {
  if !iter.Valid() {
    panic("")
  }
  return iter.dataIter.Value()
}

func (iter *tableIterator) skipEmptyDataBlocksForward() {
  for iter.dataIter == nil || !iter.dataIter.Valid() {
    if !iter.indexIter.Valid() {
      iter.dataIter = nil
      return
    }
    iter.indexIter.Next()
    iter.initDataBlock()
    if iter.dataIter != nil {
      iter.dataIter.SeekToFirst()
    }
  }
}

func (iter *tableIterator) initDataBlock() {
  if !iter.indexIter.Valid() {
    iter.dataIter = nil
  } else {
    iter.dataIter = iter.reader(&(iter.options), iter.indexIter.Value())
  }
}

// Error iterator created on error.
type errorIterator struct {
  status error
}

func newErrorIterator(status error) db.Iterator {
  iter := &errorIterator{}
  iter.status = status

  return iter
}

func (iter *errorIterator) Valid() bool {
  return false
}

func (iter *errorIterator) SeekToFirst() {
}

func (iter *errorIterator) SeekToLast() {
}

func (iter *errorIterator) Seek(key []byte) {
}

func (iter *errorIterator) Next() {
  panic("")
}

func (iter *errorIterator) Prev() {
  panic("")
}

func (iter *errorIterator) Key() []byte {
  panic("")
  return nil
}

func (iter *errorIterator) Value() []byte {
  panic("")
  return nil
}
