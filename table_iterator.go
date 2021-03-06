package leveldb

import (
)

type blockReader func(*ReadOptions, []byte) Iterator

// Two level iterator
type tableIterator struct {
  indexIter Iterator
  dataIter Iterator
  reader blockReader
  options ReadOptions
}

func newTableIterator(indexIter Iterator, reader blockReader, options *ReadOptions) Iterator {
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
  iter.indexIter.SeekToLast()
  iter.initDataBlock()
  if iter.dataIter != nil {
    iter.dataIter.SeekToLast()
  }
  iter.skipEmptyDataBlocksBackward()
}

func (iter *tableIterator) Seek(key []byte) {
  iter.indexIter.Seek(key)
  iter.initDataBlock()
  if iter.dataIter != nil {
    iter.dataIter.Seek(key)
  }
  iter.skipEmptyDataBlocksForward()
}

func (iter *tableIterator) Next() {
  if !iter.Valid() {
    panic("")
  }
  iter.dataIter.Next()
  iter.skipEmptyDataBlocksForward()
}

func (iter *tableIterator) Prev() {
  if !iter.Valid() {
    panic("")
  }
  iter.dataIter.Prev()
  iter.skipEmptyDataBlocksBackward()
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

func (iter *tableIterator) skipEmptyDataBlocksBackward() {
  for iter.dataIter == nil || !iter.dataIter.Valid() {
    if !iter.indexIter.Valid() {
      iter.dataIter = nil
      return
    }
    iter.indexIter.Prev()
    iter.initDataBlock()
    if iter.dataIter != nil {
      iter.dataIter.SeekToLast()
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
type emptyIterator struct {
  status error
}

func newEmptyIterator(status error) Iterator {
  iter := &emptyIterator{}
  iter.status = status

  return iter
}

func (iter *emptyIterator) Valid() bool {
  return false
}

func (iter *emptyIterator) SeekToFirst() {
}

func (iter *emptyIterator) SeekToLast() {
}

func (iter *emptyIterator) Seek(key []byte) {
}

func (iter *emptyIterator) Next() {
  panic("")
}

func (iter *emptyIterator) Prev() {
  panic("")
}

func (iter *emptyIterator) Key() []byte {
  panic("")
  return nil
}

func (iter *emptyIterator) Value() []byte {
  panic("")
  return nil
}
