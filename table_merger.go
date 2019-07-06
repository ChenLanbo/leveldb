package leveldb

import (
)

type mergeIteratorDirection byte
const (
  mergeForward mergeIteratorDirection = 0x0
  mergeBackward mergeIteratorDirection = 0x1
)

type mergeIterator struct {
  comparator Comparator
  current Iterator
  children []Iterator
  direction mergeIteratorDirection
}

func NewMergeIterator(comparator Comparator, children []Iterator) Iterator {
  if children == nil || len(children) == 0 {
    return newEmptyIterator(nil)
  } else if len(children) == 1 {
    return children[0]
  }
  iter := &mergeIterator{}
  iter.comparator = comparator
  iter.current = nil
  iter.children = children
  return iter
}

func (iter *mergeIterator) Valid() bool {
  return iter.current != nil
}

func (iter *mergeIterator) SeekToFirst() {
  for i := 0; i < len(iter.children); i++ {
    iter.children[i].SeekToFirst()
  }
  iter.findSmallest()
  iter.direction = mergeForward
}

func (iter *mergeIterator) SeekToLast() {
  for i := 0; i < len(iter.children); i++ {
    iter.children[i].SeekToLast()
  }
  iter.findBiggest()
  iter.direction = mergeBackward
}

func (iter *mergeIterator) Seek(key []byte) {
  for i := 0; i < len(iter.children); i++ {
    iter.children[i].Seek(key)
  }
  iter.findSmallest()
  iter.direction = mergeForward
}

func (iter *mergeIterator) Next() {
  if !iter.Valid() {
    panic("")
  }

  if iter.direction != mergeForward {
    for i := 0; i < len(iter.children); i++ {
      if iter.children[i] != iter.current {
        iter.children[i].Seek(iter.Key())
        if iter.children[i].Valid() && iter.comparator.Compare(iter.children[i].Key(), iter.Key()) == 0 {
          iter.children[i].Next()
        }
      }
    }
    iter.direction = mergeForward
  }

  iter.current.Next()
  iter.findSmallest()
}

func (iter *mergeIterator) Prev() {
  if !iter.Valid() {
    panic("")
  }

  if iter.direction != mergeBackward {
    for i := 0; i < len(iter.children); i++ {
      if iter.children[i] != iter.current {
        iter.children[i].Seek(iter.Key())
        if iter.children[i].Valid() {
          // child is at first entry >= Key().
          iter.children[i].Prev()
        } else {
          iter.children[i].SeekToLast()
        }
      }
    }
    iter.direction = mergeBackward
  }

  iter.current.Prev()
  iter.findBiggest()
}

func (iter *mergeIterator) Key() []byte {
  if !iter.Valid() {
    panic("")
  }
  return iter.current.Key()
}

func (iter *mergeIterator) Value() []byte {
  if !iter.Valid() {
    panic("")
  }
  return iter.current.Value()
}

func (iter *mergeIterator) findSmallest() {
  var smallest Iterator = nil
  for i := 0; i < len(iter.children); i++ {
    if iter.children[i].Valid() {
      if smallest == nil {
        smallest = iter.children[i]
      } else if iter.comparator.Compare(iter.children[i].Key(), smallest.Key()) < 0 {
        smallest = iter.children[i]
      }
    }
  }
  iter.current = smallest
}

func (iter *mergeIterator) findBiggest() {
  var biggest Iterator = nil
  for i := 0; i < len(iter.children); i++ {
    if iter.children[i].Valid() {
      if biggest == nil {
        biggest = iter.children[i]
      } else if iter.comparator.Compare(iter.children[i].Key(), biggest.Key()) > 0 {
        biggest = iter.children[i]
      }
    }
  }
  iter.current = biggest
}
