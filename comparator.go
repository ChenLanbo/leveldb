package leveldb

import (
  "bytes"
)

type Comparator interface {
  Compare(a, b []byte) int
  Name() string
  FindShortestSeparator(start, limit []byte) []byte
  FindShortestSuccessor(key []byte) []byte
}

var DefaultComparator Comparator = BytewiseComparator{}

type BytewiseComparator struct {
}

func (BytewiseComparator) Name() string {
  return "leveldb.BytewiseComparator"
}

func (BytewiseComparator) Compare(a, b []byte) int {
  return bytes.Compare(a, b)
}

func (BytewiseComparator) FindShortestSeparator(start, limit []byte) []byte {
  l := len(start)
  if l > len(limit) {
    l = len(limit)
  }

  diff_index := 0
  for diff_index < l && start[diff_index] == limit[diff_index] {
    diff_index++
  }

  if diff_index >= l {
  } else {
    if start[diff_index] < byte(0xff) && start[diff_index] + 1 < limit[diff_index] {
      start[diff_index]++
    }
  }

  return start[:diff_index+1]
}

func (BytewiseComparator) FindShortestSuccessor(key []byte) []byte {
  for i := 0; i < len(key); i++ {
    if key[i] != byte(0xff) {
      key[i]++
      return key[:i+1]
    }
  }
  return key
}
