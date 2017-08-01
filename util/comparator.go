package util

import (
  "bytes"
)

type Comparator interface {
  Compare(a, b []byte) int
  Name() string
}

var DefaultComparator Comparator = defaultComparator{}

type defaultComparator struct {
}

func (defaultComparator) Compare(a, b []byte) int {
  return bytes.Compare(a, b)
}

func (defaultComparator) Name() string {
  return "leveldb.DefaultComparator"
}
