package db

import (
  "testing"
)

func TestDefaultComparator(t *testing.T) {
  if DefaultComparator.Compare([]byte("a"), []byte("b")) >= 0 {
    t.Error("DefaultComparator return unexpected result.")
  }

  if DefaultComparator.Compare([]byte("a"), []byte("a")) != 0 {
    t.Error("DefaultComparator return unexpected result.")
  }

  if DefaultComparator.Compare([]byte("b"), []byte("a")) <= 0 {
    t.Error("DefaultComparator return unexpected result.")
  }
}
