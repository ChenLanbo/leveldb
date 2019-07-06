package leveldb

import (
  "fmt"
  "testing"
)

func TestMemTable(t *testing.T) {
  comparator := NewInternalKeyComparator(DefaultComparator)
  mem := NewMemTable(comparator)

  for i := 1; i <= 128; i++ {
    mem.Add(SequenceNumber(i), TypeValue, []byte("a"), []byte(fmt.Sprint(i)))
    key := NewLookupKey([]byte("a"), SequenceNumber(i))
    value, err := mem.Get(key)
    if err != nil {
      t.Error("Key 'a' should be found.")
    }
    if DefaultComparator.Compare(value, []byte(fmt.Sprint(i))) != 0 {
      t.Error("Key 'a' should have the latest value.")
    }
  }
}
