package memtable

import (
  "fmt"
  "testing"

  "github.com/chenlanbo/leveldb/db"
)

func TestMemTable(t *testing.T) {
  comparator := db.NewInternalKeyComparator(db.DefaultComparator)
  mem := NewMemTable(comparator)

  for i := 1; i <= 128; i++ {
    mem.Add(db.SequenceNumber(i), db.TypeValue, []byte("a"), []byte(fmt.Sprint(i)))
    key := db.NewLookupKey([]byte("a"), db.SequenceNumber(i))
    value, err := mem.Get(key)
    if err != nil {
      t.Error("Key 'a' should be found.")
    }
    if db.DefaultComparator.Compare(value, []byte(fmt.Sprint(i))) != 0 {
      t.Error("Key 'a' should have the latest value.")
    }
  }
}
