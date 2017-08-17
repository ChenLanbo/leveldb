package memtable

import (
  "testing"

  "github.com/chenlanbo/leveldb"
  "github.com/chenlanbo/leveldb/db"
)

func TestMemTable(t *testing.T) {
  comparator := db.NewInternalKeyComparator(leveldb.DefaultComparator)
  mem := NewMemTable(comparator)
  mem.Add(db.SequenceNumber(1), db.TypeValue, []byte("a"), []byte("a"))
  mem.Add(db.SequenceNumber(2), db.TypeValue, []byte("a"), []byte("a"))
  mem.Add(db.SequenceNumber(3), db.TypeValue, []byte("a"), []byte("a"))

  key := db.NewLookupKey([]byte("a"), db.SequenceNumber(4))

  value, err := mem.Get(key)
  if err != nil {
    t.Error("")
  }
  if leveldb.DefaultComparator.Compare(value, []byte("a")) != 0 {
    t.Error("")
  }
}
