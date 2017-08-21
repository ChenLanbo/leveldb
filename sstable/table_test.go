package sstable

import (
  "fmt"
  "testing"

  "github.com/chenlanbo/leveldb/db"
  "github.com/chenlanbo/leveldb/memtable"
  "github.com/chenlanbo/leveldb/util"
)

func TestTableBuilder(t *testing.T) {
  env := util.DefaultEnv()
  file, err := env.NewWritableFile("/tmp/table_builder_test_sstable")
  if err != nil {
    panic("Cannot create new sstable file.")
  }
  defer file.Close()
  defer env.DeleteFile("/tmp/table_builder_test_sstable")

  n := 2048
  s := memtable.NewSkipList(db.DefaultComparator, util.NewArena())
  for i := 0; i < n; i++ {
    s.Insert([]byte(fmt.Sprint(i)))
  }

  sIter := memtable.NewSkipListIterator(s)
  sIter.SeekToFirst()

  builder := NewTableBuilder(defaultOptions(), file)
  for i := 0; i < n; i++ {
    builder.Add(sIter.Key(), sIter.Key())
    sIter.Next()
  }

  err = builder.Finish()
  if err != nil {
    t.Error(fmt.Sprint("SSTable build failed: ", err))
  }
}
