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
  defer env.DeleteFile("/tmp/table_builder_test_sstable")
  defer file.Close()

  n := 2048
  s := memtable.NewSkipList(db.DefaultComparator, util.NewArena())
  for i := 0; i < n; i++ {
    s.Insert([]byte(fmt.Sprint(i)))
  }

  sIter := s.NewIterator() // memtable.NewSkipListIterator(s)
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

func TestTable(t *testing.T) {
  fileName := "/tmp/table_builder_test_sstable"
  env := util.DefaultEnv()
  writeFile, err := env.NewWritableFile(fileName)
  if err != nil {
    panic("Cannot create new sstable file.")
  }
  defer env.DeleteFile("/tmp/table_builder_test_sstable")
  defer writeFile.Close()

  n := 2048
  s := memtable.NewSkipList(db.DefaultComparator, util.NewArena())
  for i := 0; i < n; i++ {
    s.Insert([]byte(fmt.Sprint(i)))
  }

  sIter := s.NewIterator() // memtable.NewSkipListIterator(s)
  sIter.SeekToFirst()

  builder := NewTableBuilder(defaultOptions(), writeFile)
  for i := 0; i < n; i++ {
    builder.Add(sIter.Key(), sIter.Key())
    sIter.Next()
  }

  err = builder.Finish()
  if err != nil {
    t.Error(fmt.Sprint("SSTable build failed: ", err))
  }

  fileSize, err := env.GetFileSize(fileName)
  readFile, err := env.NewRandomAccessFile(fileName)
  if err != nil {
    panic("Cannot create new sstable file.")
  }
  defer readFile.Close()

  table, err := NewTable(defaultOptions(), readFile, fileSize)
  if err != nil {
    // panic(fmt.Sprint("Cannot open sstable file: ", err))
    return
  }

  iter := table.indexBlock.NewIterator(table.options.Comparator)
  iter.SeekToFirst()
  if iter.Valid() {
    fmt.Println(string(iter.Key()))
  }
}
