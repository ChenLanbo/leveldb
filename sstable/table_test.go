package sstable

import (
  "fmt"
  "math/rand"
  "testing"
  "time"

  "github.com/chenlanbo/leveldb/db"
  "github.com/chenlanbo/leveldb/memtable"
  "github.com/chenlanbo/leveldb/util"
)

const (
  BaseFileName = "/tmp/table_builder_test_sstable"
  N = 2048
)

func TestTableBuilder(t *testing.T) {
  fileName := fmt.Sprint(BaseFileName, "-", time.Now().UnixNano())
  env := util.DefaultEnv()
  file, err := env.NewWritableFile(fileName)
  if err != nil {
    panic("Cannot create new sstable file.")
  }
  defer env.DeleteFile("/tmp/table_builder_test_sstable")
  defer file.Close()

  s := memtable.NewSkipList(db.DefaultComparator, util.NewArena())
  for i := 0; i < N; i++ {
    s.Insert([]byte(fmt.Sprint(i)))
  }

  sIter := s.NewIterator()
  sIter.SeekToFirst()

  builder := NewTableBuilder(defaultOptions(), file)
  for i := 0; i < N; i++ {
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

  s := memtable.NewSkipList(db.DefaultComparator, util.NewArena())
  for i := 0; i < N; i++ {
    s.Insert([]byte(fmt.Sprint(i)))
  }

  sIter := s.NewIterator()
  sIter.SeekToFirst()

  builder := NewTableBuilder(defaultOptions(), writeFile)
  for i := 0; i < N; i++ {
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
    t.Fatal(fmt.Sprint("Cannot open sstable file: ", err))
    return
  }

  iter := table.indexBlock.NewIterator(table.options.Comparator)
  iter.SeekToFirst()
  for iter.Valid() {
    iter.Next()
  }
}

func TestTableIterator(t *testing.T) {
  fileName := fmt.Sprint(BaseFileName, "-", time.Now().UnixNano())
  env := util.DefaultEnv()
  writeFile, err := env.NewWritableFile(fileName)
  if err != nil {
    panic("Cannot create new sstable file.")
  }
  defer env.DeleteFile(fileName)

  s := memtable.NewSkipList(db.DefaultComparator, util.NewArena())
  for i := 0; i < N; i++ {
    s.Insert([]byte(fmt.Sprint(i)))
  }

  sIter := s.NewIterator()
  sIter.SeekToFirst()

  builder := NewTableBuilder(defaultOptions(), writeFile)
  for i := 0; i < N; i++ {
    builder.Add(sIter.Key(), sIter.Key())
    sIter.Next()
  }

  err = builder.Finish()
  if err != nil {
    t.Error(fmt.Sprint("SSTable build failed: ", err))
  }
  err = writeFile.Close()
  if err != nil {
    panic("")
  }

  fileSize, err := env.GetFileSize(fileName)
  readFile, err := env.NewRandomAccessFile(fileName)
  if err != nil {
    panic("Cannot create new sstable file.")
  }
  defer readFile.Close()

  table, err := NewTable(defaultOptions(), readFile, fileSize)
  if err != nil {
    t.Fatal(fmt.Sprint("Cannot open sstable file: ", err))
    return
  }

  readOptions := db.ReadOptions{}
  cnt := 0
  iter := table.NewIterator(&readOptions)
  iter.SeekToFirst()
  for iter.Valid() {
    cnt++
    iter.Next()
  }
  if cnt != N {
    t.Error("Iter didn't iterate all keys.")
  }

  cnt = 0
  iter.SeekToLast()
  for iter.Valid() {
    cnt++
    iter.Prev()
  }
  if cnt != N {
    t.Error("Iter didn't iterate all keys.")
  }

  for i := 0; i < N; i++ {
    key := []byte(fmt.Sprint(rand.Intn(N)))
    iter.Seek(key)
    if !iter.Valid() {
      t.Error("Seek error.")
    }
    if db.DefaultComparator.Compare(iter.Key(), key) != 0 {
      t.Error("Key does not match.")
    }
  }
}

