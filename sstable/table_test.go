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

func TestMergeIterator(t *testing.T) {
  fileName1 := fmt.Sprint(BaseFileName, "-", time.Now().UnixNano())
  fileName2 := fmt.Sprint(BaseFileName, "-", time.Now().UnixNano() + 100)
  env := util.DefaultEnv()
  writeFile1, err := env.NewWritableFile(fileName1)
  if err != nil {
    panic("")
  }
  writeFile2, err := env.NewWritableFile(fileName2)
  if err != nil {
    panic("")
  }
  defer env.DeleteFile(fileName1)
  defer env.DeleteFile(fileName2)

  s := memtable.NewSkipList(db.DefaultComparator, util.NewArena())
  for i := 0; i < N; i++ {
    s.Insert([]byte(fmt.Sprint(i)))
  }

  sIter := s.NewIterator()
  sIter.SeekToFirst()

  builder1 := NewTableBuilder(defaultOptions(), writeFile1)
  builder2 := NewTableBuilder(defaultOptions(), writeFile2)
  for i := 0; i < N; i++ {
    if i % 2 == 0 {
      builder1.Add(sIter.Key(), sIter.Key())
    } else {
      builder2.Add(sIter.Key(), sIter.Key())
    }
    sIter.Next()
  }

  err = builder1.Finish()
  if err != nil {
    t.Error(fmt.Sprint("SSTable build failed: ", err))
  }
  err = writeFile1.Close()
  if err != nil {
    panic("")
  }
  err = builder2.Finish()
  if err != nil {
    t.Error(fmt.Sprint("SSTable build failed: ", err))
  }
  err = writeFile2.Close()
  if err != nil {
    panic("")
  }

  fileSize1, err := env.GetFileSize(fileName1)
  readFile1, err := env.NewRandomAccessFile(fileName1)
  if err != nil {
    panic("Cannot create new sstable file.")
  }
  defer readFile1.Close()
  fileSize2, err := env.GetFileSize(fileName2)
  readFile2, err := env.NewRandomAccessFile(fileName2)
  if err != nil {
    panic("Cannot create new sstable file.")
  }
  defer readFile2.Close()

  table1, err := NewTable(defaultOptions(), readFile1, fileSize1)
  if err != nil {
    t.Fatal(fmt.Sprint("Cannot open sstable file: ", err))
    return
  }
  table2, err := NewTable(defaultOptions(), readFile2, fileSize2)
  if err != nil {
    t.Fatal(fmt.Sprint("Cannot open sstable file: ", err))
    return
  }

  readOptions := db.ReadOptions{}
  cnt := 0
  var key []byte = nil
  iter1 := table1.NewIterator(&readOptions)
  iter2 := table2.NewIterator(&readOptions)
  children := []db.Iterator{iter1, iter2}

  iter := NewMergeIterator(db.DefaultComparator, children)

  iter.SeekToFirst()
  for iter.Valid() {
    cnt++
    if key != nil && db.DefaultComparator.Compare(iter.Key(), key) <= 0 {
      t.Error(fmt.Sprint("Key out of order: ", string(iter.Key())), " ", string(key))
    }
    key = make([]byte, len(iter.Key()))
    copy(key, iter.Key())
    iter.Next()
  }
  if cnt != N {
    t.Error("")
  }

  iter.SeekToLast()
  cnt = 0
  key = nil
  for iter.Valid() {
    cnt++
    if key != nil && db.DefaultComparator.Compare(iter.Key(), key) >= 0 {
      t.Error(fmt.Sprint("Key out of order: ", string(iter.Key())), " ", string(key))
    }
    key = make([]byte, len(iter.Key()))
    copy(key, iter.Key())
    iter.Prev()
  }
  if cnt != N {
    t.Error("")
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
