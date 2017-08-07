package memtable

import (
  "math/rand"
  "testing"

  "github.com/chenlanbo/leveldb"
  "github.com/chenlanbo/leveldb/util"
)

func TestNewNode(t *testing.T) {
  arena := util.NewArena()

  n1 := newNode(arena, []byte("abc"), 12)
  t.Log(n1.next[0])
  n2 := newNode(arena, []byte("abc"), 12)
  t.Log(n2.next[0])
}

func TestInsertAndContains(t *testing.T) {
  s := NewSkipList(leveldb.DefaultComparator, util.NewArena())

  for i := 0; i < 100; i++ {
    key := make([]byte, 128)
    rand.Read(key)
    s.Insert(key)
    if !s.Contains(key) {
      t.Error("Skip list doesn't contain key just inserted.")
    }
  }
}

func TestIterator(t *testing.T) {
  s := NewSkipList(leveldb.DefaultComparator, util.NewArena())
  keys := []string{"a", "b", "c", "d", "e"}
  for _, key := range(keys) {
    s.Insert([]byte(key))
  }

  iter := NewSkipListIterator(s)
  iter.SeekToFirst()
  if leveldb.DefaultComparator.Compare(iter.Key(), []byte("a")) != 0 {
    t.Error("Skip list is not ordered.")
  }

  iter.Next()
  if leveldb.DefaultComparator.Compare(iter.Key(), []byte("b")) != 0 {
    t.Error("Expected to get key 'b', but get", string(iter.Key()))
  }

  iter.Seek([]byte("d"))
  if leveldb.DefaultComparator.Compare(iter.Key(), []byte("d")) != 0 {
    t.Error("Expected to get key 'd'")
  }
}
