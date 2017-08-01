package memtable

import (

  "github.com/chenlanbo/leveldb/util"
)

type MemTable struct {
  arena *util.Arena
}

// func (m *MemTable) Add(seq uint64, 

func NewMemTable() *MemTable {
  t := &MemTable{}
  t.arena = util.NewArena()
  return t
}
