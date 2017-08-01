package memtable

import (
  "math/rand"

  "github.com/chenlanbo/leveldb/util"
)

const (
  kMaxHeight = 12
)

// Skip list node structure
type node struct {
  key []byte
  next []*node
}

func newNode(arena *util.Arena, key []byte, height int) *node{
  n := &node{}
  n.key = arena.Allocate(len(key))
  copy(n.key, key)
  n.next = make([]*node, height)
  for i, _ := range(n.next) {
    n.next[i] = nil
  }
  return n
}

type SkipList struct {
  comparator util.Comparator
  arena *util.Arena
  head *node
  maxHeight int
}

func NewSkipList(comparator util.Comparator, arena *util.Arena) *SkipList {
  rand.Seed(71)
  s := &SkipList{}
  s.comparator = comparator
  s.arena = arena
  s.head = newNode(s.arena, []byte(""), kMaxHeight)
  s.maxHeight = 1
  return s
}

func (s *SkipList) Insert(key []byte) {
  _, prev := s.findGreaterOrEqual(key)

  height := s.randomHeight()
  if s.maxHeight < height {
    for i := s.maxHeight; i < height; i++ {
      prev[i] = s.head
    }
    s.maxHeight = height
  }

  n := newNode(s.arena, key, height)
  for i := 0; i < height; i++ {
    n.next[i] = prev[i].next[i]
    prev[i].next[i] = n
  }
}

func (s *SkipList) Contains(key []byte) bool {
  _, prev := s.findGreaterOrEqual(key)
  if prev[0].next[0] != nil && s.comparator.Compare(prev[0].next[0].key, key) == 0 {
    return true
  } else {
    return false
  }
}

func (s *SkipList) findGreaterOrEqual(key []byte) (*node, [kMaxHeight]*node) {
  var prev [kMaxHeight]*node
  x := s.head
  l := s.maxHeight - 1
  for {
    next := x.next[l]
    if s.keyIsAfterNode(key, next) {
      x = next
    } else {
      prev[l] = x
      if l > 0 {
        l--
      } else {
        return next, prev
      }
    }
  }

  return nil, prev
}

func (s *SkipList) keyIsAfterNode(key []byte, n *node) bool {
  return (n != nil) && (s.comparator.Compare(n.key, key) < 0)
}

func (s *SkipList) randomHeight() int {
  height := 1
  for height < kMaxHeight {
    if rand.Intn(4) != 0 {
      break
    }
    height++
  }
  return height
}

type SkipListIterator struct {
  s *SkipList
  n *node
}

func NewSkipListIterator(s *SkipList) *SkipListIterator {
  return &SkipListIterator{s:s, n:nil}
}

func (iter *SkipListIterator) Valid() bool {
  return iter.n != nil
}

func (iter *SkipListIterator) Next() {
  if !iter.Valid() {
    panic("")
  }
  iter.n = iter.n.next[0]
}

func (iter *SkipListIterator) Key() []byte {
  if !iter.Valid() {
    panic("")
  }
  return iter.n.key
}

func (iter *SkipListIterator) Seek(key []byte) {
  iter.n, _ = iter.s.findGreaterOrEqual(key)
}

func (iter *SkipListIterator) SeekToFirst() {
  iter.n = iter.s.head.next[0]
}
