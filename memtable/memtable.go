package memtable

import (
  "encoding/binary"
  "fmt"

  "github.com/chenlanbo/leveldb"
  "github.com/chenlanbo/leveldb/db"
  "github.com/chenlanbo/leveldb/util"
)

func getLengthPrefixedSlice(a []byte) []byte {
  l, nread := binary.Uvarint(a)
  if nread <= 0 {
    panic("")
  }
  return a[nread:nread + int(l)]
}

// Key comparator of the memtable.
type keyComparator struct {
  comparator db.InternalKeyComparator
}

func (k keyComparator) Name() string {
  return k.comparator.Name()
}

func (k keyComparator) Compare(a, b []byte) int {
  aKey := getLengthPrefixedSlice(a)
  bKey := getLengthPrefixedSlice(b)
  return k.comparator.Compare(aKey, bKey)
}

func (k keyComparator) FindShortestSeparator(start, limit []byte) []byte {
  return k.comparator.FindShortestSeparator(start, limit)
}

func (k keyComparator) FindShortestSuccessor(key []byte) []byte {
  return k.comparator.FindShortestSuccessor(key)
}

// Memtable iterator
type memTableIterator struct {
  si db.Iterator
  tmp []byte
}

func (iter *memTableIterator) Valid() bool {
  return iter.si.Valid()
}

func (iter *memTableIterator) Seek(key []byte) {
  if len(key) + 8 > len(iter.tmp) {
    iter.tmp = make([]byte, len(key) + 8)
  }
  n := binary.PutUvarint(iter.tmp, uint64(len(key)))
  copy(iter.tmp[n:n + len(key)], key)
  iter.si.Seek(iter.tmp[:n + len(key)])
}

func (iter *memTableIterator) SeekToFirst() {
  iter.si.SeekToFirst()
}

func (iter *memTableIterator) SeekToLast() {
  iter.si.SeekToLast()
}

func (iter *memTableIterator) Next() {
  iter.si.Next()
}

func (iter *memTableIterator) Prev() {
  iter.si.Prev()
}

func (iter *memTableIterator) Key() []byte {
  return getLengthPrefixedSlice(iter.si.Key())
}

func (iter *memTableIterator) Value() []byte {
  key := getLengthPrefixedSlice(iter.si.Key())
  n := binary.PutUvarint(iter.tmp, uint64(len(key)))
  return getLengthPrefixedSlice(iter.si.Key()[n + len(key):])
}

// Memtable
type MemTable struct {
  comparator keyComparator
  arena *util.Arena
  table *SkipList
  iBuf []byte
}

func NewMemTable(comparator db.InternalKeyComparator) *MemTable {
  t := &MemTable{}
  t.comparator = keyComparator{comparator:comparator}
  t.arena = util.NewArena()
  t.table = NewSkipList(t.comparator, t.arena)
  t.iBuf = t.arena.Allocate(8)
  return t
}

func (mem *MemTable) NewIterator() db.Iterator {
  iter := &memTableIterator{}
  iter.si = mem.table.NewIterator()
  iter.tmp = make([]byte, 64)
  return iter
}

func (mem *MemTable) Add(seq db.SequenceNumber, valueType db.ValueType, key []byte, value []byte) {
  internalKeySize := len(key) + 8
  nwrite1 := binary.PutUvarint(mem.iBuf, uint64(internalKeySize))
  nwrite2 := binary.PutUvarint(mem.iBuf, uint64(len(value)))
  encodedLen := nwrite1 + internalKeySize + nwrite2 + len(value)

  buf := mem.arena.Allocate(encodedLen)

  // Write varint of internalKeySize
  n := binary.PutUvarint(buf, uint64(internalKeySize))
  if n != nwrite1 {
    panic("")
  }
  // Copy key
  copy(buf[n:n + len(key)], key)
  // Copy sequence number and value type
  binary.LittleEndian.PutUint64(buf[n + len(key):n + len(key) + 8], (uint64(seq) << 8) | uint64(valueType))
  // Write varint of value length
  n1 := binary.PutUvarint(buf[n + len(key) + 8:], uint64(len(value)))
  copy(buf[n + len(key) + 8 + n1:], value)

  encoded := len(buf)
  if encoded != encodedLen {
    panic(fmt.Sprint("Encoded len: ", encoded, " Expected len: ", encodedLen))
  }
  mem.table.Insert(buf)
}

func (mem *MemTable) Get(key *db.LookupKey) ([]byte, error) {
  memKey := key.MemtableKey()
  iter := mem.table.NewIterator()
  iter.Seek(memKey)
  if !iter.Valid() {
    return nil, leveldb.ErrNoFound
  }
  entry := iter.Key()
  tmp, n := binary.Uvarint(entry)
  if n <= 0 {
    panic("")
  }
  internalKeySize := int(tmp)
  if mem.comparator.comparator.UserComparator().Compare(entry[n:n + internalKeySize - 8], key.UserKey()) == 0 {
    tag := binary.LittleEndian.Uint64(entry[n + internalKeySize - 8: n + internalKeySize])
    t := db.ValueType(tag & 0xff)
    if t == db.TypeValue {
      tmp, n1 := binary.Uvarint(entry[n + internalKeySize:])
      if n1 <= 0 {
        panic("")
      }
      valueSize := int(tmp)
      return entry[n + internalKeySize + n1:n + internalKeySize + n1 + valueSize], nil
    }
  }
  return nil, leveldb.ErrNoFound
}

