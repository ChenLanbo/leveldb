package leveldb

import (
  "encoding/binary"
)

// Sequence number
type SequenceNumber uint64

// Value type
type ValueType uint64
const (
  TypeDeletion ValueType = 0x0
  TypeValue ValueType = 0x1
)

func ExtractUserKey(internalKey []byte) []byte {
  if len(internalKey) < 8 {
    panic("Invalid internal key.")
  }
  return internalKey[:len(internalKey) - 8]
}

func ExtractValueType(internalKey []byte) ValueType {
  if len(internalKey) < 8 {
    panic("Invalid internal key.")
  }

  l := len(internalKey)
  num := binary.LittleEndian.Uint64(internalKey[l-8:l]) & 0xff
  return ValueType(num)
}

// Comparator for the interal key
type InternalKeyComparator struct {
  comparator Comparator
}

func NewInternalKeyComparator(comparator Comparator) InternalKeyComparator {
  return InternalKeyComparator{comparator:comparator}
}

func (InternalKeyComparator) Name() string {
  return "leveldb.InternalKeyComparator"
}

func (c *InternalKeyComparator) Compare(a, b []byte) int {
  r := c.comparator.Compare(ExtractUserKey(a), ExtractUserKey(b))
  if r == 0 {
    anum := binary.LittleEndian.Uint64(a[len(a) - 8: len(a)])
    bnum := binary.LittleEndian.Uint64(b[len(b) - 8: len(b)])
    if anum > bnum {
      r = -1
    } else {
      r = 1
    }
  }
  return r
}

func (c *InternalKeyComparator) FindShortestSeparator(start, limit []byte) []byte {
  return nil
}

func (c *InternalKeyComparator) FindShortestSuccessor(key []byte) []byte {
  return nil
}

func (c *InternalKeyComparator) UserComparator() Comparator {
  return c.comparator
}

// Lookup key
type LookupKey struct {
  userKeySize int
  kStart int
  data []byte
}

func NewLookupKey(userKey []byte, seq SequenceNumber) *LookupKey {
  l := &LookupKey{}
  l.userKeySize = len(userKey)
  l.data = make([]byte, l.userKeySize + 13)
  l.kStart = binary.PutUvarint(l.data, uint64(l.userKeySize + 8))
  copy(l.data[l.kStart:l.kStart + l.userKeySize], userKey)
  binary.LittleEndian.PutUint64(l.data[l.kStart + l.userKeySize:], (uint64(seq) << 8 | uint64(TypeValue)))
  return l
}

func (l *LookupKey) MemtableKey() []byte {
  return l.data[:l.kStart + l.userKeySize + 8]
}

func (l *LookupKey) InternalKey() []byte {
  return l.data[l.kStart:l.kStart + l.userKeySize + 8]
}

func (l *LookupKey) UserKey() []byte {
  return l.data[l.kStart:l.kStart + l.userKeySize]
}
