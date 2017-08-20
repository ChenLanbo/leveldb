package sstable

import (
  "bytes"
  "fmt"
  "encoding/binary"

  "github.com/chenlanbo/leveldb/db"
)

// SSTable block
type Block struct {
  data []byte
}

func NewBlock(data []byte) *Block {
  b := &Block{data:data}
  return b
}

func (block *Block) NumRestarts() int {
  r := bytes.NewReader(block.data[len(block.data) - 4:])
  var numRestarts int32
  if err := binary.Read(r, binary.LittleEndian, &numRestarts); err != nil {
    panic(err)
  }
  return int(numRestarts)
}

func (block *Block) NewIterator(comparator *db.Comparator) db.Iterator {
  iter := &BlockIterator{}
  iter.data = block.data
  iter.numRestarts = block.NumRestarts()
  iter.restartIndex = iter.numRestarts
  iter.restartOffset = len(block.data) - (iter.numRestarts + 1) * 4
  iter.currentOffset = iter.restartOffset
  iter.comparator = comparator
  iter.key = make([]byte, 0)
  iter.value = nil
  return iter
}

// SSTable block iterator
type BlockIterator struct {
  data []byte
  numRestarts int
  restartIndex int
  restartOffset int
  currentOffset int
  comparator *db.Comparator
  key []byte
  value []byte
}

func (iter *BlockIterator) Valid() bool {
  return iter.currentOffset < iter.restartOffset
}

func (iter *BlockIterator) SeekToFirst() {
  iter.seekToRestartPoint(0)
  iter.parseNextKey()
}

func (iter *BlockIterator) SeekToLast() {
  iter.seekToRestartPoint(iter.numRestarts - 1)
  for iter.Valid() {
    iter.parseNextKey()
  }
}

func (iter *BlockIterator) Seek(key []byte) {
  left, right := 0, iter.numRestarts - 1

  for left < right {
    mid := (left + right + 1) / 2
    regionOffset := iter.getRestartPoint(mid)

    var shared, nonShared, valueLength int64
    p, _ := iter.decodeEntry(iter.data[regionOffset:], &shared, &nonShared, &valueLength)
    if p == nil || shared != 0 {
      // Corruption error
      return
    }
    midKey := p[:nonShared]
    if (*iter.comparator).Compare(midKey, key) < 0 {
      left = mid
    } else {
      right = mid - 1
    }
  }

  iter.seekToRestartPoint(left)
  for {
    if !iter.parseNextKey() {
      return
    }
    if (*iter.comparator).Compare(iter.key, key) >= 0 {
      return
    }
  }
}

func (iter *BlockIterator) Next() {
  if !iter.Valid() {
    panic("")
  }
  iter.parseNextKey()
}

func (iter *BlockIterator) Prev() {
  if !iter.Valid() {
    panic("")
  }

  originalOffset := iter.currentOffset

  for iter.getRestartPoint(iter.restartIndex) >= originalOffset {
    if iter.restartIndex == 0 {
      iter.currentOffset = iter.restartOffset
      iter.restartIndex = iter.numRestarts
      return
    }
    iter.restartIndex--
  }

  iter.seekToRestartPoint(iter.restartIndex)
  for iter.parseNextKey() {
    if iter.currentOffset >= originalOffset {
      break
    }
  }
}

func (iter *BlockIterator) Key() []byte {
  return iter.key
}

func (iter *BlockIterator) Value() []byte {
  return iter.value
}

func (iter *BlockIterator) getRestartPoint(index int) int {
  if index >= iter.numRestarts {
    panic(fmt.Sprint("Beyond restart point:", iter.numRestarts))
  }

  off := iter.restartOffset + index * 4
  r := bytes.NewReader(iter.data[off:off+4])
  var off1 int32
  if err := binary.Read(r, binary.LittleEndian, &off1); err != nil {
    panic(err)
  }
  return int(off1)
}

func (iter *BlockIterator) seekToRestartPoint(index int) {
  iter.restartIndex = index
  iter.currentOffset = iter.getRestartPoint(index)
}

func (iter *BlockIterator) parseNextKey() bool {
  if iter.currentOffset >= iter.restartOffset {
    iter.restartIndex = iter.numRestarts
    return false
  }

  var shared, nonShared, valueLength int64
  p, nRead := iter.decodeEntry(iter.data[iter.currentOffset:], &shared, &nonShared, &valueLength)
  if p == nil || len(iter.key) < int(shared) {
    // Corruption error
    return false
  } else {
    iter.key = iter.key[:shared]
    for i := 0; i < int(nonShared); i++ {
      iter.key = append(iter.key, p[i])
    }
    iter.value = p[nonShared:nonShared + valueLength]

    for iter.restartIndex + 1 < iter.numRestarts {
      if iter.getRestartPoint(iter.restartIndex + 1) < iter.currentOffset {
        iter.restartIndex++
      } else {
        break
      }
    }

    iter.currentOffset += (nRead + int(nonShared) + int(valueLength))

    return true
  }
}

func (iter *BlockIterator) decodeEntry(b []byte, shared *int64, nonShared *int64, valueLength *int64) ([]byte, int) {
  reader := bytes.NewReader(b)
  var err error = nil
  if *shared, err = binary.ReadVarint(reader); err != nil {
    panic(err)
  }
  if *nonShared, err = binary.ReadVarint(reader); err != nil {
    panic(err)
  }
  if *valueLength, err = binary.ReadVarint(reader); err != nil {
    panic(err)
  }

  if int(*nonShared + *valueLength) > reader.Len() {
    return nil, 0
  }

  nRead := int(reader.Size()) - reader.Len()
  return b[nRead:], nRead
}

// SSTable Block builder
type BlockBuilder struct {
  options *db.Options
  buf *bytes.Buffer
  restartPoints []int32
  counter int
  finished bool
  lastKey []byte
  iBuf []byte
}

func NewBlockBuilder(options *db.Options) *BlockBuilder {
  builder := &BlockBuilder{}
  builder.options = options
  builder.buf = new(bytes.Buffer)
  builder.restartPoints = make([]int32, 1)
  builder.restartPoints[0] = 0
  builder.counter = 0
  builder.finished = false
  builder.lastKey = make([]byte, 0, 8)
  builder.iBuf = make([]byte, 8)
  return builder
}

func (builder *BlockBuilder) Reset() {
}

func (builder *BlockBuilder) Add(key, value []byte) {
  if builder.buf.Len() != 0 && (*builder.options.Comparator).Compare(key, builder.lastKey) <= 0 {
    panic(fmt.Sprint(string(key), " ", string(builder.lastKey)))
  }

  shared := 0
  if builder.counter < builder.options.BlockRestartInterval {
    l := len(builder.lastKey)
    if l > len(key) {
      l =len(key)
    }
    for shared < l && builder.lastKey[shared] == key[shared] {
      shared++
    }
  } else {
    builder.counter = 0
    builder.restartPoints = append(builder.restartPoints, int32(builder.buf.Len()))
  }
  nonShared := len(key) - shared

  nwrite := binary.PutVarint(builder.iBuf, int64(shared))
  builder.buf.Write(builder.iBuf[:nwrite])
  nwrite = binary.PutVarint(builder.iBuf, int64(nonShared))
  builder.buf.Write(builder.iBuf[:nwrite])
  nwrite = binary.PutVarint(builder.iBuf, int64(len(value)))
  builder.buf.Write(builder.iBuf[:nwrite])

  builder.buf.Write(key[shared:])
  builder.buf.Write(value)

  builder.lastKey = builder.lastKey[:shared]
  for _, b := range(key[shared:]) {
    builder.lastKey = append(builder.lastKey, b)
  }
  if (*builder.options.Comparator).Compare(key, builder.lastKey) != 0 {
    panic("")
  }

  builder.counter++
}

func (builder *BlockBuilder) Finish() []byte {
  for _, restartPoint := range(builder.restartPoints) {
    binary.Write(builder.buf, binary.LittleEndian, restartPoint)
  }
  binary.Write(builder.buf, binary.LittleEndian, int32(len(builder.restartPoints)))
  builder.finished = true
  return builder.buf.Bytes()
}

func (builder *BlockBuilder) CurrentEstimatedSize() int {
  return builder.buf.Len() + len(builder.restartPoints) * 4 + 4
}

func (builder *BlockBuilder) Empty() bool {
  return builder.buf.Len() == 0
}
