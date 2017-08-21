package sstable

import (
  "encoding/binary"
  "errors"
)

const (
  BlockTrailerSize = 5
  MaxBlockHandleEncodedLength = 20
  TableMagicNumber uint64 = 0xdb4775248b80fb57
)

type BlockHandle struct {
  offset uint64
  size uint64
}

func (handle *BlockHandle) SetOffset(offset uint64) {
  handle.offset = offset
}

func (handle *BlockHandle) SetSize(size uint64) {
  handle.size = size
}

func (handle *BlockHandle) EncodeTo() []byte {
  iBuf := make([]byte, 8)
  out := make([]byte, 0)
  n := binary.PutUvarint(iBuf, handle.offset)
  for i := 0; i < n; i++ {
    out = append(out, iBuf[i])
  }
  n = binary.PutUvarint(iBuf, handle.size)
  for i := 0; i < n; i++ {
    out = append(out, iBuf[i])
  }
  return out
}

func (handle *BlockHandle) DecodeFrom(data []byte) error {
  var n int
  handle.offset, n = binary.Uvarint(data)
  if n <= 0 {
    return errors.New("Corrupt block handle.")
  }
  handle.size, n = binary.Uvarint(data)
  if n <= 0 {
    return errors.New("Corrupt block handle.")
  }
  return nil
}

type BlockContents struct {
  data []byte
  cachable bool
}

type Footer struct {
  metaIndexHandle BlockHandle
  indexHandle BlockHandle
}

func (f *Footer) SetMetaIndexHandle(h *BlockHandle) {
  f.metaIndexHandle = *h
}

func (f *Footer) SetIndexHandle(h *BlockHandle) {
  f.indexHandle = *h
}

func (f *Footer) EncodeTo() []byte {
  out1 := f.metaIndexHandle.EncodeTo()
  out2 := f.indexHandle.EncodeTo()
  out := make([]byte, 2 * MaxBlockHandleEncodedLength + 8)
  copy(out[:len(out1)], out1)
  copy(out[len(out1):len(out1) + len(out2)], out2)

  t := int(2 * MaxBlockHandleEncodedLength)
  binary.LittleEndian.PutUint32(out[t:t + 4], uint32(TableMagicNumber & 0xffffffff))
  binary.LittleEndian.PutUint32(out[t + 4:t + 8], uint32(TableMagicNumber >> 32))

  return out
}
