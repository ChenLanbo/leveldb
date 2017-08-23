package sstable

import (
  "encoding/binary"
  "errors"
  "fmt"
  "hash/crc32"

  "github.com/chenlanbo/leveldb/db"
)

const (
  BlockTrailerSize = 5
  MaxBlockHandleEncodedLength = 20
  FooterLength = 48
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
  handle.size, n = binary.Uvarint(data[n:])
  if n <= 0 {
    return errors.New("Corrupt block handle.")
  }
  return nil
}

type BlockContents struct {
  data []byte
  cachable bool
}

// SSTable footer
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

func (f *Footer) DecodeFrom(data []byte) error {
  t := int(2 * MaxBlockHandleEncodedLength)
  magic := uint64(binary.LittleEndian.Uint32(data[t + 4:])) << 32 | uint64(binary.LittleEndian.Uint32(data[t:t + 4]))
  if magic != TableMagicNumber {
    return errors.New("Corrupted sstable file: bad magic number.")
  }

  err := f.metaIndexHandle.DecodeFrom(data)
  if err != nil {
    return err
  }
  _, n1 := binary.Uvarint(data)
  _, n2 := binary.Uvarint(data[n1:])

  err = f.indexHandle.DecodeFrom(data[n1 + n2:])
  return err
}

// Read the sstable block specified by the block handle
func ReadBlock(file db.RandomAccessFile, options *db.ReadOptions, handle *BlockHandle) ([]byte, error) {
  buf := make([]byte, int(handle.size) + BlockTrailerSize)

  n, err := file.ReadAt(buf, int64(handle.offset))
  if err != nil {
    return nil, err
  }
  if n != len(buf) {
    return nil, errors.New("Corrupted sstable file: truncated block read.")
  }

  if options.VerifyChecksums {
    // TODO: add verify checksums
    checksum := crc32.Checksum(buf[:int(handle.size) + 1], crc32.IEEETable)
    expected := binary.LittleEndian.Uint32(buf[int(handle.size) + 1:])
    if checksum != expected {
      panic(fmt.Sprint("Checksum mismatch: ", checksum, " ", expected))
    }
  }

  switch db.CompressionType(buf[handle.size]) {
  case db.NoCompression:
    // Skip
  case db.SnappyCompression:
    return nil, errors.New("Unsupported snappy block.")
  default:
    fmt.Println(len(buf), " ", handle.size)
    fmt.Println("BLOCK BYTES: ", buf)
    return nil, errors.New(fmt.Sprint("Corrupted sstable file: bad block type ", buf[handle.size]))
  }

  return buf[:int(handle.size)], nil
}
