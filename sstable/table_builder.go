package sstable

import (
  "encoding/binary"
  "hash/crc32"

  "github.com/chenlanbo/leveldb/db"
)

type TableBuilder struct {
  options db.Options
  indexOptions db.Options
  file db.WritableFile
  offset uint64
  status error
  dataBlock *BlockBuilder
  indexBlock *BlockBuilder
  lastKey []byte
  numEntries int
  closed bool
  pendingIndexEntry bool
  pendingHandle BlockHandle
  filterBlock *FilterBlockBuilder
}

func NewTableBuilder(opt *db.Options, file db.WritableFile) *TableBuilder {
  builder := &TableBuilder{}
  builder.options = *opt
  builder.indexOptions = *opt
  builder.indexOptions.BlockRestartInterval = 1
  builder.file = file
  builder.offset = 0
  builder.status = nil
  builder.dataBlock = NewBlockBuilder(&builder.options)
  builder.indexBlock = NewBlockBuilder(&builder.options)
  builder.lastKey = make([]byte, 0, 4)
  builder.numEntries = 0
  builder.closed = false
  builder.filterBlock = nil
  if builder.options.FilterPolicy != nil {
    builder.filterBlock = NewFilterBlockBuilder(builder.options.FilterPolicy)
    builder.filterBlock.StartBlock(0)
  }
  return builder
}

func (builder *TableBuilder) Add(key, value []byte) {
  if builder.closed {
    panic("Add to a closed builder.")
  }
  if builder.status != nil {
    return
  }
  if builder.numEntries > 0 {
    if builder.options.Comparator.Compare(key, builder.lastKey) <= 0 {
      panic("Key is not ordered.")
    }
  }

  // Add an index block entry
  if builder.pendingIndexEntry {
    if !builder.dataBlock.Empty() {
      panic("")
    }
    // TODO: add the line below.
    // builder.options.Comparator.FindShortestSeparator(builder.lastKey, key)
    out := builder.pendingHandle.EncodeTo()
    builder.indexBlock.Add(builder.lastKey, out)
    builder.pendingIndexEntry = false
  }

  if builder.filterBlock != nil {
    builder.filterBlock.AddKey(key)
  }

  if len(builder.lastKey) < len(key) {
    builder.lastKey = make([]byte, len(key))
  }
  copy(builder.lastKey[:len(key)], key)
  builder.lastKey = builder.lastKey[:len(key)]
  builder.numEntries++
  builder.dataBlock.Add(key, value)

  if builder.dataBlock.CurrentEstimatedSize() >= builder.options.BlockSize {
    builder.Flush()
  }
}

func (builder *TableBuilder) Flush() {
  if builder.closed {
    panic("Builder closed.")
  }
  if builder.status != nil {
    return
  }
  if builder.dataBlock.Empty() {
    return
  }
  if builder.pendingIndexEntry {
    panic("")
  }

  builder.writeBlock(builder.dataBlock, &builder.pendingHandle)
  if builder.status == nil {
    builder.pendingIndexEntry = true
    builder.status = builder.file.Sync()
  }

  if builder.filterBlock != nil {
    builder.filterBlock.StartBlock(builder.offset)
  }
}

func (builder *TableBuilder) Status() error {
  return nil
}

func (builder *TableBuilder) Finish() error {
  builder.Flush()
  if builder.closed {
    panic("")
  }
  builder.closed = true

  var filterBlockHandle, metaIndexBlockHandle, indexBlockHandle BlockHandle

  // Write filter block.
  if builder.status == nil && builder.filterBlock != nil {
    builder.writeRawBlock(builder.filterBlock.Finish(), db.NoCompression, &filterBlockHandle)
  }

  if builder.status == nil {
    metaIndexBlock := NewBlockBuilder(&builder.options)
    if builder.filterBlock != nil {
      key := "filter."
      key += builder.options.FilterPolicy.Name()
      metaIndexBlock.Add([]byte(key), filterBlockHandle.EncodeTo())
    }
    builder.writeBlock(metaIndexBlock, &metaIndexBlockHandle)
  }

  // Write index block.
  if builder.status == nil {
    if builder.pendingIndexEntry {
      out := builder.pendingHandle.EncodeTo()
      builder.indexBlock.Add(builder.lastKey, out)
      builder.pendingIndexEntry = false
    }
    builder.writeBlock(builder.indexBlock, &indexBlockHandle)
  }

  // Write footer.
  if builder.status == nil {
    var footer Footer
    footer.SetMetaIndexHandle(&metaIndexBlockHandle)
    footer.SetIndexHandle(&indexBlockHandle)
    out := footer.EncodeTo()

    n, err := builder.file.Write(out)
    builder.status = err
    if n != len(out) {
      panic("")
    }
    if builder.status == nil {
      builder.offset += uint64(len(out))
    }
  }

  return builder.status
}

func (builder *TableBuilder) Abandon() {
  if builder.closed {
    panic("Table builder already closed.")
  }
  builder.closed = true
}

func (builder *TableBuilder) NumEntries() int {
  return builder.numEntries
}

func (builder *TableBuilder) FileSize() int {
  return int(builder.offset)
}

func (builder *TableBuilder) writeBlock(b *BlockBuilder, h *BlockHandle) {
  if builder.status != nil {
    panic(builder.status)
  }

  raw := b.Finish()
  // TODO: add compression

  builder.writeRawBlock(raw, db.NoCompression, h)
  b.Reset()
}

func (builder *TableBuilder) writeRawBlock(raw []byte, c db.CompressionType, handle *BlockHandle) {
  handle.SetOffset(builder.offset)
  handle.SetSize(uint64(len(raw)))

  n, err := builder.file.Write(raw)
  builder.status = err
  if n != len(raw) {
    panic("")
  }

  if builder.status == nil {
    // Write block trailer.
    trailer := make([]byte, BlockTrailerSize)
    trailer[0] = byte(c)
    checksum := crc32.Checksum(raw, crc32.IEEETable)
    checksum = crc32.Update(checksum, crc32.IEEETable, []byte{0x1})
    binary.LittleEndian.PutUint32(trailer[1:], checksum)
    n, err = builder.file.Write(trailer)
    builder.status = err
    if n != len(trailer) {
      panic("")
    }
    if builder.status == nil {
      builder.offset += uint64(len(raw)) + BlockTrailerSize
    }
  }
}
