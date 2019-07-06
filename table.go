package leveldb

import (
  "errors"
  "fmt"
)

type Table struct {
  options Options
  status error
  file RandomAccessFile
  cacheId uint64
  filterBlockReader *FilterBlockReader
  metaIndexHandle BlockHandle
  indexBlock *Block
}

func NewTable(options *Options, file RandomAccessFile, size uint64) (*Table, error) {
  if size < FooterLength {
    return nil, errors.New("Corrupted sstable file: too short.")
  }

  footerSpace := make([]byte, FooterLength)
  n, err := file.ReadAt(footerSpace, int64(size - FooterLength))
  if err != nil {
    return nil, err
  }
  if n != FooterLength {
    return nil, errors.New("Corrupted sstable file: truncated footer read.")
  }

  var footer Footer
  err = footer.DecodeFrom(footerSpace)
  if err != nil {
    return nil, err
  }

  var readOptions ReadOptions
  readOptions.VerifyChecksums = true
  out, err := ReadBlock(file, &readOptions, &(footer.indexHandle))
  if err != nil {
    fmt.Println("HERE*: ", err, " ", footer.indexHandle)
    fmt.Println("Footer: ", footerSpace)
    return nil, err
  }

  table := &Table{}
  table.options = *options
  table.file = file
  table.filterBlockReader = nil
  table.metaIndexHandle = footer.metaIndexHandle
  table.indexBlock = NewBlock(out)

  return table, nil
}

func (table *Table) NewIterator(readOptions *ReadOptions) Iterator {
  indexIter := table.indexBlock.NewIterator(table.options.Comparator)
  return newTableIterator(indexIter, table.blockReader, readOptions)
}

func (table *Table) blockReader(readOptions *ReadOptions, indexValue []byte) Iterator {
  var block *Block = nil
  handle := BlockHandle{}

  err := handle.DecodeFrom(indexValue)

  if err == nil {
    if readOptions.FillCache {
      // TODO: add cache
    } else {
      out, err := ReadBlock(table.file, readOptions, &handle)
      if err == nil {
        block = NewBlock(out)
      }
    }
  }

  if block != nil {
    return block.NewIterator(table.options.Comparator)
  } else {
    return newEmptyIterator(err)
  }
}

// Parse metadata index block
func (table *Table) readMeta(footer *Footer) {
  if table.options.FilterPolicy == nil {
    return
  }

  var readOptions ReadOptions
  readOptions.VerifyChecksums = true
  out, err := ReadBlock(table.file, &readOptions, &(footer.metaIndexHandle))
  if err != nil {
    fmt.Println("HERE: ", err)
    return
  }
  metaBlock := NewBlock(out)
  iter := metaBlock.NewIterator(DefaultComparator)
  key := "filter."
  key += table.options.FilterPolicy.Name()
  iter.Seek([]byte(key))
  if iter.Valid() && DefaultComparator.Compare(iter.Key(), []byte(key)) == 0 {
    table.readFilter(iter.Value())
  }
}

// Read the filter block
func (table *Table) readFilter(rawFilterHandle []byte) {
  var filterHandle BlockHandle
  err := filterHandle.DecodeFrom(rawFilterHandle)
  if err != nil {
    return
  }

  var readOptions ReadOptions
  readOptions.VerifyChecksums = true
  out, err := ReadBlock(table.file, &readOptions, &filterHandle)
  if err != nil {
    return
  }
  table.filterBlockReader = NewFilterBlockReader(table.options.FilterPolicy, out)
}

