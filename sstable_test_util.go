package leveldb

import (
)

const (
  BaseFileName = "/tmp/table_builder_test_sstable"
  N = 2048
)

func defaultOptions() *Options {
  options := &Options{}
  options.Comparator = DefaultComparator
  options.BlockRestartInterval = 16
  options.BlockSize = 1024
  options.CompressionType = NoCompression
  options.FilterPolicy = NewBloomFilter(5)

  return options
}

