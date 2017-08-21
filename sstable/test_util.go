package sstable

import (
  "github.com/chenlanbo/leveldb/db"
  "github.com/chenlanbo/leveldb/util"
)

func defaultOptions() *db.Options {
  options := &db.Options{}
  options.Comparator = db.DefaultComparator
  options.BlockRestartInterval = 16
  options.BlockSize = 1024
  options.CompressionType = db.NoCompression
  options.FilterPolicy = util.NewBloomFilter(5)

  return options
}

