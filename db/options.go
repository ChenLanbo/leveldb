package db

import (
)

type CompressionType byte
const (
  NoCompression CompressionType = 0x0
  SnappyCompression CompressionType = 0x1
)

type Options struct {
  Comparator Comparator
  BlockRestartInterval int
  BlockSize int
  CompressionType CompressionType
  FilterPolicy FilterPolicy
}

type ReadOptions struct {
  VerifyChecksums bool
  FillCache bool
}
