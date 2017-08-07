package sstable

import (
)

type BlockHandle struct {
  offset int64
  size int64
}

type BlockContents struct {
  data []byte
  cachable bool
}
