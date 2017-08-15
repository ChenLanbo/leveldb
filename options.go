package leveldb

import (
)

type Options struct {
  Comparator *Comparator
  BlockRestartInterval int
}

func defaultOptions() *Options {
  opt := &Options{}
  opt.Comparator = &DefaultComparator
  opt.BlockRestartInterval = 16
  return opt
}

var DefaultOptions *Options = defaultOptions()
