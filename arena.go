package leveldb

import (
  "flag"
)

var blockSize = flag.Int("arena_block_size", 4096, "")

type Arena struct {
  allocPtr []byte
  allocBytesRemaining int
  memoryUsage int
  blocks [][]byte
}

// Private methods
func (a *Arena) allocateFallback(nBytes int) []byte {
  if nBytes > *blockSize / 4 {
    // Object is more than a quarter of the block size.
    s := a.allocateNewBlock(nBytes)
    return s
  }

  a.allocPtr = a.allocateNewBlock(*blockSize)
  a.allocBytesRemaining = *blockSize - nBytes

  return a.allocPtr[0:nBytes]
}

func (a *Arena) allocateNewBlock(nBytes int) []byte {
  b := make([]byte, nBytes)
  a.blocks = append(a.blocks, b)
  a.memoryUsage += nBytes
  return b
}

// Public methods
func (a *Arena) Allocate(nBytes int) []byte {
  if nBytes < 0 {
    panic("Allocate non-positive bytes.")
  }
  if nBytes == 0 {
    return make([]byte, 0)
  }

  if nBytes <= a.allocBytesRemaining {
    start := len(a.allocPtr) - a.allocBytesRemaining
    p := a.allocPtr[start:(start + nBytes)]
    a.allocBytesRemaining -= nBytes
    return p
  }

  return a.allocateFallback(nBytes)
}

func (a *Arena) MemoryUsage() int {
  return a.memoryUsage
}

func NewArena() *Arena {
  a := &Arena{}
  a.allocPtr = nil
  a.allocBytesRemaining = 0
  a.memoryUsage = 0
  a.blocks = make([][]byte, 0)
  return a
}
