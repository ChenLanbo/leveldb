package leveldb

import (
  "math/rand"
  "testing"
)

func TestRandomAllocate(t *testing.T) {
  a := NewArena()

  rand.Seed(71)
  for i := 0; i < 100; i++ {
    nBytes := rand.Intn(8192)
    if nBytes <= 0 {
      continue
    }
    buf := a.Allocate(nBytes)
    if len(buf) != nBytes {
      t.Error("Allocate wrong number of bytes", nBytes, len(buf))
    }
  }
}

func TestMemoryUsage(t *testing.T) {
  a := NewArena()

  for i := 0; i < 10; i++ {
    a.Allocate(128)
    t.Log(a.MemoryUsage(), a.allocBytesRemaining)
  }

  if a.MemoryUsage() != 4096 {
    t.Error("Memory usage is incorrect", 4096, a.MemoryUsage())
  }
}
