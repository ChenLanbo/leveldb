package leveldb

import (
  "fmt"
  "testing"
)

func TestBloomFilter(t *testing.T) {
  filter := NewBloomFilter(6)
  keys := make([][]byte, 0)
  for i := 0; i < 2048; i++ {
    keys = append(keys, []byte(fmt.Sprint(i)))
  }

  f := filter.CreateFilter(keys)

  for i := 0; i < 2048; i++ {
    if !filter.MayContain(f, keys[i]) {
      t.Error("Should contain.")
    }
  }

  if filter.MayContain(f, []byte(fmt.Sprint(2048))) {
    t.Error("Should not contain.")
  }
}
