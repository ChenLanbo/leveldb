package util

import (
  "encoding/binary"
)

// Murmur hash
func Hash(data []byte, seed uint32) uint32 {
  var m uint32 = 0xc6a4a793
  var r uint32 = 24
  var h uint32 = seed ^ (uint32(len(data)) * m)

  i := 0
  for ; i + 4 <= len(data); i += 4 {
    w := binary.LittleEndian.Uint32(data[i:i + 4])
    h += w
    h *= m
    h ^= (h >> 16)
  }

  switch len(data) - i {
  case 3:
    h += uint32(data[i+2]) << 16
  case 2:
    h += uint32(data[i+1]) << 8
  case 1:
    h += uint32(data[i])
    h *= m
    h ^= (h >> r)
  }

  return h
}
