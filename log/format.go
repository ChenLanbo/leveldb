package log

import (
)

type RecordType byte
const (
  ZeroType RecordType = 0x0
  FullType RecordType = 0x1
  FirstType RecordType = 0x2
  MiddleType RecordType = 0x3
  LastType RecordType = 0x4
)

const (
  BlockSize = 32768
  HeaderSize = 7  // checksum (4 bytes), length (2 bytes), type (1 byte).
)
