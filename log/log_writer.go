package log

import (
  "encoding/binary"
  "hash/crc32"

  "github.com/chenlanbo/leveldb/db"
)

type LogWriter struct {
  dest db.WritableFile
  blockOffset int  // Current offset in block
}

// dest must have initial length "destLength"
func NewLogWriter(dest db.WritableFile, destLength uint64) *LogWriter {
  writer := &LogWriter{}
  writer.dest = dest
  writer.blockOffset = int(destLength % BlockSize)
  return writer
}

func (writer *LogWriter) AddRecord(data []byte) error {
  var err error = nil
  begin := true
  ptr := 0
  left := len(data)
  for {
    leftOver := BlockSize - writer.blockOffset
    if leftOver < 0 {
      panic("")
    }
    if leftOver < HeaderSize {
      // Switch to a new block
      if leftOver > 0 {
        trailer := []byte("\x00\x00\x00\x00\x00\x00")
        writer.dest.Write(trailer[:leftOver])
      }
      writer.blockOffset = 0
    }

    if BlockSize - writer.blockOffset - HeaderSize < 0 {
      panic("")
    }

    avail := BlockSize - writer.blockOffset - HeaderSize
    fragmentLength := 0
    if left < avail {
      fragmentLength = left
    } else {
      fragmentLength = avail
    }

    var t RecordType = ZeroType
    end := (left == fragmentLength)
    if begin == end {
      t = FullType
    } else if begin {
      t = FirstType
    } else if end {
      t = LastType
    } else {
      t = MiddleType
    }

    err = writer.emitPhysicalRecord(t, data[ptr:ptr + fragmentLength])
    ptr += fragmentLength
    left -= fragmentLength
    begin = false

    if err != nil || left <= 0 {
      break
    }
  }

  return err
}

func (writer *LogWriter) emitPhysicalRecord(t RecordType, data []byte) error {
  if len(data) > 0xffff {
    panic("")
  }
  if writer.blockOffset + HeaderSize + len(data) > BlockSize {
    panic("")
  }
  header := make([]byte, HeaderSize)
  header[4] = byte(len(data) & 0xff)
  header[5] = byte((len(data) >> 8) & 0xff)
  header[6] = byte(t)

  checksum := crc32.Checksum([]byte{byte(t)}, crc32.IEEETable)
  checksum = crc32.Update(checksum, crc32.IEEETable, data)
  binary.LittleEndian.PutUint32(header[:4], checksum)

  nwrite, err := writer.dest.Write(header)
  if nwrite != len(header) {
    panic("")
  }
  if err == nil {
    nwrite, err = writer.dest.Write(data)
    if nwrite != len(data) {
      panic("")
    }
    if err == nil {
      writer.dest.Sync()
    }
  }
  writer.blockOffset += HeaderSize + len(data)
  return err
}
