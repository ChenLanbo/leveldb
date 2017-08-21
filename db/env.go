package db

import (
)

type Env interface {
  NewSequentialFile(string) (SequentialFile, error)
  NewRandomAccessFile(string) (RandomAccessFile, error)
  NewWritableFile(string) (WritableFile, error)
  NewAppendableFile(string) (WritableFile, error)
  DeleteFile(string) error
  GetFileSize(string) (uint64, error)
}

type SequentialFile interface {
  Close() error
  Read([]byte) (int, error)
  Skip(int64) error
}

type RandomAccessFile interface {
  Close() error
  ReadAt([]byte, int64) (int, error)
}

type WritableFile interface {
  Close() error
  Write([]byte) (int, error)
  Sync() error
}