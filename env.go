package leveldb

import (
    "os"
)

// Environment interface.
type Env interface {
  NewSequentialFile(string) (SequentialFile, error)
  NewRandomAccessFile(string) (RandomAccessFile, error)
  NewWritableFile(string) (WritableFile, error)
  NewAppendableFile(string) (WritableFile, error)
  DeleteFile(string) error
  GetFileSize(string) (uint64, error)
}

// File for sequential read.
type SequentialFile interface {
  Close() error
  Read([]byte) (int, error)
  Skip(int64) error
}

// File for random read.
type RandomAccessFile interface {
  Close() error
  ReadAt([]byte, int64) (int, error)
}

// File for sequential write.
type WritableFile interface {
  Close() error
  Write([]byte) (int, error)
  Sync() error
}

// Implementation.
type env struct {
}

func DefaultEnv() Env {
  return &env{}
}

func (e *env) NewSequentialFile(filename string) (SequentialFile, error) {
  f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
  if err != nil {
    return nil, err
  }
  return &sequentialFile{filename:filename, f:f}, nil
}

func (e *env) NewRandomAccessFile(filename string) (RandomAccessFile, error) {
  f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
  if err != nil {
    return nil, err
  }
  return &randomAccessFile{filename:filename, f:f}, nil
}

func (e *env) NewWritableFile(filename string) (WritableFile, error) {
  f, err := os.OpenFile(filename, os.O_CREATE | os.O_WRONLY, 0644)
  if err != nil {
    return nil, err
  }
  return &writableFile{filename:filename, f:f}, nil
}

func (e *env) NewAppendableFile(filename string) (WritableFile, error) {
  f, err := os.OpenFile(filename, os.O_CREATE | os.O_APPEND, 0644)
  if err != nil {
    return nil, err
  }
  return &writableFile{filename:filename, f:f}, nil
}

func (e *env) DeleteFile(filename string) error {
  return os.Remove(filename)
}

func (e *env) GetFileSize(filename string) (uint64, error) {
  info, err := os.Stat(filename)
  if err != nil {
    return 0, err
  }
  return uint64(info.Size()), nil
}

type sequentialFile struct {
  filename string
  f *os.File
}

func (f *sequentialFile) Close() error {
  return f.f.Close()
}

func (f *sequentialFile) Read(b []byte) (int, error) {
  return f.f.Read(b)
}

func (f *sequentialFile) Skip(n int64) error {
  _, err := f.f.Seek(n, os.SEEK_CUR)
  return err
}

type randomAccessFile struct {
  filename string
  f *os.File
}

func (f *randomAccessFile) Close() error {
  return f.f.Close()
}

func (f *randomAccessFile) ReadAt(b []byte, off int64) (int, error) {
  return f.f.ReadAt(b, off)
}

type writableFile struct {
  filename string
  f *os.File
}

func (f *writableFile) Close() error {
  return f.f.Close()
}

func (f *writableFile) Write(b []byte) (int, error) {
  return f.f.Write(b)
}

func (f *writableFile) Sync() error {
  return f.f.Sync()
}

