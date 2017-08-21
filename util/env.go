package util

import (
  "os"

  "github.com/chenlanbo/leveldb/db"
)

type env struct {
}

func DefaultEnv() db.Env {
  return &env{}
}

func (e *env) NewSequentialFile(filename string) (db.SequentialFile, error) {
  f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
  if err != nil {
    return nil, err
  }
  return &sequentialFile{filename:filename, f:f}, nil
}

func (e *env) NewRandomAccessFile(filename string) (db.RandomAccessFile, error) {
  f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
  if err != nil {
    return nil, err
  }
  return &randomAccessFile{filename:filename, f:f}, nil
}

func (e *env) NewWritableFile(filename string) (db.WritableFile, error) {
  f, err := os.OpenFile(filename, os.O_CREATE | os.O_WRONLY, 0644)
  if err != nil {
    return nil, err
  }
  return &writableFile{filename:filename, f:f}, nil
}

func (e *env) NewAppendableFile(filename string) (db.WritableFile, error) {
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

// File for sequential read
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

// File for random read
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

// File for sequential write
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

