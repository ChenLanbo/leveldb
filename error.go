package leveldb

import (
)

type Status struct {
	code int
	msg string
}

func (s *Status) Error() string {
	return s.msg
}

func NotFoundError(msg string) error {
	return &Status{1, msg}
}
