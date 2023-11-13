package log

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

type (
	Log interface {
		Append(Record) error
		Close() error
	}

	LogImpl struct {
		syncCnt uint
		file    *os.File
		buff    *bufio.Writer
	}
)

const (
	syncThreshold = 2 << 7
)

var (
	byteOrder = binary.BigEndian
)

var _ Log = (*LogImpl)(nil)

func New(location string) (*LogImpl, error) {
	if err := touch(location); err != nil {
		return nil, fmt.Errorf("failed to touch log file at %s: %w", location, err)
	}

	f, err := os.OpenFile(location, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file at %s: %w", location, err)
	}

	return &LogImpl{file: f, buff: bufio.NewWriter(f)}, nil
}

func (l *LogImpl) Append(r Record) error {
	if err := r.Write(l.buff); err != nil {
		return fmt.Errorf("failed to write record to file: %w", err)
	}

	l.syncCnt++

	if l.syncCnt > syncThreshold {
		l.syncCnt = 0

		if err := l.buff.Flush(); err != nil {
			return fmt.Errorf("failed to sync file: %w", err)
		}

		if err := l.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync file: %w", err)
		}
	}

	return nil
}

func (l *LogImpl) Close() error {
	return l.file.Close()
}

func touch(fileName string) error {
	d := filepath.Dir(fileName)

	if _, err := os.Stat(d); os.IsNotExist(err) {
		if err := os.MkdirAll(d, os.ModePerm); err != nil {
			return err
		}
	}

	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		f, err := os.OpenFile(fileName, os.O_RDONLY|os.O_CREATE, 0o644)
		if err != nil {
			return err
		}

		return f.Close()
	}

	return nil
}
