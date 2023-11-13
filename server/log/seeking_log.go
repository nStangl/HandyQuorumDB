package log

import (
	"bufio"
	"fmt"
	"os"
	"sync"
)

type (
	// SeekingLog extends the interface of a standard log
	// by the Seek method, which allows callers to request
	// arbitrary subsets of the log, where each log entry is indexed
	// starting from 0. This requires us to load the whole log from
	// the log file at server startup, and keep it consistent during runtime
	SeekingLog interface {
		Log
		Seek(SeekCmd) SeekResult
	}

	SeekCmd struct {
		Start, End int
	}

	SeekResult struct {
		Records    []Record
		Start, End int
	}

	SeekingLogImpl struct {
		mu      sync.RWMutex
		log     Log
		records []Record
	}
)

var _ SeekingLog = (*SeekingLogImpl)(nil)

func NewSeeking(location string) (*SeekingLogImpl, error) {
	var records []Record

	if fileExists(location) {
		f, err := os.Open(location)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file: %w", err)
		}

		scanner := NewScanner(bufio.NewReader(f))

		for scanner.Scan() {
			records = append(records, scanner.Record())
		}

		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("failed to close log file: %w", err)
		}
	}

	l, err := New(location)
	if err != nil {
		return nil, fmt.Errorf("failed to create log: %w", err)
	}

	return &SeekingLogImpl{log: l, records: records}, nil
}

func (s *SeekingLogImpl) Append(r Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.log.Append(r); err != nil {
		return err
	}

	s.records = append(s.records, r)

	return nil
}

func (s *SeekingLogImpl) Close() error {
	return s.log.Close()
}

func (s *SeekingLogImpl) Seek(cmd SeekCmd) SeekResult {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if cmd.Start == -1 && cmd.End == -1 {
		return SeekResult{Records: s.records, Start: 0, End: len(s.records)}
	}

	if cmd.Start == -1 {
		return SeekResult{Records: s.records[:cmd.End], Start: 0, End: cmd.End}
	}

	if cmd.End == -1 {
		return SeekResult{Records: s.records[cmd.Start:], Start: cmd.Start, End: len(s.records)}
	}

	return SeekResult{Records: s.records[cmd.Start:cmd.End], Start: cmd.Start, End: cmd.End}
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}

	return !info.IsDir()
}
