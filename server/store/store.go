package store

import (
	"errors"
	"fmt"
	"sync"

	"github.com/nStangl/distributed-kv-store/server/data"
	dbLog "github.com/nStangl/distributed-kv-store/server/log"
	"github.com/nStangl/distributed-kv-store/server/memtable"
	"github.com/nStangl/distributed-kv-store/server/sstable"
)

type (
	Store interface {
		Get(string) (data.Result, error)
		Set(string, string) error
		SetReplicated(string, string) error
		Del(string) error
		DelReplicated(string) error

		Close() error
		Flatten() (map[string]string, error)
	}

	StoreImpl struct {
		mu           sync.RWMutex
		log          dbLog.Log
		manager      *sstable.Manager
		memtable     memtable.Table
		memtableFunc MemtableFunc
	}

	MemtableFunc func() memtable.Table
)

const (
	keySz   = 2 << 8
	valueSz = 2 << 16
)

var _ Store = (*StoreImpl)(nil)

var (
	ErrKeyInvalid   = errors.New("key invalid")
	ErrValueTooLong = errors.New("value too long")
)

func New(
	log dbLog.Log,
	manager *sstable.Manager,
	memtableFunc MemtableFunc,
) *StoreImpl {
	return &StoreImpl{
		log:          log,
		manager:      manager,
		memtable:     memtableFunc(),
		memtableFunc: memtableFunc,
	}
}

func (s *StoreImpl) Get(key string) (data.Result, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v := s.memtable.Get(key)

	switch v.Kind {
	case data.Present, data.Deleted:
		return v, nil
	}

	return s.manager.Lookup(key)
}

func (s *StoreImpl) Set(key, value string) error {
	if len(key) == 0 || len(key) > keySz {
		return ErrKeyInvalid
	}

	if len(value) > valueSz {
		return ErrValueTooLong
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.log.Append(dbLog.NewSet(key, value)); err != nil {
		return fmt.Errorf("failed to append set record: %w", err)
	}

	s.memtable.Set(key, value)

	if s.memtable.Size() > memtable.MaxSize {
		if err := s.flushMemtable(); err != nil {
			return fmt.Errorf("failed to flush memtable: %w", err)
		}
	}

	return nil
}

func (s *StoreImpl) SetReplicated(key, value string) error {
	if len(key) == 0 || len(key) > keySz {
		return ErrKeyInvalid
	}

	if len(value) > valueSz {
		return ErrValueTooLong
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.log.Append(dbLog.NewReplicatedSet(key, value)); err != nil {
		return fmt.Errorf("failed to append set record: %w", err)
	}

	s.memtable.Set(key, value)

	if s.memtable.Size() > memtable.MaxSize {
		if err := s.flushMemtable(); err != nil {
			return fmt.Errorf("failed to flush memtable: %w", err)
		}
	}

	return nil
}

func (s *StoreImpl) Del(key string) error {
	if len(key) == 0 || len(key) > keySz {
		return ErrKeyInvalid
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.log.Append(dbLog.NewTombstone(key)); err != nil {
		return fmt.Errorf("failed to append tombstone record: %w", err)
	}

	s.memtable.Del(key)

	return nil
}

func (s *StoreImpl) DelReplicated(key string) error {
	if len(key) == 0 || len(key) > keySz {
		return ErrKeyInvalid
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.log.Append(dbLog.NewReplicatedTombstone(key)); err != nil {
		return fmt.Errorf("failed to append tombstone record: %w", err)
	}

	s.memtable.Del(key)

	return nil
}

func (s *StoreImpl) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.log.Close(); err != nil {
		return fmt.Errorf("failed to close the log: %w", err)
	}

	if s.memtable.Size() > 0 {
		if err := s.flushMemtable(); err != nil {
			return fmt.Errorf("failed to flush memtable: %w", err)
		}
	}

	if err := s.manager.Close(); err != nil {
		return fmt.Errorf("failed to close the manager: %w", err)
	}

	return nil
}

func (s *StoreImpl) Flatten() (map[string]string, error) {
	db, err := s.manager.Flatten()
	if err != nil {
		return nil, fmt.Errorf("manager failed to flatten the database: %w", err)
	}

	t := s.memtable.Iterator()

	for t.Next() {
		v := t.Value()

		switch v.Kind {
		case data.Present:
			db[v.Key] = v.Value
		case data.Deleted:
			delete(db, v.Key)
		}
	}

	return db, nil
}

func (s *StoreImpl) flushMemtable() error {
	if err := s.manager.Add(s.memtable); err != nil {
		return fmt.Errorf("failed to add new sstable: %w", err)
	}

	s.memtable = s.memtableFunc()

	return nil
}
