package sstable

import (
	"fmt"
	"time"

	"github.com/nStangl/distributed-kv-store/server/util"
	"golang.org/x/exp/mmap"
)

type Table struct {
	// Creation date
	date time.Time
	// Names of the index and table files
	tableFile string
	indexFile string
	// mmap'ed table file
	file *mmap.ReaderAt
	// in-memory index
	index IndexFunc
}

func (t *Table) Lookup(key string) (Entry, error) {
	i, ok := t.index(key)
	if !ok {
		return Entry{}, nil
	}

	h := make([]byte, entryHeaderSz)
	if _, err := t.file.ReadAt(h, int64(i)); err != nil {
		return Entry{}, fmt.Errorf("failed to read mmap'ed table entry header: %w", err)
	}

	n, keyLen, valLen, err := HeaderFromBytes(h)
	if err != nil {
		return Entry{}, fmt.Errorf("failed to parse table entry header: %w", err)
	}

	b := make([]byte, keyLen+valLen)
	if _, err := t.file.ReadAt(b, int64(i+uint32(len(h)))); err != nil {
		return Entry{}, fmt.Errorf("failed to read mmap'ed table entry: %w", err)
	}

	e := Entry{
		Kind: n,
		Key:  util.BytesToString(b[:keyLen]),
	}

	if valLen > 0 {
		e.Value = util.BytesToString(b[keyLen : keyLen+valLen])
	}

	return e, nil
}

func (t *Table) Close() error {
	if err := t.file.Close(); err != nil {
		return fmt.Errorf("failed to close mmap'ed file: %w", err)
	}

	return nil
}
