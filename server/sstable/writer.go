package sstable

import (
	"fmt"
	"io"

	"github.com/nStangl/distributed-kv-store/server/data"
	"github.com/nStangl/distributed-kv-store/server/memtable"
)

type (
	Iterator interface {
		Next() bool
		Value() Entry
	}

	memtableIterator struct{ memtable.Iterator }
)

var _ Iterator = (*memtableIterator)(nil)

func writeTable(iterator Iterator, table, index io.Writer) error {
	var idx uint32

	for iterator.Next() {
		var (
			e = iterator.Value()
			n = IndexEntry{key: e.Key, idx: idx}
		)

		if err := e.Write(table); err != nil {
			return fmt.Errorf("failed to write entry to table: %w", err)
		}

		if err := n.Write(index); err != nil {
			return fmt.Errorf("failed to write entry to table index: %w", err)
		}

		idx += uint32(e.Size())
	}

	return nil
}

func newMemetableIterator(m memtable.Table) *memtableIterator {
	return &memtableIterator{Iterator: m.Iterator()}
}

func (i *memtableIterator) Next() bool { return i.Iterator.Next() }

func (i *memtableIterator) Value() Entry {
	elem := i.Iterator.Value()

	return Entry{
		Kind:  dataToSSKind(elem.Kind),
		Key:   elem.Key,
		Value: elem.Value,
	}
}

func dataToSSKind(k data.ResultKind) EntryKind {
	switch k {
	case data.Present:
		return Set
	case data.Deleted:
		return Tombstone
	default:
		return Set
	}
}
