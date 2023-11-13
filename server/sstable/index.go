package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/nStangl/distributed-kv-store/server/memtable"
	"github.com/nStangl/distributed-kv-store/server/util"
)

type (
	IndexFunc func(string) (uint32, bool)

	IndexEntry struct {
		key string
		idx uint32
	}
)

const (
	indexHeaderSz = 8
)

func NewIndex(r io.Reader) IndexFunc {
	var (
		keys    = make([]string, 0, memtable.MaxSize)
		indices = make([]uint32, 0, memtable.MaxSize)
		scanner = NewScanner(r)
	)

	for scanner.Scan() {
		e := scanner.Entry()

		keys = append(keys, e.key)
		indices = append(indices, e.idx)
	}

	return func(key string) (uint32, bool) {
		if idx := sort.SearchStrings(keys, key); idx < len(keys) && keys[idx] == key {
			return indices[idx], true
		}

		return 0, false
	}
}

func (r IndexEntry) String() string {
	return fmt.Sprintf("(%q, %d)", r.key, r.idx)
}

func (r IndexEntry) Size() int {
	return indexHeaderSz + len(r.key)
}

func (r IndexEntry) Write(w io.Writer) error {
	_, err := w.Write(r.ToBytes())
	return err
}

func (r IndexEntry) ToBytes() []byte {
	var (
		lenSz = 4
		idx1  = lenSz
		idx2  = idx1 + lenSz
		idx3  = idx2 + len(r.key)

		k = util.StringToBytes(r.key)
		p = make([]byte, idx3)
	)

	byteOrder.PutUint32(p[:idx1], uint32(len(k)))
	byteOrder.PutUint32(p[idx1:idx2], r.idx)

	copy(p[idx2:idx3], k)

	return p
}

func (r IndexEntry) ToWriter(w io.Writer) error {
	k := util.StringToBytes(r.key)

	_ = binary.Write(w, byteOrder, uint32(len(k)))
	_ = binary.Write(w, byteOrder, r.idx)

	return binary.Write(w, byteOrder, k)
}

func IndexEntryFromBytes(p []byte) (IndexEntry, error) {
	const (
		lenSz = 4
		idx1  = lenSz
		idx2  = idx1 + lenSz
	)

	if len(p) < idx2 {
		return IndexEntry{}, fmt.Errorf("index metadata shorter than %d: %d", idx2, len(p))
	}

	var (
		keyLen = byteOrder.Uint32(p[:idx1])
		index  = byteOrder.Uint32(p[idx1:idx2])

		idx3 = idx2 + int(keyLen)
	)

	if len(p) < idx3 {
		return IndexEntry{}, fmt.Errorf("index data shorter than %d: %d", idx3, len(p))
	}

	k := make([]byte, keyLen)

	copy(k, p[idx2:idx3])

	return IndexEntry{key: util.BytesToString(k), idx: index}, nil
}
