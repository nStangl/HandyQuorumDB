package sstable

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/nStangl/distributed-kv-store/server/util"
)

type (
	Entry struct {
		Kind  EntryKind
		Key   string
		Value string
	}

	EntryKind uint8
)

const (
	Set EntryKind = iota + 1
	Tombstone
)

const entryHeaderSz = 9

var (
	byteOrder = binary.BigEndian
	kindStr   = []string{"set", "tombstone"}
)

func NewEntry(kind EntryKind, key, value string) *Entry {
	return &Entry{Kind: kind, Key: key, Value: value}
}

func (k EntryKind) String() string {
	return kindStr[k-1]
}

func (e *Entry) String() string {
	return fmt.Sprintf("(%s, %q, %q)", e.Kind, e.Key, e.Value)
}

func (e *Entry) Size() int {
	return entryHeaderSz + len(e.Key) + len(e.Value)
}

func (e *Entry) Write(w io.Writer) error {
	_, err := w.Write(e.ToBytes())
	return err
}

func (e *Entry) ToBytes() []byte {
	var (
		lenSz = 4
		idx1  = 1
		idx2  = idx1 + lenSz
		idx3  = idx2 + lenSz
		idx4  = idx3 + len(e.Key)
		idx5  = idx4 + len(e.Value)

		p = make([]byte, idx5)
		k = util.StringToBytes(e.Key)
		v = util.StringToBytes(e.Value)
	)

	p[0] = byte(e.Kind)
	byteOrder.PutUint32(p[idx1:idx2], uint32(len(k)))
	byteOrder.PutUint32(p[idx2:idx3], uint32(len(v)))

	copy(p[idx3:idx4], k)
	copy(p[idx4:idx5], v)

	return p
}

func (e *Entry) ToWriter(w io.Writer) error {
	var (
		k = util.StringToBytes(e.Key)
		v = util.StringToBytes(e.Value)
	)

	_ = binary.Write(w, byteOrder, byte(e.Kind))
	_ = binary.Write(w, byteOrder, uint32(len(k)))
	_ = binary.Write(w, byteOrder, uint32(len(v)))
	_ = binary.Write(w, byteOrder, k)

	return binary.Write(w, byteOrder, v)
}

func EntryFromBytes(p []byte) (Entry, error) {
	const (
		lenSz = 4
		idx1  = 1
		idx2  = idx1 + lenSz
		idx3  = idx2 + lenSz
	)

	if len(p) < idx3 {
		return Entry{}, fmt.Errorf("entry metadata shorter than %d: %d", idx3, len(p))
	}

	var (
		kind   = EntryKind(p[0])
		keyLen = int(byteOrder.Uint32(p[idx1:idx2]))
		valLen = int(byteOrder.Uint32(p[idx2:idx3]))

		idx4 = idx3 + keyLen
		idx5 = idx4 + valLen
	)

	if len(p) < idx5 {
		return Entry{}, fmt.Errorf("entry data shorter than %d: %d", idx5, len(p))
	}

	b := make([]byte, keyLen+valLen)

	copy(b[:keyLen], p[idx3:idx4])
	copy(b[keyLen:keyLen+valLen], p[idx4:idx5])

	e := Entry{
		Kind: kind,
		Key:  util.BytesToString(b[:keyLen]),
	}

	if valLen > 0 {
		e.Value = util.BytesToString(b[keyLen : keyLen+valLen])
	}

	return e, nil
}

func HeaderFromBytes(p []byte) (kind EntryKind, keyLen, valLen uint32, err error) {
	const (
		lenSz = 4
		idx1  = 1
		idx2  = idx1 + lenSz
		idx3  = idx2 + lenSz
	)

	if len(p) < idx3 {
		err = fmt.Errorf("payload metadata shorter than %d: %d", idx3, len(p))
		return
	}

	kind = EntryKind(p[0])
	keyLen = byteOrder.Uint32(p[idx1:idx2])
	valLen = byteOrder.Uint32(p[idx2:idx3])

	return
}
