package log

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/nStangl/distributed-kv-store/server/util"
)

type (
	Record struct {
		Replica bool
		Kind    RecordKind
		Key     string
		Value   string
	}

	RecordKind uint8
)

const (
	Set RecordKind = iota + 1
	Tombstone
)

const (
	headerSz = 9
)

var kindStr = []string{"set", "tombstone"}

func (k RecordKind) String() string {
	return kindStr[k-1]
}

func NewSet(key, val string) Record {
	return Record{Kind: Set, Key: key, Value: val}
}

func NewReplicatedSet(key, val string) Record {
	return Record{Replica: true, Kind: Set, Key: key, Value: val}
}

func NewTombstone(key string) Record {
	return Record{Kind: Tombstone, Key: key}
}

func NewReplicatedTombstone(key string) Record {
	return Record{Replica: true, Kind: Tombstone, Key: key}
}

func (r *Record) String() string {
	return fmt.Sprintf("(%s, %q, %q)", r.Kind, r.Key, r.Value)
}

func (r *Record) Write(w io.Writer) error {
	_, err := w.Write(r.ToBytes())
	return err
}

func (r *Record) ToBytes() []byte {
	var (
		lenSz = 4
		idx1  = 1
		idx2  = idx1 + lenSz
		idx3  = idx2 + lenSz
		idx4  = idx3 + len(r.Key)
		idx5  = idx4 + len(r.Value)

		p = make([]byte, idx5)
		k = util.StringToBytes(r.Key)
		v = util.StringToBytes(r.Value)
	)

	p[0] = byte(r.Kind)
	byteOrder.PutUint32(p[idx1:idx2], uint32(len(k)))
	byteOrder.PutUint32(p[idx2:idx3], uint32(len(v)))

	copy(p[idx3:idx4], k)
	copy(p[idx4:idx5], v)

	return p
}

func (r *Record) ToWriter(w io.Writer) error {
	var (
		k = util.StringToBytes(r.Key)
		v = util.StringToBytes(r.Value)
	)

	_ = binary.Write(w, byteOrder, byte(r.Kind))
	_ = binary.Write(w, byteOrder, uint32(len(k)))
	_ = binary.Write(w, byteOrder, uint32(len(v)))
	_ = binary.Write(w, byteOrder, k)

	return binary.Write(w, byteOrder, v)
}

func (r *Record) Size() int {
	return headerSz + len(r.Key) + len(r.Value)
}

func FromBytes(p []byte) (Record, error) {
	const (
		lenSz = 4
		idx1  = 1
		idx2  = idx1 + lenSz
		idx3  = idx2 + lenSz
	)

	if len(p) < idx3 {
		return Record{}, fmt.Errorf("payload metadata shorter than %d: %d", idx3, len(p))
	}

	var (
		kind   = p[0]
		keyLen = int(byteOrder.Uint32(p[idx1:idx2]))
		valLen = int(byteOrder.Uint32(p[idx2:idx3]))

		idx4 = idx3 + keyLen
		idx5 = idx4 + valLen
	)

	if len(p) < idx5 {
		return Record{}, fmt.Errorf("payload data shorter than %d: %d", idx5, len(p))
	}

	b := make([]byte, keyLen+valLen)

	copy(b[:keyLen], p[idx3:idx4])
	copy(b[keyLen:keyLen+valLen], p[idx4:idx5])

	r := Record{
		Kind: RecordKind(kind),
		Key:  util.BytesToString(b[:keyLen]),
	}

	if valLen > 0 {
		r.Value = util.BytesToString(b[keyLen : keyLen+valLen])
	}

	return r, nil
}
