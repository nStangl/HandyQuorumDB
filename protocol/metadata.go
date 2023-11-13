package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/nStangl/distributed-kv-store/util"
	"lukechampine.com/uint128"
)

type (
	KeyRange struct {
		// Unique ID of each node
		ID uuid.UUID
		// Is this node a replica for this keyrange
		Replica Replica
		// The address of the node
		Addr *net.TCPAddr
		// The private address of the node
		PrivateAddr *net.TCPAddr
		// The actual key range
		Start, End uint128.Uint128
	}

	Replica struct {
		Read, Write bool
	}

	Metadata []KeyRange
)

func (k *KeyRange) Copy() KeyRange {
	return KeyRange{
		ID:          k.ID,
		Replica:     k.Replica,
		Addr:        util.CopyAddr(k.Addr),
		PrivateAddr: util.CopyAddr(k.PrivateAddr),
		Start:       k.Start,
		End:         k.End,
	}
}

func (k *KeyRange) String() string {
	return fmt.Sprintf("%s,%t,%s,%s,%s,%s", k.ID, k.Replica, k.Start, k.End, k.PrivateAddr, k.Addr)
}

func (k *KeyRange) IsReplica() bool {
	return k.Replica.Read || k.Replica.Write
}

func (m Metadata) String() string {
	return m.HexString()
}

func (m Metadata) HexString() string {
	var sb strings.Builder
	for _, r := range m {
		var (
			s = util.Uint128BigEndian(r.Start)
			e = util.Uint128BigEndian(r.End)
		)

		sb.WriteString(fmt.Sprintf("%s,%s,%s;", s, e, r.Addr))
	}

	return sb.String()
}

func (m Metadata) Bytes() []byte {
	var b bytes.Buffer

	for i := range m {
		if m[i].Replica.Read {
			b.WriteString("1")
		} else {
			b.WriteString("0")
		}
		b.WriteRune(',')

		if m[i].Replica.Write {
			b.WriteString("1")
		} else {
			b.WriteString("0")
		}
		b.WriteRune(',')

		b.WriteString(m[i].ID.String())
		b.WriteRune(',')

		b.WriteString(m[i].Start.String())
		b.WriteRune(',')

		b.WriteString(m[i].End.String())
		b.WriteRune(',')

		b.WriteString(m[i].PrivateAddr.String())
		b.WriteRune(',')

		b.WriteString(m[i].Addr.String())
		b.WriteRune(';')
	}

	return b.Bytes()
}

func (m Metadata) Identical(o Metadata) bool {
	if len(m) != len(o) {
		return false
	}

	var eq int

	for i := range m {
		for j := range o {
			if m[i].String() == o[j].String() {
				eq++
			}
		}
	}

	return eq == len(m)
}

func (m Metadata) GetCoordinator(id uuid.UUID) *KeyRange {
	for i := range m {
		if m[i].ID == id && !m[i].Replica.Read {
			return &m[i]
		}
	}

	return nil
}

func (k *KeyRange) Covers(key string) bool {
	// If start == end then keyrange covers all keys
	if k.Start.Cmp(k.End) == 0 {
		return true
	}
	h := util.MD5HashUint128(key)
	if k.Start.Cmp(k.End) == 1 {
		// Wrap around case
		return k.Start.Cmp(h) == -1 || h.Cmp(k.End) <= 0
	}
	return k.Start.Cmp(h) == -1 && h.Cmp(k.End) <= 0
}

func parseMetadata(raw string) (Metadata, error) {
	ranges := make(Metadata, 0)

	scanner := bufio.NewScanner(strings.NewReader(raw))
	scanner.Split(splitBy(';'))

	for scanner.Scan() {
		p := strings.SplitN(scanner.Text(), ",", 7)
		if len(p) != 7 {
			return nil, ErrInvalidKeyRange
		}

		readRep, err := parseBool(p[0])
		if err != nil {
			return nil, fmt.Errorf("failed to parse read replica flag: %w", err)
		}

		writeRep, err := parseBool(p[1])
		if err != nil {
			return nil, fmt.Errorf("failed to parse write replica flag: %w", err)
		}

		id, err := uuid.Parse(p[2])
		if err != nil {
			return nil, fmt.Errorf("failed to parse node id: %w", err)
		}

		start, err := uint128.FromString(p[3])
		if err != nil {
			return nil, fmt.Errorf("failed to parse range start: %w", err)
		}

		end, err := uint128.FromString(p[4])
		if err != nil {
			return nil, fmt.Errorf("failed to parse range end: %w", err)
		}

		privateAddr, err := net.ResolveTCPAddr("tcp", p[5])
		if err != nil {
			return nil, fmt.Errorf("failed to parse range private address: %w", err)
		}

		addr, err := net.ResolveTCPAddr("tcp", p[6])
		if err != nil {
			return nil, fmt.Errorf("failed to parse range address: %w", err)
		}

		ranges = append(ranges, KeyRange{
			ID:          id,
			Replica:     Replica{readRep, writeRep},
			Start:       start,
			End:         end,
			Addr:        addr,
			PrivateAddr: privateAddr,
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, ErrInvalidKeyRange
	}

	return ranges, nil
}

func (m Metadata) FilterReplicas() Metadata {
	var m_ Metadata

	for i := range m {
		if !m[i].IsReplica() {
			m_ = append(m_, m[i])
		}
	}

	return m_
}

// Makes new Metadata based only on coordinator nodes and adds replicationFactor-times replicas for each coordinator.
func (m Metadata) MakeReplicatedMetadata(readReplication, writeReplication int) Metadata {
	metadata := m.FilterReplicas()

	if len(metadata) < (util.Min(readReplication, writeReplication) + 1) {
		return metadata
	}

	sort.Slice(metadata, func(i, j int) bool {
		return metadata[i].Start.Cmp(metadata[j].Start) == -1
	})

	result := make(Metadata, 0, len(metadata)*(util.Max(readReplication, writeReplication)+1))

	for i := range metadata {
		kr := &metadata[i]
		result = append(result, kr.Copy())

		for j := 1; j <= util.Max(readReplication, writeReplication); j++ {
			idx := util.Modulo(i+j, len(metadata))
			replica := metadata[idx].Copy()

			if j <= readReplication {
				replica.Replica.Read = true
			}

			if j <= writeReplication {
				replica.Replica.Write = true
			}

			replica.Start = kr.Start
			replica.End = kr.End

			result = append(result, replica)
		}
	}

	// Maybe we need to sort it again? Not sure
	sort.Slice(result, func(i, j int) bool {
		return result[i].Start.Cmp(result[j].Start) == -1
	})

	return result
}

// Takes current metadata and returns new metadata with key ranges that a client can read from.
// Example for replicationFactor = 2:
//
// Start: current.getPred.getPred.Start, End: current.End
func (m Metadata) ReadableFrom(replicationFactor int) (Metadata, error) {
	coordinators := m.FilterReplicas()

	if replicationFactor >= len(coordinators) {
		return coordinators, errors.New("replicationFactor must be smaller than number of stores")
	}

	sort.Slice(coordinators, func(i, j int) bool {
		return coordinators[i].Start.Cmp(coordinators[j].Start) == -1
	})

	result := make(Metadata, 0, len(coordinators))
	for i := range coordinators {
		kr := coordinators[i]
		kr.Start = coordinators[util.Modulo(i-replicationFactor, len(coordinators))].Start
		result = append(result, kr)
	}

	// Maybe we need to sort it again? Not sure
	sort.Slice(result, func(i, j int) bool {
		return result[i].Start.Cmp(result[j].Start) == -1
	})

	return result, nil
}

func splitBy(delim rune) bufio.SplitFunc {
	return func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}

		if i := bytes.IndexRune(data, delim); i >= 0 {
			return i + 1, data[0:i], nil
		}

		if atEOF {
			return len(data), data, nil
		}

		return
	}
}

func parseBool(s string) (bool, error) {
	switch s {
	case "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, fmt.Errorf("value %q is not a boolean", s)
	}
}
