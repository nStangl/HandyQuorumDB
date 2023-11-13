package protocol

import (
	"bytes"
	"fmt"
	"net"
	"strings"
)

type (
	// Message exchanged server <-> server
	ServerMessage struct {
		Ack   bool
		Type  ServerType
		Key   string
		Value string
	}

	ServerType uint8
)

const (
	// Send by server to server to begin transfer of data (requires ACK)
	BeginTransfer ServerType = iota + 1
	// Send by server to server to end transfer of data (requires ACK)
	EndTransfer
	// Send by server to server to transfer a single key-value pair
	TransferTuple
	// ReplicateTuple from coordinator to replica
	ReplicateTuple
	// Send by server to server responsible for this tuple
	// (requires ACK; informs if successful or not)
	ForwardTuple
)

var serverTypeKeys = [...]string{
	"begin_transfer",
	"end_transfer",
	"transfer_tuple",
	"replicate_tuple",
}

func ForServer(conn *net.TCPConn) Protocol[ServerMessage] {
	return New(conn, parseServerMessage)
}

func (t ServerType) String() string {
	return serverTypeKeys[t-1]
}

func (m ServerMessage) String() string {
	return fmt.Sprintf("server(%t,%s,%s,%s)", m.Ack, m.Type, m.Key, m.Value)
}

func (m ServerMessage) Bytes() []byte {
	b := bytes.NewBuffer(make([]byte, 0, len(m.Key)+len(m.Value)))

	if m.Ack {
		b.WriteString(ackPrefix)
		b.WriteString(m.Type.String())

		b.WriteRune('\r')
		b.WriteRune('\n')

		return b.Bytes()
	}

	b.WriteString(m.Type.String())

	if m.Key != "" {
		b.WriteRune(' ')
		b.WriteString(m.Key)
	}

	if m.Value != "" {
		b.WriteRune(' ')
		b.WriteString(m.Value)
	}

	b.WriteRune('\r')
	b.WriteRune('\n')

	return b.Bytes()
}

func (m ServerMessage) MakeAck() ServerMessage { return ServerMessage{Ack: true, Type: m.Type} }

func parseServerMessage(raw string) (ServerMessage, error) {
	if strings.HasPrefix(raw, ackPrefix) {
		t, ok := parseServerType(strings.TrimPrefix(raw, ackPrefix))
		if !ok {
			return ServerMessage{}, ErrInvalidFormat
		}

		return ServerMessage{Ack: true, Type: t}, nil
	}

	p := strings.SplitN(raw, " ", 3)
	if len(p) == 0 {
		return ServerMessage{}, ErrInvalidFormat
	}

	t, ok := parseServerType(p[0])
	if !ok {
		return ServerMessage{}, ErrInvalidFormat
	}

	msg := ServerMessage{Type: t}

	switch msg.Type {
	case TransferTuple, ReplicateTuple:
		switch len(p) {
		case 3:
			msg.Key = p[1]
			msg.Value = p[2]
		case 2:
			msg.Key = p[1]
		default:
			return msg, ErrInvalidFormat
		}
	}

	return msg, nil
}

func parseServerType(s string) (ServerType, bool) {
	for i := range serverTypeKeys {
		if serverTypeKeys[i] == s {
			return ServerType(i + 1), true
		}
	}

	return ServerType(0), false
}
