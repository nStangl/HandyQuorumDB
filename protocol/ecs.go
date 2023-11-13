package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"net"
	"strings"
)

type (
	// Message exchanged ecs <-> server
	ECSMessage struct {
		Ack            bool
		ID             uuid.UUID
		Type           ECSType
		Address        string
		PrivateAddress string
		Metadata       Metadata
	}

	ECSType uint8
)

const (
	// Send by server when it first connects to the ECS
	JoinNetwork ECSType = iota + 1
	// Sends by the server to initiate graceful shutdown
	LeaveNetwork
	// Sent by the ECS to update metadata (=keyranges) on the server (requires ACK)
	SendMetadata
	// Sent by the ECS to server to impose a write lock
	SetWriteLock
	// Sent by the ECS to server to lift a write lock (requires ACK)
	LiftWriteLock
	// Sent by the ECS to server to invoke data transfer to another server (requires ACK)
	InvokeTransfer
	// Sent by the server to ECS to confirm finished data data transfer to another server (requires ACK)
	NotifyTransferFinished
	// Sent every N seconds from server to ECS
	Heartbeat
)

/* Graceful shutdown:
 * Server A wants to shut down (got sigterm, sigkill)
 * Server A sends LeaveNetwork to ECS
 * ECS sends SetWriteLock to Server A
 * ECS sends (InvokeTransfer, ip of Server B) to server A
 * Server A connects to server B on private port
 * Server A sends BeginTransfer to Server B
 * For each (k,v): Server A sends (TransferTuple, k, v) to Server B
 * Server A sends EndTransfer to Server B
 * Server A sends NotifyTransferFinished to ECS
 * Server A finishes
 */

/* Work:
 * 1) Consistent hashing & partitioning (pure logic)
 * 2) ECS component: ECS <-> Server proto, partitioning mapping, rebalancing etc.
 * 3) Server component: handling the requests/responses from ECS, graceful shutdown, receiving metadata updates, write locK?
 * 4) Client path (caching of the metadata)
 */

const ackPrefix = "ack "

var (
	ErrInvalidKeyRange = errors.New("invalid_key_range")

	ercTypeKeys = [...]string{
		"join_network",
		"leave_network",
		"send_metadata",
		"set_write_lock",
		"lift_write_lock",
		"invoke_transfer",
		"notify_transfer_finished",
		"heartbeat",
	}
)

func ForECS(conn *net.TCPConn) Protocol[ECSMessage] {
	return New(conn, parseECSMessage)
}

func (t ECSType) String() string {
	return ercTypeKeys[t-1]
}

func (m ECSMessage) String() string {
	return fmt.Sprintf("ecs(%t,%s,%s,%s)", m.Ack, m.Type, m.Address, m.Metadata)
}

func (m ECSMessage) Bytes() []byte {
	b := bytes.NewBuffer(make([]byte, 0))

	if m.Ack {
		b.WriteString(ackPrefix)
		b.WriteString(m.Type.String())

		b.WriteRune('\r')
		b.WriteRune('\n')

		return b.Bytes()
	}

	b.WriteString(m.Type.String())

	switch m.Type {
	case JoinNetwork:
		b.WriteRune(' ')
		b.WriteString(m.ID.String())
		b.WriteRune(' ')
		b.WriteString(m.Address)
		b.WriteRune(' ')
		b.WriteString(m.PrivateAddress)
	case SendMetadata:
		b.WriteRune(' ')
		b.Write(m.Metadata.Bytes())
	case InvokeTransfer:
		b.WriteRune(' ')
		b.WriteString(m.Address)

		if len(m.Metadata) > 0 {
			b.WriteRune(' ')
			b.Write(m.Metadata.Bytes())
		}
	}

	b.WriteRune('\r')
	b.WriteRune('\n')

	return b.Bytes()
}

func (m *ECSMessage) MakeAck() ECSMessage { return ECSMessage{Ack: true, Type: m.Type} }

func (m *ECSMessage) AckFor(a *ECSMessage) bool {
	return a.Ack && a.Type == m.Type
}

func parseECSMessage(raw string) (ECSMessage, error) {
	if strings.HasPrefix(raw, ackPrefix) {
		t, ok := parseECSType(strings.TrimPrefix(raw, ackPrefix))
		if !ok {
			return ECSMessage{}, ErrInvalidFormat
		}

		return ECSMessage{Ack: true, Type: t}, nil
	}

	p := strings.SplitN(raw, " ", 4)
	if len(p) == 0 {
		return ECSMessage{}, ErrInvalidFormat
	}

	t, ok := parseECSType(p[0])
	if !ok {
		return ECSMessage{}, ErrInvalidFormat
	}

	msg := ECSMessage{Type: t}

	switch msg.Type {
	case JoinNetwork:
		if len(p) != 4 {
			return msg, ErrInvalidFormat
		}

		msg.ID = uuid.MustParse(p[1])
		msg.Address = p[2]
		msg.PrivateAddress = p[3]
	case SendMetadata:
		if len(p) != 2 {
			return msg, ErrInvalidFormat
		}

		r, err := parseMetadata(p[1])
		if err != nil {
			return msg, err
		}

		msg.Metadata = r
	case InvokeTransfer:
		switch len(p) {
		case 2:
			msg.Address = p[1]
		case 3:
			msg.Address = p[1]

			r, err := parseMetadata(p[2])
			if err != nil {
				return msg, ErrInvalidFormat
			}

			msg.Metadata = r
		}
	}

	return msg, nil
}

func parseECSType(s string) (ECSType, bool) {
	for i := range ercTypeKeys {
		if ercTypeKeys[i] == s {
			return ECSType(i + 1), true
		}
	}

	return ECSType(0), false
}
