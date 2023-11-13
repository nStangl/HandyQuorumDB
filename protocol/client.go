package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"strings"
)

type (
	// Message exchanged client <-> server
	ClientMessage struct {
		Type  ClientType
		Key   string
		Value string
	}

	ClientType uint8
)

const (
	Put ClientType = iota + 1
	PutSuccess
	PutUpdate
	PutError
	Get
	GetSuccess
	GetError
	Delete
	DeleteSucess
	DeleteError
	Keyrange
	KeyrangeSuccess
	KeyrangeError
	KeyrangeRead
	KeyrangeReadSuccess
	KeyrangeReadError
	ServerNotResponsible
	Error
)

var (
	ErrGetError = errors.New("get_error")
	ErrPutError = errors.New("put_error")

	clientTypeKeys = [...]string{
		"put",
		"put_success",
		"put_update",
		"put_error",
		"get",
		"get_success",
		"get_error",
		"delete",
		"delete_success",
		"delete_error",
		"keyrange",
		"keyrange_success",
		"keyrange_error",
		"keyrange_read",
		"keyrange_read_success",
		"keyrange_read_error",
		"server_not_responsible",
		"error",
	}
)

func ForClient(conn *net.TCPConn) Protocol[ClientMessage] {
	return New(conn, parseClientMessage)
}

func (t ClientType) String() string {
	return clientTypeKeys[t-1]
}

func (m ClientMessage) String() string {
	return fmt.Sprintf("client(%s,%s,%s)", m.Type, m.Key, m.Value)
}

func (m ClientMessage) Bytes() []byte {
	b := bytes.NewBuffer(make([]byte, 0, len(m.Key)+len(m.Value)))

	b.WriteString(m.Type.String())

	if m.Key != "" {
		b.WriteRune(' ')
		b.WriteString(m.Key)
	}

	if m.Value != "" {
		b.WriteRune(' ')
		b.WriteString(m.Value)
	}

	b.WriteByte('\r')
	b.WriteByte('\n')

	return b.Bytes()
}

func parseClientType(s string) (ClientType, bool) {
	for i := range clientTypeKeys {
		if clientTypeKeys[i] == s {
			return ClientType(i + 1), true
		}
	}

	return ClientType(0), false
}

func parseClientMessage(raw string) (ClientMessage, error) {
	p := strings.SplitN(raw, " ", 3)
	if len(p) == 0 {
		return ClientMessage{}, ErrInvalidFormat
	}

	t, ok := parseClientType(p[0])
	if !ok {
		return ClientMessage{}, ErrInvalidFormat
	}

	msg := ClientMessage{Type: t}

	switch msg.Type {
	case Get:
		if len(p) == 2 {
			msg.Key = p[1]
		} else {
			return msg, ErrGetError
		}
	case GetSuccess:
		if len(p) == 3 {
			msg.Key = p[1]
			msg.Value = p[2]
		} else {
			return msg, ErrInvalidFormat
		}
	case GetError:
		if len(p) == 2 {
			msg.Key = p[1]
		} else {
			return msg, ErrInvalidFormat
		}
	case Put:
		if len(p) == 2 {
			msg.Type = Delete
			msg.Key = p[1]
		} else if len(p) == 3 {
			msg.Key = p[1]
			msg.Value = p[2]
		} else {
			return msg, ErrPutError
		}
	case PutSuccess:
		if len(p) == 2 {
			msg.Key = p[1]
		} else {
			return msg, ErrInvalidFormat
		}
	case PutUpdate:
		if len(p) == 2 {
			msg.Key = p[1]
		} else {
			return msg, ErrInvalidFormat
		}
	case Delete:
		if len(p) == 2 {
			msg.Key = p[1]
		} else {
			return msg, ErrInvalidFormat
		}
	case Keyrange:
		if len(p) != 1 {
			return msg, ErrInvalidFormat
		}
	case KeyrangeSuccess:
		if len(p) == 2 {
			msg.Key = p[1]
		} else {
			return msg, ErrInvalidFormat
		}
	case KeyrangeRead:
		if len(p) != 1 {
			return msg, ErrInvalidFormat
		}
	case KeyrangeReadSuccess:
		if len(p) == 2 {
			msg.Key = p[1]
		} else {
			return msg, ErrInvalidFormat
		}
	}

	return msg, nil
}
