package protocol

import (
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/nStangl/distributed-kv-store/server/util"
)

type (
	// A generic interface representing
	// a message of some protocol
	Message interface {
		fmt.Stringer

		Bytes() []byte
	}

	// Parser for a given protocol
	ParserFunc[M Message] func(string) (M, error)

	Protocol[M Message] interface {
		fmt.Stringer

		Conn() *net.TCPConn
		Close() error
		Closed() bool
		Send(M) error
		Receive() (*M, error)
	}

	// Implementation of a given protocol
	ProtocolImpl[M Message] struct {
		rmu, wmu sync.Mutex
		tag      string
		conn     *net.TCPConn
		closed   bool
		parser   ParserFunc[M]
		scanner  *Scanner
	}
)

var _ Protocol[ClientMessage] = (*ProtocolImpl[ClientMessage])(nil)

var (
	ErrConnClosed      = errors.New("conn_closed")
	ErrInvalidFormat   = errors.New("message_format_invalid")
	ErrBusyWaitTimeout = errors.New("busy_wait_timeout")
)

func New[M Message](conn *net.TCPConn, parser ParserFunc[M]) *ProtocolImpl[M] {
	h, p := util.ParseAddress(conn.RemoteAddr())

	return &ProtocolImpl[M]{
		tag:     fmt.Sprintf("%s:%d", h, p),
		conn:    conn,
		parser:  parser,
		scanner: NewScanner(conn),
	}
}

func (c *ProtocolImpl[M]) String() string {
	return c.tag
}

func (c *ProtocolImpl[M]) Send(msg M) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()

	if _, err := c.conn.Write(msg.Bytes()); err != nil {
		return fmt.Errorf("failed to write to socket %v: %w", c.conn.RemoteAddr(), err)
	}

	return nil
}

func (c *ProtocolImpl[M]) Receive() (*M, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()

	r, err := c.scanner.Scan()
	if err != nil {
		return nil, fmt.Errorf("scanning connection %v failed: %w", c.conn.RemoteAddr(), err)
	}

	switch r.Case {
	case DataBuffered:
		return nil, nil
	case ClientDisconnected:
		return nil, ErrConnClosed
	}

	m, err := c.parser(r.Token)
	if err != nil {
		return nil, fmt.Errorf("failed to parse message %q: %w", r.Token, err)
	}

	return &m, nil
}

func (c *ProtocolImpl[M]) Conn() *net.TCPConn {
	return c.conn
}

func (c *ProtocolImpl[M]) Close() error {
	c.wmu.Lock()
	defer c.wmu.Unlock()

	c.closed = true

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("failed to close conn to %s: %w", c.String(), err)
	}

	return nil
}

func (c *ProtocolImpl[M]) Closed() bool {
	c.wmu.Lock()
	defer c.wmu.Unlock()

	return c.closed
}

func Connect(addr string) (*net.TCPConn, error) {
	const typ = "tcp"

	s, err := net.ResolveTCPAddr(typ, addr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve address %s: %w", addr, err)
	}

	c, err := net.DialTCP(typ, nil, s)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	return c, nil
}
