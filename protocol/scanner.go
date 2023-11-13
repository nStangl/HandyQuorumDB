package protocol

import (
	"bytes"
	"io"
	"net"
	"time"
)

type (
	Scanner struct {
		buf    *bytes.Buffer
		conn   *net.TCPConn
		tokens []string
	}

	ScanResult struct {
		Case  ScanCase
		Token string
	}

	ScanCase uint8
)

const (
	DataAvailable ScanCase = iota + 1
	DataBuffered
	ClientDisconnected
)

const (
	timeout = 15 * time.Second
	bufSz   = 2 << 12
	chunkSz = 2 << 10
)

func NewScanner(conn *net.TCPConn) *Scanner {
	return &Scanner{
		buf:  bytes.NewBuffer(make([]byte, 0, bufSz)),
		conn: conn,
	}
}

func (s *Scanner) Scan() (ScanResult, error) {
	if len(s.tokens) > 0 {
		var t string
		t, s.tokens = s.tokens[0], s.tokens[1:]
		return ScanResult{Case: DataAvailable, Token: t}, nil
	}

	if err := s.conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return ScanResult{Case: ClientDisconnected}, nil
	}

	data := make([]byte, chunkSz)

	n, err := s.conn.Read(data)
	if err != nil {
		if err == io.EOF {
			return ScanResult{Case: ClientDisconnected}, nil
		} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return ScanResult{Case: DataBuffered}, nil
		}

		return ScanResult{Case: ClientDisconnected}, nil
	}

	if n > 0 && n < len(data) {
		data = data[:n]
	}

	var p int

	for {
		if i := bytes.IndexAny(data[p:], "\r\n"); i >= 0 {
			s.buf.Write(data[p : p+i])
			s.tokens = append(s.tokens, s.buf.String())
			s.buf.Reset()

			p += i + 2
		} else {
			s.buf.Write(data[p:])

			break
		}
	}

	if len(s.tokens) > 0 {
		var t string
		t, s.tokens = s.tokens[0], s.tokens[1:]

		return ScanResult{Case: DataAvailable, Token: t}, nil
	}

	return ScanResult{Case: DataBuffered}, nil
}
