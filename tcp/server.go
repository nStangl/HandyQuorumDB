package tcp

import (
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nStangl/distributed-kv-store/protocol"
	log "github.com/sirupsen/logrus"
)

type Server[M protocol.Message] struct {
	wg             sync.WaitGroup
	done           chan interface{}
	quit           chan interface{}
	close          atomic.Bool
	handler        HandleFunc[M]
	listener       *net.TCPListener
	closeFunc      CloseFunc
	gracefulFunc   CloseFunc
	connectFunc    ProtoFunc[M]
	disconnectFunc ProtoFunc[M]
	protocolFunc   ProtocolProducerFunc[M]
}

const clientInactivty time.Duration = 60 * time.Second

func NewServer[M protocol.Message](address string, port int, options ...Option[M]) (*Server[M], error) {
	s := Server[M]{
		done: make(chan interface{}),
		quit: make(chan interface{}),
	}

	for _, o := range options {
		o(&s)
	}

	const typ = "tcp"

	addr, err := net.ResolveTCPAddr(typ, fmt.Sprintf("%s:%d", address, port))
	if err != nil {
		return nil, fmt.Errorf("failed to resolve address %q: %w", addr, err)
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on address %q: %w", addr, err)
	}

	s.listener = l

	return &s, nil
}

func (s *Server[M]) Serve() (<-chan struct{}, <-chan error) {
	var (
		don = make(chan struct{})
		ers = make(chan error, 10)
	)

	go func() {
		defer close(don)
		defer close(ers)

		log.Info("server is listening for incoming connections on ", s.listener.Addr())

	outer:
		for {
			conn, err := s.listener.AcceptTCP()
			if err != nil {
				select {
				case <-s.quit:
					log.Info("server is shutting down")
					s.wg.Wait()
					break outer
				default:
					ers <- fmt.Errorf("failed to accept connection: %w", err)
				}
			} else {
				s.wg.Add(1)

				proto := s.protocolFunc(conn)

				go func() {
					defer s.wg.Done()

					if s.connectFunc != nil {
						s.connectFunc(proto)
					}

					s.handleConnection(proto)
				}()
			}
		}

		if s.closeFunc != nil {
			if err := s.closeFunc(); err != nil {
				ers <- err
			}
		}
	}()

	return don, ers
}

func (s *Server[M]) Close() error {
	if s.gracefulFunc != nil {
		if err := s.gracefulFunc(); err != nil {
			log.Errorf("failed to perform graceful shutdown: %v", err)
		}
	}

	close(s.quit)
	s.close.Store(true)
	return s.listener.Close()
}

func (s *Server[M]) handleConnection(proto protocol.Protocol[M]) {
	log.Infof("handling connection on %s", proto.Conn().RemoteAddr())

	defer func() {
		if s.disconnectFunc != nil {
			s.disconnectFunc(proto)
		}

		if proto.Closed() {
			return
		}

		_ = proto.Close()
	}()

	lastClientActivity := time.Now()

	for {
		if s.close.Load() {
			log.Infof("disconnecting client due to server shutdown: %s", proto.Conn().RemoteAddr())
			return
		}

		if isConnClosed(proto.Conn()) {
			log.Infof("client disconnected: %s", proto.Conn().RemoteAddr())
			return
		}

		if diff := time.Since(lastClientActivity); diff > clientInactivty {
			log.Infof("client is inactive, disconnecting: %s", proto.Conn().RemoteAddr())
			return
		}

		lastClientActivity = time.Now()

		if ok := s.handler(proto); !ok {
			return
		}
	}
}

func isConnClosed(conn *net.TCPConn) bool {
	one := make([]byte, 1)
	_ = conn.SetReadDeadline(time.Now())

	if _, err := conn.Read(one); err == io.EOF {
		return true
	} else {
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
		return false
	}
}
