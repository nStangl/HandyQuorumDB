package tcp

import (
	"net"

	"github.com/nStangl/distributed-kv-store/protocol"
)

type (
	Option[M protocol.Message] func(*Server[M])

	CloseFunc func() error

	ProtoFunc[M protocol.Message] func(protocol.Protocol[M])

	HandleFunc[M protocol.Message] func(protocol.Protocol[M]) bool

	ProtocolProducerFunc[M protocol.Message] func(*net.TCPConn) protocol.Protocol[M]
)

func WithProtocol[M protocol.Message](proto ProtocolProducerFunc[M]) Option[M] {
	return func(s *Server[M]) {
		s.protocolFunc = proto
	}
}

func OnHandle[M protocol.Message](handleFunc HandleFunc[M]) Option[M] {
	return func(s *Server[M]) {
		s.handler = handleFunc
	}
}

func OnConnect[M protocol.Message](protoFunc ProtoFunc[M]) Option[M] {
	return func(s *Server[M]) {
		s.connectFunc = protoFunc
	}
}

func OnDisconnect[M protocol.Message](protoFunc ProtoFunc[M]) Option[M] {
	return func(s *Server[M]) {
		s.disconnectFunc = protoFunc
	}
}

func OnShutdown[M protocol.Message](closeFunc CloseFunc) Option[M] {
	return func(s *Server[M]) {
		s.closeFunc = closeFunc
	}
}

func OnGracefulShutdown[M protocol.Message](gracefulFunc CloseFunc) Option[M] {
	return func(s *Server[M]) {
		s.gracefulFunc = gracefulFunc
	}
}
