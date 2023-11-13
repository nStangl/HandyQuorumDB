package web

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/nStangl/distributed-kv-store/protocol"
	"github.com/nStangl/distributed-kv-store/server/data"
	"github.com/nStangl/distributed-kv-store/server/store"
	"github.com/nStangl/distributed-kv-store/tcp"
	"github.com/nStangl/distributed-kv-store/util"
	log "github.com/sirupsen/logrus"
)

type PublicServer struct {
	id           uuid.UUID
	mu           sync.Mutex
	ecs          ECSClient
	addr         *net.TCPAddr
	store        store.Store
	metadata     protocol.Metadata
	writeLock    atomic.Bool
	transferDone chan struct{}
}

type RequestKind uint8

const (
	Read RequestKind = iota + 1
	Write
)

const replicationFactor = 2

func NewPublicServer(cfg *Config, s store.Store, ecs ECSClient) (*PublicServer, error) {
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", cfg.Address, cfg.Port))
	if err != nil {
		return nil, fmt.Errorf("failed to resolve address %q: %w", addr, err)
	}

	return &PublicServer{id: uuid.New(), ecs: ecs, addr: addr, store: s, transferDone: make(chan struct{}, 1)}, nil
}

func (s *PublicServer) ID() uuid.UUID {
	return s.id
}

func (s *PublicServer) Server() (*tcp.Server[protocol.ClientMessage], error) {
	return tcp.NewServer(
		s.addr.IP.String(), s.addr.Port,
		tcp.WithProtocol(protocol.ForClient),
		tcp.OnHandle(s.OnHandle()),
		tcp.OnShutdown[protocol.ClientMessage](s.OnShutdown()),
		tcp.OnGracefulShutdown[protocol.ClientMessage](s.OnGracefulShutdown()),
		tcp.OnConnect(func(proto protocol.Protocol[protocol.ClientMessage]) {
			if err := proto.Send(protocol.ClientMessage{Type: protocol.GetSuccess, Key: "hello", Value: "there"}); err != nil {
				log.Errorf("Failed to write initial message to the client: %v", proto.Conn().RemoteAddr())
			}
		}),
	)
}

func (s *PublicServer) OnHandle() tcp.HandleFunc[protocol.ClientMessage] {
	return func(proto protocol.Protocol[protocol.ClientMessage]) bool {
		m, err := proto.Receive()
		if err != nil {
			if errors.Is(err, protocol.ErrConnClosed) {
				log.Infof("Client is disconnected: %v", proto.Conn().RemoteAddr())
				return false
			}

			o := protocol.ClientMessage{Type: protocol.Error, Key: err.Error()}

			switch err {
			case protocol.ErrGetError:
				o.Type = protocol.GetError
			case protocol.ErrPutError:
				o.Type = protocol.PutError
			}

			if err := proto.Send(o); err != nil {
				log.Errorf("error when sending put reply: %v", err)
			}

			return true
		}

		if m == nil {
			return true
		}

		switch m.Type {
		case protocol.Get:
			if err := s.handleGet(proto, m.Key); err != nil {
				log.Errorf("error when sending get reply: %v", err)
			}
		case protocol.Put:
			if s.writeLock.Load() {
				if err := proto.Send(protocol.ClientMessage{Type: protocol.Error, Key: "write_lock"}); err != nil {
					log.Errorf("error when sending write lock reply: %v", err)
				}
			}

			if err := s.handlePut(proto, m.Key, m.Value); err != nil {
				log.Errorf("error when sending get reply: %v", err)
			}
		case protocol.Delete:
			if s.writeLock.Load() {
				if err := proto.Send(protocol.ClientMessage{Type: protocol.Error, Key: "write_lock"}); err != nil {
					log.Errorf("error when sending write lock reply: %v", err)
				}
			}

			if err := s.handleDelete(proto, m.Key); err != nil {
				log.Errorf("error when sending del reply: %v", err)
			}
		case protocol.Keyrange:
			serialized := s.metadata.FilterReplicas().HexString()
			if err := proto.Send(protocol.ClientMessage{Type: protocol.KeyrangeSuccess, Key: serialized}); err != nil {
				log.Errorf("error when sending keyrange reply: %v", err)
			}
		case protocol.KeyrangeRead:
			readableRanges, err := s.metadata.ReadableFrom(replicationFactor)
			if err != nil {
				log.Errorf("failed to compute readable ranges: %v", err)
				return true
			}

			if err := proto.Send(protocol.ClientMessage{Type: protocol.KeyrangeReadSuccess, Key: readableRanges.HexString()}); err != nil {
				log.Errorf("error when sending keyrange read reply: %v", err)
			}
		default:
			log.Warnf("received unknown message %#v from %v", m, proto.Conn().RemoteAddr())

			o := protocol.ClientMessage{Type: protocol.Error, Key: "invalid_input"}

			if err := proto.Send(o); err != nil {
				log.Errorf("error when sending put reply: %v", err)
			}
		}

		return true
	}
}

// This gets called once the graceful shutdown has completed
func (s *PublicServer) OnShutdown() tcp.CloseFunc {
	return func() error {
		if err := s.store.Close(); err != nil {
			return fmt.Errorf("failed to close the store: %w", err)
		}

		return nil
	}
}

// This gets executed once we receive a sigterm (ctrl+c)
// and should perform the graceful shutdown logic
func (s *PublicServer) OnGracefulShutdown() tcp.CloseFunc {
	return func() error {
		// Immediatelly stop accepting new writes
		// when we're shutting down
		s.setWriteLock()

		log.Info("doing the graceful shutdown")

		if err := s.ecs.Transition(context.Background(), &protocol.ECSMessage{Type: protocol.LeaveNetwork}); err != nil {
			return fmt.Errorf("failed to send LeaveNetwork to ECS: %w", err)
		}

		select {
		case <-s.transferDone:
			return nil
		case <-time.After(2 * time.Second):
			log.Info("timed-out waiting for graceful shutdown")
			return nil
		}
	}
}

func (s *PublicServer) OnTransferDone() {
	close(s.transferDone)
}

func (s *PublicServer) handlePut(proto protocol.Protocol[protocol.ClientMessage], key, value string) error {
	msg := protocol.ClientMessage{Type: protocol.PutError}

	if s.responsibleFor(key, Write) {
		if result, err := s.store.Get(key); err == nil {
			switch result.Kind {
			case data.Present:
				if err := s.store.Set(key, value); err == nil {
					msg = protocol.ClientMessage{Type: protocol.PutUpdate, Key: key}
				} else {
					log.Errorf("failed to set existing key: %v", err)
				}
			case data.Deleted, data.Missing:
				if err := s.store.Set(key, value); err == nil {
					msg = protocol.ClientMessage{Type: protocol.PutSuccess, Key: key}
				} else {
					log.Errorf("failed to set fresh key: %v", err)
				}
			}
		} else {
			log.Errorf("failed to get key: %v", err)
		}
	} else {
		msg = protocol.ClientMessage{Type: protocol.ServerNotResponsible}
	}

	if err := proto.Send(msg); err != nil {
		return fmt.Errorf("error when sending put reply: %v", err)
	}

	return nil
}

// is the current server/replica responsible for this key?
func (s *PublicServer) responsibleFor(key string, kind RequestKind) bool {
	for _, kr := range s.metadata {
		// if this range covers the key and
		// the node IDs match, return true
		if kr.ID == s.id && kr.Covers(key) {
			if !kr.IsReplica() {
				return true
			}

			switch kind {
			case Read:
				return kr.Replica.Read
			case Write:
				return kr.Replica.Write
			}
		}
	}

	var us protocol.KeyRange
	for i := range s.metadata {
		if s.metadata[i].ID == s.id && !s.metadata[i].Replica.Read {
			us = s.metadata[i]
		}
	}

	// Debug
	hash := util.MD5HashUint128(key)
	hashH := util.Uint128BigEndian(hash)
	log.Warnf("Current server %s:%s:%d (%s) not responsible for key %s with hash(dec): %s hash(hex): %s", s.id, s.addr.IP, s.addr.Port, us.String(), key, hash, hashH)
	for _, kr := range s.metadata {
		if kr.Covers(key) {
			log.Warnf("Keyrange %s covers, Replica: %t", kr.String(), kr.Replica)
		}
	}
	// -- Debug

	return false
}

func (s *PublicServer) handleGet(proto protocol.Protocol[protocol.ClientMessage], key string) error {
	msg := protocol.ClientMessage{Type: protocol.GetError}

	if s.responsibleFor(key, Read) {
		if result, err := s.store.Get(key); err == nil {
			if result.Kind == data.Present {
				msg = protocol.ClientMessage{Type: protocol.GetSuccess, Key: key, Value: result.Value}
			} else {
				msg = protocol.ClientMessage{Type: protocol.GetError, Key: key}
			}
		} else {
			log.Errorf("failed to gey key: %v", err)
		}
	} else {
		msg = protocol.ClientMessage{Type: protocol.ServerNotResponsible}
	}

	if err := proto.Send(msg); err != nil {
		return fmt.Errorf("error when sending get reply: %v", err)
	}

	return nil
}

func (s *PublicServer) handleDelete(proto protocol.Protocol[protocol.ClientMessage], key string) error {
	msg := protocol.ClientMessage{Type: protocol.DeleteError, Key: key}

	if s.responsibleFor(key, Write) {
		if result, err := s.store.Get(key); err == nil {
			if result.Kind == data.Present {
				if err := s.store.Del(key); err == nil {
					msg = protocol.ClientMessage{Type: protocol.DeleteSucess, Key: key, Value: result.Value}
				} else {
					log.Errorf("failed to del key: %v", err)
				}
			}
		} else {
			log.Errorf("failed to get key: %v", err)
		}
	} else {
		msg = protocol.ClientMessage{Type: protocol.ServerNotResponsible}
	}

	if err := proto.Send(msg); err != nil {
		return fmt.Errorf("error when sending del reply: %v", err)
	}

	return nil
}

func (s *PublicServer) setWriteLock() {
	s.writeLock.Store(true)
}

func (s *PublicServer) liftWriteLock() {
	s.writeLock.Store(false)
}

func (s *PublicServer) setMetadata(metadata protocol.Metadata) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.metadata = metadata
}
