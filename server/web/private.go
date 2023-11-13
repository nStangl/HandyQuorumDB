package web

import (
	"errors"
	"fmt"
	"net"

	"github.com/nStangl/distributed-kv-store/protocol"
	"github.com/nStangl/distributed-kv-store/server/store"
	"github.com/nStangl/distributed-kv-store/tcp"
	log "github.com/sirupsen/logrus"
)

type PrivateServer struct {
	ecs   ECSClient
	addr  *net.TCPAddr
	store store.Store
}

func NewPrivateServer(cfg *Config, s store.Store, ecs ECSClient) (*PrivateServer, error) {
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", cfg.Address, cfg.Port))
	if err != nil {
		return nil, fmt.Errorf("failed to resolve address %q: %w", addr, err)
	}

	return &PrivateServer{addr: addr, store: s, ecs: ecs}, nil
}

func (s *PrivateServer) Server() (*tcp.Server[protocol.ServerMessage], error) {
	return tcp.NewServer(
		s.addr.IP.String(), s.addr.Port,
		tcp.WithProtocol(protocol.ForServer),
		tcp.OnHandle(s.Handler()),
	)
}

func (s *PrivateServer) Handler() tcp.HandleFunc[protocol.ServerMessage] {
	return func(proto protocol.Protocol[protocol.ServerMessage]) bool {
		m, err := proto.Receive()
		if err != nil {
			if errors.Is(err, protocol.ErrConnClosed) {
				return false
			}

			log.Errorf("error while receiving from %s: %v", proto, err)

			return true
		}

		if m == nil {
			return true
		}

		// Handler server <-> server msg
		log.Infof("received %s from KVStore %s", m.String(), proto)

		switch m.Type {
		case protocol.TransferTuple:
			if err := s.handleTransfer(m.Key, m.Value); err != nil {
				log.Errorf("failed to receive tuple: %v", err)
			}
		case protocol.ReplicateTuple:
			if err := s.handleReplication(m.Key, m.Value); err != nil {
				log.Errorf("failed to replicate tuple tuple: %v", err)
			}
		}

		return true
	}
}

func (s *PrivateServer) handleTransfer(key, value string) error {
	var err error

	if value == "" {
		err = s.store.Del(key)
	} else {
		err = s.store.Set(key, value)
	}

	return err
}

func (s *PrivateServer) handleReplication(key, value string) error {
	var err error

	if value == "" {
		err = s.store.DelReplicated(key)
	} else {
		err = s.store.SetReplicated(key, value)
	}

	return err
}
