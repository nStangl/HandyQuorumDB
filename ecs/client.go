package ecs

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/looplab/fsm"
	"github.com/nStangl/distributed-kv-store/protocol"
)

type KVClient struct {
	proto      protocol.Protocol[protocol.ECSMessage]
	machine    *fsm.FSM
	heartbeats int
}

func NewKVClient(proto protocol.Protocol[protocol.ECSMessage], e *ECS) KVClient {
	return KVClient{proto: proto, machine: NewClientFSM(e)}
}

func (s *KVClient) String() string {
	var sb strings.Builder

	sb.WriteRune('(')
	sb.WriteString(s.proto.String())
	sb.WriteRune(',')
	sb.WriteString(strconv.Itoa(s.heartbeats))
	sb.WriteRune(')')

	return sb.String()
}

func (s *KVClient) Heartbeat() {
	s.heartbeats = 0
}

func (s *KVClient) MissHeartbeat() {
	s.heartbeats++
}

func (s *KVClient) Healthy() bool {
	return s.heartbeats < maxMissedHeartbeats
}

func (s *KVClient) Close() error {
	if s.proto.Closed() {
		return nil
	}

	if err := s.proto.Conn().Close(); err != nil {
		return fmt.Errorf("failed to close conn of client %s: %w", s.proto, err)
	}

	return nil
}
