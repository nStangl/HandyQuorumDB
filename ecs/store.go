package ecs

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/nStangl/distributed-kv-store/util"

	"github.com/looplab/fsm"
	"github.com/nStangl/distributed-kv-store/protocol"
)

type (
	KVStore struct {
		id          uuid.UUID
		addr        *net.TCPAddr
		proto       protocol.Protocol[protocol.ECSMessage]
		machine     *fsm.FSM
		successor   fmt.Stringer
		predecessor fmt.Stringer
		privateAddr *net.TCPAddr
		heartbeats  int
	}

	KVStoreState = string
)

const maxMissedHeartbeats = 10

const (
	Fresh                  KVStoreState = "fresh"
	CatchingUp             KVStoreState = "catching_up"
	Available              KVStoreState = "available"
	WriteLocked            KVStoreState = "write_locked"
	WriteLockRequested     KVStoreState = "write_lock_requested"
	WriteLockLiftRequested KVStoreState = "write_lock_lift_requested"
	TransferingData        KVStoreState = "transfering_data"
	TransferingLastData    KVStoreState = "transfering_last_data"
	ReceivingData          KVStoreState = "receiving_data"
	Leaving                KVStoreState = "leaving"
	LeftNetwork            KVStoreState = "left_network"
)

var ALL_STATES = [...]string{Fresh, CatchingUp, Available, WriteLocked, WriteLockRequested,
	WriteLockLiftRequested, TransferingData, TransferingLastData, ReceivingData, Leaving, LeftNetwork}

func NewKVStore(proto protocol.Protocol[protocol.ECSMessage], s *ECS) KVStore {
	return KVStore{proto: proto, machine: NewFSM(s)}
}

func (s *KVStore) String() string {
	var sb strings.Builder

	sb.WriteRune('(')
	sb.WriteString(s.proto.String())
	sb.WriteRune(',')
	sb.WriteString(s.machine.Current())
	sb.WriteRune(',')

	if s.successor != nil {
		sb.WriteString(s.successor.String())
		sb.WriteRune(',')
	}
	if s.predecessor != nil {
		sb.WriteString(s.predecessor.String())
		sb.WriteRune(',')
	}
	if s.privateAddr != nil {
		sb.WriteString(s.privateAddr.String())
		sb.WriteRune(',')
	}

	sb.WriteString(strconv.Itoa(s.heartbeats))
	sb.WriteRune(')')

	return sb.String()
}

func (s *KVStore) Heartbeat() {
	s.heartbeats = 0
}

func (s *KVStore) MissHeartbeat() {
	s.heartbeats++
}

func (s *KVStore) Healthy() bool {
	return s.heartbeats < maxMissedHeartbeats
}

func (s *KVStore) Close() error {
	if s.proto.Closed() {
		return nil
	}

	if err := s.proto.Conn().Close(); err != nil {
		return fmt.Errorf("failed to close conn of store %s: %w", s.proto, err)
	}

	return nil
}

func (s *KVStore) Copy() KVStore {
	return KVStore{
		id:          s.id,
		addr:        util.CopyAddr(s.addr),
		privateAddr: util.CopyAddr(s.privateAddr),
	}
}
