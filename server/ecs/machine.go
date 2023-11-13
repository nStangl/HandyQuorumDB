package ecs

import (
	"context"
	"fmt"

	"github.com/looplab/fsm"
	ec "github.com/nStangl/distributed-kv-store/ecs"
	"github.com/nStangl/distributed-kv-store/protocol"
	log "github.com/sirupsen/logrus"
)

var (
	// The transitions
	transitions = fsm.Events{
		{Name: ec.JoinNetwork, Src: []string{ec.Fresh}, Dst: ec.CatchingUp},
		{Name: ec.JoinNetwork, Src: []string{ec.CatchingUp}, Dst: ec.Available},
		{Name: ec.SetWriteLock, Src: []string{ec.Available}, Dst: ec.WriteLocked},
		{Name: ec.LiftWriteLock, Src: []string{ec.WriteLocked}, Dst: ec.Available},
		{Name: ec.LeaveNetwork, Src: ec.ALL_STATES[:], Dst: ec.Leaving},
		{Name: ec.InvokeTransfer, Src: []string{ec.WriteLocked}, Dst: ec.WriteLocked},
		{Name: ec.InvokeTransfer, Src: []string{ec.Leaving}, Dst: ec.LeftNetwork},
	}
)

type (
	Machine struct {
		machine       *fsm.FSM
		publicServer  TransferClosable
		privateServer Closable
	}

	Closable interface {
		Close() error
	}

	TransferClosable interface {
		OnTransferDone()
	}
)

func NewFSM(client *Client) *Machine {
	var m Machine
	m.machine = fsm.NewFSM(ec.Fresh, transitions, newCallbacks(client, &m))
	return &m
}

func newCallbacks(client *Client, mach *Machine) fsm.Callbacks {
	return fsm.Callbacks{
		ec.JoinNetwork: func(ctx context.Context, e *fsm.Event) {
			switch e.Src {
			case ec.Fresh:
				m := e.Args[0].(*protocol.ECSMessage)

				if err := client.proto.Send(*m); err != nil {
					e.Err = fmt.Errorf("failed to join network of %s: %w", client.proto, err)
				}
			case ec.CatchingUp:
				log.Info("ready to accept requests")
			}
		},
		ec.SetWriteLock: func(ctx context.Context, e *fsm.Event) {
			if err := client.callbacks.OnWriteLockSet(); err != nil {
				e.Err = fmt.Errorf("failed to set write lock: %w", err)
				return
			}

			if err := client.proto.Send(protocol.ECSMessage{Ack: true, Type: protocol.SetWriteLock}); err != nil {
				e.Err = fmt.Errorf("failed to send ack to %s", client.proto)
				return
			}
		},
		ec.InvokeTransfer: func(ctx context.Context, e *fsm.Event) {
			m := e.Args[0].(*protocol.ECSMessage)

			log.Infof("about to begin transfer to server %s", m.Metadata[0].Addr)

			if err := client.callbacks.OnTransferRequested(m.Metadata); err != nil {
				e.Err = fmt.Errorf("failed to begin data transfer: %w", err)
				return
			}

			log.Info("sending NotifyTransferFinished to ECS")

			if err := client.proto.Send(protocol.ECSMessage{Type: protocol.NotifyTransferFinished}); err != nil {
				e.Err = fmt.Errorf("failed to send ack to %s: %w", client.proto, err)
				return
			}

			if e.Src == ec.Leaving {
				mach.publicServer.OnTransferDone()

				if err := mach.privateServer.Close(); err != nil {
					log.Errorf("failed to close private server: %v", err)
				}
			}
		},
		ec.LiftWriteLock: func(ctx context.Context, e *fsm.Event) {
			m := e.Args[0].(*protocol.ECSMessage)

			if err := client.callbacks.OnWriteLockLifted(); err != nil {
				e.Err = fmt.Errorf("failed to lift write lock: %w", err)
				return
			}

			if err := client.proto.Send(m.MakeAck()); err != nil {
				e.Err = fmt.Errorf("failed to send ack to %s", client.proto)
				return
			}
		},
		ec.Leaving: func(ctx context.Context, e *fsm.Event) {
			m := e.Args[0].(*protocol.ECSMessage)

			if err := client.proto.Send(*m); err != nil {
				e.Err = fmt.Errorf("failed to send msg to %s: %w", client.proto, err)
			}
		},
	}
}
