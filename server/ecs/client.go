package ecs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nStangl/distributed-kv-store/protocol"
	log "github.com/sirupsen/logrus"
)

type (
	Client struct {
		mu        sync.Mutex
		quit      chan struct{}
		proto     protocol.Protocol[protocol.ECSMessage]
		machine   *Machine
		callbacks Callbacks
	}

	Callbacks interface {
		OnWriteLockSet() error
		OnWriteLockLifted() error
		OnTransferRequested(protocol.Metadata) error
		OnMetadataUpdated(protocol.Metadata) error
	}

	CallbackFunc func(error)
)

const heartbeatInterval = time.Second

var heartbeat = protocol.ECSMessage{Type: protocol.Heartbeat}

func NewClient(addr string) (*Client, error) {
	c, err := protocol.Connect(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create ECS client: %w", err)
	}

	client := Client{proto: protocol.ForECS(c), quit: make(chan struct{})}
	client.machine = NewFSM(&client)

	return &client, nil
}

func (e *Client) SetCallbacks(callbacks Callbacks) {
	e.callbacks = callbacks
}

func (e *Client) SetPublicServer(server TransferClosable) {
	e.machine.publicServer = server
}

func (e *Client) SetPrivateServer(server Closable) {
	e.machine.privateServer = server
}

func (e *Client) Transition(ctx context.Context, msg *protocol.ECSMessage) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	c := e.machine.machine.Current()

	if msg.Type == protocol.SendMetadata {
		if err := e.callbacks.OnMetadataUpdated(msg.Metadata); err != nil {
			return fmt.Errorf("failed to receive metadata from ECS %s: %w", e.proto, err)
		}
	} else {
		if err := e.machine.machine.Event(ctx, msg.Type.String(), msg); err != nil {
			return fmt.Errorf("failed to transition ECS state from %s via %s: %w", e.machine.machine.Current(), msg.Type, err)
		}

		log.Infof("transitioned kvstore state from %q to %q via %q", c, e.machine.machine.Current(), msg.Type.String())
	}

	return nil
}

func (e *Client) Close() {
	close(e.quit)
}

func (e *Client) Listen() <-chan error {
	ers := make(chan error, 10)

	go func() {
		defer close(ers)

		for {
			select {
			case <-e.quit:
				if err := e.proto.Close(); err != nil {
					ers <- fmt.Errorf("failed to close server proto: %w", err)
				}

				return
			default:
				m, err := e.proto.Receive()
				if err != nil {
					if errors.Is(err, protocol.ErrConnClosed) {
						return
					}

					ers <- fmt.Errorf("failed to receive from ECS %s: %w", e.proto, err)

					continue
				}

				if m == nil {
					continue
				}

				log.Infof("received %s from ECS %s in state %s", m.String(), e.proto, e.machine.machine.Current())

				if err := e.Transition(context.Background(), m); err != nil {
					ers <- err
				}
			}
		}
	}()

	return ers
}

func (e *Client) SendHeartbeats() <-chan error {
	var (
		ers  = make(chan error, 10)
		tick = time.NewTicker(heartbeatInterval)
	)

	go func() {
		for {
			select {
			case <-e.quit:
				return
			case <-tick.C:
				if e.proto.Closed() {
					return
				}

				if err := e.proto.Send(heartbeat); err != nil {
					ers <- fmt.Errorf("failed to send heartbeat to ECS %s: %w", e.proto, err)
				}
			}
		}
	}()

	return ers
}
