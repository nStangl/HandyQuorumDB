package ecs

import (
	"context"
	"errors"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
	"go.uber.org/multierr"

	"github.com/nStangl/distributed-kv-store/protocol"
	"github.com/nStangl/distributed-kv-store/tcp"
)

func (e *ECS) ClientServer() (*tcp.Server[protocol.ECSMessage], error) {
	return tcp.NewServer(
		e.cfg.Address,
		e.cfg.ClientPort,
		tcp.WithProtocol(protocol.ForECS),
		tcp.OnHandle(e.OnHandleClient()),
		tcp.OnConnect(e.OnConnectClient()),
		tcp.OnDisconnect(e.OnDisconnectClient()),
	)
}

func (e *ECS) OnHandleClient() tcp.HandleFunc[protocol.ECSMessage] {
	return func(proto protocol.Protocol[protocol.ECSMessage]) bool {
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

		if err := e.handleClientEvent(m, proto); err != nil {
			log.Errorf("failed to handle client event %s: %v", *m, err)
		}

		return true
	}
}

func (e *ECS) OnConnectClient() tcp.ProtoFunc[protocol.ECSMessage] {
	return func(proto protocol.Protocol[protocol.ECSMessage]) {
		log.Infof("new client at %s connected", proto)

		if err := e.addClient(proto); err != nil {
			log.Errorf("failed to add client %s: %v", proto, err)
		}
	}
}

func (e *ECS) OnDisconnectClient() tcp.ProtoFunc[protocol.ECSMessage] {
	return func(proto protocol.Protocol[protocol.ECSMessage]) {
		log.Infof("client at %s disconnected", proto)

		if err := e.removeClient(proto); err != nil {
			log.Errorf("failed to remove client %s: %v", proto, err)
		}
	}
}

func (e *ECS) handleClientEvent(event *protocol.ECSMessage, proto protocol.Protocol[protocol.ECSMessage]) error {
	return e.withLockedClient(proto, func(kv *KVClient) error {
		c := kv.machine.Current()

		if err := kv.machine.Event(context.Background(), event.Type.String(), kv.proto, event); err != nil {
			return fmt.Errorf("failed to transition kvclient %s from state %q via %q: %v", kv.proto, kv.machine.Current(), event, err)
		}

		log.Infof("transitioned state of client %s from %q to %q via %q", proto, c, kv.machine.Current(), event.Type)

		return nil
	})
}

func (e *ECS) addClient(proto protocol.Protocol[protocol.ECSMessage]) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.findClient(proto) != nil {
		return fmt.Errorf("client %s already exists", proto)
	}

	kv := NewKVClient(proto, e)

	e.clients = append(e.clients, kv)

	log.Infof("added new client %s", kv.String())

	return nil
}

func (e *ECS) removeClient(proto fmt.Stringer) error {
	log.Infof("removing client %s", proto)

	idx := -1

	for i := range e.clients {
		if e.clients[i].proto.String() == proto.String() {
			idx = i
			break
		}
	}

	if idx == -1 {
		return fmt.Errorf("we don't have client %s", proto)
	}

	r := &e.clients[idx]

	if err := r.Close(); err != nil {
		log.Errorf("failed to close store: %v", err)
	}

	e.clients[idx] = e.clients[len(e.clients)-1]
	e.clients = e.clients[:len(e.clients)-1]

	return nil
}

func (e *ECS) withClient(proto fmt.Stringer, fn func(*KVClient) error) error {
	for i := range e.clients {
		if e.clients[i].proto.String() == proto.String() {
			if err := fn(&e.clients[i]); err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *ECS) withLockedClient(proto fmt.Stringer, fn func(*KVClient) error) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.withClient(proto, fn)
}

func (e *ECS) findClient(proto fmt.Stringer) *KVClient {
	for i := range e.clients {
		if e.clients[i].proto.String() == proto.String() {
			return &e.clients[i]
		}
	}

	return nil
}

func (e *ECS) broadcastToClients(msg protocol.ECSMessage) error {
	var (
		wg  sync.WaitGroup
		ers = make(chan error, len(e.stores))
	)

	log.Infof("broadcasting %s to %d clients", msg, len(e.clients))

	for i := range e.clients {
		wg.Add(1)

		go func(kv *KVClient) {
			defer wg.Done()

			if err := kv.proto.Send(msg); err != nil {
				ers <- fmt.Errorf("failed to broadcast %v to client %s: %w", msg, kv.proto, err)
			}
		}(&e.clients[i])
	}

	wg.Wait()

	close(ers)

	var result error
	for r := range ers {
		result = multierr.Append(result, r)
	}

	return result
}
