package ecs

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nStangl/distributed-kv-store/protocol"
	"github.com/nStangl/distributed-kv-store/tcp"
	log "github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

func (e *ECS) StoreServer() (*tcp.Server[protocol.ECSMessage], error) {
	return tcp.NewServer(
		e.cfg.Address,
		e.cfg.Port,
		tcp.WithProtocol(protocol.ForECS),
		tcp.OnHandle(e.OnHandleStore()),
		tcp.OnConnect(e.OnConnectStore()),
		tcp.OnDisconnect(e.OnDisconnectStore()),
		tcp.OnShutdown[protocol.ECSMessage](e.OnShutdown()),
	)
}

func (e *ECS) OnHandleStore() tcp.HandleFunc[protocol.ECSMessage] {
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

		switch m.Type {
		case protocol.Heartbeat:
			if err := e.handleStoreHeartbeat(proto); err != nil {
				log.Errorf("failed to handle heartbeat: %v", err)
			}
		default:
			log.Infof("received %s from KVStore %s", m.String(), proto)

			meta := e.getMetadata()

			if err := e.handleStoreEvent(m, proto); err != nil {
				log.Errorf("failed to handle event %s: %v", *m, err)
			}

			if !meta.Identical(e.getMetadata()) {
				e.broadcastMetadata(readReplicationFactor, writeReplicationFactor)
			}
		}

		return true
	}
}

func (e *ECS) broadcastMetadata(readReplication, writeReplication int) {
	m := e.getMetadata().MakeReplicatedMetadata(readReplication, writeReplication)

	msg := protocol.ECSMessage{Type: protocol.SendMetadata, Metadata: m}
	if err := e.broadcast(msg); err != nil {
		log.Error(fmt.Errorf("failed to broadcast %s message: %v", msg, err))
	}
}

func (s *ECS) broadcast(msg protocol.ECSMessage) error {
	var result error

	result = multierr.Append(result, s.broadcastToStores(msg))
	result = multierr.Append(result, s.broadcastToClients(msg))

	return result
}

func (e *ECS) OnConnectStore() tcp.ProtoFunc[protocol.ECSMessage] {
	return func(proto protocol.Protocol[protocol.ECSMessage]) {
		log.Infof("new store at %s connected", proto)

		if err := e.addStore(proto); err != nil {
			log.Errorf("failed to add store %s: %v", proto, err)
		}
	}
}

func (e *ECS) OnDisconnectStore() tcp.ProtoFunc[protocol.ECSMessage] {
	return func(proto protocol.Protocol[protocol.ECSMessage]) {
		log.Infof("store at %s disconnected", proto)

		if err := e.removeStore(proto); err != nil {
			log.Errorf("failed to remove store %s: %v", proto, err)
		}
	}
}

func (e *ECS) handleStoreEvent(event *protocol.ECSMessage, proto protocol.Protocol[protocol.ECSMessage]) error {
	return e.withLockedStore(proto, func(kv *KVStore) error {
		c := kv.machine.Current()

		if err := kv.machine.Event(context.Background(), event.Type.String(), kv.proto, event); err != nil {
			return fmt.Errorf("failed to transition kvstore %s from state %q via %q: %v", kv.proto, kv.machine.Current(), event, err)
		}

		log.Infof("transitioned state of %s from %q to %q via %q", proto, c, kv.machine.Current(), event.Type)

		return nil
	})
}

func (e *ECS) handleStoreHeartbeat(proto protocol.Protocol[protocol.ECSMessage]) error {
	return e.withLockedStore(proto, func(kv *KVStore) error {
		kv.Heartbeat()

		return nil
	})
}

func (e *ECS) broadcastToStores(msg protocol.ECSMessage) error {
	var (
		wg  sync.WaitGroup
		ers = make(chan error, len(e.stores))
	)

	log.Infof("broadcasting %s to %d stores", msg, len(e.stores))

	for i := range e.stores {
		wg.Add(1)

		go func(kv *KVStore) {
			defer wg.Done()

			if err := kv.proto.Send(msg); err != nil {
				ers <- fmt.Errorf("failed to broadcast %v to store %s: %w", msg, kv.proto, err)
			}
		}(&e.stores[i])
	}

	wg.Wait()

	close(ers)

	var result error
	for r := range ers {
		result = multierr.Append(result, r)
	}

	return result
}

func (e *ECS) addStore(proto protocol.Protocol[protocol.ECSMessage]) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.findStore(proto) != nil {
		return fmt.Errorf("store %s already exists", proto)
	}

	kv := NewKVStore(proto, e)

	e.stores = append(e.stores, kv)

	log.Infof("added new %s", kv.String())

	return nil
}

func (e *ECS) removeStore(proto fmt.Stringer) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.removeStoreUnprotected(proto)
}

func (e *ECS) removeStoreUnprotected(proto fmt.Stringer) error {
	log.Infof("removing store %s", proto)

	idx := -1

	for i := range e.stores {
		if e.stores[i].proto.String() == proto.String() {
			idx = i
			break
		}
	}

	if idx == -1 {
		return fmt.Errorf("we don't have store %s", proto)
	}

	r := &e.stores[idx]

	e.consistent.Remove(NewServerMember(r).String())

	if err := r.Close(); err != nil {
		log.Errorf("failed to close store: %v", err)
	}

	e.stores[idx] = e.stores[len(e.stores)-1]
	e.stores = e.stores[:len(e.stores)-1]

	return nil
}

func (e *ECS) withStore(proto fmt.Stringer, fn func(*KVStore) error) error {
	for i := range e.stores {
		if e.stores[i].proto.String() == proto.String() {
			if err := fn(&e.stores[i]); err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *ECS) withLockedStore(proto fmt.Stringer, fn func(*KVStore) error) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.withStore(proto, fn)
}

func (e *ECS) findStore(proto fmt.Stringer) *KVStore {
	for i := range e.stores {
		if e.stores[i].proto.String() == proto.String() {
			return &e.stores[i]
		}
	}

	return nil
}
