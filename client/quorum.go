package client

import (
	"errors"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
	"go.uber.org/multierr"

	"github.com/nStangl/distributed-kv-store/protocol"
)

type QuorumClient struct {
	mu       sync.RWMutex
	quit     chan struct{}
	proto    protocol.Protocol[protocol.ECSMessage]
	metadata protocol.Metadata
}

var _ Client = (*QuorumClient)(nil)

var (
	readReplicas  = func(k *protocol.KeyRange) bool { return k.IsReplica() && k.Replica.Read }
	writeReplicas = func(k *protocol.KeyRange) bool { return k.IsReplica() && k.Replica.Write }
)

func (c *QuorumClient) Connect(addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, err := protocol.Connect(addr)
	if err != nil {
		return fmt.Errorf("failed to connect to ecs: %v", err)
	}

	c.proto = protocol.ForECS(conn)

	return nil
}

func (c *QuorumClient) Listen() <-chan error {
	ers := make(chan error, 10)

	go func() {
		defer close(ers)

		for {
			select {
			case <-c.quit:
				if err := c.proto.Close(); err != nil {
					ers <- fmt.Errorf("failed to close quorum client proto: %w", err)
				}

				return
			default:
				m, err := c.proto.Receive()
				if err != nil {
					if errors.Is(err, protocol.ErrConnClosed) {
						return
					}

					ers <- fmt.Errorf("failed to receive from ECS %s: %w", c.proto, err)

					continue
				}

				if m == nil || m.Type != protocol.SendMetadata {
					continue
				}

				c.mu.Lock()
				c.metadata = m.Metadata
				log.Info("received client metadata", c.metadata)
				c.mu.Unlock()
			}
		}
	}()

	return ers
}

func (c *QuorumClient) Kickstart() error {
	if err := c.proto.Send(protocol.ECSMessage{Type: protocol.JoinNetwork}); err != nil {
		return fmt.Errorf("error while sending message to ECS %s: %w", c.proto, err)
	}

	return nil
}

func (c *QuorumClient) Close() {
	close(c.quit)
}

func (c *QuorumClient) getReplicasFor(key string, filter func(*protocol.KeyRange) bool) protocol.Metadata {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var preferred protocol.Metadata

	for i := range c.metadata {
		if c.metadata[i].Covers(key) && filter(&c.metadata[i]) {
			preferred = append(preferred, c.metadata[i])
		}
	}

	return preferred
}

func (c *QuorumClient) callReplicas(msg protocol.ClientMessage, reps protocol.Metadata) ([]protocol.ClientMessage, error) {
	var (
		wg  sync.WaitGroup
		ers = make(chan error, len(reps))
		rsp = make(chan protocol.ClientMessage, len(reps))
	)

	for i := range reps {
		wg.Add(1)

		go func(kv *protocol.KeyRange) {
			defer wg.Done()

			proto, err := c.connectToReplica(kv)
			if err != nil {
				ers <- fmt.Errorf("failed to connect to replica %s: %w", kv.Addr, err)
				return
			}

			if err := proto.Send(msg); err != nil {
				ers <- fmt.Errorf("error while sending message to replica %s: %w", kv.Addr, err)
				return
			}

			r, err := proto.Receive()
			if err != nil {
				ers <- fmt.Errorf("error while receiving message from replica %s: %w", kv.Addr, err)
				return
			}

			rsp <- *r
		}(&reps[i])
	}

	wg.Wait()

	close(ers)
	close(rsp)

	var result error
	for r := range ers {
		result = multierr.Append(result, r)
	}

	var resps []protocol.ClientMessage
	for r := range rsp {
		resps = append(resps, r)
	}

	return resps, result
}

func (c *QuorumClient) connectToReplica(k *protocol.KeyRange) (protocol.Protocol[protocol.ClientMessage], error) {
	n, err := protocol.Connect(k.Addr.String())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	proto := protocol.ForClient(n)

	_, err = proto.Receive()
	if err != nil {
		return nil, fmt.Errorf("failed to receive initial data from server: %w", err)
	}

	return proto, nil
}

func (c *QuorumClient) Put(key, value string) (Response, error) {
	reps := c.getReplicasFor(key, writeReplicas)

	resps, err := c.callReplicas(protocol.ClientMessage{Type: protocol.Put, Key: key, Value: value}, reps)
	if err != nil {
		return Response{}, fmt.Errorf("failed to call replicas: %w", err)
	}

	var (
		r    = Response{Kind: Success}
		uniq map[protocol.ClientMessage]struct{}
	)

	for i := range resps {
		if _, ok := uniq[resps[i]]; !ok {
			r.Values = append(r.Values, resps[i])
		}
	}

	if len(r.Values) > 1 {
		r.Kind = Conflict
	}

	return r, nil
}

func (c *QuorumClient) Get(key string) (Response, error) {
	reps := c.getReplicasFor(key, readReplicas)

	resps, err := c.callReplicas(protocol.ClientMessage{Type: protocol.Get, Key: key}, reps)
	if err != nil {
		return Response{}, fmt.Errorf("failed to call replicas: %w", err)
	}

	var (
		r    = Response{Kind: Success}
		uniq map[protocol.ClientMessage]struct{}
	)

	for i := range resps {
		if _, ok := uniq[resps[i]]; !ok {
			r.Values = append(r.Values, resps[i])
		}
	}

	if len(r.Values) > 1 {
		r.Kind = Conflict
	}

	return r, nil
}

func (c *QuorumClient) Delete(key string) (Response, error) {
	reps := c.getReplicasFor(key, writeReplicas)

	resps, err := c.callReplicas(protocol.ClientMessage{Type: protocol.Delete, Key: key}, reps)
	if err != nil {
		return Response{}, fmt.Errorf("failed to call replicas: %w", err)
	}

	var (
		r    = Response{Kind: Success}
		uniq map[protocol.ClientMessage]struct{}
	)

	for i := range resps {
		if _, ok := uniq[resps[i]]; !ok {
			r.Values = append(r.Values, resps[i])
		}
	}

	if len(r.Values) > 1 {
		r.Kind = Conflict
	}

	return r, nil
}
