package web

import (
	"context"
	"fmt"

	"github.com/nStangl/distributed-kv-store/server/replication"

	"github.com/nStangl/distributed-kv-store/protocol"
	"github.com/nStangl/distributed-kv-store/server/ecs"
	log "github.com/sirupsen/logrus"
)

type (
	ECSCallbacks struct {
		server     *PublicServer
		replicator *replication.Manager
	}

	ECSClient interface {
		Transition(context.Context, *protocol.ECSMessage) error
	}
)

var _ ecs.Callbacks = (*ECSCallbacks)(nil)

func NewECSCallbacks(server *PublicServer, replicator *replication.Manager) *ECSCallbacks {
	return &ECSCallbacks{server: server, replicator: replicator}
}

func (c *ECSCallbacks) OnWriteLockSet() error {
	c.server.setWriteLock()

	log.Info("set write lock")

	return nil
}

func (c *ECSCallbacks) OnWriteLockLifted() error {
	c.server.liftWriteLock()

	log.Info("lifted write lock")

	return nil
}

func (c *ECSCallbacks) OnTransferRequested(meta protocol.Metadata) error {
	if len(meta) != 1 {
		return fmt.Errorf("expected metadata of length 1, got %d", len(meta))
	}

	n, err := protocol.Connect(meta[0].PrivateAddr.String())
	if err != nil {
		return fmt.Errorf("failed to establish connection to store %s: %w", meta[0].PrivateAddr.String(), err)
	}

	return c.tranferData(protocol.ForServer(n), meta)
}

func (c *ECSCallbacks) OnMetadataUpdated(meta protocol.Metadata) error {
	// Set the metadata for the current server
	c.server.setMetadata(meta)
	// Run replica reconciliation, knowing the new metadata and the IP address
	// of current server (the coordinator)
	if err := c.replicator.Reconcile(replication.FromMetadata(meta, c.server.id)); err != nil {
		log.Errorf("failed to reconcile new metadata for replication: %v", err)
	}

	log.Infof("received new metadata: %s", meta)

	return nil
}

func (c *ECSCallbacks) tranferData(
	proto protocol.Protocol[protocol.ServerMessage],
	meta protocol.Metadata,
) error {
	defer func() {
		if proto.Closed() {
			return
		}

		if err := proto.Conn().Close(); err != nil {
			log.Errorf("failed to close connection to %s: %v", proto, err)
		}
	}()

	db, err := c.server.store.Flatten()
	if err != nil {
		return fmt.Errorf("failed to flatten database: %w", err)
	}

	log.Infof("flattened our database: %d", len(db))

	if err := proto.Send(protocol.ServerMessage{Type: protocol.BeginTransfer}); err != nil {
		return fmt.Errorf("failed to send BeginTransfer to server %s: %w", proto, err)
	}

	log.Infof("sent BeginTransfer to %s", proto)

	for k, v := range db {
		if meta[0].Covers(k) {
			if err := proto.Send(protocol.ServerMessage{Type: protocol.TransferTuple, Key: k, Value: v}); err != nil {
				return fmt.Errorf("failed to send TransferTuple to server %s: %w", proto, err)
			}

			log.Infof("sent TransferTuple(%s,%s) to %s", k, v, proto)
		}
	}

	if err := proto.Send(protocol.ServerMessage{Type: protocol.EndTransfer}); err != nil {
		return fmt.Errorf("failed to send EndTransfer to server %s: %w", proto, err)
	}

	log.Infof("sent EndTransfer to %s", proto)

	return nil
}
