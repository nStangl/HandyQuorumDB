package ecs

import (
	"fmt"
	"sync"
	"time"

	"github.com/nStangl/distributed-kv-store/protocol"
	"github.com/nStangl/distributed-kv-store/tcp"
	log "github.com/sirupsen/logrus"
)

type ECS struct {
	// Mutex to protect stores
	mu sync.Mutex
	// The config
	cfg *Config
	// Servers currently in our network
	stores []KVStore
	// Clients currently in our network
	clients []KVClient
	// Consistent Hashing ring for managing partitioning
	consistent *Consistent
	// Graceful shutdown
	done chan<- struct{}
}

const (
	debugInterval   = 2 * time.Second
	janitorInterval = time.Second
)

func New(cfg *Config) *ECS {
	ecs := ECS{
		cfg:        cfg,
		stores:     make([]KVStore, 0),
		clients:    make([]KVClient, 0),
		consistent: NewConsistent(nil, HashConfig{}),
	}

	ecs.done = ecs.startJanitor()

	if cfg.Debug {
		ecs.debugState()
	}

	return &ecs
}

func (e *ECS) OnShutdown() tcp.CloseFunc {
	return func() error {
		if e.done != nil {
			e.done <- struct{}{}
		}

		return nil
	}
}

func (e *ECS) getMetadata() protocol.Metadata {
	return e.consistent.GetRanges()
}

func (e *ECS) startJanitor() chan<- struct{} {
	var (
		done = make(chan struct{})
		tick = time.NewTicker(janitorInterval)
	)

	go func() {
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				e.removeDeadStores()
			}
		}
	}()

	return done
}

func (e *ECS) removeDeadStores() {
	e.mu.Lock()
	defer e.mu.Unlock()

	toRemove := make([]fmt.Stringer, 0, len(e.stores))

	for i := range e.stores {
		a := &e.stores[i]
		a.MissHeartbeat()

		if !a.Healthy() {
			toRemove = append(toRemove, a.proto)
		}
	}

	for _, n := range toRemove {
		log.Infof("removing timed-out store at %s", n)

		if err := e.removeStoreUnprotected(n); err != nil {
			log.Errorf("failed to remove timed-out store at %s: %v", n, err)
		}
	}

	if len(toRemove) > 0 {
		e.broadcastMetadata(readReplicationFactor, writeReplicationFactor)
	}
}

func (e *ECS) debugState() {
	tick := time.NewTicker(debugInterval)

	go func() {
		for range tick.C {
			e.mu.Lock()

			for i := range e.stores {
				log.Infof("store %d: %s", i, e.stores[i].String())
			}

			e.mu.Unlock()
		}
	}()
}
