package replication

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nStangl/distributed-kv-store/protocol"
	dbLog "github.com/nStangl/distributed-kv-store/server/log"
	log "github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

type (
	// Manager is responsible for the runtime management of
	// replicas. It maintains connections to replicas and periodically
	// flushes new log entries to the tasks
	Manager struct {
		mu       sync.Mutex
		tasks    []task
		seeker   dbLog.SeekingLog
		replicas []Replica
	}

	task struct {
		checkpoint int
		replica    Replica
		tasks      chan dbLog.SeekResult
		proto      protocol.Protocol[protocol.ServerMessage]
		done       chan struct{}
	}
)

const interval = time.Second

func (t *task) String() string {
	return t.replica.String()
}

func (t *task) Close() {
	close(t.tasks)
}

func NewManager(seeker dbLog.SeekingLog) *Manager {
	return &Manager{seeker: seeker}
}

func (m *Manager) Start(ctx context.Context) <-chan struct{} {
	var (
		done = make(chan struct{})
		tick = time.NewTicker(interval)
	)

	go func(ctx context.Context) {
	outer:
		for {
			select {
			case <-ctx.Done():
				break outer
			case <-tick.C:
				m.schedule()
			}
		}

		var wg sync.WaitGroup

		m.mu.Lock()

		for j := range m.tasks {
			m.tasks[j].Close()

			wg.Add(1)

			go func(r *task) {
				defer wg.Done()
				<-r.done
			}(&m.tasks[j])
		}

		wg.Wait()

		m.mu.Unlock()

		done <- struct{}{}
	}(ctx)

	return done
}

// Reconcile - Every time we receive new metadata we need to
// reconcile them with the old version to see if our
// replica membership has changed, and then act accordingly
func (m *Manager) Reconcile(replicas []Replica) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	reps := make([]Replica, len(replicas))
	copy(reps, replicas)

	err := m.reconcile(m.replicas, replicas)

	m.replicas = reps

	if err != nil {
		return fmt.Errorf("failed to reconcile replicas: %w", err)
	}

	return nil
}

func (m *Manager) reconcile(before, after []Replica) error {
	var (
		result error
		deltas = ComputeDelta(before, after)
	)

	for i := range deltas {
		d := &deltas[i]

		switch d.Kind {
		case Added:
			c, err := protocol.Connect(d.Replica.PrivateAddr.String())
			if err != nil {
				result = multierr.Append(result, fmt.Errorf("failed to connect to %s: %w", d.Replica, err))
				continue
			}

			log.Infof("added replica for %s", c.RemoteAddr().String())

			m.tasks = append(m.tasks, task{
				replica: d.Replica,
				proto:   protocol.ForServer(c),
				tasks:   make(chan dbLog.SeekResult, 10),
				done:    make(chan struct{}),
			})

			go processTask(m.tasks[len(m.tasks)-1])
		case Removed:
			newReplicas := make([]task, 0, len(m.tasks))

			for j := range m.tasks {
				if r := m.tasks[j]; r.String() == d.Replica.String() {
					r.Close()
					log.Infof("removed replica %s", &r)
				} else {
					newReplicas = append(newReplicas, r)
				}
			}

			m.tasks = newReplicas
		}
	}

	return result
}

func (m *Manager) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range m.tasks {
		t := &m.tasks[i]

		res := m.seeker.Seek(dbLog.SeekCmd{Start: t.checkpoint, End: -1})
		if res.End == t.checkpoint {
			continue
		}

		t.checkpoint = res.End
		t.tasks <- res
	}
}

func processTask(r task) {
	defer func() {
		close(r.done)

		if !r.proto.Closed() {
			_ = r.proto.Close()
		}
	}()

	for t := range r.tasks {
		for i := range t.Records {
			c := &t.Records[i]
			if c.Replica {
				continue
			}

			log.Infof("replicating %s to %s", c, r.proto)

			msg := protocol.ServerMessage{Type: protocol.ReplicateTuple, Key: c.Key}
			if c.Kind == dbLog.Set {
				msg.Value = c.Value
			}

			if err := r.proto.Send(msg); err != nil {
				log.Errorf("failed to replicate %s to %s: %v", c, r.proto, err)
			}
		}
	}
}
