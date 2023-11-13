package ecs

import (
	"context"
	"fmt"
	"net"

	"github.com/looplab/fsm"
	"github.com/nStangl/distributed-kv-store/protocol"
	log "github.com/sirupsen/logrus"
)

var (
	// ECS messages act as transitions in our state machine
	JoinNetwork            = protocol.JoinNetwork.String()
	LeaveNetwork           = protocol.LeaveNetwork.String()
	SetWriteLock           = protocol.SetWriteLock.String()
	LiftWriteLock          = protocol.LiftWriteLock.String()
	InvokeTransfer         = protocol.InvokeTransfer.String()
	NotifyTransferFinished = protocol.NotifyTransferFinished.String()

	// The transitions
	transitions = fsm.Events{
		{Name: JoinNetwork, Src: []string{Fresh}, Dst: CatchingUp},
		{Name: JoinNetwork, Src: []string{CatchingUp}, Dst: Available},
		{Name: SetWriteLock, Src: []string{Available}, Dst: WriteLockRequested},
		{Name: SetWriteLock, Src: []string{WriteLockRequested}, Dst: WriteLocked},
		{Name: InvokeTransfer, Src: []string{WriteLocked}, Dst: TransferingData},
		{Name: NotifyTransferFinished, Src: []string{TransferingData}, Dst: WriteLocked},
		{Name: NotifyTransferFinished, Src: []string{Leaving}, Dst: LeftNetwork},
		{Name: LiftWriteLock, Src: []string{WriteLocked}, Dst: WriteLockLiftRequested},
		{Name: LiftWriteLock, Src: []string{WriteLockLiftRequested}, Dst: Available},
		{Name: LeaveNetwork, Src: ALL_STATES[:], Dst: Leaving},
	}
)

func NewFSM(s *ECS) *fsm.FSM {
	return fsm.NewFSM(Fresh, transitions, newCallbacks(s))
}

// All callbacks are executed from within `handleEvent`,
// meaning we have an exclusive lock for the `stores`
// As go mutexes are not reentrant, trying to obtain the same lock
// in any of the below callbacks would result in a deadlock
func newCallbacks(s *ECS) fsm.Callbacks {
	return fsm.Callbacks{
		JoinNetwork: func(ctx context.Context, e *fsm.Event) {
			k := e.Args[0].(fmt.Stringer)

			switch e.Src {
			case Fresh:
				e.Err = s.withStore(k, func(kv *KVStore) error {
					// Parse the private address of this newly joined server
					if m, ok := e.Args[1].(*protocol.ECSMessage); ok && m != nil {
						publicAddr, err := net.ResolveTCPAddr("tcp", m.Address)
						if err != nil {
							return fmt.Errorf("failed to resolve public address of store %s: %w", kv.proto, err)
						}

						privateAddr, err := net.ResolveTCPAddr("tcp", m.PrivateAddress)
						if err != nil {
							return fmt.Errorf("failed to resolve private address of store %s: %w", kv.proto, err)
						}

						kv.id = m.ID
						kv.addr = publicAddr
						kv.privateAddr = privateAddr
					}

					// Add the new server to our ring
					newMember := NewServerMember(kv)
					s.consistent.Add(newMember)

					nextMember, err := s.consistent.GetSuccessor(newMember.String())
					if err != nil {
						return fmt.Errorf("failed to get successor nodeof %s: %v", kv.proto, err)
					}

					succ := s.findStore(nextMember)
					log.Infof("found successor of %s (%p): %s (%p)", k, kv, succ.proto, succ)

					// The first server was added
					if succ == kv {
						// Transition the first server to Available
						if err := kv.machine.Event(ctx, JoinNetwork, kv.proto); err != nil {
							return fmt.Errorf("failed to transition first server %s via %s: %w", kv.proto, JoinNetwork, err)
						}
					} else {
						// Add predecessor and successor links
						previousMember, err := s.consistent.GetPredecessor(newMember.String())
						if err != nil {
							return fmt.Errorf("failed to get predecessor node of %s: %w", kv.proto, err)
						}

						pre := s.findStore(previousMember)

						kv.successor = succ.proto
						kv.predecessor = pre.proto
						succ.predecessor = kv.proto
						pre.successor = kv.proto

						// Transition the successor server to WriteLockRequested
						if err := succ.machine.Event(ctx, SetWriteLock, succ.proto); err != nil {
							return fmt.Errorf("failed to transition successor %s via %s: %w", succ.proto, SetWriteLock, err)
						}
					}

					return nil
				})
			case CatchingUp:
				e.Err = s.withStore(k, func(kv *KVStore) error {
					if err := kv.proto.Send(protocol.ECSMessage{Type: protocol.JoinNetwork}); err != nil {
						return fmt.Errorf("failed to send %s message to %s: %w", JoinNetwork, kv.proto, err)
					}

					log.Infof("new server officially joined %s", kv.proto)

					return nil
				})
			}
		},
		SetWriteLock: func(ctx context.Context, e *fsm.Event) {
			k := e.Args[0].(fmt.Stringer)

			switch e.Src {
			case Available:
				e.Err = s.withStore(k, func(kv *KVStore) error {
					// Set the write lock on the old server
					if err := kv.proto.Send(protocol.ECSMessage{Type: protocol.SetWriteLock}); err != nil {
						return fmt.Errorf("failed to send %s message to %s: %w", SetWriteLock, kv.proto, err)
					}

					return nil
				})
			case WriteLockRequested:
				e.Err = s.withStore(k, func(kv *KVStore) error {
					pred := s.findStore(kv.predecessor)
					if pred == nil {
						return fmt.Errorf("failed to find predecessor store %s of %s", kv.predecessor, kv.proto)
					}

					predKeyrange, ok := s.consistent.FindRange(pred.proto.Conn().RemoteAddr().String())
					if !ok {
						return fmt.Errorf("failed to find predecessor %s of %s in consistent", kv.predecessor, kv.proto)
					}

					var (
						metadata = protocol.Metadata{{
							ID:          pred.id,
							Addr:        pred.proto.Conn().RemoteAddr().(*net.TCPAddr),
							PrivateAddr: pred.privateAddr,
							Start:       predKeyrange.Start,
							End:         predKeyrange.End,
						}}
						transferTo = protocol.ECSMessage{
							Type:     protocol.InvokeTransfer,
							Address:  pred.privateAddr.String(),
							Metadata: metadata,
						}
					)

					log.Infof("about to send transfer to %s with address %s and meta %s", kv.proto, pred.privateAddr, metadata)

					// Ask the old server to perform the transfer
					if err := kv.proto.Send(transferTo); err != nil {
						return fmt.Errorf("failed to send message to %s: %w", kv.proto, err)
					}

					// Transition the old server to InvokeTransfer
					if err := kv.machine.Event(ctx, InvokeTransfer, kv.proto); err != nil {
						return fmt.Errorf("failed to transition old server %s via %s: %w", kv.proto, InvokeTransfer, err)
					}

					// Transition the new server to InvokeTransfer
					// if err := pred.machine.Event(ctx, InvokeTransfer, kv.proto); err != nil {
					// 	return fmt.Errorf("failed to transition new server %s via %s: %w", pred.proto, InvokeTransfer, err)
					// }

					return nil
				})
			}

		},
		InvokeTransfer: func(ctx context.Context, e *fsm.Event) {
			k := e.Args[0].(fmt.Stringer)

			switch e.Src {
			case WriteLocked:
				// Actually nothing to do?
				log.Infof("received %q in state %q from %s, skipping...", e.Event, e.Src, k)
			case CatchingUp:
				// Actually nothing to do?
				log.Infof("received %q in state %q from %s, skipping...", e.Event, e.Src, k)
			}
		},
		NotifyTransferFinished: func(ctx context.Context, e *fsm.Event) {
			k := e.Args[0].(fmt.Stringer)

			switch e.Src {
			case TransferingData:
				e.Err = s.withStore(k, func(kv *KVStore) error {
					// Transition the successorStore server to WriteLockRequested
					if err := kv.machine.Event(ctx, LiftWriteLock, kv.proto); err != nil {
						return fmt.Errorf("failed to transition successorStore %s via %s: %w", kv.proto, LiftWriteLock, err)
					}

					// pred := s.findStore(kv.predecessor)
					// if pred == nil {
					// 	return fmt.Errorf("failed to find predecessor %s of %s", kv.predecessor, kv.proto)
					// }
					//
					// // Transition the pred server via NotifyTransferFinished
					// if err := pred.machine.Event(ctx, NotifyTransferFinished, kv.proto); err != nil {
					// 	return fmt.Errorf("failed to transition pred server %s via %s: %w", pred.proto, NotifyTransferFinished, err)
					// }

					return nil
				})
			case ReceivingData:
				log.Infof("received %q in state %q from %s, skipping...", e.Event, e.Src, k)
				// Actually nothing to do?
				//case Leaving:
				//	e.Err = s.withStore(k, func(kv *KVStore) error {
				//		nodeToRemove := NewServerMember(kv)
				//
				//		// Update neighbouring successor and predecessor links
				//		nextMember, err := s.consistent.GetSuccessor(nodeToRemove.String())
				//		if err != nil {
				//			return fmt.Errorf("failed to find successor of %s: %v", nodeToRemove, err)
				//		}
				//
				//		prevMember, err := s.consistent.GetPredecessor(nodeToRemove.String())
				//		if err != nil {
				//			return fmt.Errorf("failed to find predecessor of %s: %v", nodeToRemove, err)
				//		}
				//
				//		succ := s.findStore(nextMember)
				//		pred := s.findStore(prevMember)
				//
				//		succ.predecessor = pred.proto
				//		pred.successor = succ.proto
				//
				//		kv.predecessor = nil
				//		kv.successor = nil
				//
				//		e.Err = s.removeStoreUnprotected(k)
				//
				//		return nil
				//	})
			}
		},
		LiftWriteLock: func(ctx context.Context, e *fsm.Event) {
			k := e.Args[0].(fmt.Stringer)

			switch e.Src {
			case WriteLocked:
				e.Err = s.withStore(k, func(kv *KVStore) error {
					// Transition the successorStore server to WriteLockRequested
					if err := kv.proto.Send(protocol.ECSMessage{Type: protocol.LiftWriteLock}); err != nil {
						return fmt.Errorf("failed to send message to %s", kv.proto)
					}

					return nil
				})
			case WriteLockLiftRequested:
				// Broadcast the new metadata to all servers
				// now that we have successfully transferred data
				// from the successorStore to new server, and lifted the write lock
				// msg := protocol.ECSMessage{Type: protocol.SendMetadata, Metadata: s.getMetadata()}
				// if err := s.broadcast(msg); err != nil {
				// 	e.Err = fmt.Errorf("failed to broadcast %s after lifting write lock: %w", msg, err)
				// }

				e.Err = s.withStore(k, func(kv *KVStore) error {
					pred := s.findStore(kv.predecessor)
					if pred == nil {
						return fmt.Errorf("failed to find predecessor %s of %s", kv.predecessor, kv.proto)
					}

					//if err := pred.proto.Send(protocol.ECSMessage{Type: protocol.NotifyTransferFinished}); err != nil {
					//	return fmt.Errorf("failed to send message to %s: %w", kv.proto, err)
					//}

					//if err := kv.machine.Event(ctx, LiftWriteLock, kv.proto); err != nil {
					//	return fmt.Errorf("failed to transition successorStore %s via %s: %w", kv.proto, LiftWriteLock, err)
					//}

					if err := pred.machine.Event(ctx, JoinNetwork, pred.proto); err != nil {
						return fmt.Errorf("failed to transition pred %s via %s: %w", kv.proto, JoinNetwork, err)
					}

					return nil
				})
			}
		},
		Leaving: func(ctx context.Context, e *fsm.Event) {
			k := e.Args[0].(fmt.Stringer)

			switch e.Src {
			case Available:
				e.Err = s.withStore(k, func(kv *KVStore) error {
					name := NewServerMember(kv)

					nextMember, err := s.consistent.GetSuccessor(name.String())
					if err != nil {
						return fmt.Errorf("failed to get successor of %s: %v", name.String(), err)
					}

					currKeyrange, ok := s.consistent.FindRange(name.String())
					if !ok {
						return fmt.Errorf("failed to get successor of %s in consistent: %v", name.String(), err)
					}

					var (
						succ     = s.findStore(nextMember)
						metadata = protocol.Metadata{{
							ID:          succ.id,
							Addr:        succ.proto.Conn().RemoteAddr().(*net.TCPAddr),
							PrivateAddr: succ.privateAddr,
							Start:       currKeyrange.Start,
							End:         currKeyrange.End,
						}}
						transferTo = protocol.ECSMessage{
							Type:     protocol.InvokeTransfer,
							Address:  succ.privateAddr.String(),
							Metadata: metadata,
						}
					)

					log.Infof("about to send transfer to %s with address %s and meta %s", kv.proto, succ.privateAddr, metadata)

					if err := kv.proto.Send(transferTo); err != nil {
						return fmt.Errorf("failed to send message to %s: %v", kv.proto, err)
					}

					return nil
				})
			case Leaving:
				log.Infof("received %q in state %q from %s, skipping...", e.Event, e.Src, k)
			}
		},
	}
}
