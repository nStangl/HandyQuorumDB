package ecs

import (
	"context"
	"fmt"

	"github.com/looplab/fsm"
	"github.com/nStangl/distributed-kv-store/protocol"
)

//nolint:unused //nope
var clientTransitions = fsm.Events{
	{Name: JoinNetwork, Src: []string{Fresh}, Dst: Available},
	{Name: LeaveNetwork, Src: ALL_STATES[:], Dst: Leaving},
}

func NewClientFSM(e *ECS) *fsm.FSM {
	return fsm.NewFSM(Fresh, clientTransitions, newClientCallbacks(e))
}

func newClientCallbacks(ec *ECS) fsm.Callbacks {
	return fsm.Callbacks{
		JoinNetwork: func(ctx context.Context, e *fsm.Event) {
			k := e.Args[0].(fmt.Stringer)

			e.Err = ec.withClient(k, func(client *KVClient) error {
				meta := ec.getMetadata().MakeReplicatedMetadata(readReplicationFactor, writeReplicationFactor)
				msg := protocol.ECSMessage{Type: protocol.SendMetadata, Metadata: meta}

				if err := client.proto.Send(msg); err != nil {
					return fmt.Errorf("failed to send %s message to %s: %w", protocol.SendMetadata, client.proto, err)
				}

				return nil
			})
		},
	}
}
