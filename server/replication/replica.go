package replication

import (
	"fmt"
	"net"

	"github.com/google/uuid"
	"github.com/nStangl/distributed-kv-store/protocol"
	log "github.com/sirupsen/logrus"
	"lukechampine.com/uint128"
)

type Replica struct {
	Start, End  uint128.Uint128
	PrivateAddr *net.TCPAddr
}

func (r *Replica) String() string {
	return fmt.Sprintf("replica(%s,%s,%s)", r.PrivateAddr, r.Start, r.End)
}

// FromMetadata - get tasks for a given coordinator
func FromMetadata(m protocol.Metadata, coordinator uuid.UUID) []Replica {
	c := m.GetCoordinator(coordinator)
	if c == nil {
		return nil
	}

	var replicas []Replica

	for i := range m {
		log.Infof("looking at %v", m[i])
		if m[i].Start == c.Start && m[i].End == c.End && m[i].Replica.Read {
			replicas = append(replicas, Replica{
				Start:       m[i].Start,
				End:         m[i].End,
				PrivateAddr: m[i].PrivateAddr,
			})
		}
	}

	return replicas
}
