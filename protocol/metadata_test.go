package protocol

import (
	"net"
	"testing"

	"github.com/google/uuid"

	"github.com/nStangl/distributed-kv-store/util"
)

func TestCoverSingle(t *testing.T) {
	key := "key"
	uhash := util.MD5HashUint128(key)

	kr := KeyRange{
		ID:      uuid.New(),
		Replica: Replica{Read: false, Write: true},
		Addr:    &net.TCPAddr{IP: net.ParseIP("localhost"), Port: 8080},
		Start:   uhash,
		End:     uhash,
	}

	if !kr.Covers(key) {
		t.Errorf("KeyRange %s should cover %s", kr.String(), key)
	}

	if !kr.Covers("otherkey") {
		t.Errorf("KeyRange %s should cover %s", kr.String(), "otherkey")
	}
}
