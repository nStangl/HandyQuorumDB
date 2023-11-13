package client

import "github.com/nStangl/distributed-kv-store/protocol"

type (
	Client interface {
		Get(key string) (Response, error)
		Put(key, value string) (Response, error)
		Delete(key string) (Response, error)
	}

	Response struct {
		Kind   ResponseKind
		Values []protocol.ClientMessage
	}

	ResponseKind uint8
)

const (
	Success ResponseKind = iota + 1
	Conflict
)
