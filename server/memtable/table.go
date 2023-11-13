package memtable

import "github.com/nStangl/distributed-kv-store/server/data"

// This package defined the memtable,
// the in-memory data strucuture storing
// most recent key value pairs

type (
	Table interface {
		Sizable
		Iterable

		Get(string) data.Result
		Set(string, string)
		Del(string)
	}

	Sizable interface {
		Size() int
	}

	Iterable interface {
		Iterator() Iterator
	}

	Iterator interface {
		Next() bool
		Value() Element
	}

	Element struct {
		Kind  data.ResultKind
		Key   string
		Value string
	}
)

const MaxSize = 2 << 15
