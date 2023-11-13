package data

import "fmt"

type (
	Result struct {
		Kind  ResultKind
		Value string
	}

	ResultKind uint8
)

const (
	Present ResultKind = iota + 1
	Deleted
	Missing
)

var (
	resultKindStr = []string{"present", "deleted", "missing"}
)

func (k ResultKind) String() string {
	return resultKindStr[k-1]
}

func (r Result) String() string {
	switch r.Kind {
	case Missing, Deleted:
		return fmt.Sprintf("(%s)", r.Kind)
	case Present:
		return fmt.Sprintf("(%s, %q)", r.Kind, string(r.Value))
	default:
		return ""
	}
}
