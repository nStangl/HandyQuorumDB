package replication

type (
	Delta struct {
		Kind    DeltaKind
		Replica Replica
	}

	DeltaKind uint8
)

const (
	Removed DeltaKind = iota + 1
	Added
)

func ComputeDelta(before, after []Replica) []Delta {
	var (
		l = make(map[string]struct{})
		r = make(map[string]struct{})

		deltas []Delta
	)

	for i := range before {
		l[before[i].String()] = struct{}{}
	}

	for i := range after {
		r[after[i].String()] = struct{}{}
	}

	for i := range before {
		if _, ok := r[before[i].String()]; !ok {
			deltas = append(deltas, Delta{Kind: Removed, Replica: before[i]})
		}
	}

	for i := range after {
		if _, ok := l[after[i].String()]; !ok {
			deltas = append(deltas, Delta{Kind: Added, Replica: after[i]})
		}
	}

	return deltas
}
