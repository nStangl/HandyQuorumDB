package log

import (
	"bufio"
	"io"
)

type Scanner struct {
	scanner *bufio.Scanner
}

func NewScanner(r io.Reader) *Scanner {
	const (
		bufSz = 2 << 11
		max   = 2 << 10
	)

	scan := bufio.NewScanner(r)

	scan.Buffer(make([]byte, bufSz), headerSz+max)
	scan.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}

		r, err := FromBytes(data)
		if err != nil {
			return 0, nil, nil
		}

		adv := r.Size()

		return adv, data[:adv], nil
	})

	return &Scanner{scanner: scan}
}

func (r *Scanner) Scan() bool {
	return r.scanner.Scan()
}

func (r *Scanner) Record() Record {
	data := r.scanner.Bytes()
	record, _ := FromBytes(data)

	return record
}
