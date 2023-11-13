package sstable

import (
	"bufio"
	"io"
)

type IndexScanner struct {
	scanner *bufio.Scanner
}

func NewScanner(r io.Reader) *IndexScanner {
	const (
		bufSz = 2 << 11
		max   = 2 << 10
	)

	scan := bufio.NewScanner(r)

	scan.Buffer(make([]byte, bufSz), indexHeaderSz+max)
	scan.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}

		r, err := IndexEntryFromBytes(data)
		if err != nil {
			return 0, nil, nil
		}

		adv := r.Size()

		return adv, data[:adv], nil
	})

	return &IndexScanner{scanner: scan}
}

func (r *IndexScanner) Scan() bool {
	return r.scanner.Scan()
}

func (r *IndexScanner) Entry() IndexEntry {
	data := r.scanner.Bytes()
	entry, _ := IndexEntryFromBytes(data)

	return entry
}
