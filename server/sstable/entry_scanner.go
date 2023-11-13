package sstable

import (
	"bufio"
	"io"
)

type EntryScanner struct {
	scanner *bufio.Scanner
}

func NewEntryScanner(r io.Reader) *EntryScanner {
	const (
		bufSz = 2 << 11
		max   = 2 << 10
	)

	scan := bufio.NewScanner(r)

	scan.Buffer(make([]byte, bufSz), entryHeaderSz+max)
	scan.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}

		r, err := EntryFromBytes(data)
		if err != nil {
			return 0, nil, nil
		}

		adv := r.Size()

		return adv, data[:adv], nil
	})

	return &EntryScanner{scanner: scan}
}

func (r *EntryScanner) Scan() bool {
	return r.scanner.Scan()
}

func (r *EntryScanner) Entry() Entry {
	data := r.scanner.Bytes()
	entry, _ := EntryFromBytes(data)

	return entry
}
