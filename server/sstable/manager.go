package sstable

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/nStangl/distributed-kv-store/server/data"
	"github.com/nStangl/distributed-kv-store/server/memtable"
	"go.uber.org/multierr"
	"golang.org/x/exp/mmap"
)

type Manager struct {
	mu        sync.RWMutex
	root      string
	done      chan struct{}
	work      chan memtable.Table
	tables    []Table
	memtables []memtable.Table
}

var (
	tableNameRe = regexp.MustCompile(`sstable-(\d+)\.db`)
	indexNameRe = regexp.MustCompile(`sstable-(\d+)\.index`)

	res = []*regexp.Regexp{tableNameRe, indexNameRe}
)

// Load all the sstables in root
// and parse them to the tables slice
func NewManager(root string, buffer uint) (*Manager, error) {
	return &Manager{
		root: root,
		done: make(chan struct{}),
		work: make(chan memtable.Table, buffer),
	}, nil
}

func (m *Manager) Startup() error {
	t, err := m.loadTables()
	if err != nil {
		return fmt.Errorf("failed to load tables: %w", err)
	}

	m.tables = t

	return nil
}

func (m *Manager) Add(table memtable.Table) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.work <- table
	m.memtables = append(m.memtables, table)

	return nil
}

func (m *Manager) Lookup(key string) (data.Result, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for i := len(m.memtables) - 1; i >= 0; i-- {
		v := m.memtables[i].Get(key)

		switch v.Kind {
		case data.Present, data.Deleted:
			return v, nil
		}
	}

	for i := len(m.tables) - 1; i >= 0; i-- {
		r, err := m.tables[i].Lookup(key)
		if err != nil {
			return data.Result{}, fmt.Errorf("failed to lookup key in table: %w", err)
		}

		if r.Kind == 0 {
			continue
		}

		switch r.Kind {
		case Set:
			return data.Result{Kind: data.Present, Value: r.Value}, nil
		case Tombstone:
			return data.Result{Kind: data.Deleted}, nil
		}
	}

	return data.Result{Kind: data.Missing}, nil
}

func (m *Manager) Flatten() (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	db := make(map[string]string, len(m.tables)*memtable.MaxSize)

	for i := 0; i < len(m.tables); i++ {
		t := &m.tables[i]

		f, err := os.Open(filepath.Join(m.root, t.tableFile))
		if err != nil {
			return nil, fmt.Errorf("failed to open table file: %w", err)
		}

		scanner := NewEntryScanner(bufio.NewReader(f))

		for scanner.Scan() {
			e := scanner.Entry()

			switch e.Kind {
			case Set:
				db[e.Key] = e.Value
			case Tombstone:
				delete(db, e.Key)
			}
		}

		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("failed to close table file: %w", err)
		}
	}

	for i := 0; i < len(m.memtables); i++ {
		t := m.memtables[i].Iterator()

		for t.Next() {
			v := t.Value()

			switch v.Kind {
			case data.Present:
				db[v.Key] = v.Value
			case data.Deleted:
				delete(db, v.Key)
			}
		}
	}

	return db, nil
}

func (m *Manager) Process() <-chan error {
	ers := make(chan error, 10)

	go func() {
		defer close(ers)
		defer close(m.done)

		for table := range m.work {
			t, err := m.newTable(newMemetableIterator(table))
			if err != nil {
				ers <- fmt.Errorf("failed to create new table: %w", err)
			}

			m.mu.Lock()

			_, m.memtables = m.memtables[len(m.memtables)-1], m.memtables[:len(m.memtables)-1]
			m.tables = append(m.tables, *t)

			m.mu.Unlock()
		}
	}()

	return ers
}

func (m *Manager) Close() error {
	close(m.work)

	<-m.done

	m.mu.Lock()
	defer m.mu.Unlock()

	var result error

	for i := range m.tables {
		if err := m.tables[i].Close(); err != nil {
			result = multierr.Append(result, err)
		}
	}

	return result
}

func (m *Manager) loadTables() ([]Table, error) {
	d, err := os.ReadDir(m.root)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %q: %w", m.root, err)
	}

	mapped := make(map[time.Time][2]string, len(d))

	for _, f := range d {
		if f.IsDir() {
			continue
		}

		for i := range res {
			if n := res[i].FindStringSubmatch(f.Name()); len(n) == 2 {
				t, err := strconv.ParseInt(n[1], 10, 64)
				if err != nil {
					return nil, fmt.Errorf("failed to parse unix time: %w", err)
				}

				d := time.Unix(0, t*int64(time.Millisecond))

				k := mapped[d]
				k[i] = f.Name()
				mapped[d] = k
			}
		}
	}

	tables := make([]Table, 0, len(mapped))
	for k, v := range mapped {
		if v[0] == "" || v[1] == "" {
			return nil, fmt.Errorf("table for date %v is not complete", k)
		}

		t := Table{
			date:      k,
			tableFile: v[0],
			indexFile: v[1],
		}

		if err := m.loadTable(&t); err != nil {
			return nil, fmt.Errorf("failed to load table: %w", err)
		}

		tables = append(tables, t)
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i].date.Before(tables[j].date)
	})

	return tables, nil
}

func (m *Manager) newTable(iterator Iterator) (*Table, error) {
	var (
		tableName, indexName, date = tableFileNames()

		tablePath = filepath.Join(m.root, tableName)
		indexPath = filepath.Join(m.root, indexName)
	)

	table, err := os.Create(tablePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create table file: %w", err)
	}

	index, err := os.Create(indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create table index file: %w", err)
	}

	var (
		tableBuf = bufio.NewWriter(table)
		indexBuf = bufio.NewWriter(index)
	)

	if err := writeTable(iterator, tableBuf, indexBuf); err != nil {
		return nil, fmt.Errorf("failed to write entries to table: %w", err)
	}

	if err := tableBuf.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush table file: %w", err)
	}

	if err := indexBuf.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush index file: %w", err)
	}

	if err := table.Close(); err != nil {
		return nil, fmt.Errorf("failed to close table file: %w", err)
	}

	if err := index.Close(); err != nil {
		return nil, fmt.Errorf("failed to close index file: %w", err)
	}

	t := Table{
		date:      date,
		tableFile: tableName,
		indexFile: indexName,
	}

	if err := m.loadTable(&t); err != nil {
		return nil, fmt.Errorf("failed to load table: %w", err)
	}

	return &t, nil
}

func (m *Manager) loadTable(table *Table) error {
	var (
		tablePath = filepath.Join(m.root, table.tableFile)
		indexPath = filepath.Join(m.root, table.indexFile)
	)

	openIndex, err := os.Open(indexPath)
	if err != nil {
		return fmt.Errorf("failed to open table index file: %w", err)
	}

	idx := NewIndex(bufio.NewReader(openIndex))

	if err := openIndex.Close(); err != nil {
		return fmt.Errorf("failed to close open index file: %w", err)
	}

	openTable, err := mmap.Open(tablePath)
	if err != nil {
		return fmt.Errorf("failed to mmap table file: %w", err)
	}

	table.file = openTable
	table.index = idx

	return nil
}

func tableFileNames() (table, index string, date time.Time) {
	now := time.Now().UTC()

	return fmt.Sprintf("sstable-%d.db", now.UnixMilli()),
		fmt.Sprintf("sstable-%d.index", now.UnixMilli()),
		now
}
