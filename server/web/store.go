package web

import (
	"fmt"
	"path/filepath"

	dbLog "github.com/nStangl/distributed-kv-store/server/log"
	"github.com/nStangl/distributed-kv-store/server/memtable"
	"github.com/nStangl/distributed-kv-store/server/sstable"
	"github.com/nStangl/distributed-kv-store/server/store"
	log "github.com/sirupsen/logrus"
)

func NewStore(cfg *Config) (store.Store, error) {
	dbLog, err := dbLog.NewSeeking(filepath.Join(cfg.Directory, cfg.Logfile))
	if err != nil {
		return nil, fmt.Errorf("failed to initiate db commit log: %w", err)
	}

	ssTableManager, err := sstable.NewManager(cfg.Directory, 4)
	if err != nil {
		return nil, fmt.Errorf("failed to initiate sstable manager: %w", err)
	}

	if err := ssTableManager.Startup(); err != nil {
		return nil, fmt.Errorf("failed to start sstable manager up: %w", err)
	}

	ers := ssTableManager.Process()

	go func() {
		for err := range ers {
			log.Printf("error from manager: %v", err)
		}
	}()

	store := store.New(dbLog, ssTableManager, func() memtable.Table {
		return memtable.NewRedBlackTree()
	})

	return store, nil
}
