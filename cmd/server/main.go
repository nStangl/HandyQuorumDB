package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	dbLog "github.com/nStangl/distributed-kv-store/server/log"
	"github.com/nStangl/distributed-kv-store/server/memtable"
	"github.com/nStangl/distributed-kv-store/server/replication"
	"github.com/nStangl/distributed-kv-store/server/sstable"
	"github.com/nStangl/distributed-kv-store/server/store"

	log "github.com/sirupsen/logrus"

	"github.com/nStangl/distributed-kv-store/protocol"
	"github.com/nStangl/distributed-kv-store/server/ecs"
	"github.com/nStangl/distributed-kv-store/server/web"
	"github.com/nStangl/distributed-kv-store/util"
	"github.com/spf13/cobra"
)

const version = "0.0.1"

var (
	cfg     web.Config
	rootCmd = &cobra.Command{
		Use:     "server",
		Short:   "server",
		Long:    "server",
		Version: version,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			cmd.SilenceUsage = true

			if unparsed := util.ExtractUnknownArgs(cmd.Flags(), args); len(unparsed) == 1 {
				cfg.Loglevel = unparsed[0]
			}

			setLogLevel(cfg.Loglevel)

			// Create new ECS client with callbacks
			// to manage the public server
			ecs, err := ecs.NewClient(cfg.Bootstrap)
			if err != nil {
				return fmt.Errorf("failed to create new ECS client: %w", err)
			}

			log.Info("created ECS client")

			// Create a new log with seeking capabilities
			lg, err := dbLog.NewSeeking(filepath.Join(cfg.Directory, cfg.Logfile))
			if err != nil {
				return fmt.Errorf("failed to initiate db commit log: %w", err)
			}

			// Create the sstable manager
			ssTableManager, err := sstable.NewManager(cfg.Directory, 4)
			if err != nil {
				return fmt.Errorf("failed to initiate sstable manager: %w", err)
			}

			// Start up the sstable manager
			if err := ssTableManager.Startup(); err != nil {
				return fmt.Errorf("failed to start sstable manager up: %w", err)
			}

			// Begin sstable compaction process
			go func(ers <-chan error) {
				for err := range ers {
					log.Printf("error from manager: %v", err)
				}
			}(ssTableManager.Process())

			// Create new store
			store := store.New(lg, ssTableManager, func() memtable.Table {
				return memtable.NewRedBlackTree()
			})

			var (
				r     = replication.NewManager(lg)
				rdone = r.Start(ctx)
			)

			// Create new public server (for client requests)
			s, err := web.NewPublicServer(&cfg, store, ecs)
			if err != nil {
				return fmt.Errorf("failed to create new public server: %w", err)
			}

			ecs.SetCallbacks(web.NewECSCallbacks(s, r))
			ecs.SetPublicServer(s)

			log.Infof("created public server on %s:%d", cfg.Address, cfg.Port)

			// Use some hardcoded port for server <-> server communication
			// and hope it does not coincide with the assigned public port
			pcfg := cfg
			pcfg.Port = randomPort()

			privateAddr := fmt.Sprintf("%s:%d", pcfg.Address, pcfg.Port)

			// Create new private server (for server requests)
			ps, err := web.NewPrivateServer(&pcfg, store, ecs)
			if err != nil {
				return fmt.Errorf("failed to create new private server: %w", err)
			}

			log.Info("created private server on", privateAddr)

			// Create the actual TCP public server
			ts, err := s.Server()
			if err != nil {
				return fmt.Errorf("failed to create new public TCP server: %w", err)
			}

			// Actually start serving public requests
			done, ers := ts.Serve()

			log.Info("serving public requests")

			// Log errors from public server
			go func() {
				for err := range ers {
					log.Printf("error from public server: %v", err)
				}
			}()

			// Create the actual TCP private server
			pts, err := ps.Server()
			if err != nil {
				return fmt.Errorf("failed to create new private TCP server: %w", err)
			}

			ecs.SetPrivateServer(pts)

			// Actually start serving private requests
			_, pers := pts.Serve()

			log.Info("serving private requests")

			// Log errors from private server
			go func() {
				for err := range pers {
					log.Printf("error from private server: %v", err)
				}
			}()

			// Start the ECS listener
			go func(ers <-chan error) {
				for err := range ers {
					log.Printf("error from ECS client: %v", err)
				}
			}(ecs.Listen())

			log.Info("starting ECS polling")

			// Start the ECS heartbeat ticker
			go func(ers <-chan error) {
				for err := range ers {
					log.Printf("error from ECS heartbeats: %v", err)
				}
			}(ecs.SendHeartbeats())

			log.Info("starting ECS heartbeats")

			// Kickstart the process by sending JoinNetwork to ECS
			if err := ecs.Transition(ctx, &protocol.ECSMessage{
				Type:           protocol.JoinNetwork,
				ID:             s.ID(),
				Address:        fmt.Sprintf("%s:%d", cfg.Address, cfg.Port),
				PrivateAddress: privateAddr,
			}); err != nil {
				return fmt.Errorf("failed to send initial JoinNetwork msg to ECS: %w", err)
			}

			log.Info("kickstarting ECS communication")

			// Catch the interrupts (ctrl+c)
			quit := make(chan os.Signal, 1)

			signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

			// Graceful shutdown in its own goroutine
			go func() {
				<-quit
				defer cancel()

				log.Info("server about to close")

				if err := ts.Close(); err != nil {
					log.Printf("error closing public server: %v", err)
				}

				ecs.Close()
			}()

			// Wait for server(s) to gracefully shutdown
			<-done
			log.Info("<-done")
			<-rdone
			log.Info("<-rdone")

			return nil
		},
	}
)

func init() {
	log.SetLevel(log.InfoLevel)
	log.SetOutput(os.Stdout)

	rootCmd.PersistentFlags().IntVarP(&cfg.Port, "port", "p", 8080, "Sets the port of the server")
	rootCmd.PersistentFlags().IntVarP(&cfg.PrivatePort, "private port", "q", 4242, "Sets the private port for the server to server communication")
	rootCmd.PersistentFlags().StringVarP(&cfg.Address, "address", "a", "127.0.0.1", "Which address the server should listen to, default is 127.0.0.1")
	rootCmd.PersistentFlags().StringVarP(&cfg.Bootstrap, "bootstrap", "b", "127.0.0.1:9090", "Bootstrap server (Used by the server for ECS address in Milestone 3)")
	rootCmd.PersistentFlags().StringVarP(&cfg.Directory, "directory", "d", "db-data", "Directory for files (Put here the files you need to persist the data, the directory is created upfront and you can rely on that it exists)")
	rootCmd.PersistentFlags().StringVarP(&cfg.Logfile, "logfile", "l", "log.db", "Relative path of the logfile, e.g., “echo.log”")
	rootCmd.PersistentFlags().StringVarP(&cfg.Loglevel, "loglevel", "o", "ALL", "Loglevel, e.g., INFO, ALL, . . .")
	rootCmd.PersistentFlags().StringVar(&cfg.Loglevel, "ll", "ALL", "Loglevel, e.g., INFO, ALL, . . .")
	rootCmd.PersistentFlags().IntVarP(&cfg.CacheSize, "cache_size", "c", 1024, "Size of the cache, e.g., 100 keys")
	rootCmd.PersistentFlags().StringVarP(&cfg.DisStrategy, "dis_strategy", "s", "FIFO", "Cache displacement strategy, FIFO, LRU, LFU,")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Whoops. There was an error while executing your CLI '%s'", err)
		os.Exit(1)
	}
}

func setLogLevel(level string) {
	switch strings.ToLower(level) {
	case "all":
		log.SetLevel(log.DebugLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	default:
		log.SetLevel(log.InfoLevel)
		fmt.Printf("Invalid log level '%s'. Setting log level to 'info'\n", level)
	}

	log.SetOutput(os.Stderr)
}

func randomPort() int {
	const (
		min = 2000
		max = 2200
	)

	return rand.Intn(max-min) + min
}
