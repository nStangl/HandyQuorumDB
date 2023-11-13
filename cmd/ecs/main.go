package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"

	"github.com/nStangl/distributed-kv-store/ecs"
	"github.com/nStangl/distributed-kv-store/util"
	"github.com/spf13/cobra"
)

const version = "0.0.1"

var (
	cfg     ecs.Config
	rootCmd = &cobra.Command{
		Use:     "ecs",
		Short:   "ecs",
		Long:    "ecs",
		Version: version,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			if unparsed := util.ExtractUnknownArgs(cmd.Flags(), args); len(unparsed) == 1 {
				cfg.Loglevel = unparsed[0]
			}

			setLogLevel(cfg.Loglevel)

			cfg.Debug = true

			log.Info("creating the ECS")

			e := ecs.New(&cfg)

			estore, err := e.StoreServer()
			if err != nil {
				return fmt.Errorf("failed to create new ECS store server: %w", err)
			}

			log.Info("created the ECS store server")

			eclient, err := e.ClientServer()
			if err != nil {
				return fmt.Errorf("failed to create new ECS client server: %w", err)
			}

			log.Info("created the ECS client server")

			// Actually start serving requests on both servers
			estoreDone, estoreErs := estore.Serve()
			eclientDone, eclientErs := eclient.Serve()

			log.Info("serving ECS requests")

			// Log errors from store server
			go func() {
				for err := range estoreErs {
					log.Printf("error from ECS store server: %v", err)
				}
			}()

			// Log errors from client server
			go func() {
				for err := range eclientErs {
					log.Printf("error from ECS client server: %v", err)
				}
			}()

			// Catch the interrupts (ctrl+c)
			quit := make(chan os.Signal, 1)

			signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

			// Graceful shutdown in its own goroutine
			go func() {
				<-quit

				log.Info("about to close ECS")

				if err := estore.Close(); err != nil {
					log.Printf("error closing ECS store server: %v", err)
				}

				if err := eclient.Close(); err != nil {
					log.Printf("error closing ECS client server: %v", err)
				}
			}()

			// Wait for servers to gracefully shutdown
			<-estoreDone
			<-eclientDone

			return nil
		},
	}
)

func init() {
	log.SetLevel(log.InfoLevel)
	log.SetOutput(os.Stdout)

	rootCmd.PersistentFlags().IntVarP(&cfg.Port, "port", "p", 9090, "Sets the port of the server")
	rootCmd.PersistentFlags().IntVarP(&cfg.ClientPort, "client-port", "k", 9091, "Sets the client port of the server")
	rootCmd.PersistentFlags().StringVarP(&cfg.Address, "address", "a", "127.0.0.1", "Which address the server should listen to, default is 127.0.0.1")
	rootCmd.PersistentFlags().StringVarP(&cfg.Logfile, "logfile", "l", "log.db", "Relative path of the logfile, e.g., “echo.log”")
	rootCmd.PersistentFlags().StringVarP(&cfg.Loglevel, "loglevel", "o", "ALL", "Loglevel, e.g., INFO, ALL, . . .")
	rootCmd.PersistentFlags().StringVar(&cfg.Loglevel, "ll", "ALL", "Loglevel, e.g., INFO, ALL, . . .")
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
	}

	log.SetOutput(os.Stderr)
}
