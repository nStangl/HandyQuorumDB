package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/nStangl/distributed-kv-store/client"
	log "github.com/sirupsen/logrus"
)

// Global variables
var (
	qc   client.QuorumClient
	help = [8][2]string{
		{"connect", "connect to ECS at given address (addr port)"},
		{"disconnect", "disconnect from the ECS"},
		{"put", "inserts key-value pair in server (key value)"},
		{"get", "retrieves value for given key from server (key)"},
		{"delete", "deletes key-value pair from server (key)"},
		{"logLevel", "set log level"},
		{"help", "print this help"},
		{"quit", "exit the program"},
	}
)

var echo = ">"

func init() {
	// Initialize the logger
	log.SetLevel(log.InfoLevel)
	log.SetOutput(os.Stdout)
}

// The main function
func main() {
	var (
		// The input stream
		in = bufio.NewReader(os.Stdin)
		// The channel to catch interrupts (ctrl+c)
		quit = make(chan os.Signal, 1)
		// The cleanup function
		cleanup = func() {
			qc.Close()
		}
	)

	// Catch the interrupts (ctrl+c)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// Graceful shutdown in it's own goroutine
	go func() {
		<-quit
		cleanup()
		os.Exit(0)
	}()

	for {
		s, err := in.ReadString('\n')
		if err != nil {
			log.Errorf("failed to read line: %v", err)
			continue
		}

		// Trim the newline, split into an array of strings
		p := strings.Fields(strings.TrimSuffix(s, "\n"))
		if len(p) == 0 {
			continue
		}

		switch p[0] {
		case "connect":
			promptConnect(p)
		case "disconnect":
			promptDisconnect()
		case "put":
			promptPut(p)
		case "get":
			promptGet(p)
		case "delete":
			promptDelete(p)
		case "logLevel":
			promptLogLevel(p)
		case "help":
			promptHelp()
		case "quit":
			fmt.Println(echo, "Application exit!")
			log.Info("Application exit!")
			cleanup()
			return
		default:
			log.Warnf("Unknown command %q", strings.Join(p, " "))
			promptHelp()
		}
	}
}

func promptDisconnect() {
	// TODO
}

func promptConnect(p []string) {
	if err := client.ValidateInput(p, 3); err != nil {
		log.Warn(err)
		return
	}

	addr := fmt.Sprintf("%s:%s", p[1], p[2])
	if err := qc.Connect(addr); err != nil {
		log.Warnf("failed to connect to %s: %v", addr, err)
		return
	}

	go func(ers <-chan error) {
		for err := range ers {
			log.Printf("error from quorum client: %v", err)
		}
	}(qc.Listen())

	if err := qc.Kickstart(); err != nil {
		log.Warnf("failed to connect kickstart quorum client: %v", err)
		return
	}
}

func promptPut(p []string) {
	if err := client.ValidateInput(p, 3); err != nil {
		log.Warn(err)
		return
	}

	m, err := qc.Put(p[1], p[2])
	if err != nil {
		log.Errorf("error while sending put message %v: %v", p[1], err)
		return
	}

	fmt.Println(echo, m)
}

func promptGet(p []string) {
	if err := client.ValidateInput(p, 2); err != nil {
		log.Warn(err)
		return
	}

	m, err := qc.Get(p[1])
	if err != nil {
		log.Errorf("error while sending get message %v: %v", p[1], err)
		return
	}

	fmt.Println(echo, m)
}

func promptDelete(p []string) {
	if err := client.ValidateInput(p, 2); err != nil {
		log.Warn(err)
		return
	}

	m, err := qc.Delete(p[1])
	if err != nil {
		log.Errorf("error while sending delete message %v: %v", p[1], err)
		return
	}

	fmt.Println(echo, m)
}

func promptLogLevel(p []string) {
	if err := client.ValidateInput(p, 2); err != nil {
		log.Warn(err)
		return
	}

	prevLvl := log.GetLevel().String()

	switch p[1] {
	case "off":
		log.SetLevel(log.PanicLevel)
		fmt.Println(echo, "loglevel set from", prevLvl, "to", p[1])
	case "info":
		log.SetLevel(log.InfoLevel)
		fmt.Println(echo, "loglevel set from", prevLvl, "to", p[1])
	case "warning":
		log.SetLevel(log.WarnLevel)
		fmt.Println(echo, "loglevel set from", prevLvl, "to", p[1])
	case "severe":
		log.SetLevel(log.ErrorLevel)
		fmt.Println(echo, "loglevel set from", prevLvl, "to", p[1])
	case "all", "config", "fine", "finest":
		log.SetLevel(log.DebugLevel)
		fmt.Println(echo, "loglevel set from", prevLvl, "to", p[1])
	default:
		log.Warnf("log level %q is unrecognized", p[1])
	}
}

func promptHelp() {
	for _, h := range help {
		fmt.Printf("%s %-12s %s\n", echo, h[0], h[1])
	}
}
