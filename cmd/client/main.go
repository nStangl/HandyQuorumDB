package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/nStangl/distributed-kv-store/client"
	"github.com/nStangl/distributed-kv-store/protocol"
	log "github.com/sirupsen/logrus"
)

// Global variables
var (
	addr  string
	host  string
	port  string
	conn  *net.TCPConn
	proto protocol.Protocol[protocol.ClientMessage]
	help  = [11][2]string{
		{"connect", "connect to an addres (addr port)"},
		{"disconnect", "disconnect from the server"},
		{"put", "inserts key-value pair in server (key value)"},
		{"get", "retrieves value for given key from server (key)"},
		{"delete", "deletes key-value pair from server (key)"},
		{"send", "send message to server (free form text)"},
		{"logLevel", "set log level"},
		{"keyrange", "get the keyranges each server is responsible for"},
		{"keyrange_read", "get the keyranges each server is responsible for including the replicas"},
		{"help", "print this help"},
		{"quit", "exit the program"},
	}
)

var echo = "EchoClient>"

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
			if conn != nil {
				if err := conn.Close(); err != nil {
					log.Fatalf("failed to disconnect socket: %v", err)
				}

				log.Infof("disconnected from %s", addr)
			}
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
		case "keyrange":
			promptKeyrange(p)
		case "keyrange_read":
			promptKeyrangeRead(p)
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
	if conn == nil {
		log.Warn("Error! Not connected!")
		return
	}

	if err := conn.Close(); err != nil {
		log.Fatalf("failed to disconnect socket: %v", err)
	}

	log.Infof("Connection terminated: /%s / %s", host, port)

	conn = nil
	proto = nil
	addr = ""
	host = ""
	port = ""
}

func promptConnect(p []string) {
	if err := client.ValidateInput(p, 3); err != nil {
		log.Warn(err)
		return
	}

	if conn != nil {
		log.Warn("you're already connected")
		return
	}

	host = p[1]
	port = p[2]
	addr = fmt.Sprintf("%s:%s", p[1], p[2])

	c, err := protocol.Connect(addr)
	if err != nil {
		log.Warnf("failed to connect to server: %v", err)
		return
	}

	conn = c
	proto = protocol.ForClient(c)

	r, err := proto.Receive()
	if err != nil {
		log.Warnf("failed to receive data: %v", err)
		promptDisconnect()
		return
	}

	if r == nil {
		log.Warnf("failed to receive data: %v", err)
		promptDisconnect()
	} else {
		fmt.Printf("%s %s\n", echo, r)
	}
}

func promptPut(p []string) {
	if err := client.ValidateInput(p, 3); err != nil {
		log.Warn(err)
		return
	}

	if conn == nil {
		log.Warn("Error! Not connected!")
		return
	}

	msg := protocol.ClientMessage{Type: protocol.Put, Key: p[1], Value: p[2]}

	if err := proto.Send(msg); err != nil {
		log.Errorf("error while sending put message %v: %v", p[1], err)
		return
	}

	r, err := proto.Receive()
	if err != nil {
		log.Warnf("failed to receive data: %v", err)
		promptDisconnect()
		return
	}

	m, err := client.ValidatePutResponse(r, echo)
	if err != nil {
		log.Error(err)
		promptDisconnect()
		return
	}
	fmt.Println(m)
}

func promptGet(p []string) {
	if err := client.ValidateInput(p, 2); err != nil {
		log.Warn(err)
		return
	}

	if conn == nil {
		log.Warn("Error! Not connected!")
		return
	}

	msg := protocol.ClientMessage{Type: protocol.Get, Key: p[1]}

	if err := proto.Send(msg); err != nil {
		log.Errorf("error while sending get message %v: %v", p[1], err)
		return
	}

	r, err := proto.Receive()
	if err != nil {
		log.Warnf("failed to receive data: %v", err)
		promptDisconnect()
		return
	}

	m, err := client.ValidateGetResponse(r, echo)
	if err != nil {
		log.Error(err)
		promptDisconnect()
		return
	}
	fmt.Println(m)
}

func promptDelete(p []string) {
	if err := client.ValidateInput(p, 2); err != nil {
		log.Warn(err)
		return
	}

	if conn == nil {
		log.Warn("Error! Not connected!")
		return
	}

	msg := protocol.ClientMessage{Type: protocol.Delete, Key: p[1]}

	if err := proto.Send(msg); err != nil {
		log.Errorf("error while sending get message %v: %v", p[1], err)
		return
	}

	r, err := proto.Receive()
	if err != nil {
		log.Warnf("failed to receive data: %v", err)
		promptDisconnect()
		return
	}

	m, err := client.ValidateDeleteResponse(r, echo)
	if err != nil {
		log.Error(err)
		promptDisconnect()
		return
	}
	fmt.Println(m)
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

func promptKeyrange(p []string) {
	if err := client.ValidateInput(p, 1); err != nil {
		log.Warn(err)
		return
	}

	if conn == nil {
		log.Warn("Error! Not connected!")
		return
	}

	msg := protocol.ClientMessage{Type: protocol.Keyrange}

	if err := proto.Send(msg); err != nil {
		log.Errorf("error while sending keyrange message: %v", err)
		return
	}

	r, err := proto.Receive()
	if err != nil {
		log.Warnf("failed to receive data: %v", err)
		promptDisconnect()
		return
	}

	if r == nil {
		log.Warn("received null data")
		promptDisconnect()
		return
	}

	switch r.Type {
	case protocol.Error:
		log.Errorf("Received unknown error while receiving reply to keyrange message")
	case protocol.KeyrangeSuccess:
		fmt.Printf("%s keyrange_success %s\n", echo, r.Key)
	case protocol.KeyrangeError:
		fmt.Printf("%s keyrange_error\n", echo)
	default:
		log.Errorf("received unexpected reply to put message: %#v", r)
	}
}

func promptKeyrangeRead(p []string) {
	if err := client.ValidateInput(p, 1); err != nil {
		log.Warn(err)
		return
	}

	if conn == nil {
		log.Warn("Error! Not connected!")
		return
	}

	msg := protocol.ClientMessage{Type: protocol.KeyrangeRead}

	if err := proto.Send(msg); err != nil {
		log.Errorf("error while sending keyrange_read message: %v", err)
		return
	}

	r, err := proto.Receive()
	if err != nil {
		log.Warnf("failed to receive data: %v", err)
		promptDisconnect()
		return
	}

	if r == nil {
		log.Warn("received null data")
		promptDisconnect()
		return
	}

	switch r.Type {
	case protocol.Error:
		log.Errorf("Received unknown error while receiving reply to keyrange_read message")
	case protocol.KeyrangeReadSuccess:
		fmt.Printf("%s keyrange_read_success %s\n", echo, r.Key)
	case protocol.KeyrangeReadError:
		fmt.Printf("%s keyrange_read_error\n", echo)
	default:
		log.Errorf("received unexpected reply to put message: %#v", r)
	}
}

func promptHelp() {
	for _, h := range help {
		fmt.Printf("%s %-12s %s\n", echo, h[0], h[1])
	}
}
