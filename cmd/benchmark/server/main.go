package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/nStangl/distributed-kv-store/protocol"
	"github.com/nStangl/distributed-kv-store/util"
)

type (
	Pair struct {
		key, value string
	}

	Result struct {
		succesful bool
		pro       protocol.ClientType
		dur       time.Duration
	}
)

const (
	// Change these values to change the benchmark
	concurrentClients = 1
	iterations        = 10 // max = len(words.txt) == 1000
	wordsPath         = "cmd/benchmark/words.txt"
	operation         = protocol.Delete
	// Include operation type (get, put, delete), number of clients, number of iterations in the file name
	csvOutputPath = "cmd/benchmark/server/results-testing.csv"
)

var (
	wg       sync.WaitGroup
	wgResult sync.WaitGroup
	results  = make(chan Result, concurrentClients*100)
)

func main() {
	words, err := util.ReadWords(wordsPath)
	if err != nil {
		log.Fatal(err)
	}

	var data []Pair
	for _, w := range words {
		data = append(data, Pair{key: w, value: w})
	}

	// Collect results
	wgResult.Add(1)
	go func() {
		defer func() {
			wgResult.Done()
		}()

		csvHeaders := []string{"succesful", "protocol type", "duration"}

		var collected_ [][]string
		for r := range results {
			x := make([]string, 3)
			x[0] = fmt.Sprintf("%t", r.succesful)
			x[1] = r.pro.String()
			x[2] = fmt.Sprintf("%d", r.dur.Microseconds())
			collected_ = append(collected_, x)
		}

		// Write to CSV
		f, err := os.Create(csvOutputPath)
		if err != nil {
			panic(err)
		}

		w := csv.NewWriter(f)
		err = w.Write(csvHeaders)
		if err != nil {
			panic(err)
		}
		err = w.WriteAll(collected_)
		if err != nil {
			panic(err)
		}
	}()

	// Start clients
	for i := 0; i < concurrentClients; i++ {
		addr := fmt.Sprintf("localhost:%d", 8080+i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			client(addr, operation, data)
		}()
		fmt.Println("Started client", i)
	}

	wg.Wait()
	close(results)
	wgResult.Wait()
}

func client(addr string, op protocol.ClientType, data []Pair) {
	conn, err := protocol.Connect(addr)
	proto := protocol.ForClient(conn)

	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	log.Println("Executing benchmark for operation", op)
	startTests := time.Now()

	for _, pair := range data {
		msg := protocol.ClientMessage{Type: op, Key: pair.key}
		if op == protocol.Put {
			msg.Value = pair.value
		}
		start := time.Now()
		if err := proto.Send(msg); err != nil {
			log.Fatalf("Error sending message, aborting benchmark: %v", err)
			break
		}

		resp, err := proto.Receive()
		d := time.Since(start)
		if err != nil {
			results <- Result{succesful: false, pro: resp.Type, dur: d}
			continue
		}
		if resp.Type == protocol.Error || resp.Type == protocol.GetError || resp.Type == protocol.PutError || resp.Type == protocol.DeleteError {
			results <- Result{succesful: false, pro: resp.Type, dur: d}
		}

		results <- Result{succesful: true, pro: resp.Type, dur: d}
	}

	log.Println("Worker done with benchmark for operation", op, "in", time.Since(startTests))
}
