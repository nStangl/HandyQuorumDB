package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/aybabtme/uniplot/histogram"
	"github.com/nStangl/distributed-kv-store/server/data"
	dbLog "github.com/nStangl/distributed-kv-store/server/log"
	"github.com/nStangl/distributed-kv-store/server/memtable"
	"github.com/nStangl/distributed-kv-store/server/sstable"
	"github.com/nStangl/distributed-kv-store/server/store"
	"github.com/nStangl/distributed-kv-store/util"

	_ "net/http/pprof"
)

const managerBacklog = 4

// Server executable defined here
func main() {
	temp := "temp"

	if err := os.RemoveAll(temp); err != nil {
		log.Println(err)
	}

	err := os.MkdirAll(temp, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}

	words, err := util.ReadWords("cmd/benchmark/words.txt")
	if err != nil {
		log.Fatal(err)
	}
	shuffled := make([]string, len(words))
	copy(shuffled, words)

	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	var (
		keys   = make([]string, 0, len(words)^2)
		values = make([]string, 0, len(words)^2)
	)

	for i := range words {
		for j := range shuffled {
			keys = append(keys, words[i]+shuffled[j])
			values = append(values, fmt.Sprintf("%s%s%d", words[i], shuffled[j], (i+j)))
		}
	}

	log.Println("loaded example keys", len(keys))

	l, err := dbLog.New(filepath.Join(temp, "db.log"))
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := l.Close(); err != nil {
			log.Printf("failed to close log: %v", err)
		}
	}()

	m, err := sstable.NewManager(temp, managerBacklog)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := m.Close(); err != nil {
			log.Printf("failed to close manager: %v", err)
		}
	}()

	ers := m.Process()

	go func() {
		for err := range ers {
			log.Printf("error from manager: %v", err)
		}
	}()

	if err := m.Startup(); err != nil {
		panic(err)
	}

	log.Println("about to start")

	store := store.New(l, m, func() memtable.Table {
		return memtable.NewRedBlackTree()
	})

	type res struct {
		w bool
		r data.Result
		d time.Duration
	}

	var (
		wg  sync.WaitGroup
		wg2 sync.WaitGroup
		rs  = make(chan res, 1000)
	)

	readTimes := make([]float64, 0, len(keys))
	writeTimes := make([]float64, 0, len(keys))
	outcomeRead := make(map[data.ResultKind]uint)

	go func() {
		defer wg2.Done()

		for r := range rs {
			if r.w {
				writeTimes = append(writeTimes, float64(r.d.Nanoseconds()))
			} else {
				outcomeRead[r.r.Kind]++
				if r.d.Nanoseconds() > 30000 {
					continue
				}

				readTimes = append(readTimes, float64(r.d.Nanoseconds()))
			}
		}
	}()

	log.Println("setting data", len(keys))

	start := time.Now()

	for i := 0; i < len(keys); i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			var (
				k = keys[i]
				v = values[i]
			)

			s := time.Now()

			if err := store.Set(k, v); err != nil {
				panic(err)
			}

			rs <- res{w: true, d: time.Since(s)}

			if rand.Intn(10) == 1 {
				if err := store.Del(k); err != nil {
					panic(err)
				}
			}
		}(i)
	}

	wg.Wait()

	log.Printf("setting took %s", time.Since(start))

	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	log.Println("getting data", len(keys))

	start = time.Now()

	for i := 0; i < len(keys); i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			s := time.Now()

			r, err := store.Get(keys[i])
			if err != nil {
				panic(err)
			}

			rs <- res{r: r, d: time.Since(s)}
		}(i)
	}

	wg.Wait()

	log.Printf("getting took %s", time.Since(start))

	wg2.Add(1)

	close(rs)

	wg2.Wait()

	fmt.Println("Showing histogram for reads (in nanoseconds)")

	h := histogram.Hist(5, readTimes)
	maxWidth := 5
	_ = histogram.Fprint(os.Stdout, h, histogram.Linear(maxWidth))

	// fmt.Println(readTimes)

	fmt.Println("Showing histogram for writes (in nanoseconds)")

	h2 := histogram.Hist(5, writeTimes)
	_ = histogram.Fprint(os.Stdout, h2, histogram.Linear(maxWidth))

	// fmt.Println(writeTimes)

	// Convert the array to a string
	data := ""
	for _, t := range readTimes {
		data += strconv.FormatFloat(t, 'f', -1, 64) + "\n"
	}

	// // Write the string data to a file
	// os.WriteFile("readtimes.txt", []byte(data), 0644)

	// fmt.Println("Array written to file successfully.")

	// Convert the array to a string
	data = ""
	for _, t := range writeTimes {
		data += strconv.FormatFloat(t, 'f', -1, 64) + "\n"
	}

	// // Write the string data to a file
	// os.WriteFile("writeTimes.txt", []byte(data), 0644)

	// fmt.Println("Array written to file successfully.")

}
