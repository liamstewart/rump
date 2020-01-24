package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/gomodule/redigo/redis"
	"golang.org/x/time/rate"
)

// Report all errors to stdout.
func handle(err error) {
	if err != nil && err != redis.ErrNil {
		fmt.Println(err)
		os.Exit(1)
	}
}

//type Source struct {
//	Url string
//	Conn redis.Conn
//	Queue chan<- map[string]string
//}

type Entry struct {
	Data string
	TTL  string
}

// Scan and queue source keys.
func get(source string, conn redis.Conn, queue chan<- map[string]Entry, count int64, match string, limit float64, syncTTL, verbose bool) {
	var (
		cursor int64
		keys   []string
	)

	fmt.Printf("running get for %s\n", source)

	limiter := rate.NewLimiter(rate.Limit(limit), 5)
	totalScanned := 0

	args := []interface{}{cursor, "COUNT", count}
	if len(match) > 0 {
		args = append(args, "MATCH", match)
	}

	for {
		err := limiter.Wait(context.TODO())
		handle(err)

		// Scan a batch of keys.
		args[0] = cursor
		values, err := redis.Values(conn.Do("SCAN", args...))
		handle(err)
		values, err = redis.Scan(values, &cursor, &keys)
		handle(err)

		totalScanned += len(keys)
		if len(keys) > 0 {
			fmt.Printf("scanned %d key(s) from %s [%d total]\n", len(keys), source, totalScanned)
		}

		// Get pipelined dumps.
		for _, key := range keys {
			if verbose {
				fmt.Printf("DUMPing key: %s\n", key)
			}
			conn.Send("DUMP", key)
		}
		dumps, err := redis.Strings(conn.Do(""))
		handle(err)

		// Get pipelined TTLs.
		for _, key := range keys {
			conn.Send("PTTL", key)
		}
		ttls, err := redis.Ints(conn.Do(""))
		handle(err)

		// Build batch map.
		batch := make(map[string]Entry)
		for i := range keys {
			ttl := "0"
			if syncTTL && ttls[i] >= 0 {
				ttl = strconv.Itoa(ttls[i])
			}
			batch[keys[i]] = Entry{dumps[i], ttl}
		}

		// Last iteration of scan.
		if cursor == 0 {
			// queue last batch.
			select {
			case queue <- batch:
			}
			close(queue)
			break
		}

		fmt.Printf(">")
		if len(batch) > 0 {
			queue <- batch
		}
	}
}

// Restore a batch of keys on destination.
func put(conn redis.Conn, queue <-chan map[string]Entry, limit float64, dryRun, replace bool) {
	limiter := rate.NewLimiter(rate.Limit(limit), 5)

	for batch := range queue {
		err := limiter.Wait(context.TODO())
		handle(err)

		if !dryRun {
			for key, entry := range batch {
				args := []interface{}{key, entry.TTL, entry.Data}
				if replace {
					args = append(args, "REPLACE")
				}
				conn.Send("RESTORE", args...)
			}
			_, err = conn.Do("")
			handle(err)
		}

		fmt.Printf(".")
	}
}

func merge(cs []chan map[string]Entry) <-chan map[string]Entry {
	var wg sync.WaitGroup
	out := make(chan map[string]Entry)
	output := func(c <-chan map[string]Entry) {
		for msg := range c {
			out <- msg
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func connect(host string) (redis.Conn, error) {
	var (
		conn redis.Conn
		err  error
	)
	if strings.HasPrefix(host, "redis://") {
		conn, err = redis.DialURL(host)
	} else {
		conn, err = redis.Dial("tcp", host)
	}
	return conn, err
}

func main() {
	from := flag.String("from", "", "example: redis://127.0.0.1:6379/0")
	to := flag.String("to", "", "example: redis://127.0.0.1:6379/1")
	count := flag.Int64("count", 100, "scan size")
	match := flag.String("match", "", "glob to use for matching keys")
	fromLimit := flag.Float64("fromLimit", math.MaxFloat64, "source rate limit")
	toLimit := flag.Float64("toLimit", math.MaxFloat64, "destination rate limit")
	dryRun := flag.Bool("dryRun", false, "do a dry run")
	verbose := flag.Bool("verbose", false, "be extra verbose")
	syncTTL := flag.Bool("syncTTL", false, "sync TTL")
	replace := flag.Bool("replace", false, "replace")

	flag.Parse()

	sources := strings.Split(*from, ",")
	conns := make([]redis.Conn, len(sources))
	chans := make([]chan map[string]Entry, len(sources))
	for i := range sources {
		conn, err := connect(sources[i])
		handle(err)
		conns[i] = conn
		chans[i] = make(chan map[string]Entry)
	}
	fmt.Printf("sources: %v\n", sources)
	fmt.Printf("destination: %s\n", *to)

	destination, err := connect(*to)
	handle(err)
	defer destination.Close()

	// Channel where batches of keys will pass.
	queue := merge(chans)

	// Scan and send to queue.
	for i := range conns {
		fmt.Printf("Starting get for %s\n", sources[i])
		go get(sources[i], conns[i], chans[i], *count, *match, *fromLimit, *syncTTL, *verbose)
	}

	// Restore keys as they come into queue.
	put(destination, queue, *toLimit, *dryRun, *replace)

	fmt.Println("Sync done.")

	for _, c := range conns {
		c.Close()
	}
}
