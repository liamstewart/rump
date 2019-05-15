package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
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

// Scan and queue source keys.
func get(source string, conn redis.Conn, queue chan<- map[string]string, count int64, match string, limit float64) {
	var (
		cursor int64
		keys   []string
	)

	limiter := rate.NewLimiter(rate.Limit(limit), 5)

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

		// Get pipelined dumps.
		fmt.Printf("scanned %d key(s) from %s\n", len(keys), source)
		for _, key := range keys {
			conn.Send("DUMP", key)
		}
		dumps, err := redis.Strings(conn.Do(""))
		handle(err)

		// Build batch map.
		batch := make(map[string]string)
		for i := range keys {
			batch[keys[i]] = dumps[i]
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
func put(conn redis.Conn, queue <-chan map[string]string, limit float64) {
	limiter := rate.NewLimiter(rate.Limit(limit), 5)

	for batch := range queue {
		err := limiter.Wait(context.TODO())
		handle(err)
		for key, value := range batch {
			conn.Send("RESTORE", key, "0", value)
		}
		_, err = conn.Do("")
		handle(err)

		fmt.Printf(".")
	}
}

func merge(cs []chan map[string]string) <-chan map[string]string {
	var wg sync.WaitGroup
	out := make(chan map[string]string)
	output := func(c <-chan map[string]string) {
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

	flag.Parse()

	sources := strings.Split(*from, ",")
	conns := make([]redis.Conn, len(sources))
	chans := make([]chan map[string]string, len(sources))
	for i := range sources {
		conn, err := connect(sources[i])
		handle(err)
		conns[i] = conn
		chans[i] = make(chan map[string]string)
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
		go get(sources[i], conns[i], chans[i], *count, *match, *fromLimit)
	}

	// Restore keys as they come into queue.
	put(destination, queue, *toLimit)

	fmt.Println("Sync done.")

	for _, c := range conns {
		c.Close()
	}
}
