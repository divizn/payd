package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

type sample struct {
	op      string
	latency time.Duration
	ok      bool
}

func main() {
	base := flag.String("url", "http://localhost:8080", "payd base URL")
	key := flag.String("key", os.Getenv("PAYD_API_KEY"), "API key")
	conc := flag.Int("c", 16, "concurrent clients")
	dur := flag.Duration("d", 30*time.Second, "test duration")
	warmup := flag.Duration("warmup", 5*time.Second, "warmup excluded from results")
	flag.Parse()

	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: *conc}, Timeout: 10 * time.Second}
	post := func(path, body string) (int, []byte, time.Duration, error) {
		req, _ := http.NewRequest(http.MethodPost, *base+path, bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer "+*key)
		req.Header.Set("Idempotency-Key", uuid.NewString())
		start := time.Now()
		res, err := client.Do(req)
		if err != nil {
			return 0, nil, 0, err
		}
		defer res.Body.Close()
		b, err := io.ReadAll(res.Body)
		return res.StatusCode, b, time.Since(start), err
	}

	ctx, cancel := context.WithTimeout(context.Background(), *warmup+*dur)
	defer cancel()
	measureFrom := time.Now().Add(*warmup)
	var mu sync.Mutex
	var samples []sample
	record := func(op string, lat time.Duration, ok bool) {
		if time.Now().Before(measureFrom) {
			return
		}
		mu.Lock()
		samples = append(samples, sample{op, lat, ok})
		mu.Unlock()
	}

	var wg sync.WaitGroup
	for range *conc {
		wg.Go(func() {
			for ctx.Err() == nil {
				status, body, lat, err := post("/payments", `{"amount":1000,"currency":"GBP"}`)
				record("create", lat, err == nil && status == http.StatusCreated)
				var p struct {
					ID string `json:"id"`
				}
				if err != nil || json.Unmarshal(body, &p) != nil || p.ID == "" {
					continue
				}
				for _, step := range []struct{ op, body string }{{"authorise", ""}, {"capture", ""}, {"refund", `{"amount":400}`}} {
					status, _, lat, err := post("/payments/"+p.ID+"/"+step.op, step.body)
					record(step.op, lat, err == nil && status == http.StatusOK)
					if err != nil || status != http.StatusOK {
						break
					}
				}
			}
		})
	}
	wg.Wait()

	if len(samples) == 0 {
		log.Fatal("no samples")
	}
	byOp := map[string][]time.Duration{}
	failed := 0
	for _, s := range samples {
		if !s.ok {
			failed++
			continue
		}
		byOp[s.op] = append(byOp[s.op], s.latency)
		byOp["all"] = append(byOp["all"], s.latency)
	}
	fmt.Printf("concurrency %d, %s measured, %d requests, %d failed, %.0f req/s\n", *conc, *dur, len(samples), failed, float64(len(samples)-failed)/dur.Seconds())
	for _, op := range []string{"create", "authorise", "capture", "refund", "all"} {
		l := byOp[op]
		if len(l) == 0 {
			continue
		}
		slices.Sort(l)
		q := func(p float64) time.Duration { return l[int(p*float64(len(l)-1))].Round(10 * time.Microsecond) }
		fmt.Printf("  %-9s n=%-7d p50=%-9s p99=%-9s max=%s\n", op, len(l), q(0.50), q(0.99), l[len(l)-1].Round(10*time.Microsecond))
	}
}
