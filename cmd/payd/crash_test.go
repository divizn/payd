package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/divizn/payd/internal/ledger"
	"github.com/divizn/payd/internal/pgtest"
	"github.com/google/uuid"
)

var (
	crashKills   = flag.Int("crash.kills", 10, "number of SIGKILLs during the crash test")
	crashWorkers = flag.Int("crash.workers", 16, "concurrent clients during the crash test")
	crashSeed    = flag.Uint64("crash.seed", 0, "seed for kill timing and client operations, 0 picks one")
)

const apiKey = "sk_crash"

func TestCrashKeepsLedgerAndOutboxConsistent(t *testing.T) {
	seed := *crashSeed
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	t.Logf("seed %d", seed)

	ctx := context.Background()
	dsn, db, stop, err := pgtest.Start(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	sum := sha256.Sum256([]byte(apiKey))
	if _, err := db.Exec(ctx, `INSERT INTO api_keys (key_hash, merchant_id) VALUES ($1, 'm_crash')`, sum[:]); err != nil {
		t.Fatal(err)
	}

	bin := filepath.Join(t.TempDir(), "payd")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()

	var logs []*bytes.Buffer
	start := func() *exec.Cmd {
		var out bytes.Buffer
		logs = append(logs, &out)
		cmd := exec.Command(bin)
		cmd.Env = append(os.Environ(), fmt.Sprintf("DATABASE_URL=%s&pool_max_conns=%d", dsn, 2*(*crashWorkers)+4), "ADDR="+addr)
		cmd.Stdout, cmd.Stderr = &out, &out
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			if status, _, err := call(http.MethodGet, "http://"+addr+"/payments", "", ""); err == nil && status == http.StatusOK {
				return cmd
			}
			if time.Now().After(deadline) {
				t.Fatalf("payd did not become ready:\n%s", out.String())
			}
		}
	}

	cmd := start()
	load, stopLoad := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for w := range *crashWorkers {
		rng := rand.New(rand.NewPCG(seed, uint64(w)))
		wg.Go(func() { client{"http://" + addr, rng}.run(load) })
	}

	rng := rand.New(rand.NewPCG(seed, ^uint64(0)))
	for range *crashKills {
		time.Sleep(time.Duration(20+rng.IntN(180)) * time.Millisecond)
		cmd.Process.Kill()
		cmd.Wait()
		cmd = start()
	}
	stopLoad()
	wg.Wait()

	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		var pending int
		if err := db.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d outbox events still unpublished", pending)
		}
	}
	cmd.Process.Signal(syscall.SIGTERM)
	if err := cmd.Wait(); err != nil {
		t.Fatalf("graceful shutdown: %v\n%s", err, logs[len(logs)-1])
	}

	if err := ledger.Check(ctx, db); err != nil {
		t.Error(err)
	}

	var payments, changes, events, mismatched, wrongLast int
	err = db.QueryRow(ctx, `
		WITH ok AS (
			SELECT (convert_from(response_body, 'UTF8')::jsonb->>'id')::uuid AS payment_id, count(*) AS n
			FROM idempotency_keys WHERE response_status < 300 GROUP BY 1
		), ev AS (
			SELECT payment_id, count(*) AS n FROM outbox_events GROUP BY 1
		)
		SELECT count(*), coalesce(sum(ok.n), 0), coalesce(sum(ev.n), 0), count(*) FILTER (WHERE coalesce(ok.n, 0) <> coalesce(ev.n, 0))
		FROM payments p LEFT JOIN ok ON ok.payment_id = p.id LEFT JOIN ev ON ev.payment_id = p.id`).Scan(&payments, &changes, &events, &mismatched)
	if err != nil {
		t.Fatal(err)
	}
	err = db.QueryRow(ctx, `
		SELECT count(*) FROM payments p
		WHERE 'payment.' || p.status <> (SELECT type FROM outbox_events e WHERE e.payment_id = p.id ORDER BY id DESC LIMIT 1)`).Scan(&wrongLast)
	if err != nil {
		t.Fatal(err)
	}
	if mismatched > 0 {
		t.Errorf("%d payments where outbox events do not match committed changes", mismatched)
	}
	if wrongLast > 0 {
		t.Errorf("%d payments whose last event is not their current status", wrongLast)
	}

	published := map[int64]int{}
	for _, out := range logs {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			var line struct {
				Msg   string `json:"msg"`
				Event struct {
					ID int64 `json:"id"`
				} `json:"event"`
			}
			if json.Unmarshal(sc.Bytes(), &line) == nil && line.Msg == "event published" {
				published[line.Event.ID]++
			}
		}
	}
	redelivered := 0
	for _, n := range published {
		redelivered += n - 1
	}
	if len(published) != events {
		t.Errorf("%d distinct events published, %d in the outbox", len(published), events)
	}
	if changes == 0 {
		t.Fatal("no committed changes, load did not run")
	}
	t.Logf("%d kills, %d payments, %d committed changes, %d events, %d redelivered after a crash", *crashKills, payments, changes, events, redelivered)
}

type client struct {
	base string
	rng  *rand.Rand
}

func (c client) run(ctx context.Context) {
	for ctx.Err() == nil {
		amount := 100 + c.rng.Int64N(10000)
		var p struct {
			ID string `json:"id"`
		}
		status, body := c.send(ctx, "/payments", fmt.Sprintf(`{"amount":%d,"currency":"GBP"}`, amount))
		if status != http.StatusCreated || json.Unmarshal(body, &p) != nil {
			continue
		}
		base := "/payments/" + p.ID
		if status, _ := c.send(ctx, base+"/authorise", ""); status != http.StatusOK {
			continue
		}
		if c.rng.IntN(5) == 0 {
			c.send(ctx, base+"/void", "")
			continue
		}
		captured := 1 + c.rng.Int64N(amount)
		c.send(ctx, base+"/capture", fmt.Sprintf(`{"amount":%d}`, captured))
		for range c.rng.IntN(4) {
			c.send(ctx, base+"/refund", fmt.Sprintf(`{"amount":%d}`, 1+c.rng.Int64N(captured)))
		}
	}
}

// send retries one write with the same idempotency key until the server gives a definite answer
func (c client) send(ctx context.Context, path, body string) (int, []byte) {
	key := uuid.NewString()
	for ctx.Err() == nil {
		status, resp, err := call(http.MethodPost, c.base+path, key, body)
		if err == nil && status < 500 && !bytes.Contains(resp, []byte("idempotency_key_in_use")) {
			return status, resp
		}
		time.Sleep(5 * time.Millisecond)
	}
	return 0, nil
}

var httpClient = &http.Client{Timeout: 5 * time.Second}

func call(method, url, key, body string) (int, []byte, error) {
	req, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	res, err := httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	return res.StatusCode, b, err
}
