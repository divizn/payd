package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/divizn/payd/internal/ledger"
	"github.com/divizn/payd/internal/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const racers = 50

var (
	db  *pgxpool.Pool
	srv *httptest.Server
)

func TestMain(m *testing.M) {
	_, pool, stop, err := pgtest.Start(context.Background(), racers+10)
	if err != nil {
		log.Fatal(err)
	}
	db = pool
	srv = httptest.NewServer(New(db, slog.New(slog.NewTextHandler(io.Discard, nil))))
	srv.Client().Transport.(*http.Transport).MaxIdleConnsPerHost = racers

	code := m.Run()
	srv.Close()
	stop()
	os.Exit(code)
}

type merchant struct {
	id, apiKey string
}

func newMerchant(t *testing.T) merchant {
	t.Helper()
	m := merchant{id: "m_" + uuid.NewString(), apiKey: "sk_" + uuid.NewString()}
	sum := sha256.Sum256([]byte(m.apiKey))
	if _, err := db.Exec(context.Background(), `INSERT INTO api_keys (key_hash, merchant_id) VALUES ($1, $2)`, sum[:], m.id); err != nil {
		t.Fatal(err)
	}
	return m
}

type response struct {
	status int
	body   []byte
}

func (r response) code() string {
	var e apiError
	json.Unmarshal(r.body, &e)
	return e.Error.Code
}

func (m merchant) do(t *testing.T, method, path, idemKey, body string) response {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+m.apiKey)
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	res, err := srv.Client().Do(req)
	if err != nil {
		t.Error(err)
		return response{}
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return response{res.StatusCode, b}
}

// doRetrying retries while the key is in flight, as a client following the API contract would
func (m merchant) doRetrying(t *testing.T, method, path, idemKey, body string) response {
	for {
		r := m.do(t, method, path, idemKey, body)
		if r.status != http.StatusConflict || r.code() != "idempotency_key_in_use" {
			return r
		}
	}
}

func (m merchant) payment(t *testing.T, path, key, body string, want int) paymentJSON {
	t.Helper()
	r := m.do(t, http.MethodPost, path, key, body)
	if r.status != want {
		t.Fatalf("POST %s = %d %s, want %d", path, r.status, r.body, want)
	}
	var p paymentJSON
	if err := json.Unmarshal(r.body, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func (m merchant) authorised(t *testing.T, amount int64) paymentJSON {
	t.Helper()
	p := m.payment(t, "/payments", uuid.NewString(), fmt.Sprintf(`{"amount":%d,"currency":"GBP"}`, amount), http.StatusCreated)
	return m.payment(t, "/payments/"+p.ID+"/authorise", uuid.NewString(), "", http.StatusOK)
}

func (m merchant) countPayments(t *testing.T) int {
	t.Helper()
	var n int
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM payments WHERE merchant_id = $1`, m.id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func race(n int, f func(i int)) {
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Go(func() {
			<-start
			f(i)
		})
	}
	close(start)
	wg.Wait()
}

func TestConcurrentDuplicateCreateChargesOnce(t *testing.T) {
	m := newMerchant(t)
	res := make([]response, racers)
	race(racers, func(i int) {
		res[i] = m.doRetrying(t, http.MethodPost, "/payments", "same-key", `{"amount":1000,"currency":"GBP"}`)
	})
	if n := m.countPayments(t); n != 1 {
		t.Fatalf("%d payments created by %d duplicate requests, want 1", n, racers)
	}
	for i, r := range res {
		if r.status != http.StatusCreated || !bytes.Equal(r.body, res[0].body) {
			t.Fatalf("caller %d got %d %s, caller 0 got %d %s", i, r.status, r.body, res[0].status, res[0].body)
		}
	}
}

func TestConcurrentDuplicateCaptureCapturesOnce(t *testing.T) {
	m := newMerchant(t)
	p := m.authorised(t, 1000)
	res := make([]response, racers)
	race(racers, func(i int) {
		res[i] = m.doRetrying(t, http.MethodPost, "/payments/"+p.ID+"/capture", "cap-key", `{"amount":400}`)
	})
	for i, r := range res {
		if r.status != http.StatusOK || !bytes.Equal(r.body, res[0].body) {
			t.Fatalf("caller %d got %d %s, caller 0 got %d %s", i, r.status, r.body, res[0].status, res[0].body)
		}
	}
}

func TestConcurrentCapturesWithDifferentKeysCaptureOnce(t *testing.T) {
	m := newMerchant(t)
	p := m.authorised(t, 1000)
	res := make([]response, racers)
	race(racers, func(i int) {
		res[i] = m.do(t, http.MethodPost, "/payments/"+p.ID+"/capture", fmt.Sprint("cap-", i), "")
	})
	ok := 0
	for _, r := range res {
		switch {
		case r.status == http.StatusOK:
			ok++
		case r.status != http.StatusConflict || r.code() != "illegal_transition":
			t.Errorf("unexpected response %d %s", r.status, r.body)
		}
	}
	if ok != 1 {
		t.Fatalf("%d of %d captures succeeded, want 1", ok, racers)
	}
}

func TestConcurrentRefundsNeverExceedCapture(t *testing.T) {
	m := newMerchant(t)
	p := m.authorised(t, 1000)
	m.payment(t, "/payments/"+p.ID+"/capture", uuid.NewString(), `{"amount":100}`, http.StatusOK)
	res := make([]response, racers)
	race(racers, func(i int) {
		res[i] = m.do(t, http.MethodPost, "/payments/"+p.ID+"/refund", fmt.Sprint("ref-", i), `{"amount":30}`)
	})
	ok := 0
	for _, r := range res {
		if r.status == http.StatusOK {
			ok++
		}
	}
	var refunded int64
	if err := db.QueryRow(context.Background(), `SELECT refunded_amount FROM payments WHERE id = $1`, p.ID).Scan(&refunded); err != nil {
		t.Fatal(err)
	}
	if ok != 3 || refunded != 90 {
		t.Fatalf("%d refunds of 30 succeeded against a capture of 100, refunded_amount=%d; want 3 and 90", ok, refunded)
	}
}

func TestReusedKeyWithDifferentBodyIsRejected(t *testing.T) {
	m := newMerchant(t)
	m.payment(t, "/payments", "k", `{"amount":1000,"currency":"GBP"}`, http.StatusCreated)
	r := m.do(t, http.MethodPost, "/payments", "k", `{"amount":2000,"currency":"GBP"}`)
	if r.status != http.StatusUnprocessableEntity || r.code() != "idempotency_key_reused" {
		t.Fatalf("got %d %s, want 422 idempotency_key_reused", r.status, r.body)
	}
	if n := m.countPayments(t); n != 1 {
		t.Fatalf("%d payments, want 1", n)
	}
}

func TestKeysExpireAfter24Hours(t *testing.T) {
	m := newMerchant(t)
	first := m.payment(t, "/payments", "k", `{"amount":1000,"currency":"GBP"}`, http.StatusCreated)
	if _, err := db.Exec(context.Background(), `UPDATE idempotency_keys SET created_at = now() - interval '23 hours' WHERE merchant_id = $1`, m.id); err != nil {
		t.Fatal(err)
	}
	if again := m.payment(t, "/payments", "k", `{"amount":1000,"currency":"GBP"}`, http.StatusCreated); again.ID != first.ID {
		t.Fatalf("key replayed a different payment before expiry: %s vs %s", again.ID, first.ID)
	}
	if _, err := db.Exec(context.Background(), `UPDATE idempotency_keys SET created_at = now() - interval '25 hours' WHERE merchant_id = $1`, m.id); err != nil {
		t.Fatal(err)
	}
	if again := m.payment(t, "/payments", "k", `{"amount":1000,"currency":"GBP"}`, http.StatusCreated); again.ID == first.ID {
		t.Fatal("expired key still replayed the old response")
	}
}

func TestMissingIdempotencyKeyIsRejected(t *testing.T) {
	m := newMerchant(t)
	if r := m.do(t, http.MethodPost, "/payments", "", `{"amount":1000,"currency":"GBP"}`); r.status != http.StatusBadRequest {
		t.Fatalf("got %d %s, want 400", r.status, r.body)
	}
	if n := m.countPayments(t); n != 0 {
		t.Fatalf("%d payments created without a key", n)
	}
}

func TestMerchantsAreIsolated(t *testing.T) {
	owner, other := newMerchant(t), newMerchant(t)
	p := owner.authorised(t, 1000)
	if r := other.do(t, http.MethodGet, "/payments/"+p.ID, "", ""); r.status != http.StatusNotFound {
		t.Errorf("other merchant GET = %d, want 404", r.status)
	}
	if r := other.do(t, http.MethodPost, "/payments/"+p.ID+"/capture", "k", ""); r.status != http.StatusNotFound {
		t.Errorf("other merchant capture = %d, want 404", r.status)
	}
	if r := other.do(t, http.MethodGet, "/payments", "", ""); !bytes.Contains(r.body, []byte(`"data":[]`)) {
		t.Errorf("other merchant list = %s, want empty", r.body)
	}
}

func TestLedgerAndOutboxFollowEachCommittedChange(t *testing.T) {
	m := newMerchant(t)
	p := m.authorised(t, 1000)
	base := "/payments/" + p.ID
	m.payment(t, base+"/capture", uuid.NewString(), `{"amount":600}`, http.StatusOK)
	m.payment(t, base+"/refund", uuid.NewString(), `{"amount":100}`, http.StatusOK)
	if r := m.do(t, http.MethodPost, base+"/capture", uuid.NewString(), ""); r.status != http.StatusConflict {
		t.Fatalf("capture after refund = %d %s, want 409", r.status, r.body)
	}
	m.payment(t, base+"/refund", uuid.NewString(), "", http.StatusOK)

	ctx := context.Background()
	if err := ledger.Check(ctx, db); err != nil {
		t.Fatal(err)
	}
	var balance int64
	if err := db.QueryRow(ctx, `SELECT sum(amount) FROM ledger_entries WHERE payment_id = $1 AND account = $2`, p.ID, ledger.MerchantAccount(m.id)).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance != 0 {
		t.Fatalf("merchant balance after full refund = %d, want 0", balance)
	}
	rows, err := db.Query(ctx, `SELECT type FROM outbox_events WHERE payment_id = $1 ORDER BY id`, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	types, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"payment.created", "payment.authorised", "payment.captured", "payment.partially_refunded", "payment.refunded"}
	if fmt.Sprint(types) != fmt.Sprint(want) {
		t.Fatalf("outbox events = %v, want %v", types, want)
	}
}
