package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/divizn/payd/internal/acquirer"
	"github.com/divizn/payd/internal/ledger"
	"github.com/divizn/payd/internal/payment"
	"github.com/divizn/payd/internal/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const racers = 50

var (
	db         *pgxpool.Pool
	srv        *httptest.Server
	acq        = &scriptedAcquirer{fallback: &acquirer.Fake{}}
	testPolicy = acquirer.Policy{Attempts: 3, Timeout: 100 * time.Millisecond, Backoff: 10 * time.Millisecond}
)

func TestMain(m *testing.M) {
	_, pool, stop, err := pgtest.Start(context.Background(), racers+10)
	if err != nil {
		log.Fatal(err)
	}
	db = pool
	srv = httptest.NewServer(New(db, slog.New(slog.NewTextHandler(io.Discard, nil)), acq, testPolicy))
	srv.Client().Transport.(*http.Transport).MaxIdleConnsPerHost = racers

	code := m.Run()
	srv.Close()
	stop()
	os.Exit(code)
}

type scriptedAcquirer struct {
	fallback acquirer.Acquirer
	mu       sync.Mutex
	errs     map[string][]error
	calls    map[string]int
}

func (a *scriptedAcquirer) fail(ref string, errs ...error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.errs == nil {
		a.errs = map[string][]error{}
	}
	a.errs[ref] = append(a.errs[ref], errs...)
}

func (a *scriptedAcquirer) callsFor(ref string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls[ref]
}

func (a *scriptedAcquirer) Authorise(ctx context.Context, ref string, m payment.Money) (bool, error) {
	a.mu.Lock()
	if a.calls == nil {
		a.calls = map[string]int{}
	}
	a.calls[ref]++
	var err error
	if q := a.errs[ref]; len(q) > 0 {
		err, a.errs[ref] = q[0], q[1:]
	}
	a.mu.Unlock()
	if err != nil {
		return false, err
	}
	return a.fallback.Authorise(ctx, ref, m)
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
		t.Error(err)
		return response{}
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
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		r := m.do(t, method, path, idemKey, body)
		if r.status != http.StatusConflict || r.code() != "idempotency_key_in_use" {
			return r
		}
	}
	t.Errorf("%s %s: key %q still in flight after 10s", method, path, idemKey)
	return response{}
}

func checkLedger(t *testing.T) {
	t.Helper()
	if err := ledger.Check(context.Background(), db); err != nil {
		t.Error(err)
	}
}

func (m merchant) ledgerEntries(t *testing.T, paymentID string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM ledger_entries WHERE payment_id = $1`, paymentID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
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
	if n := m.ledgerEntries(t, p.ID); n != 2 {
		t.Fatalf("%d ledger entries after %d duplicate captures, want one pair", n, racers)
	}
	checkLedger(t)
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
	checkLedger(t)
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
		switch {
		case r.status == http.StatusOK:
			ok++
		case r.status != http.StatusUnprocessableEntity || r.code() != "invalid_amount":
			t.Errorf("unexpected response %d %s", r.status, r.body)
		}
	}
	var refunded int64
	if err := db.QueryRow(context.Background(), `SELECT refunded_amount FROM payments WHERE id = $1`, p.ID).Scan(&refunded); err != nil {
		t.Fatal(err)
	}
	if ok != 3 || refunded != 90 {
		t.Fatalf("%d refunds of 30 succeeded against a capture of 100, refunded_amount=%d; want 3 and 90", ok, refunded)
	}
	checkLedger(t)
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

func (m merchant) eventTypes(t *testing.T, paymentID string) []string {
	t.Helper()
	rows, err := db.Query(context.Background(), `SELECT type FROM outbox_events WHERE payment_id = $1 ORDER BY id`, paymentID)
	if err != nil {
		t.Fatal(err)
	}
	types, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return types
}

func TestDeclinedAuthorisationFailsThePayment(t *testing.T) {
	m := newMerchant(t)
	p := m.payment(t, "/payments", uuid.NewString(), `{"amount":1051,"currency":"GBP"}`, http.StatusCreated)
	if got := m.payment(t, "/payments/"+p.ID+"/authorise", uuid.NewString(), "", http.StatusOK); got.Status != "failed" {
		t.Fatalf("status after decline = %s, want failed", got.Status)
	}
	if got := fmt.Sprint(m.eventTypes(t, p.ID)); got != "[payment.created payment.failed]" {
		t.Fatalf("events = %s", got)
	}
	if r := m.do(t, http.MethodPost, "/payments/"+p.ID+"/capture", uuid.NewString(), ""); r.status != http.StatusConflict {
		t.Fatalf("capture of failed payment = %d, want 409", r.status)
	}
}

func TestAcquirerOutageLeavesTheKeyRetryable(t *testing.T) {
	m := newMerchant(t)
	p := m.payment(t, "/payments", uuid.NewString(), `{"amount":1000,"currency":"GBP"}`, http.StatusCreated)
	acq.fail(p.ID, acquirer.ErrUnavailable, acquirer.ErrUnavailable, acquirer.ErrUnavailable)
	path := "/payments/" + p.ID + "/authorise"
	if r := m.do(t, http.MethodPost, path, "auth", ""); r.status != http.StatusServiceUnavailable {
		t.Fatalf("authorise during outage = %d %s, want 503", r.status, r.body)
	}
	if got := fmt.Sprint(m.eventTypes(t, p.ID)); got != "[payment.created]" {
		t.Fatalf("events after outage = %s, want only created", got)
	}
	first := m.do(t, http.MethodPost, path, "auth", "")
	if first.status != http.StatusOK {
		t.Fatalf("retry with same key = %d %s, want 200", first.status, first.body)
	}
	if again := m.do(t, http.MethodPost, path, "auth", ""); !bytes.Equal(again.body, first.body) {
		t.Fatalf("replay differs: %s vs %s", again.body, first.body)
	}
	if n := acq.callsFor(p.ID); n != 4 {
		t.Fatalf("acquirer called %d times, want 3 failed attempts and 1 success", n)
	}
}

func TestConcurrentAuthorisesCallTheAcquirerOnce(t *testing.T) {
	m := newMerchant(t)
	p := m.payment(t, "/payments", uuid.NewString(), `{"amount":1000,"currency":"GBP"}`, http.StatusCreated)
	ok := 0
	var mu sync.Mutex
	race(racers, func(i int) {
		r := m.do(t, http.MethodPost, "/payments/"+p.ID+"/authorise", fmt.Sprint("auth-", i), "")
		switch {
		case r.status == http.StatusOK:
			mu.Lock()
			ok++
			mu.Unlock()
		case r.status != http.StatusConflict || r.code() != "illegal_transition":
			t.Errorf("unexpected response %d %s", r.status, r.body)
		}
	})
	if ok != 1 || acq.callsFor(p.ID) != 1 {
		t.Fatalf("%d authorises succeeded and the acquirer was called %d times, want 1 and 1", ok, acq.callsFor(p.ID))
	}
}

func TestMetricsLabelRoutesByPatternNotPath(t *testing.T) {
	m := newMerchant(t)
	ids := []string{uuid.NewString(), uuid.NewString()}
	for _, id := range ids {
		m.do(t, http.MethodGet, "/payments/"+id, "", "")
	}
	r, err := srv.Client().Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	if !bytes.Contains(body, []byte(`payd_http_request_duration_seconds_count{route="GET /payments/{id}",status="404"}`)) {
		t.Fatal("no series for the GET /payments/{id} pattern in /metrics")
	}
	for _, id := range ids {
		if bytes.Contains(body, []byte(id)) {
			t.Fatalf("payment id %s leaked into a metric label", id)
		}
	}
}

func TestKeyIsScopedToThePath(t *testing.T) {
	m := newMerchant(t)
	p := m.authorised(t, 1000)
	m.payment(t, "/payments/"+p.ID+"/capture", "k", "", http.StatusOK)
	if r := m.do(t, http.MethodPost, "/payments/"+p.ID+"/refund", "k", ""); r.status != http.StatusUnprocessableEntity || r.code() != "idempotency_key_reused" {
		t.Fatalf("refund reusing the capture's key = %d %s, want 422 idempotency_key_reused", r.status, r.body)
	}
	if got := m.payment(t, "/payments/"+p.ID+"/refund", uuid.NewString(), `{"amount":1}`, http.StatusOK); got.RefundedAmount != 1 {
		t.Fatalf("refunded %d, want only the 1 from the fresh key", got.RefundedAmount)
	}
}

func TestKeysAreScopedToTheMerchant(t *testing.T) {
	a, b := newMerchant(t), newMerchant(t)
	pa := a.payment(t, "/payments", "shared", `{"amount":1000,"currency":"GBP"}`, http.StatusCreated)
	pb := b.payment(t, "/payments", "shared", `{"amount":1000,"currency":"GBP"}`, http.StatusCreated)
	if pa.ID == pb.ID || pb.MerchantID != b.id {
		t.Fatalf("merchant b got %+v from a key merchant a used", pb)
	}
}

func TestKeyInFlightReturns409(t *testing.T) {
	m := newMerchant(t)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	lockID := sha256.Sum256([]byte(m.id + "\x00" + "busy"))
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(binary.BigEndian.Uint64(lockID[:8]))); err != nil {
		t.Fatal(err)
	}
	if r := m.do(t, http.MethodPost, "/payments", "busy", `{"amount":1000,"currency":"GBP"}`); r.status != http.StatusConflict || r.code() != "idempotency_key_in_use" {
		t.Fatalf("request while the key is held = %d %s, want 409 idempotency_key_in_use", r.status, r.body)
	}
	tx.Rollback(ctx)
	m.payment(t, "/payments", "busy", `{"amount":1000,"currency":"GBP"}`, http.StatusCreated)
}

func TestMalformedRequestsAreRejected(t *testing.T) {
	m := newMerchant(t)
	p := m.authorised(t, 1000)
	capture := "/payments/" + p.ID + "/capture"
	for _, tc := range []struct {
		name, path, key, body string
		status                int
		code                  string
	}{
		{"key over 255 bytes", "/payments", strings.Repeat("k", 256), `{"amount":1,"currency":"GBP"}`, 400, "idempotency_key_required"},
		{"key not utf-8", "/payments", "\xff", `{"amount":1,"currency":"GBP"}`, 400, "idempotency_key_required"},
		{"invalid json", "/payments", "", `{"amount":`, 400, "invalid_json"},
		{"trailing data", "/payments", "", `{"amount":1,"currency":"GBP"} {}`, 400, "invalid_json"},
		{"unknown field", "/payments", "", `{"amount":1,"currency":"GBP","amonut":2}`, 400, "invalid_json"},
		{"body over 1MiB", "/payments", "", `{"currency":"` + strings.Repeat("x", 1<<20) + `"}`, 413, "body_too_large"},
		{"null amount", capture, "", `{"amount":null}`, 422, "invalid_amount"},
		{"fractional amount", capture, "", `{"amount":1.5}`, 422, "invalid_amount"},
		{"uppercase id", "/payments/" + strings.ToUpper(p.ID) + "/capture", "", "", 404, "not_found"},
	} {
		key := tc.key
		if key == "" {
			key = uuid.NewString()
		}
		if r := m.do(t, http.MethodPost, tc.path, key, tc.body); r.status != tc.status || r.code() != tc.code {
			t.Errorf("%s: got %d %s, want %d %s", tc.name, r.status, r.body, tc.status, tc.code)
		}
	}
	if n := m.countPayments(t); n != 1 {
		t.Errorf("%d payments after rejected creates, want 1", n)
	}
	if got := m.payment(t, capture, uuid.NewString(), "", http.StatusOK); got.CapturedAmount != 1000 {
		t.Errorf("payment changed by a rejected capture: %+v", got)
	}
}

func TestSchemaRejectsImpossibleAmounts(t *testing.T) {
	m := newMerchant(t)
	p := m.authorised(t, 1000)
	m.payment(t, "/payments/"+p.ID+"/capture", uuid.NewString(), `{"amount":100}`, http.StatusOK)
	for _, set := range []string{"captured_amount = 1001", "refunded_amount = 101", "refunded_amount = -1", "amount = 0"} {
		var pgErr *pgconn.PgError
		_, err := db.Exec(context.Background(), `UPDATE payments SET `+set+` WHERE id = $1`, p.ID)
		if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
			t.Errorf("SET %s: err = %v, want a check violation", set, err)
		}
	}
}
