package server

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"testing"
	"time"

	"github.com/divizn/payd/internal/ledger"
	"github.com/google/uuid"
)

var (
	modelRuns = flag.Int("model.runs", 5, "independent runs of the model test")
	modelOps  = flag.Int("model.ops", 100, "operations per model test run")
	modelSeed = flag.Uint64("model.seed", 0, "seed for the model test, 0 picks one")
)

type modelPayment struct {
	id, currency, status       string
	amount, captured, refunded int64
}

type sentWrite struct {
	path, key, body string
	res             response
}

type modelRun struct {
	t        *testing.T
	m        merchant
	rng      *rand.Rand
	payments []*modelPayment
	writes   []sentWrite
}

func TestModel(t *testing.T) {
	seed := *modelSeed
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	t.Logf("seed %d", seed)
	for run := range *modelRuns {
		r := modelRun{t: t, m: newMerchant(t), rng: rand.New(rand.NewPCG(seed, uint64(run)))}
		for op := range *modelOps {
			if !r.step() {
				t.Fatalf("run %d op %d diverged from the model (seed %d)", run, op, seed)
			}
		}
		if err := ledger.Check(context.Background(), db); err != nil {
			t.Fatalf("run %d: %v (seed %d)", run, err, seed)
		}
	}
	t.Logf("%d runs, %d ops each, %d ops total", *modelRuns, *modelOps, *modelRuns**modelOps)
}

func (r *modelRun) pick(choices ...int64) int64 { return choices[r.rng.IntN(len(choices))] }

func (r *modelRun) step() bool {
	if len(r.payments) == 0 {
		return r.create()
	}
	switch r.rng.IntN(10) {
	case 0:
		return r.create()
	case 1:
		return r.authorise()
	case 2:
		return r.capture()
	case 3:
		return r.refund()
	case 4:
		return r.void()
	case 5:
		return r.get()
	case 6:
		return r.list()
	case 7:
		return r.replay()
	case 8:
		return r.reuse()
	default:
		return r.unknown()
	}
}

func (r *modelRun) write(path, body string) response {
	key := uuid.NewString()
	res := r.m.do(r.t, http.MethodPost, path, key, body)
	r.writes = append(r.writes, sentWrite{path, key, body, res})
	return res
}

func (r *modelRun) expectError(res response, status int, code, op string) bool {
	if res.status != status || res.code() != code {
		r.t.Errorf("%s: got %d %s, want %d %s", op, res.status, res.body, status, code)
		return false
	}
	return true
}

func (r *modelRun) expectPayment(res response, status int, want *modelPayment, op string) bool {
	var got paymentJSON
	if res.status != status || json.Unmarshal(res.body, &got) != nil {
		r.t.Errorf("%s: got %d %s, want %d", op, res.status, res.body, status)
		return false
	}
	if got.ID != want.id || got.MerchantID != r.m.id || got.Amount != want.amount || got.Currency != want.currency ||
		got.Status != want.status || got.CapturedAmount != want.captured || got.RefundedAmount != want.refunded {
		r.t.Errorf("%s: got %+v, model has %+v", op, got, *want)
		return false
	}
	return true
}

func (r *modelRun) create() bool {
	amount := r.pick(-1, 0, 1+r.rng.Int64N(10000), 1+r.rng.Int64N(10000))
	currency := []string{"GBP", "EUR", "JPY", "XYZ", "gbp"}[r.rng.IntN(5)]
	res := r.write("/payments", fmt.Sprintf(`{"amount":%d,"currency":%q}`, amount, currency))
	if amount <= 0 || currency == "XYZ" || currency == "gbp" {
		return r.expectError(res, http.StatusUnprocessableEntity, "invalid_money", "create")
	}
	var got paymentJSON
	json.Unmarshal(res.body, &got)
	p := &modelPayment{id: got.ID, currency: currency, status: "created", amount: amount}
	r.payments = append(r.payments, p)
	return r.expectPayment(res, http.StatusCreated, p, "create")
}

func (r *modelRun) payment() *modelPayment { return r.payments[r.rng.IntN(len(r.payments))] }

func (r *modelRun) authorise() bool {
	p := r.payment()
	res := r.write("/payments/"+p.id+"/authorise", "")
	if p.status != "created" {
		return r.expectError(res, http.StatusConflict, "illegal_transition", "authorise")
	}
	var got paymentJSON
	json.Unmarshal(res.body, &got)
	if got.Status != "authorised" && got.Status != "failed" {
		r.t.Errorf("authorise: got %d %s, want authorised or failed", res.status, res.body)
		return false
	}
	p.status = got.Status
	return r.expectPayment(res, http.StatusOK, p, "authorise")
}

func (r *modelRun) capture() bool {
	p := r.payment()
	amount, body := p.amount, ""
	if r.rng.IntN(3) > 0 {
		amount = r.pick(0, -5, 1+r.rng.Int64N(p.amount), p.amount, p.amount+1)
		body = fmt.Sprintf(`{"amount":%d}`, amount)
	}
	res := r.write("/payments/"+p.id+"/capture", body)
	switch {
	case p.status != "authorised":
		return r.expectError(res, http.StatusConflict, "illegal_transition", "capture")
	case amount <= 0 || amount > p.amount:
		return r.expectError(res, http.StatusUnprocessableEntity, "invalid_amount", "capture")
	}
	p.status, p.captured = "captured", amount
	return r.expectPayment(res, http.StatusOK, p, "capture")
}

func (r *modelRun) refund() bool {
	p := r.payment()
	remaining := p.captured - p.refunded
	amount, body := remaining, ""
	if r.rng.IntN(3) > 0 {
		amount = r.pick(0, 1+r.rng.Int64N(max(remaining, 1)), remaining, remaining+1)
		body = fmt.Sprintf(`{"amount":%d}`, amount)
	}
	res := r.write("/payments/"+p.id+"/refund", body)
	switch {
	case p.status != "captured" && p.status != "partially_refunded":
		return r.expectError(res, http.StatusConflict, "illegal_transition", "refund")
	case amount <= 0 || amount > remaining:
		return r.expectError(res, http.StatusUnprocessableEntity, "invalid_amount", "refund")
	}
	p.refunded += amount
	p.status = "partially_refunded"
	if p.refunded == p.captured {
		p.status = "refunded"
	}
	return r.expectPayment(res, http.StatusOK, p, "refund")
}

func (r *modelRun) void() bool {
	p := r.payment()
	res := r.write("/payments/"+p.id+"/void", "")
	if p.status != "authorised" {
		return r.expectError(res, http.StatusConflict, "illegal_transition", "void")
	}
	p.status = "voided"
	return r.expectPayment(res, http.StatusOK, p, "void")
}

func (r *modelRun) get() bool {
	p := r.payment()
	return r.expectPayment(r.m.do(r.t, http.MethodGet, "/payments/"+p.id, "", ""), http.StatusOK, p, "get")
}

func (r *modelRun) list() bool {
	limit := r.pick(0, 101, 1+r.rng.Int64N(100), 1+r.rng.Int64N(int64(len(r.payments))))
	res := r.m.do(r.t, http.MethodGet, fmt.Sprintf("/payments?limit=%d", limit), "", "")
	if limit < 1 || limit > 100 {
		return r.expectError(res, http.StatusBadRequest, "invalid_limit", "list")
	}
	var got struct {
		Data []paymentJSON `json:"data"`
	}
	if res.status != http.StatusOK || json.Unmarshal(res.body, &got) != nil {
		r.t.Errorf("list: got %d %s", res.status, res.body)
		return false
	}
	if want := min(int(limit), len(r.payments)); len(got.Data) != want {
		r.t.Errorf("list limit %d: got %d payments, want %d", limit, len(got.Data), want)
		return false
	}
	byID := map[string]*modelPayment{}
	for _, p := range r.payments {
		byID[p.id] = p
	}
	for i, g := range got.Data {
		if i > 0 && g.CreatedAt.After(got.Data[i-1].CreatedAt) {
			r.t.Errorf("list: not newest first at %d", i)
			return false
		}
		b, _ := json.Marshal(g)
		p, ok := byID[g.ID]
		if !ok || !r.expectPayment(response{http.StatusOK, b}, http.StatusOK, p, "list") {
			return false
		}
	}
	return true
}

func (r *modelRun) replay() bool {
	w := r.writes[r.rng.IntN(len(r.writes))]
	res := r.m.do(r.t, http.MethodPost, w.path, w.key, w.body)
	if res.status != w.res.status || !bytes.Equal(res.body, w.res.body) {
		r.t.Errorf("replay of %s: got %d %s, first response %d %s", w.path, res.status, res.body, w.res.status, w.res.body)
		return false
	}
	return true
}

func (r *modelRun) reuse() bool {
	w := r.writes[r.rng.IntN(len(r.writes))]
	res := r.m.do(r.t, http.MethodPost, w.path, w.key, w.body+" ")
	return r.expectError(res, http.StatusUnprocessableEntity, "idempotency_key_reused", "reuse")
}

func (r *modelRun) unknown() bool {
	id := uuid.NewString()
	if r.rng.IntN(2) == 0 {
		return r.expectError(r.m.do(r.t, http.MethodGet, "/payments/"+id, "", ""), http.StatusNotFound, "not_found", "get unknown")
	}
	return r.expectError(r.write("/payments/"+id+"/capture", ""), http.StatusNotFound, "not_found", "capture unknown")
}
