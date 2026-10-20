package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/divizn/payd/internal/payment"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	db  *pgxpool.Pool
	log *slog.Logger
}

func New(db *pgxpool.Pool, log *slog.Logger) http.Handler {
	s := &Server{db: db, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /payments", s.idempotent(s.create))
	mux.HandleFunc("POST /payments/{id}/authorise", s.idempotent(s.transition(func(p *payment.Payment, _ *int64) error { return p.Authorise() })))
	mux.HandleFunc("POST /payments/{id}/void", s.idempotent(s.transition(func(p *payment.Payment, _ *int64) error { return p.Void() })))
	mux.HandleFunc("POST /payments/{id}/capture", s.idempotent(s.transition(func(p *payment.Payment, amount *int64) error {
		if amount == nil {
			return p.Capture(p.Money.Amount)
		}
		return p.Capture(*amount)
	})))
	mux.HandleFunc("POST /payments/{id}/refund", s.idempotent(s.transition(func(p *payment.Payment, amount *int64) error {
		if amount == nil {
			return p.Refund(p.CapturedAmount - p.RefundedAmount)
		}
		return p.Refund(*amount)
	})))
	mux.HandleFunc("GET /payments/{id}", s.get)
	mux.HandleFunc("GET /payments", s.list)
	return s.auth(mux)
}

type merchantKey struct{}

func merchantFrom(ctx context.Context) string { return ctx.Value(merchantKey{}).(string) }

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		h := r.Header.Get("Authorization")
		if len(h) <= len(prefix) || h[:len(prefix)] != prefix {
			writeError(w, http.StatusUnauthorized, "unauthorised", "missing API key")
			return
		}
		sum := sha256.Sum256([]byte(h[len(prefix):]))
		var merchant string
		err := s.db.QueryRow(r.Context(), `SELECT merchant_id FROM api_keys WHERE key_hash = $1`, sum[:]).Scan(&merchant)
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusUnauthorized, "unauthorised", "invalid API key")
			return
		}
		if err != nil {
			s.internalError(w, r, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), merchantKey{}, merchant)))
	})
}

type paymentJSON struct {
	ID             string    `json:"id"`
	MerchantID     string    `json:"merchant_id"`
	Amount         int64     `json:"amount"`
	Currency       string    `json:"currency"`
	Status         string    `json:"status"`
	CapturedAmount int64     `json:"captured_amount"`
	RefundedAmount int64     `json:"refunded_amount"`
	CreatedAt      time.Time `json:"created_at"`
}

func toJSON(p *payment.Payment) paymentJSON {
	return paymentJSON{p.ID, p.MerchantID, p.Money.Amount, p.Money.Currency, string(p.Status), p.CapturedAmount, p.RefundedAmount, p.CreatedAt.UTC()}
}

const paymentCols = `id::text, merchant_id, amount, currency, status, captured_amount, refunded_amount, created_at`

func scanPayment(row pgx.Row) (*payment.Payment, error) {
	var p payment.Payment
	err := row.Scan(&p.ID, &p.MerchantID, &p.Money.Amount, &p.Money.Currency, &p.Status, &p.CapturedAmount, &p.RefundedAmount, &p.CreatedAt)
	return &p, err
}

func (s *Server) create(ctx context.Context, tx pgx.Tx, merchant string, _ *http.Request, body []byte) (int, any, error) {
	var req struct {
		Amount   int64  `json:"amount"`
		Currency string `json:"currency"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return errorBody(http.StatusBadRequest, "invalid_json", err.Error())
	}
	m, err := payment.NewMoney(req.Amount, req.Currency)
	if err != nil {
		return errorBody(http.StatusUnprocessableEntity, "invalid_money", err.Error())
	}
	p, err := scanPayment(tx.QueryRow(ctx, `INSERT INTO payments (merchant_id, amount, currency) VALUES ($1, $2, $3) RETURNING `+paymentCols,
		merchant, m.Amount, m.Currency))
	if err != nil {
		return 0, nil, err
	}
	return http.StatusCreated, toJSON(p), nil
}

func (s *Server) transition(apply func(p *payment.Payment, amount *int64) error) op {
	return func(ctx context.Context, tx pgx.Tx, merchant string, r *http.Request, body []byte) (int, any, error) {
		var req struct {
			Amount *int64 `json:"amount"`
		}
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				return errorBody(http.StatusBadRequest, "invalid_json", err.Error())
			}
		}
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			return errorBody(http.StatusNotFound, "not_found", "payment not found")
		}
		p, err := scanPayment(tx.QueryRow(ctx, `SELECT `+paymentCols+` FROM payments WHERE id = $1 AND merchant_id = $2 FOR UPDATE`, id, merchant))
		if errors.Is(err, pgx.ErrNoRows) {
			return errorBody(http.StatusNotFound, "not_found", "payment not found")
		}
		if err != nil {
			return 0, nil, err
		}
		switch err := apply(p, req.Amount); {
		case errors.Is(err, payment.ErrIllegalTransition):
			return errorBody(http.StatusConflict, "illegal_transition", err.Error())
		case err != nil:
			return errorBody(http.StatusUnprocessableEntity, "invalid_amount", err.Error())
		}
		if _, err := tx.Exec(ctx, `UPDATE payments SET status = $2, captured_amount = $3, refunded_amount = $4, updated_at = now() WHERE id = $1`,
			id, p.Status, p.CapturedAmount, p.RefundedAmount); err != nil {
			return 0, nil, err
		}
		return http.StatusOK, toJSON(p), nil
	}
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", "payment not found")
		return
	}
	p, err := scanPayment(s.db.QueryRow(r.Context(), `SELECT `+paymentCols+` FROM payments WHERE id = $1 AND merchant_id = $2`, id, merchantFrom(r.Context())))
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "payment not found")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toJSON(p))
}

// ponytail: limit only, add keyset pagination (created_at, id) when merchants page past 100
func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 100 {
			writeError(w, http.StatusBadRequest, "invalid_limit", "limit must be 1 to 100")
			return
		}
		limit = n
	}
	rows, err := s.db.Query(r.Context(), `SELECT `+paymentCols+` FROM payments WHERE merchant_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2`,
		merchantFrom(r.Context()), limit)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := []paymentJSON{}
	for rows.Next() {
		p, err := scanPayment(rows)
		if err != nil {
			s.internalError(w, r, err)
			return
		}
		out = append(out, toJSON(p))
	}
	if err := rows.Err(); err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": out})
}

type apiError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func errorBody(status int, code, msg string) (int, any, error) {
	var e apiError
	e.Error.Code, e.Error.Message = code, msg
	return status, e, nil
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	_, v, _ := errorBody(status, code, msg)
	writeJSON(w, status, v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, _ := json.Marshal(v)
	writeRaw(w, status, b)
}

func writeRaw(w http.ResponseWriter, status int, b []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(b)
}

func (s *Server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.log.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "err", err)
	writeError(w, http.StatusInternalServerError, "internal", "internal error")
}
