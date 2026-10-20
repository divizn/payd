package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"unicode/utf8"

	"github.com/divizn/payd/internal/acquirer"
	"github.com/jackc/pgx/v5"
)

const maxBody = 1 << 20

type op func(ctx context.Context, tx pgx.Tx, merchant string, r *http.Request, body []byte) (int, any, error)

func (s *Server) idempotent(fn op) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		merchant := merchantFrom(ctx)
		key := r.Header.Get("Idempotency-Key")
		if key == "" || len(key) > 255 || !utf8.ValidString(key) {
			writeError(w, http.StatusBadRequest, "idempotency_key_required", "Idempotency-Key header must be 1 to 255 bytes of UTF-8")
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "body_too_large", "request body over 1MiB")
			return
		}
		if err != nil {
			writeError(w, http.StatusBadRequest, "unreadable_body", err.Error())
			return
		}
		h := sha256.New()
		io.WriteString(h, r.Method+" "+r.URL.Path+"\n")
		h.Write(body)
		reqHash := h.Sum(nil)

		tx, err := s.db.Begin(ctx)
		if err != nil {
			s.internalError(w, r, err)
			return
		}
		defer tx.Rollback(ctx)

		lockID := sha256.Sum256([]byte(merchant + "\x00" + key))
		var locked bool
		if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, int64(binary.BigEndian.Uint64(lockID[:8]))).Scan(&locked); err != nil {
			s.internalError(w, r, err)
			return
		}
		if !locked {
			writeError(w, http.StatusConflict, "idempotency_key_in_use", "a request with this Idempotency-Key is in flight")
			return
		}

		if _, err := tx.Exec(ctx, `DELETE FROM idempotency_keys WHERE merchant_id = $1 AND key = $2 AND created_at <= now() - interval '24 hours'`, merchant, key); err != nil {
			s.internalError(w, r, err)
			return
		}
		var storedHash, storedBody []byte
		var storedStatus int
		err = tx.QueryRow(ctx, `SELECT request_hash, response_status, response_body FROM idempotency_keys WHERE merchant_id = $1 AND key = $2`, merchant, key).
			Scan(&storedHash, &storedStatus, &storedBody)
		switch {
		case err == nil && !bytes.Equal(storedHash, reqHash):
			writeError(w, http.StatusUnprocessableEntity, "idempotency_key_reused", "Idempotency-Key was used with a different request")
			return
		case err == nil:
			writeRaw(w, storedStatus, storedBody)
			return
		case !errors.Is(err, pgx.ErrNoRows):
			s.internalError(w, r, err)
			return
		}

		status, v, err := fn(ctx, tx, merchant, r, body)
		if errors.Is(err, acquirer.ErrUnavailable) {
			writeError(w, http.StatusServiceUnavailable, "acquirer_unavailable", "acquirer unavailable, retry with the same Idempotency-Key")
			return
		}
		if err != nil {
			s.internalError(w, r, err)
			return
		}
		resp, err := json.Marshal(v)
		if err != nil {
			s.internalError(w, r, err)
			return
		}
		if _, err := tx.Exec(ctx, `INSERT INTO idempotency_keys (merchant_id, key, request_hash, response_status, response_body) VALUES ($1, $2, $3, $4, $5)`,
			merchant, key, reqHash, status, resp); err != nil {
			s.internalError(w, r, err)
			return
		}
		if err := tx.Commit(ctx); err != nil {
			s.internalError(w, r, err)
			return
		}
		writeRaw(w, status, resp)
	}
}
